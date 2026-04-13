const TelegramBot = require('node-telegram-bot-api');
const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();
const fs = require('fs');
const path = require('path');
const os = require('os');
const sqlite3 = require('sqlite3').verbose();
const http = require('http');

chromium.use(stealth);

const PORT = process.env.PORT || 3000;
http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Bot is running');
}).listen(PORT, () => {});

const BOT_TOKEN = process.env.BOT_TOKEN || 'ضع_توكن_البوت_هنا_إذا_لم_يكن_في_البيئة';
if (!BOT_TOKEN || BOT_TOKEN === 'ضع_توكن_البوت_هنا_إذا_لم_يكن_في_البيئة') process.exit(1);

const ADMIN_ID = '643309456';
const MEMBER_LIMIT = 6;
const WATCH_INTERVAL_MS = 15000;
const DATA_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH || path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const bot = new TelegramBot(BOT_TOKEN, { polling: true });
const sessions = {};
const activeContexts = {};
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const isAdmin = (chatId) => String(chatId) === ADMIN_ID;
const normalizeEmail = (v) => String(v || '').trim().toLowerCase();

const dbPath = path.join(DATA_DIR, 'workspaces_v13.db');
const db = new sqlite3.Database(dbPath);

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS workspaces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id TEXT,
    name TEXT,
    email TEXT,
    password TEXT,
    url2fa TEXT,
    profile_dir TEXT
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS allowed_emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ws_id INTEGER,
    email TEXT
  )`);
  db.run('CREATE UNIQUE INDEX IF NOT EXISTS idx_allowed_unique ON allowed_emails(ws_id, email)');
});

const dbRun = (query, params = []) => new Promise((resolve, reject) => db.run(query, params, function(err) { err ? reject(err) : resolve(this.lastID); }));
const dbGet = (query, params = []) => new Promise((resolve, reject) => db.get(query, params, (err, row) => err ? reject(err) : resolve(row)));
const dbAll = (query, params = []) => new Promise((resolve, reject) => db.all(query, params, (err, rows) => err ? reject(err) : resolve(rows)));

async function rememberAllowedEmail(wsId, email) {
  const clean = normalizeEmail(email);
  if (!clean) return;
  await dbRun('INSERT OR IGNORE INTO allowed_emails (ws_id, email) VALUES (?, ?)', [wsId, clean]);
}

async function getContext(wsId, profileDir) {
  if (activeContexts[wsId]) {
    try { activeContexts[wsId].pages(); return activeContexts[wsId]; } catch (_) { delete activeContexts[wsId]; }
  }
  const context = await chromium.launchPersistentContext(profileDir, {
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled', '--disable-dev-shm-usage'],
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    viewport: { width: 1366, height: 768 }
  });
  const pages = context.pages();
  if (pages.length > 0) await pages[0].close().catch(() => {});
  activeContexts[wsId] = context;
  context.on('close', () => { if (activeContexts[wsId] === context) delete activeContexts[wsId]; });
  return context;
}

async function smartWait(page, ms = 1200) {
  await Promise.race([
    page.waitForLoadState('networkidle', { timeout: ms }).catch(() => {}),
    sleep(ms)
  ]);
}

async function waitForAnyVisible(page, selectors, timeout = 12000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    for (const selector of selectors) {
      try {
        const loc = page.locator(selector).first();
        if (await loc.isVisible({ timeout: 300 }).catch(() => false)) return loc;
      } catch (_) {}
    }
    await sleep(250);
  }
  return null;
}

async function humanFill(locator, value, delay = 55) {
  await locator.click({ force: true });
  try { await locator.fill(''); } catch (_) {}
  await locator.press('Control+A').catch(() => {});
  await locator.press('Meta+A').catch(() => {});
  await locator.press('Backspace').catch(() => {});
  await locator.type(String(value || ''), { delay });
}

async function ensureWorkspaceLogin(page, creds, updateStatus) {
  await updateStatus('1/8 فتح صفحة الدخول');
  await page.goto('https://chatgpt.com/auth/login', { waitUntil: 'domcontentloaded', timeout: 30000 });
  await smartWait(page, 2200);

  try {
    const loginBtn = page.locator('text="Log in"').first();
    if (await loginBtn.isVisible({ timeout: 1500 }).catch(() => false)) {
      await loginBtn.click({ force: true });
      await smartWait(page, 1800);
    }
  } catch (_) {}

  await updateStatus('2/8 انتظار حقل الإيميل');
  const emailField = await waitForAnyVisible(page, [
    'input[type="email"]',
    'input[name="email"]',
    'input[autocomplete="username"]',
    'input[autocomplete="email"]'
  ], 15000);
  if (!emailField) throw new Error('لم يظهر حقل الإيميل');

  await updateStatus('3/8 كتابة الإيميل بهدوء');
  await humanFill(emailField, creds.email, 70);
  await sleep(350);
  await emailField.press('Enter').catch(() => {});
  await smartWait(page, 2600);

  await updateStatus('4/8 انتظار حقل الباسورد');
  const passwordField = await waitForAnyVisible(page, [
    'input[type="password"]',
    'input[name="password"]',
    'input[autocomplete="current-password"]'
  ], 18000);
  if (!passwordField) throw new Error('لم يظهر حقل الباسورد');

  await updateStatus('5/8 كتابة الباسورد بهدوء');
  await humanFill(passwordField, creds.password, 65);
  await sleep(450);
  await passwordField.press('Enter').catch(() => {});
  await smartWait(page, 3200);

  if (creds.url2fa) {
    await updateStatus('6/8 جلب وإدخال كود 2FA');
    const mfaPage = await page.context().newPage();
    await mfaPage.goto(creds.url2fa, { waitUntil: 'domcontentloaded', timeout: 20000 });
    await smartWait(mfaPage, 1200);
    const bodyText = await mfaPage.innerText('body').catch(() => '');
    const codeMatch = bodyText.match(/\b\d{3}\s*\d{3}\b/) || bodyText.match(/\b\d{6}\b/);
    await mfaPage.close().catch(() => {});
    if (!codeMatch) throw new Error('لم يتم العثور على كود 2FA');
    const code6 = codeMatch[0].replace(/\s+/g, '');
    const otpField = await waitForAnyVisible(page, [
      'input[inputmode="numeric"]',
      'input[autocomplete="one-time-code"]',
      'input[name*="code" i]',
      'input[type="tel"]'
    ], 12000);
    if (!otpField) throw new Error('لم يظهر حقل 2FA');
    await humanFill(otpField, code6, 85);
    await sleep(450);
    await otpField.press('Enter').catch(() => {});
    await smartWait(page, 3500);
  }

  await updateStatus('7/8 التحقق من نجاح الدخول');
  const loginStillVisible = await waitForAnyVisible(page, [
    'input[type="password"]',
    'input[type="email"]',
    'input[autocomplete="username"]'
  ], 2000);
  if (loginStillVisible) throw new Error('ما زالت صفحة تسجيل الدخول ظاهرة، تحقق من البيانات');

  const currentUrl = page.url();
  if (/auth\/login|auth0|login/i.test(currentUrl)) {
    throw new Error('لم يكتمل تسجيل الدخول، ما زلت في صفحة الدخول');
  }

  await updateStatus('8/8 فتح لوحة الإدارة للتأكد');
  let ok = false;
  for (const url of ['https://chatgpt.com/admin/settings', 'https://chatgpt.com/admin/members?tab=members', 'https://chatgpt.com/admin']) {
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await smartWait(page, 1800);
      const u = page.url();
      if (/chatgpt\.com\/admin/i.test(u) && !/auth\/login/i.test(u)) { ok = true; break; }
    } catch (_) {}
  }
  if (!ok) throw new Error('فشل التحقق من لوحة الإدارة بعد تسجيل الدخول');
}

async function extractAllEmails(page, options = {}) {
  const rounds = options.rounds || 5;
  const pause = options.pause || 500;
  const emails = new Set();
  let lastKey = '';
  for (let i = 0; i < rounds; i++) {
    const batch = await page.evaluate(() => {
      const html = document.body ? document.body.innerText : '';
      const found = html.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g) || [];
      const unique = [...new Set(found.map(v => v.toLowerCase()))];
      const scrollables = Array.from(document.querySelectorAll('*')).filter(el => el.scrollHeight > el.clientHeight + 20);
      if (scrollables.length) scrollables[scrollables.length - 1].scrollTop += 2400;
      window.scrollBy(0, 2400);
      return unique;
    }).catch(() => []);
    batch.forEach(e => emails.add(e));
    const currentKey = [...emails].sort().join('|');
    if (currentKey === lastKey && i >= 1) break;
    lastKey = currentKey;
    await sleep(pause);
  }
  return [...emails];
}

async function dynamicGeometryAction(page, email, actionType) {
  try {
    await page.waitForTimeout(400);
    const dotsCoords = await page.evaluate((targetEmail) => {
      const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
      let emailNode = null;
      let node;
      while ((node = walker.nextNode())) {
        if ((node.nodeValue || '').toLowerCase().includes(targetEmail.toLowerCase())) {
          emailNode = node.parentElement;
          break;
        }
      }
      if (!emailNode) return null;
      emailNode.scrollIntoView({ behavior: 'instant', block: 'center' });
      const emailRect = emailNode.getBoundingClientRect();
      const emailCenterY = emailRect.top + (emailRect.height / 2);
      const allButtons = Array.from(document.querySelectorAll('button, [role="button"], a'));
      const rowButtons = allButtons.filter(btn => {
        const rect = btn.getBoundingClientRect();
        const btnCenterY = rect.top + (rect.height / 2);
        return rect.width > 0 && rect.height > 0 && Math.abs(btnCenterY - emailCenterY) < 40 && rect.left > emailRect.left;
      });
      if (!rowButtons.length) return null;
      rowButtons.sort((a, b) => b.getBoundingClientRect().right - a.getBoundingClientRect().right);
      const rect = rowButtons[0].getBoundingClientRect();
      return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
    }, email);
    if (!dotsCoords) return false;
    await page.mouse.click(dotsCoords.x, dotsCoords.y);
    await page.waitForTimeout(250);

    const actionRegexStr = actionType === 'remove' ? 'Remove' : '(Revoke|Cancel)';
    const actionCoords = await page.evaluate((regexStr) => {
      const regex = new RegExp(regexStr, 'i');
      const items = Array.from(document.querySelectorAll('button, [role="menuitem"], a, span, li')).filter(el => {
        const rect = el.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0 && el.innerText && regex.test(el.innerText);
      });
      if (!items.length) return null;
      const target = items[items.length - 1];
      target.scrollIntoView({ behavior: 'instant', block: 'center' });
      const rect = target.getBoundingClientRect();
      return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
    }, actionRegexStr);
    if (actionCoords) await page.mouse.click(actionCoords.x, actionCoords.y);
    else {
      const regex = actionType === 'remove' ? /Remove/i : /(Revoke|Cancel)/i;
      const loc = page.locator('button, [role="menuitem"], a').filter({ hasText: regex }).last();
      if (await loc.isVisible({ timeout: 1000 }).catch(() => false)) await loc.click({ force: true });
      else return false;
    }
    await page.waitForTimeout(250);

    const confirmCoords = await page.evaluate((regexStr) => {
      const regex = new RegExp(regexStr, 'i');
      const dialogs = Array.from(document.querySelectorAll('[role="dialog"]'));
      const container = dialogs.length ? dialogs[dialogs.length - 1] : document.body;
      const btns = Array.from(container.querySelectorAll('button')).filter(b => {
        const rect = b.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0 && getComputedStyle(b).visibility !== 'hidden';
      });
      let confirmBtn = btns.find(b => regex.test(b.innerText) && (String(b.className).match(/red|danger/i) || getComputedStyle(b).backgroundColor === 'rgb(220, 38, 38)'));
      if (!confirmBtn) confirmBtn = btns.find(b => regex.test(b.innerText));
      if (!confirmBtn) confirmBtn = btns.find(b => String(b.className).match(/red|danger/i));
      if (!confirmBtn && dialogs.length) confirmBtn = btns[btns.length - 1];
      if (!confirmBtn) return null;
      const rect = confirmBtn.getBoundingClientRect();
      return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
    }, actionRegexStr);
    if (confirmCoords) await page.mouse.click(confirmCoords.x, confirmCoords.y);
    else await page.keyboard.press('Enter').catch(() => {});
    await page.waitForTimeout(450);
    return true;
  } catch (_) { return false; }
}

function parseCredentials(text) {
  const raw = String(text || '');
  const emailMatch = raw.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
  const urlMatch = raw.match(/https?:\/\/2fa\.[^\s]+|https?:\/\/[^\s]*2fa[^\s]*/i);
  const lines = raw.split(/\n+/).map(v => v.trim()).filter(Boolean);
  const ignored = new Set(['تم', 'إنشاء', 'الحساب', 'وتفعيل', 'المصادقة', 'الثنائية', 'بنجاح', 'الإيميل', 'الباسورد', 'رابط']);
  let password = null;
  const candidates = [];
  for (const line of lines) {
    const clean = line.replace(/[\[\]✅🔑📧🔗:،]/g, ' ').trim();
    if (!clean) continue;
    const pieces = clean.split(/\s+/).filter(Boolean);
    for (const p of pieces) {
      if (/@/.test(p) || /^https?:\/\//i.test(p)) continue;
      if (/^[\d:/.]+$/.test(p)) continue;
      if ([...ignored].some(w => p.includes(w))) continue;
      if (p.length >= 6) candidates.push(p);
    }
  }
  if (candidates.length) password = candidates.sort((a, b) => b.length - a.length)[0];
  return {
    email: emailMatch ? normalizeEmail(emailMatch[0]) : null,
    password,
    url2fa: urlMatch ? urlMatch[0].trim() : null
  };
}

function getDashboardKeyboard(state, chatId) {
  const rows = [
    [{ text: '🖼️ إرسال صورة الأعضاء + الدعوات', callback_data: 'ws_toggle' }],
    [{ text: '🛡️ توثيق الأعضاء', callback_data: 'ws_sync_whitelist' }],
    [{ text: 'اضافة عضو تلقائيا في مساحة شاغرة', callback_data: 'ws_add_person' }, { text: 'جلب الإيميلات', callback_data: 'ws_fetch_emails' }],
    [{ text: 'إزالة عضو', callback_data: 'ws_remove_member' }, { text: 'إلغاء دعوة', callback_data: 'ws_revoke_invite' }],
    [{ text: 'تغيير اسم المساحة', callback_data: 'ws_change_name' }],
    [{ text: '❌ إزالة المساحة', callback_data: 'ws_delete' }],
    [{ text: '🔙 العودة للقائمة', callback_data: 'ws_back' }]
  ];
  if (isAdmin(chatId)) rows.splice(1, 0, [{ text: '🖥️ وضع الكمبيوتر', callback_data: 'ws_computer_mode' }]);
  return rows;
}

async function sendInteractiveMenu(chatId, text = '🎮 وضع الكمبيوتر جاهز') {
  if (!isAdmin(chatId)) return;
  const opts = { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [
    [{ text: '🌐 فتح رابط', callback_data: 'int_goto_url' }, { text: '📸 تحديث الشاشة', callback_data: 'int_refresh' }],
    [{ text: '⌨️ كتابة نص', callback_data: 'int_type_text' }, { text: '↩️ انتر', callback_data: 'int_press_enter' }],
    [{ text: '🖱️ شبكة الماوس', callback_data: 'int_show_grid' }, { text: '✅ إنهاء التسجيل اليدوي', callback_data: 'int_finish_login' }]
  ] } };
  await bot.sendMessage(chatId, text, opts);
}

async function safeNewPage(context) {
  const page = await context.newPage();
  page.setDefaultTimeout(15000);
  page.setDefaultNavigationTimeout(30000);
  return page;
}

async function captureWorkspaceTabs(ws, tempPrefix = 'dash') {
  const context = await getContext(ws.id, ws.profile_dir);
  const page = await safeNewPage(context);
  const membersPath = path.join(os.tmpdir(), `${tempPrefix}_${Date.now()}_members.png`);
  const invitesPath = path.join(os.tmpdir(), `${tempPrefix}_${Date.now()}_invites.png`);
  await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 30000 });
  await smartWait(page, 1800);
  await page.screenshot({ path: membersPath, fullPage: false });
  await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 30000 });
  await smartWait(page, 1800);
  await page.screenshot({ path: invitesPath, fullPage: false });
  await page.close().catch(() => {});
  return { membersPath, invitesPath };
}

async function getWorkspaceOccupancy(ws) {
  const context = await getContext(ws.id, ws.profile_dir);
  const page = await safeNewPage(context);
  await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 30000 });
  await smartWait(page, 1400);
  const members = await extractAllEmails(page, { rounds: 4, pause: 300 });
  await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 30000 });
  await smartWait(page, 1400);
  const invites = await extractAllEmails(page, { rounds: 4, pause: 300 });
  await page.close().catch(() => {});
  const all = [...new Set([...members.map(normalizeEmail), ...invites.map(normalizeEmail)])];
  return { members, invites, total: all.length };
}

async function inviteEmailToWorkspace(ws, email) {
  const target = normalizeEmail(email);
  const status = await getWorkspaceOccupancy(ws);
  const alreadyExists = new Set([...status.members, ...status.invites].map(normalizeEmail)).has(target);
  if (alreadyExists) return { ok: false, reason: 'exists' };
  if (status.total >= MEMBER_LIMIT) return { ok: false, reason: 'full' };
  const context = await getContext(ws.id, ws.profile_dir);
  const page = await safeNewPage(context);
  try {
    await page.goto('https://chatgpt.com/admin/members', { waitUntil: 'domcontentloaded', timeout: 30000 });
    await smartWait(page, 1200);
    await page.locator('button:has-text("Invite member"), button:has-text("Invite members")').first().click({ force: true });
    await smartWait(page, 400);
    const input = page.locator('input[type="email"], textarea[placeholder*="email" i], input[placeholder*="email" i]').first();
    await input.click({ force: true });
    await input.fill(target).catch(async () => { await page.keyboard.type(target, { delay: 10 }); });
    await smartWait(page, 250);
    await page.locator('button:has-text("Send invites"), button:has-text("Send invite")').first().click({ force: true });
    await smartWait(page, 1400);
    await page.close().catch(() => {});
    return { ok: true, reason: 'invited', totalBefore: status.total };
  } catch (e) {
    await page.close().catch(() => {});
    return { ok: false, reason: e.message };
  }
}

async function fastGuardWorkspace(ws) {
  if (!fs.existsSync(ws.profile_dir)) return;
  const allowedRows = await dbAll('SELECT email FROM allowed_emails WHERE ws_id = ?', [ws.id]);
  const allowedEmails = new Set(allowedRows.map(r => normalizeEmail(r.email)).filter(Boolean));
  allowedEmails.add(normalizeEmail(ws.email));
  const context = await getContext(ws.id, ws.profile_dir);
  const page = await safeNewPage(context);
  try {
    if (allowedEmails.size <= 1) {
      await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 30000 });
      await smartWait(page, 1200);
      const members = await extractAllEmails(page, { rounds: 4, pause: 250 });
      await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 30000 });
      await smartWait(page, 1200);
      const invites = await extractAllEmails(page, { rounds: 4, pause: 250 });
      for (const e of [...new Set([...members, ...invites])]) await rememberAllowedEmail(ws.id, e);
      await page.close().catch(() => {});
      return;
    }

    await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 30000 });
    await smartWait(page, 1200);
    const foundMembers = await extractAllEmails(page, { rounds: 4, pause: 250 });
    for (const email of foundMembers) {
      const clean = normalizeEmail(email);
      if (!allowedEmails.has(clean)) {
        if (await dynamicGeometryAction(page, clean, 'remove')) {
          bot.sendMessage(ws.chat_id, `🚨 الحارس السريع حذف عضو غير مصرح به\nالإيميل: \`${clean}\`\nالمساحة: ${ws.name}`, { parse_mode: 'Markdown' }).catch(() => {});
        }
      }
    }

    await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 30000 });
    await smartWait(page, 1200);
    const pendingEmails = await extractAllEmails(page, { rounds: 4, pause: 250 });
    for (const email of pendingEmails) {
      const clean = normalizeEmail(email);
      if (!allowedEmails.has(clean)) {
        if (await dynamicGeometryAction(page, clean, 'revoke')) {
          bot.sendMessage(ws.chat_id, `🚨 الحارس السريع ألغى دعوة غير مصرح بها\nالإيميل: \`${clean}\`\nالمساحة: ${ws.name}`, { parse_mode: 'Markdown' }).catch(() => {});
        }
      }
    }
  } catch (_) {
  } finally {
    await page.close().catch(() => {});
  }
}

bot.onText(/\/start/, async (msg) => {
  const chatId = String(msg.chat.id);
  if (!sessions[chatId]) sessions[chatId] = { step: null, currentTab: 'members' };
  const state = sessions[chatId];
  state.step = null;
  state.currentWsId = null;
  state.currentTab = 'members';
  state.credentialBuffer = '';

  const workspaces = await dbAll('SELECT id, name FROM workspaces WHERE chat_id = ?', [chatId]);
  const inline_keyboard = [];
  for (const ws of workspaces) inline_keyboard.push([{ text: `🏢 ${ws.name}`, callback_data: `ws_open_${ws.id}` }]);
  inline_keyboard.push([{ text: 'اضافة مساحة (تلقائي سريع ⚡)', callback_data: 'add_workspace_auto' }]);
  if (isAdmin(chatId)) inline_keyboard.push([{ text: 'تسجيل يدوي / وضع الكمبيوتر', callback_data: 'add_workspace_manual' }]);

  await bot.sendMessage(chatId, 'مرحبا، اختر المساحة أو أضف واحدة جديدة:', { reply_markup: { inline_keyboard } });
});

bot.on('callback_query', async (query) => {
  const chatId = String(query.message.chat.id);
  bot.answerCallbackQuery(query.id).catch(() => {});
  if (!sessions[chatId]) sessions[chatId] = { step: null, currentTab: 'members' };
  const state = sessions[chatId];
  const data = query.data;

  if (data === 'add_workspace_auto' || data === 'add_workspace_manual') {
    if (data === 'add_workspace_manual' && !isAdmin(chatId)) return bot.sendMessage(chatId, '❌ هذا الخيار للأدمن فقط.');
    state.mode = data === 'add_workspace_auto' ? 'auto' : 'manual';
    state.step = 'awaiting_credentials';
    state.credentialBuffer = '';
    return bot.sendMessage(chatId, state.mode === 'auto'
      ? 'أرسل الإيميل والباسورد ورابط 2FA بأي صيغة، وأنا ألتقطهم تلقائيا.'
      : 'أرسل الإيميل والباسورد، ورابط 2FA اختياري. أستطيع التقاطهم من أي رسالة غير مرتبة.');
  }

  if (data.startsWith('ws_open_')) {
    const wsId = data.split('_')[2];
    state.currentWsId = wsId;
    state.currentTab = 'members';
    const ws = await dbGet('SELECT * FROM workspaces WHERE id = ? AND chat_id = ?', [wsId, chatId]);
    if (!ws) return bot.sendMessage(chatId, '❌ المساحة غير موجودة.');
    const statusMsg = await bot.sendMessage(chatId, `⏳ جاري فتح مساحة: ${ws.name}...`);
    try {
      const context = await getContext(ws.id, ws.profile_dir);
      const page = await safeNewPage(context);
      await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 30000 });
      await smartWait(page, 1500);
      const p = path.join(os.tmpdir(), `dash_${Date.now()}.png`);
      await page.screenshot({ path: p });
      await page.close().catch(() => {});
      await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
      const msgOut = await bot.sendPhoto(chatId, p, { caption: `🏢 المساحة: **${ws.name}**`, parse_mode: 'Markdown', reply_markup: { inline_keyboard: getDashboardKeyboard(state, chatId) } });
      state.dashMsgId = msgOut.message_id;
      fs.unlinkSync(p);
    } catch (e) {
      await bot.sendMessage(chatId, `❌ خطأ في فتح المساحة: ${e.message}`);
    }
    return;
  }

  if (data === 'ws_toggle') {
    const wsId = state.currentWsId;
    if (!wsId) return;
    const statusMsg = await bot.sendMessage(chatId, '📸 جاري إرسال صورتين: الأعضاء ثم الدعوات...');
    try {
      const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
      const { membersPath, invitesPath } = await captureWorkspaceTabs(ws, 'duo');
      await bot.sendMediaGroup(chatId, [
        { type: 'photo', media: membersPath, caption: `👥 الأعضاء - ${ws.name}` },
        { type: 'photo', media: invitesPath, caption: `📨 الدعوات - ${ws.name}` }
      ]);
      await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
      [membersPath, invitesPath].forEach(p => { try { fs.unlinkSync(p); } catch (_) {} });
    } catch (e) {
      await bot.sendMessage(chatId, `❌ خطأ في التصوير: ${e.message}`);
    }
    return;
  }

  if (data === 'ws_sync_whitelist') {
    const wsId = state.currentWsId; if (!wsId) return;
    const statusMsg = await bot.sendMessage(chatId, '⏳ جاري توثيق الأعضاء والدعوات الحالية...');
    try {
      const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
      const info = await getWorkspaceOccupancy(ws);
      const allEmails = [...new Set([...info.members, ...info.invites])];
      let addedCount = 0;
      for (const e of allEmails) {
        const exists = await dbGet('SELECT id FROM allowed_emails WHERE ws_id = ? AND email = ?', [ws.id, normalizeEmail(e)]);
        if (!exists) { await rememberAllowedEmail(ws.id, e); addedCount++; }
      }
      await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
      await bot.sendMessage(chatId, `✅ تم التوثيق. تمت إضافة ${addedCount} إيميل جديد إلى القائمة البيضاء.`);
    } catch (e) {
      await bot.sendMessage(chatId, `❌ خطأ أثناء التوثيق: ${e.message}`);
    }
    return;
  }

  if (data === 'ws_back') {
    state.step = null;
    state.currentWsId = null;
    return bot.sendMessage(chatId, 'أرسل /start للعودة للقائمة.');
  }
  if (data === 'ws_add_person') {
    state.step = 'ws_awaiting_add_person';
    return bot.sendMessage(chatId, 'أرسل الإيميل، وسأبحث عن مساحة أقل من 6 وغير موجود فيها هذا الإيميل في Users أو Pending invites.');
  }
  if (data === 'ws_change_name') {
    state.step = 'ws_awaiting_change_name';
    return bot.sendMessage(chatId, 'أرسل الاسم الجديد للمساحة:');
  }
  if (data === 'ws_revoke_invite') {
    state.step = 'ws_awaiting_revoke_invite';
    return bot.sendMessage(chatId, 'أرسل الإيميل الذي تريد إلغاء دعوته:');
  }
  if (data === 'ws_remove_member') {
    state.step = 'ws_awaiting_remove_member';
    return bot.sendMessage(chatId, 'أرسل الإيميل الذي تريد إزالته:');
  }
  if (data === 'ws_fetch_emails') {
    const wsId = state.currentWsId; if (!wsId) return;
    const statusMsg = await bot.sendMessage(chatId, '⏳ جاري الاستخراج السريع للإيميلات...');
    try {
      const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
      const info = await getWorkspaceOccupancy(ws);
      const uniqueEmails = [...new Set([...info.members, ...info.invites])];
      await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
      await bot.sendMessage(chatId, uniqueEmails.length ? `📋 الإيميلات (${uniqueEmails.length}):\n\n${uniqueEmails.join('\n')}` : '⚠️ لم يتم العثور على أي إيميلات.');
    } catch (e) {
      await bot.sendMessage(chatId, `❌ خطأ: ${e.message}`);
    }
    return;
  }
  if (data === 'ws_delete') {
    return bot.sendMessage(chatId, '⚠️ متأكد من إزالة المساحة؟', { reply_markup: { inline_keyboard: [[{ text: '✅ نعم', callback_data: 'confirm_ws_del' }], [{ text: '❌ إلغاء', callback_data: 'ws_back' }]] } });
  }
  if (data === 'confirm_ws_del') {
    const wsId = state.currentWsId;
    const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
    if (!ws) return;
    if (activeContexts[wsId]) { await activeContexts[wsId].close().catch(() => {}); delete activeContexts[wsId]; }
    try { fs.rmSync(ws.profile_dir, { recursive: true, force: true }); } catch (_) {}
    await dbRun('DELETE FROM workspaces WHERE id = ?', [wsId]);
    await dbRun('DELETE FROM allowed_emails WHERE ws_id = ?', [wsId]);
    state.currentWsId = null;
    return bot.sendMessage(chatId, '🗑️ تمت إزالة المساحة. أرسل /start');
  }
  if (data === 'ws_computer_mode') {
    if (!isAdmin(chatId)) return bot.sendMessage(chatId, '❌ هذا الخيار للأدمن فقط.');
    return sendInteractiveMenu(chatId, '🖥️ تم فتح وضع الكمبيوتر لهذه الجلسة.');
  }
});

bot.on('message', async (msg) => {
  const chatId = String(msg.chat.id);
  const text = (msg.text || '').trim();
  if (!text || text.startsWith('/')) return;
  if (!sessions[chatId]) sessions[chatId] = { step: null, currentTab: 'members' };
  const state = sessions[chatId];

  if (state.step === 'awaiting_credentials') {
    state.credentialBuffer = `${state.credentialBuffer || ''}\n${text}`.trim();
    const parsed = parseCredentials(state.credentialBuffer);
    state.email = parsed.email || state.email;
    state.password = parsed.password || state.password;
    state.url2fa = parsed.url2fa || state.url2fa;

    if (!state.email || !state.password || (state.mode === 'auto' && !state.url2fa)) {
      return bot.sendMessage(chatId, `📥 تم الالتقاط الحالي:\nالإيميل: ${state.email || '—'}\nالباسورد: ${state.password || '—'}\n2FA: ${state.url2fa || '—'}\n\nأرسل المزيد إذا كان شيء ناقص.`);
    }

    state.step = 'processing';
    const profileDir = path.join(DATA_DIR, `ws_profile_${Date.now()}_${Math.floor(Math.random() * 1000)}`);
    state.profileDir = profileDir;
    const statusMsg = await bot.sendMessage(chatId, state.mode === 'manual' ? '⏳ جاري فتح جلسة التسجيل اليدوي...' : '⏳ جاري الدخول التلقائي السريع...');

    try {
      const context = await chromium.launchPersistentContext(profileDir, {
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled', '--disable-dev-shm-usage'],
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        viewport: { width: 1366, height: 768 }
      });
      const page = await safeNewPage(context);
      state.context = context;
      state.page = page;

      await page.goto('https://chatgpt.com/auth/login', { waitUntil: 'domcontentloaded', timeout: 30000 });
      await smartWait(page, 1500);

      if (state.mode === 'manual') {
        await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
        await bot.sendMessage(chatId, `✅ تم تجهيز وضع التسجيل اليدوي.\nالإيميل: ${state.email}\nالباسورد: ${state.password}\n2FA: ${state.url2fa || 'غير موجود'}`);
        await sendInteractiveMenu(chatId, '🖥️ وضع الكمبيوتر جاهز.');
        return;
      }

      const updateStatus = async (t) => bot.editMessageText(`⏳ ${t}`, { chat_id: chatId, message_id: statusMsg.message_id }).catch(() => {});
      await ensureWorkspaceLogin(state.page, { email: state.email, password: state.password, url2fa: state.url2fa }, updateStatus);

      await updateStatus('دخول ناجح، تخطي الإعدادات الأولى بسرعة');
      try {
        const emptyWsBtn = state.page.locator('text="Start as empty workspace"').first();
        if (await emptyWsBtn.isVisible({ timeout: 1500 }).catch(() => false)) await emptyWsBtn.click({ force: true });
      } catch (_) {}
      try {
        const contBtn = state.page.locator('text="Continue"').last();
        if (await contBtn.isVisible({ timeout: 1500 }).catch(() => false)) await contBtn.click({ force: true });
      } catch (_) {}
      await smartWait(state.page, 1200);

      await updateStatus('حفظ المساحة في القاعدة');
      await state.page.goto('https://chatgpt.com/admin/settings', { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(async () => {
        await state.page.goto('https://chatgpt.com/admin', { waitUntil: 'domcontentloaded', timeout: 30000 });
      });
      await smartWait(state.page, 1200);
      let wsName = state.email.split('@')[0];
      try {
        const nameInput = state.page.locator('input[type="text"]:not([placeholder*="Search" i]), input[name="name"]').first();
        if (await nameInput.isVisible().catch(() => false)) wsName = (await nameInput.inputValue().catch(() => '')) || wsName;
      } catch (_) {}

      const insertedId = await dbRun('INSERT INTO workspaces (chat_id, name, email, password, url2fa, profile_dir) VALUES (?, ?, ?, ?, ?, ?)', [chatId, wsName, state.email, state.password, state.url2fa || '', profileDir]);
      activeContexts[insertedId] = context;
      context.on('close', () => { delete activeContexts[insertedId]; });
      await rememberAllowedEmail(insertedId, state.email);

      await updateStatus('7/7 توثيق أولي وحفظ نهائي');
      const info = await getWorkspaceOccupancy({ id: insertedId, profile_dir: profileDir });
      for (const e of [...new Set([...info.members, ...info.invites])]) await rememberAllowedEmail(insertedId, e);
      await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
      const p = path.join(os.tmpdir(), `admin_${Date.now()}.png`);
      await state.page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 30000 });
      await smartWait(state.page, 1200);
      await state.page.screenshot({ path: p });
      state.currentWsId = insertedId;
      const msgOut = await bot.sendPhoto(chatId, p, { caption: `✅ تمت إضافة المساحة بنجاح\nالمساحة: ${wsName}`, reply_markup: { inline_keyboard: getDashboardKeyboard(state, chatId) } });
      state.dashMsgId = msgOut.message_id;
      fs.unlinkSync(p);
      await state.page.close().catch(() => {});
      state.page = null;
      state.context = null;
      state.step = null;
      state.credentialBuffer = '';
      return;
    } catch (error) {
      await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
      state.step = 'awaiting_credentials';
      return bot.sendMessage(chatId, `❌ فشل التنفيذ: ${error.message}`);
    }
  }

  if (!state.currentWsId || !state.step) return;
  const wsId = state.currentWsId;
  const textInput = text.trim();

  if (state.step === 'ws_awaiting_add_person') {
    state.step = 'processing';
    const statusMsg = await bot.sendMessage(chatId, '⏳ جاري البحث عن مساحة مناسبة وإضافة الإيميل...');
    try {
      const targetEmail = normalizeEmail(textInput);
      const workspaces = await dbAll('SELECT * FROM workspaces WHERE chat_id = ? ORDER BY id ASC', [chatId]);
      let selected = null;
      let selectedInfo = null;
      for (const ws of workspaces) {
        if (!fs.existsSync(ws.profile_dir)) continue;
        const info = await getWorkspaceOccupancy(ws);
        const exists = new Set([...info.members, ...info.invites].map(normalizeEmail)).has(targetEmail);
        if (exists) {
          selected = null;
          selectedInfo = { ws, duplicate: true };
          break;
        }
        if (info.total < MEMBER_LIMIT) {
          selected = ws;
          selectedInfo = info;
          break;
        }
      }
      await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
      if (selectedInfo && selectedInfo.duplicate) {
        return bot.sendMessage(chatId, `⚠️ هذا الإيميل موجود مسبقًا ضمن Users أو Pending invites في إحدى المساحات: ${targetEmail}`);
      }
      if (!selected) {
        return bot.sendMessage(chatId, `❌ لا توجد أي مساحة شاغرة أقل من ${MEMBER_LIMIT} أعضاء/دعوات لإضافة هذا الإيميل: ${targetEmail}`);
      }
      const result = await inviteEmailToWorkspace(selected, targetEmail);
      if (!result.ok) {
        return bot.sendMessage(chatId, `❌ فشل إضافة الإيميل ${targetEmail}\nالسبب: ${result.reason}`);
      }
      await rememberAllowedEmail(selected.id, targetEmail);
      return bot.sendMessage(chatId, `✅ تمت إضافة هذا الإيميل بنجاح\nالإيميل: ${targetEmail}\nالمساحة: ${selected.name}`);
    } catch (e) {
      return bot.sendMessage(chatId, `❌ خطأ: ${e.message}`);
    } finally {
      state.step = null;
    }
  }

  if (state.step === 'ws_awaiting_change_name') {
    state.step = 'processing';
    const statusMsg = await bot.sendMessage(chatId, '⏳ جاري تغيير الاسم...');
    try {
      const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
      const context = await getContext(wsId, ws.profile_dir);
      const page = await safeNewPage(context);
      await page.goto('https://chatgpt.com/admin/settings', { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(async () => {
        await page.goto('https://chatgpt.com/admin', { waitUntil: 'domcontentloaded', timeout: 30000 });
      });
      await smartWait(page, 1400);
      const input = page.locator('input[type="text"], input[name="name"], input[name="workspace_name"]').first();
      await input.click({ clickCount: 3, force: true });
      await page.keyboard.press('Backspace');
      await page.keyboard.type(textInput, { delay: 10 });
      const saveBtn = page.locator('button:has-text("Save"), button:has-text("Update")').first();
      if (await saveBtn.isVisible({ timeout: 1000 }).catch(() => false)) await saveBtn.click({ force: true });
      else await page.keyboard.press('Enter');
      await smartWait(page, 1200);
      await dbRun('UPDATE workspaces SET name = ? WHERE id = ?', [textInput, wsId]);
      await page.close().catch(() => {});
      await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
      await bot.sendMessage(chatId, '✅ تم تغيير الاسم وحفظه في قاعدة البيانات.');
    } catch (e) {
      await bot.sendMessage(chatId, `❌ خطأ: ${e.message}`);
    } finally { state.step = null; }
    return;
  }

  if (state.step === 'ws_awaiting_revoke_invite' || state.step === 'ws_awaiting_remove_member') {
    const actionType = state.step === 'ws_awaiting_revoke_invite' ? 'revoke' : 'remove';
    const tab = actionType === 'revoke' ? 'invites' : 'members';
    state.step = 'processing';
    const statusMsg = await bot.sendMessage(chatId, '⏳ جاري تنفيذ الإجراء...');
    try {
      const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
      const context = await getContext(wsId, ws.profile_dir);
      const page = await safeNewPage(context);
      await page.goto(`https://chatgpt.com/admin/members?tab=${tab}`, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await smartWait(page, 1200);
      const ok = await dynamicGeometryAction(page, textInput, actionType);
      await page.close().catch(() => {});
      await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
      if (!ok) return bot.sendMessage(chatId, '❌ لم يتم العثور على الإيميل أو فشل الإجراء.');
      await dbRun('DELETE FROM allowed_emails WHERE ws_id = ? AND email = ?', [wsId, normalizeEmail(textInput)]);
      return bot.sendMessage(chatId, actionType === 'revoke' ? `✅ تم إلغاء الدعوة: ${textInput}` : `✅ تمت إزالة العضو: ${textInput}`);
    } catch (e) {
      await bot.sendMessage(chatId, `❌ خطأ: ${e.message}`);
    } finally { state.step = null; }
  }
});

let watcherBusy = false;
setInterval(async () => {
  if (watcherBusy) return;
  watcherBusy = true;
  try {
    const workspaces = await dbAll('SELECT * FROM workspaces');
    await Promise.all(workspaces.map(ws => fastGuardWorkspace(ws)));
  } catch (_) {
  } finally {
    watcherBusy = false;
  }
}, WATCH_INTERVAL_MS);

process.on('uncaughtException', () => {});
process.on('unhandledRejection', () => {});
