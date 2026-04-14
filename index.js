/*
 * ==========================================================
 * ChatGPT Admin Workspace Automator - V12.1 (Old-Code Fixed)
 * ==========================================================
 * - مبني على كودك القديم الذي كان يسجل الدخول بشكل صحيح.
 * - إصلاح استخراج الإيميل/الباسورد/رابط 2FA بفلترة ذكية.
 * - إصلاح جلب كود 2FA بإعادة المحاولة وعدم الفشل السريع.
 * - تصوير كل مراحل الدخول للأدمن فقط: 643309456
 * - إصلاح وضع التسجيل اليدوي / وضع الكمبيوتر.
 * - زر 🔁 يرسل صورتين: الأعضاء + الدعوات.
 * - تسريع الحارس إلى 15 ثانية.
 * - عند الإضافة يتم البحث عن مساحة فيها أقل من 6 أعضاء وغير موجود فيها الإيميل
 *   لا في Users ولا Pending invites.
 * - حفظ المساحات في قاعدة البيانات كما هي حتى بعد تحديث الكود.
 * ==========================================================
 */

const TelegramBot = require('node-telegram-bot-api');
const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();
const fs = require('fs');
const path = require('path');
const os = require('os');
const sqlite3 = require('sqlite3').verbose();
const http = require('http');

chromium.use(stealth);

// ==========================================================
// 🚂 1. الخادم الوهمي لـ Railway
// ==========================================================
const PORT = process.env.PORT || 3000;
http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Bot V12.1 (Old-Code Fixed) is successfully running!');
}).listen(PORT, () => {});

// ==========================================================
// 🔐 2. إعدادات المتغيرات وقاعدة البيانات
// ==========================================================
const BOT_TOKEN = process.env.BOT_TOKEN || 'ضع_توكن_البوت_هنا_إذا_لم_يكن_في_البيئة';
if (!BOT_TOKEN || BOT_TOKEN === 'ضع_توكن_البوت_هنا_إذا_لم_يكن_في_البيئة') {
    process.exit(1);
}

const ADMIN_ID = '643309456';
const MEMBER_LIMIT = 6;
const WATCH_INTERVAL_MS = Number(process.env.WATCH_INTERVAL_MS || 15000);

const DATA_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH || path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const bot = new TelegramBot(BOT_TOKEN, { polling: true });
const sessions = {};
const activeContexts = {};
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, scaledMs(ms)));

const dbPath = path.join(DATA_DIR, 'workspaces_v12.db');
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
    db.run(`CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS user_access (
        chat_id TEXT PRIMARY KEY,
        role TEXT,
        can_refresh INTEGER DEFAULT 0,
        trader_price_iqd INTEGER DEFAULT 0,
        real_name TEXT DEFAULT '',
        approved_by TEXT DEFAULT '',
        approved_at TEXT DEFAULT ''
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS access_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id TEXT,
        requested_role TEXT,
        status TEXT DEFAULT 'pending',
        name TEXT DEFAULT '',
        created_at TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS member_additions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        actor_chat_id TEXT,
        actor_role TEXT,
        workspace_id INTEGER,
        email TEXT,
        status TEXT DEFAULT 'pending',
        settled INTEGER DEFAULT 0,
        added_at TEXT,
        migrated_to_workspace_id INTEGER
    )`);
});

const dbRun = (query, params = []) => new Promise((resolve, reject) => {
    db.run(query, params, function(err) { err ? reject(err) : resolve(this.lastID); });
});
const dbGet = (query, params = []) => new Promise((resolve, reject) => db.get(query, params, (err, row) => err ? reject(err) : resolve(row)));
const dbAll = (query, params = []) => new Promise((resolve, reject) => db.all(query, params, (err, rows) => err ? reject(err) : resolve(rows)));

function isAdmin(chatId) {
    return String(chatId) === ADMIN_ID;
}

function escapeRegex(str) {
    return String(str).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function normalizeEmail(v) {
    return String(v || '').trim().toLowerCase();
}


const SPEED_MULTIPLIERS = { 5: 1.8, 10: 1.4, 30: 0.9, 60: 0.55, 80: 0.32, 100: 0.18 };
let CURRENT_SPEED_PERCENT = 60;
let DEFAULT_TRADER_PRICE = 2500;
let DEFAULT_PAYMENT_METHOD = '';

function nowIso() {
    return new Date().toISOString();
}

function getSpeedMultiplier() {
    return SPEED_MULTIPLIERS[CURRENT_SPEED_PERCENT] || 0.55;
}

function scaledMs(ms) {
    return Math.max(120, Math.round(ms * getSpeedMultiplier()));
}

function typeDelay(base = 55) {
    return Math.max(8, Math.round(base * getSpeedMultiplier()));
}

async function loadSettingsCache() {
    try {
        const speed = await dbGet('SELECT value FROM settings WHERE key = ?', ['speed_percent']);
        if (speed && speed.value) CURRENT_SPEED_PERCENT = Number(speed.value) || 60;
        const price = await dbGet('SELECT value FROM settings WHERE key = ?', ['trader_price_iqd']);
        if (price && price.value) DEFAULT_TRADER_PRICE = Number(price.value) || 2500;
        const pay = await dbGet('SELECT value FROM settings WHERE key = ?', ['payment_method']);
        if (pay && pay.value != null) DEFAULT_PAYMENT_METHOD = String(pay.value || '');
    } catch (e) {}
}

setTimeout(() => { loadSettingsCache().catch(() => {}); }, 0);

async function getSetting(key, fallback = '') {
    const row = await dbGet('SELECT value FROM settings WHERE key = ?', [key]);
    return row ? row.value : fallback;
}

async function setSetting(key, value) {
    await dbRun('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', [key, String(value)]);
    await loadSettingsCache();
}

async function getUserAccess(chatId) {
    return await dbGet('SELECT * FROM user_access WHERE chat_id = ?', [String(chatId)]);
}

async function hasBotAccess(chatId) {
    if (isAdmin(chatId)) return true;
    const row = await getUserAccess(chatId);
    return !!row;
}

async function isTrader(chatId) {
    const row = await getUserAccess(chatId);
    return !!row && row.role === 'trader';
}

async function isWorkspaceManager(chatId) {
    const row = await getUserAccess(chatId);
    return !!row && row.role === 'manager';
}

async function canUseRefresh(chatId) {
    if (isAdmin(chatId)) return true;
    const row = await getUserAccess(chatId);
    return !!row && Number(row.can_refresh || 0) === 1;
}

async function getVisibleWorkspaces(chatId) {
    if (isAdmin(chatId)) return await dbAll('SELECT id, name FROM workspaces ORDER BY id DESC');
    if (await isTrader(chatId)) return await dbAll('SELECT id, name FROM workspaces WHERE chat_id = ? ORDER BY id DESC', [ADMIN_ID]);
    return await dbAll('SELECT id, name FROM workspaces WHERE chat_id = ? ORDER BY id DESC', [String(chatId)]);
}

async function sendMainMenu(chatId, headerText) {
    const text = headerText || 'مرحبا ببوت ادارة مساحات العمل';
    if (!(await hasBotAccess(chatId))) {
        return await bot.sendMessage(chatId, `${text}

هل انت تاجر؟`, {
            reply_markup: { inline_keyboard: [
                [{ text: 'نعم', callback_data: 'request_role_trader' }, { text: 'لا', callback_data: 'request_role_manager' }]
            ] }
        });
    }
    const workspaces = await getVisibleWorkspaces(chatId);
    const inline_keyboard = [];
    for (const ws of workspaces) inline_keyboard.push([{ text: `🏢 ${ws.name}`, callback_data: `ws_open_${ws.id}` }]);

    if (!(await isTrader(chatId))) {
        inline_keyboard.push([{ text: 'اضافة مساحة (تلقائي سريع ⚡)', callback_data: 'add_workspace_auto' }]);
        if (isAdmin(chatId)) inline_keyboard.push([{ text: 'اضافة مساحة (يدوي لتسجيل السكربت ✍️)', callback_data: 'add_workspace_manual' }]);
    }
    if (isAdmin(chatId)) inline_keyboard.push([{ text: '⚙️ الاعدادات', callback_data: 'admin_settings' }]);
    return await bot.sendMessage(chatId, text, { reply_markup: { inline_keyboard } });
}

async function sendAdminRequestCard(reqId, requesterChatId, role) {
    const roleLabel = role === 'trader' ? 'تاجر' : 'مدير مساحات';
    await bot.sendMessage(ADMIN_ID, `📥 طلب جديد لاستخدام البوت
ID: ${requesterChatId}
النوع المطلوب: ${roleLabel}`, {
        reply_markup: { inline_keyboard: [
            [{ text: '✅ تاجر', callback_data: `approve_request_trader_${reqId}` }, { text: '✅ مدير مساحات', callback_data: `approve_request_manager_${reqId}` }],
            [{ text: '❌ رفض', callback_data: `reject_request_${reqId}` }]
        ] }
    });
}

async function sendTraderDebtCard(chatId) {
    const row = await getUserAccess(chatId);
    const pay = await getSetting('payment_method', '');
    const price = Number((row && row.trader_price_iqd) || DEFAULT_TRADER_PRICE || 0);
    const debtCountRow = await dbGet(`SELECT COUNT(*) AS c FROM member_additions WHERE actor_chat_id = ? AND actor_role = 'trader' AND settled = 0 AND status IN ('pending','member','migrated')`, [String(chatId)]);
    const count = Number((debtCountRow && debtCountRow.c) || 0);
    const total = count * price;
    return await bot.sendMessage(chatId, `لقد اضفت ${count} دعوات
المبلغ المراد دفعه هو: ${total} د.ع`, {
        reply_markup: { inline_keyboard: [[{ text: '💳 سداد الديون', callback_data: 'trader_show_payment' }]] }
    });
}

async function getOwnerChatForAction(chatId) {
    return (await isTrader(chatId)) ? ADMIN_ID : String(chatId);
}

async function traderDebtBlocked(chatId) {
    const debtCountRow = await dbGet(`SELECT COUNT(*) AS c FROM member_additions WHERE actor_chat_id = ? AND actor_role = 'trader' AND settled = 0 AND status IN ('pending','member','migrated')`, [String(chatId)]);
    return Number((debtCountRow && debtCountRow.c) || 0) >= 7;
}

async function logMemberAddition(actorChatId, actorRole, workspaceId, email, status='pending') {
    await dbRun('INSERT INTO member_additions (actor_chat_id, actor_role, workspace_id, email, status, added_at) VALUES (?, ?, ?, ?, ?, ?)', [String(actorChatId), actorRole, workspaceId, normalizeEmail(email), status, nowIso()]);
}

async function canTraderManageEmail(chatId, email) {
    const row = await dbGet(`SELECT * FROM member_additions WHERE actor_chat_id = ? AND actor_role = 'trader' AND email = ? AND status IN ('pending','member','migrated') ORDER BY id DESC LIMIT 1`, [String(chatId), normalizeEmail(email)]);
    return row;
}

async function markTraderEmailStatus(chatId, email, nextStatus) {
    const row = await canTraderManageEmail(chatId, email);
    if (!row) return false;
    await dbRun('UPDATE member_additions SET status = ? WHERE id = ?', [nextStatus, row.id]);
    return true;
}

async function relocateRecentMembersFromWorkspace(ws) {
    const since = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
    const rows = await dbAll(`SELECT * FROM member_additions WHERE workspace_id = ? AND added_at >= ? AND status IN ('pending','member')`, [ws.id, since]);
    const moved = [];
    for (const row of rows) {
        const ownerChatId = row.actor_role === 'trader' ? ADMIN_ID : row.actor_chat_id;
        const result = await addEmailIntoAvailableWorkspace(ownerChatId, row.email, { actorChatId: row.actor_chat_id, actorRole: row.actor_role, excludeWorkspaceId: ws.id, silentLog: false });
        if (result && result.ok) {
            await dbRun('UPDATE member_additions SET status = ?, migrated_to_workspace_id = ? WHERE id = ?', ['migrated', result.workspaceId, row.id]);
            moved.append({ email: row.email, workspaceName: result.workspaceName, actorChatId: row.actor_chat_id });
        }
    }
    return moved;
}

// ==========================================================
// 🌐 دوال المتصفح
// ==========================================================
async function getContext(wsId, profileDir) {
    if (activeContexts[wsId]) {
        try {
            activeContexts[wsId].pages();
            return activeContexts[wsId];
        } catch (e) {
            delete activeContexts[wsId];
        }
    }

    const context = await chromium.launchPersistentContext(profileDir, {
        headless: true,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage'
        ],
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        viewport: { width: 1366, height: 768 }
    });

    const pages = context.pages();
    if (pages.length > 0) {
        await pages[0].close().catch(() => {});
    }

    activeContexts[wsId] = context;
    context.on('close', () => {
        if (activeContexts[wsId] === context) delete activeContexts[wsId];
    });

    return context;
}

async function extractAllEmails(page) {
    let emails = new Set();
    let prevHeight = 0;
    for (let i = 0; i < 15; i++) {
        const html = await page.content();
        const matches = html.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g) || [];
        matches.forEach(e => emails.add(e.toLowerCase()));

        await page.mouse.wheel(0, 3000).catch(() => {});
        await page.evaluate(() => {
            const scrollables = Array.from(document.querySelectorAll('*')).filter(el => el.scrollHeight > el.clientHeight);
            if (scrollables.length > 0) scrollables[scrollables.length - 1].scrollTop += 3000;
            window.scrollTo(0, document.body.scrollHeight);
        }).catch(() => {});

        await sleep(1000);
        let newHeight = await page.evaluate('document.body.scrollHeight').catch(() => 0);
        if (newHeight === prevHeight && i > 3) break;
        prevHeight = newHeight;
    }
    return [...emails];
}

async function sendAdminStep(chatId, page, caption) {
    if (!isAdmin(chatId) || !page) return;
    try {
        const file = path.join(os.tmpdir(), `adm_${Date.now()}_${Math.random().toString(36).slice(2)}.png`);
        await page.screenshot({ path: file, fullPage: false });
        await bot.sendPhoto(chatId, file, { caption: `🧩 ${caption}` });
        fs.unlinkSync(file);
    } catch (e) {}
}

async function fetch2FACode(context, url2fa) {
    if (!url2fa) throw new Error('لا يوجد رابط 2FA');

    const mfaPage = await context.newPage();
    try {
        await mfaPage.goto(url2fa, { waitUntil: 'domcontentloaded', timeout: 45000 });

        for (let attempt = 1; attempt <= 10; attempt++) {
            await sleep(attempt === 1 ? 2500 : 3000);

            const bodyText = await mfaPage.evaluate(() => {
                return [
                    document.body ? document.body.innerText : '',
                    document.documentElement ? document.documentElement.innerText : ''
                ].join('\n');
            }).catch(() => '');

            let codeMatch = bodyText.match(/\b\d{3}\s*\d{3}\b/) || bodyText.match(/\b\d{6}\b/);
            if (codeMatch) {
                return codeMatch[0].replace(/\s+/g, '');
            }

            await mfaPage.reload({ waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
        }

        throw new Error('لم يتم العثور على كود 2FA');
    } finally {
        await mfaPage.close().catch(() => {});
    }
}

async function ensureLoginField(page) {
    const selectors = [
        'input[type="email"]',
        'input[name="username"]',
        'input[autocomplete="username"]',
        'input[placeholder*="email" i]'
    ];
    for (const sel of selectors) {
        const loc = page.locator(sel).first();
        if (await loc.isVisible({ timeout: 4000 }).catch(() => false)) return loc;
    }
    return null;
}

async function ensurePasswordField(page) {
    const selectors = [
        'input[type="password"]',
        'input[name="password"]',
        'input[autocomplete="current-password"]'
    ];
    for (const sel of selectors) {
        const loc = page.locator(sel).first();
        if (await loc.isVisible({ timeout: 4000 }).catch(() => false)) return loc;
    }
    return null;
}

async function pageLooksLoggedIn(page) {
    const url = page.url();
    if (/\/admin(\/|$)/i.test(url)) return true;
    const loggedSignals = [
        'text="Members"',
        'text="Settings"',
        'text="Invite member"',
        'text="Invite members"'
    ];
    for (const s of loggedSignals) {
        if (await page.locator(s).first().isVisible({ timeout: 1500 }).catch(() => false)) return true;
    }
    return false;
}

async function completeLoginFlow(page, state, chatId) {
    await page.goto('https://chatgpt.com/auth/login', { waitUntil: 'domcontentloaded', timeout: 60000 });
    await sleep(3500);
    await sendAdminStep(chatId, page, 'فتح صفحة تسجيل الدخول');

    try {
        const cfBox = page.frameLocator('iframe').locator('.ctp-checkbox-label, input[type="checkbox"]').first();
        if (await cfBox.isVisible({ timeout: 3000 }).catch(() => false)) {
            await cfBox.click({ force: true });
            await sleep(5000);
        }
    } catch (e) {}

    const loginBtn = page.locator('text="Log in"').first();
    if (await loginBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
        await loginBtn.click({ force: true }).catch(() => {});
        await sleep(2000);
    }

    const emailField = await ensureLoginField(page);
    if (!emailField) throw new Error('لم يظهر حقل الإيميل');
    await emailField.click({ force: true });
    await page.keyboard.type(state.email, { delay: typeDelay(55) });
    await sendAdminStep(chatId, page, `بعد كتابة الإيميل: ${state.email}`);
    await sleep(700);
    await page.keyboard.press('Enter');

    await sleep(4500);
    const passwordField = await ensurePasswordField(page);
    if (!passwordField) throw new Error('لم يظهر حقل الباسورد');
    await passwordField.click({ force: true });
    await page.keyboard.type(state.password, { delay: typeDelay(55) });
    await sendAdminStep(chatId, page, `بعد كتابة الباسورد: ${state.password}`);
    await sleep(700);
    await page.keyboard.press('Enter');

    await sleep(5000);
    if (await pageLooksLoggedIn(page)) {
        await sendAdminStep(chatId, page, 'تم الدخول مباشرة بدون طلب 2FA');
        return;
    }

    const code6 = await fetch2FACode(page.context(), state.url2fa);
    await page.bringToFront();
    await sendAdminStep(chatId, page, `تم جلب كود 2FA: ${code6}`);

    const otpInput = page.locator('input[inputmode="numeric"], input[autocomplete="one-time-code"], input[name*="otp" i], input[name*="code" i], input[maxlength="6"]').first();
    if (await otpInput.isVisible({ timeout: 5000 }).catch(() => false)) {
        await otpInput.click({ force: true });
        await page.keyboard.type(code6, { delay: typeDelay(55) });
        await sleep(500);
        await page.keyboard.press('Enter').catch(() => {});
    } else {
        const splitInputs = page.locator('input[maxlength="1"], input[size="1"]');
        const count = await splitInputs.count().catch(() => 0);
        if (count >= 6) {
            for (let i = 0; i < 6; i++) {
                await splitInputs.nth(i).click({ force: true }).catch(() => {});
                await page.keyboard.type(code6[i], { delay: typeDelay(45) }).catch(() => {});
            }
            await page.keyboard.press('Enter').catch(() => {});
        } else {
            await page.keyboard.type(code6, { delay: typeDelay(55) });
            await sleep(500);
            await page.keyboard.press('Enter').catch(() => {});
        }
    }

    await sleep(7000);
    await sendAdminStep(chatId, page, 'بعد إدخال 2FA');

    try {
        await page.mouse.click(561.58, 230.4).catch(() => {});
        await sleep(1000);
        const emptyWsBtn = page.locator('text="Start as empty workspace"').first();
        if (await emptyWsBtn.isVisible({ timeout: 5000 }).catch(() => false)) {
            await emptyWsBtn.click({ force: true }).catch(() => {});
            await sleep(1000);
        }
        const contBtn = page.locator('text="Continue"').last();
        if (await contBtn.isVisible({ timeout: 5000 }).catch(() => false)) {
            await contBtn.click({ force: true }).catch(() => {});
            await sleep(2500);
        }
    } catch (e) {}

    await page.goto('https://chatgpt.com/admin/settings', { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(async () => {
        await page.goto('https://chatgpt.com/admin', { waitUntil: 'domcontentloaded', timeout: 45000 });
    });
    await sleep(5000);
    await sendAdminStep(chatId, page, 'بعد الوصول إلى لوحة الإدارة');

    if (!(await pageLooksLoggedIn(page))) {
        throw new Error('ما زالت صفحة تسجيل الدخول ظاهرة، تحقق من البيانات');
    }
}

function extractCredentialsSmart(rawText) {
    const text = String(rawText || '').replace(/\r/g, '\n');
    const lines = text.split('\n').map(v => v.trim()).filter(Boolean);

    const emailMatch = text.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
    const urlMatch = text.match(/https?:\/\/2fa\.fb\.tools\/[^\s]+/i) || text.match(/https?:\/\/[^\s]+/i);

    const email = emailMatch ? emailMatch[0].trim() : null;
    const url2fa = urlMatch ? urlMatch[0].trim() : null;

    let password = null;

    const passwordLabel = text.match(/(?:password|pass|الباسورد|كلمة\s*المرور|🔑)\s*[:：]?\s*([^\n\s]+(?: [^\n\s]+)*)/i);
    if (passwordLabel) {
        let candidate = passwordLabel[1].trim().split(/\s+/)[0];
        if (candidate && !candidate.includes('http') && candidate !== email) password = candidate;
    }

    if (!password) {
        for (const line of lines) {
            if (email && line.includes(email)) continue;
            if (url2fa && line.includes(url2fa)) continue;
            if (/https?:\/\//i.test(line)) {
                const cleaned = line.replace(/https?:\/\/\S+/ig, '').trim();
                if (cleaned) {
                    password = cleaned.split(/\s+/)[0];
                    break;
                }
                continue;
            }
            if (/^[^\s]{3,}$/.test(line) && !/@/.test(line)) {
                password = line.split(/\s+/)[0];
                break;
            }
        }
    }

    if (!password) {
        const collapsed = text.replace(url2fa || '', ' ').replace(email || '', ' ');
        const tokens = collapsed.split(/\s+/).map(v => v.trim()).filter(Boolean);
        password = tokens.find(t => !/^https?:\/\//i.test(t) && !/@/.test(t) && t.length >= 3) || null;
    }

    return {
        email: email ? email.trim() : null,
        password: password ? password.trim() : null,
        url2fa: url2fa ? url2fa.trim() : null
    };
}

async function getWorkspaceName(page, fallbackEmail) {
    let wsName = String(fallbackEmail || '').split('@')[0];
    try {
        const nameInput = page.locator('input[type="text"]:not([placeholder*="Search" i]), input[name="name"]').first();
        if (await nameInput.isVisible({ timeout: 3000 }).catch(() => false)) {
            wsName = (await nameInput.inputValue().catch(() => wsName)) || wsName;
        }
    } catch (e) {}
    return wsName;
}

async function countWorkspaceOccupancy(page) {
    const emails = await extractAllEmails(page);
    return new Set(emails.map(normalizeEmail)).size;
}

// ==========================================================
// 🎯 المنطق العام والديناميكي الهندسي الشامل (Geometry Anchor)
// ==========================================================
async function dynamicGeometryAction(page, email, actionType) {
    try {
        await page.waitForTimeout(1500);

        const dotsCoords = await page.evaluate((targetEmail) => {
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
            let emailNode = null;
            let node;
            while ((node = walker.nextNode())) {
                if (node.nodeValue.toLowerCase().includes(targetEmail.toLowerCase())) {
                    emailNode = node.parentElement;
                    break;
                }
            }
            if (!emailNode) return null;

            const emailRect = emailNode.getBoundingClientRect();
            if (emailRect.width === 0 || emailRect.height === 0) return null;
            const emailCenterY = emailRect.top + (emailRect.height / 2);

            const allButtons = Array.from(document.querySelectorAll('button, [role="button"], a'));
            const rowButtons = allButtons.filter(btn => {
                const rect = btn.getBoundingClientRect();
                if (rect.width === 0 || rect.height === 0) return false;
                const btnCenterY = rect.top + (rect.height / 2);
                return Math.abs(btnCenterY - emailCenterY) < 40 && rect.left > emailRect.left;
            });

            if (rowButtons.length > 0) {
                rowButtons.sort((a, b) => b.getBoundingClientRect().right - a.getBoundingClientRect().right);
                const targetBtn = rowButtons[0];
                targetBtn.scrollIntoView({ behavior: 'instant', block: 'center' });
                const rect = targetBtn.getBoundingClientRect();
                return { x: rect.left + (rect.width / 2), y: rect.top + (rect.height / 2) };
            }
            return null;
        }, email);

        if (!dotsCoords) return false;

        await page.mouse.click(dotsCoords.x, dotsCoords.y);
        await page.waitForTimeout(1200);

        const actionRegexStr = actionType === 'remove' ? 'Remove' : '(Revoke|Cancel)';
        const actionCoords = await page.evaluate((regexStr) => {
            const regex = new RegExp(regexStr, 'i');
            const items = Array.from(document.querySelectorAll('button, [role="menuitem"], a, span, li'))
                .filter(el => {
                    const rect = el.getBoundingClientRect();
                    return rect.width > 0 && rect.height > 0 && el.innerText && regex.test(el.innerText);
                });
            if (items.length > 0) {
                const target = items[items.length - 1];
                target.scrollIntoView({ behavior: 'instant', block: 'center' });
                const rect = target.getBoundingClientRect();
                return { x: rect.left + (rect.width / 2), y: rect.top + (rect.height / 2) };
            }
            return null;
        }, actionRegexStr);

        if (!actionCoords) {
            const regex = actionType === 'remove' ? /Remove/i : /(Revoke|Cancel)/i;
            const loc = page.locator('button, [role="menuitem"], a').filter({ hasText: regex }).last();
            if (await loc.isVisible({ timeout: 2000 }).catch(() => false)) await loc.click({ force: true });
            else return false;
        } else {
            await page.mouse.click(actionCoords.x, actionCoords.y);
        }

        await page.waitForTimeout(1200);

        const confirmCoords = await page.evaluate((regexStr) => {
            const regex = new RegExp(regexStr, 'i');
            const dialogs = Array.from(document.querySelectorAll('[role="dialog"]'));
            const container = dialogs.length > 0 ? dialogs[dialogs.length - 1] : document.body;

            const btns = Array.from(container.querySelectorAll('button')).filter(b => {
                const rect = b.getBoundingClientRect();
                return rect.width > 0 && rect.height > 0 && window.getComputedStyle(b).visibility !== 'hidden';
            });

            let confirmBtn = btns.find(b => regex.test(b.innerText) && (b.className.match(/red|danger/i) || window.getComputedStyle(b).backgroundColor === 'rgb(220, 38, 38)'));
            if (!confirmBtn) confirmBtn = btns.find(b => regex.test(b.innerText));
            if (!confirmBtn) confirmBtn = btns.find(b => b.className.match(/red|danger/i));
            if (!confirmBtn && dialogs.length > 0) confirmBtn = btns[btns.length - 1];

            if (confirmBtn) {
                confirmBtn.scrollIntoView({ behavior: 'instant', block: 'center' });
                const rect = confirmBtn.getBoundingClientRect();
                return { x: rect.left + (rect.width / 2), y: rect.top + (rect.height / 2) };
            }
            return null;
        }, actionRegexStr);

        if (!confirmCoords) {
            const regex = actionType === 'remove' ? /Remove/i : /(Revoke|Cancel)/i;
            const confirmLoc = page.locator('[role="dialog"] button, button[class*="danger"], button[class*="red"]').filter({ hasText: regex }).last();
            if (await confirmLoc.isVisible({ timeout: 2000 }).catch(() => false)) await confirmLoc.click({ force: true });
            else await page.keyboard.press('Enter').catch(() => {});
        } else {
            await page.mouse.click(confirmCoords.x, confirmCoords.y);
        }

        await page.waitForTimeout(1500);
        return true;
    } catch (e) {
        return false;
    }
}

// ================= نظام الوضع اليدوي =================
class PlaywrightCodeGenerator {
    constructor() {
        this.codeLines = [];
        this.stepCounter = 1;
    }
    addStep(comment) {
        this.codeLines.push(`\n    // === الخطوة ${this.stepCounter}: ${comment} ===`);
        this.stepCounter++;
    }
    addCommand(cmd) {
        this.codeLines.push(`    ${cmd}`);
    }
    getFinalScript() {
        return `// 🤖 سكربت Playwright\nconst { chromium } = require('playwright');\n(async () => {\n    const browser = await chromium.launch({ headless: false });\n    const context = await browser.newContext({ viewport: { width: 1366, height: 768 } });\n    const page = await context.newPage();\n${this.codeLines.join('\n')}\n})();`;
    }
}

async function sendInteractiveMenu(chatId, text = '🎮 أنت الآن تتحكم بالمتصفح:') {
    const opts = {
        reply_markup: {
            inline_keyboard: [
                [{ text: '🌐 فتح رابط', callback_data: 'int_goto_url' }, { text: '📸 تحديث الشاشة', callback_data: 'int_refresh' }],
                [{ text: '⌨️ كتابة نص', callback_data: 'int_type_text' }, { text: '↩️ انتر', callback_data: 'int_press_enter' }],
                [{ text: '🖱️ شبكة الماوس', callback_data: 'int_show_grid' }, { text: '🔐 جلب 2FA', callback_data: 'int_fetch_2fa' }],
                [{ text: '✅ إنهاء التسجيل اليدوي', callback_data: 'int_finish_login' }]
            ]
        }
    };
    await bot.sendMessage(chatId, text, opts);
}


function getDashboardKeyboard(state, chatId, accessRow = null) {
    const keyboard = [];
    const role = isAdmin(chatId) ? 'admin' : (accessRow ? accessRow.role : 'manager');
    if (isAdmin(chatId) || (accessRow && Number(accessRow.can_refresh || 0) === 1)) {
        keyboard.push([{ text: '🔁 تحديث الشاشة (صورتين)', callback_data: 'ws_toggle' }]);
    }
    if (role !== 'trader') {
        keyboard.push([{ text: '🛡️ توثيق الأعضاء (حماية من الحارس)', callback_data: 'ws_sync_whitelist' }]);
    }
    keyboard.push([{ text: 'اضافة عضو', callback_data: 'ws_add_person' }]);
    keyboard.push([{ text: 'إزالة عضو', callback_data: 'ws_remove_member' }, { text: 'إلغاء دعوة', callback_data: 'ws_revoke_invite' }]);
    if (role !== 'trader') {
        keyboard.push([{ text: 'جلب الإيميلات', callback_data: 'ws_fetch_emails' }]);
        keyboard.push([{ text: 'تغيير اسم المساحة', callback_data: 'ws_change_name' }]);
        keyboard.push([{ text: '❌ إزالة المساحة (تسجيل الخروج)', callback_data: 'ws_delete' }]);
    }
    if (isAdmin(chatId)) {
        keyboard.splice(1, 0, [{ text: '🖥 وضع الكمبيوتر / التسجيل اليدوي', callback_data: 'admin_open_computer' }]);
    }
    if (role === 'trader') {
        keyboard.unshift([{ text: '💳 سداد الديون', callback_data: 'trader_show_payment' }]);
    }
    keyboard.push([{ text: '🔙 العودة للقائمة', callback_data: 'ws_back' }]);
    return keyboard;
}

async function refreshTwoShots(chatId, ws, state) {
    const accessRow = await getUserAccess(chatId);
    const context = await getContext(ws.id, ws.profile_dir);
    const p1 = path.join(os.tmpdir(), `members_${Date.now()}.png`);
    const p2 = path.join(os.tmpdir(), `invites_${Date.now()}.png`);
    const page1 = await context.newPage();
    await page1.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 });
    await sleep(2200);
    await page1.screenshot({ path: p1 });
    await page1.close().catch(() => {});
    const page2 = await context.newPage();
    await page2.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 45000 });
    await sleep(2200);
    await page2.screenshot({ path: p2 });
    await page2.close().catch(() => {});
    await bot.sendMediaGroup(chatId, [
        { type: 'photo', media: p1, caption: `🏢 ${ws.name}
👥 الأعضاء.` },
        { type: 'photo', media: p2, caption: `🏢 ${ws.name}
📨 الدعوات.` }
    ]);
    fs.unlinkSync(p1); fs.unlinkSync(p2);
    const extra = [];
    if (await isTrader(chatId)) {
        const row = await dbGet(`SELECT COUNT(*) AS c FROM member_additions WHERE actor_chat_id = ? AND actor_role = 'trader' AND settled = 0 AND status IN ('pending','member','migrated')`, [String(chatId)]);
        const count = Number((row && row.c) || 0);
        const price = Number((accessRow && accessRow.trader_price_iqd) || DEFAULT_TRADER_PRICE || 0);
        extra.push(`لقد اضفت ${count} دعوات`);
        extra.push(`المبلغ المراد دفعه هو: ${count * price} د.ع`);
    }
    await bot.sendMessage(chatId, extra.join('\n') || `🏢 ${ws.name}`, { reply_markup: { inline_keyboard: getDashboardKeyboard(state, chatId, accessRow) } });
}



// ================= القائمة الرئيسية =================
bot.onText(/\/start/, async (msg) => {
    const chatId = msg.chat.id.toString();
    if (!sessions[chatId]) sessions[chatId] = { step: null, currentTab: 'members' };
    sessions[chatId].step = null;
    sessions[chatId].currentWsId = null;
    await sendMainMenu(chatId);
});

function getAdminSettingsKeyboard() {
    return { inline_keyboard: [
        [{ text: `⚡ السرعة الحالية: ${CURRENT_SPEED_PERCENT}%`, callback_data: 'noop' }],
        [{ text: '⚡ السرعة', callback_data: 'admin_speed_menu' }],
        [{ text: '👥 تفعيل مدير', callback_data: 'admin_enable_manager' }, { text: '🛒 تفعيل تاجر', callback_data: 'admin_enable_trader' }],
        [{ text: '🗑️ إزالة صلاحية مستخدم', callback_data: 'admin_remove_access' }, { text: '📋 اظهار المستخدمين', callback_data: 'admin_list_users' }],
        [{ text: '💳 اضافة طريقة دفع', callback_data: 'admin_set_payment' }, { text: '💰 تحديد سعر التاجر', callback_data: 'admin_set_trader_price' }],
        [{ text: '💸 تصفير دين تاجر', callback_data: 'admin_clear_trader_debt' }],
        [{ text: '🔙 رجوع', callback_data: 'ws_back' }]
    ]};
}

function getSpeedKeyboard() {
    const vals = [5,10,30,60,80,100];
    const rows = [];
    for (let i = 0; i < vals.length; i += 2) rows.push(vals.slice(i, i + 2).map(v => ({ text: `السرعة ${v}%`, callback_data: `admin_speed_${v}` })));
    rows.push([{ text: '🔙 رجوع للاعدادات', callback_data: 'admin_settings' }]);
    return { inline_keyboard: rows };
}

bot.on('callback_query', async (query) => {
    const chatId = query.message.chat.id.toString();
    bot.answerCallbackQuery(query.id).catch(() => {});
    if (!sessions[chatId]) sessions[chatId] = { step: null, currentTab: 'members' };
    const state = sessions[chatId];
    const data = query.data;

    if (data === 'noop') return;

    if (data === 'request_role_trader' || data === 'request_role_manager') {
        if (await hasBotAccess(chatId)) return bot.sendMessage(chatId, '✅ لديك صلاحية بالفعل.');
        const role = data === 'request_role_trader' ? 'trader' : 'manager';
        const exists = await dbGet('SELECT id FROM access_requests WHERE chat_id = ? AND status = ?', [chatId, 'pending']);
        if (exists) return bot.sendMessage(chatId, '⏳ لديك طلب قيد المراجعة بالفعل.');
        const reqId = await dbRun('INSERT INTO access_requests (chat_id, requested_role, created_at) VALUES (?, ?, ?)', [chatId, role, nowIso()]);
        await bot.sendMessage(chatId, '✅ تم ارسال طلبك الى الادمن.');
        await sendAdminRequestCard(reqId, chatId, role);
        return;
    }

    if (data.startsWith('approve_request_') && isAdmin(chatId)) {
        const parts = data.split('_');
        const role = parts[2];
        const reqId = parts[3];
        const req = await dbGet('SELECT * FROM access_requests WHERE id = ?', [reqId]);
        if (!req) return bot.sendMessage(chatId, '❌ الطلب غير موجود.');
        await dbRun('UPDATE access_requests SET status = ? WHERE id = ?', ['approved', reqId]);
        const traderPrice = Number(await getSetting('trader_price_iqd', String(DEFAULT_TRADER_PRICE)) || DEFAULT_TRADER_PRICE);
        await dbRun('INSERT OR REPLACE INTO user_access (chat_id, role, can_refresh, trader_price_iqd, approved_by, approved_at) VALUES (?, ?, ?, ?, ?, ?)', [req.chat_id, role, 0, traderPrice, chatId, nowIso()]);
        if (role === 'trader') {
            sessions[req.chat_id] = sessions[req.chat_id] || { step: null, currentTab: 'members' };
            sessions[req.chat_id].step = 'trader_awaiting_real_name';
            await bot.sendMessage(req.chat_id, '✅ تمت الموافقة عليك كتاجر. ارسل اسمك الحقيقي الآن.');
        } else {
            await bot.sendMessage(req.chat_id, '✅ تمت الموافقة عليك كمدير مساحات. أرسل /start');
        }
        return bot.sendMessage(chatId, '✅ تمت الموافقة على الطلب.');
    }

    if (data.startsWith('reject_request_') && isAdmin(chatId)) {
        const reqId = data.split('_')[2];
        const req = await dbGet('SELECT * FROM access_requests WHERE id = ?', [reqId]);
        if (req) {
            await dbRun('UPDATE access_requests SET status = ? WHERE id = ?', ['rejected', reqId]);
            await bot.sendMessage(req.chat_id, '❌ تم رفض الطلب.');
        }
        return bot.sendMessage(chatId, '✅ تم رفض الطلب.');
    }

    if (data === 'admin_settings' && isAdmin(chatId)) {
        return bot.sendMessage(chatId, '⚙️ اعدادات الادمن', { reply_markup: getAdminSettingsKeyboard() });
    }
    if (data === 'admin_speed_menu' && isAdmin(chatId)) {
        return bot.sendMessage(chatId, 'اختر السرعة:', { reply_markup: getSpeedKeyboard() });
    }
    if (/^admin_speed_\d+$/.test(data) && isAdmin(chatId)) {
        const val = Number(data.split('_').pop());
        CURRENT_SPEED_PERCENT = val;
        await setSetting('speed_percent', String(val));
        return bot.sendMessage(chatId, `✅ تم ضبط السرعة على ${val}%`);
    }
    if (data === 'admin_enable_manager' && isAdmin(chatId)) { state.step = 'admin_enable_manager'; return bot.sendMessage(chatId, 'ارسل ID تيليجرام للمدير الذي تريد تفعيله.'); }
    if (data === 'admin_enable_trader' && isAdmin(chatId)) { state.step = 'admin_enable_trader'; return bot.sendMessage(chatId, 'ارسل ID تيليجرام للتاجر الذي تريد تفعيله.'); }
    if (data === 'admin_remove_access' && isAdmin(chatId)) { state.step = 'admin_remove_access'; return bot.sendMessage(chatId, 'ارسل ID تيليجرام الذي تريد إزالة صلاحيته.'); }
    if (data === 'admin_set_payment' && isAdmin(chatId)) { state.step = 'admin_set_payment'; return bot.sendMessage(chatId, 'ارسل طريقة الدفع الجديدة أو ارسل skip لمسحها.'); }
    if (data === 'admin_set_trader_price' && isAdmin(chatId)) { state.step = 'admin_set_trader_price'; return bot.sendMessage(chatId, 'ارسل سعر الدعوة للتاجر بالدينار العراقي.'); }
    if (data === 'admin_clear_trader_debt' && isAdmin(chatId)) { state.step = 'admin_clear_trader_debt'; return bot.sendMessage(chatId, 'ارسل ID التاجر لتصفير ديونه.'); }
    if (data === 'admin_list_users' && isAdmin(chatId)) {
        const rows = await dbAll('SELECT * FROM user_access ORDER BY role, chat_id');
        if (!rows.length) return bot.sendMessage(chatId, 'لا يوجد مستخدمون مفعلون.');
        return bot.sendMessage(chatId, rows.map(r => `ID: ${r.chat_id} | النوع: ${r.role} | تحديث الصور: ${Number(r.can_refresh) ? 'مفعل' : 'غير مفعل'} | الاسم: ${r.real_name || '-'}`).join('\n'));
    }
    if (data === 'trader_show_payment') {
        const method = await getSetting('payment_method', '');
        if (!method) return bot.sendMessage(chatId, 'لا توجد طريقة دفع مضافة حالياً.');
        return bot.sendMessage(chatId, `طريقة الدفع الحالية\n${method}`);
    }

    if (!(await hasBotAccess(chatId))) return bot.sendMessage(chatId, '❌ البوت غير مفعل لديك بعد.');

    if (data === 'add_workspace_auto' || data === 'add_workspace_manual') {
        if (await isTrader(chatId)) return bot.sendMessage(chatId, '❌ هذا الخيار غير متاح للتاجر.');
        if (data === 'add_workspace_manual' && !isAdmin(chatId)) return bot.sendMessage(chatId, '❌ هذا الخيار للأدمن فقط.');
        state.mode = data === 'add_workspace_auto' ? 'auto' : 'manual';
        state.step = 'awaiting_credentials';
        return bot.sendMessage(chatId, 'أرسل الإيميل والباسورد ورابط 2FA بأي تنسيق.');
    }

    if (data.startsWith('ws_open_')) {
        const wsId = data.split('_')[2];
        const visible = await getVisibleWorkspaces(chatId);
        if (!visible.find(x => String(x.id) === String(wsId))) return bot.sendMessage(chatId, '❌ لا تملك صلاحية هذه المساحة.');
        state.currentWsId = wsId;
        state.currentTab = 'members';
        const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
        if (!ws) return bot.sendMessage(chatId, '❌ المساحة غير موجودة.');
        let statusMsg = await bot.sendMessage(chatId, `⏳ جاري فتح مساحة: ${ws.name}...`);
        try {
            if (await canUseRefresh(chatId)) await refreshTwoShots(chatId, ws, state);
            else await bot.sendMessage(chatId, `🏢 ${ws.name}`, { reply_markup: { inline_keyboard: getDashboardKeyboard(state, chatId, await getUserAccess(chatId)) } });
            await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
            if (await isTrader(chatId)) await sendTraderDebtCard(chatId);
        } catch (e) { bot.sendMessage(chatId, `❌ خطأ في فتح المساحة: ${e.message}`); }
        return;
    }

    if (data === 'ws_toggle') {
        const wsId = state.currentWsId; if (!wsId) return;
        if (!(await canUseRefresh(chatId))) return bot.sendMessage(chatId, '❌ تحديث الشاشة غير مفعل لديك.');
        let statusMsg = await bot.sendMessage(chatId, '📸 جاري إرسال صورتين معاً...');
        try {
            const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
            await refreshTwoShots(chatId, ws, state);
            await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
        } catch (e) { bot.sendMessage(chatId, `❌ خطأ في التحديث: ${e.message}`); }
        return;
    }

    if (data === 'admin_open_computer') {
        if (!isAdmin(chatId)) return bot.sendMessage(chatId, '❌ هذا الخيار للأدمن فقط.');
        const wsId = state.currentWsId; if (!wsId) return bot.sendMessage(chatId, '❌ افتح مساحة أولاً.');
        state.step = 'manual_idle'; state.codeGen = new PlaywrightCodeGenerator();
        return await sendInteractiveMenu(chatId, '🖥 تم فتح وضع الكمبيوتر للأدمن.');
    }
    if (data === 'int_refresh') {
        if (!isAdmin(chatId)) return;
        const wsId = state.currentWsId; if (!wsId) return;
        const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
        const context = await getContext(wsId, ws.profile_dir); const page = await context.newPage();
        await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 });
        await sleep(1200); const p = path.join(os.tmpdir(), `manual_refresh_${Date.now()}.png`);
        await page.screenshot({ path: p }); await bot.sendPhoto(chatId, p, { caption: '📸 تحديث الشاشة' }); fs.unlinkSync(p); await page.close().catch(() => {}); return;
    }
    if (data === 'int_fetch_2fa') {
        if (!isAdmin(chatId)) return;
        if (!state.url2fa || !state.context) return bot.sendMessage(chatId, '❌ لا يوجد رابط 2FA محفوظ في الجلسة اليدوية.');
        try { const code = await fetch2FACode(state.context, state.url2fa); return bot.sendMessage(chatId, `✅ كود 2FA الحالي: ${code}`); } catch (e) { return bot.sendMessage(chatId, `❌ فشل جلب 2FA: ${e.message}`); }
    }
    if (data === 'int_finish_login') {
        if (!isAdmin(chatId)) return;
        if (!state.context || !state.page || !state.email) return bot.sendMessage(chatId, '❌ لا توجد جلسة يدوية فعالة.');
        try {
            await state.page.goto('https://chatgpt.com/admin/settings', { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(async () => { await state.page.goto('https://chatgpt.com/admin', { waitUntil: 'domcontentloaded', timeout: 45000 }); });
            await sleep(2200);
            if (!(await pageLooksLoggedIn(state.page))) throw new Error('لم يتم الوصول إلى لوحة الإدارة، أكمل تسجيل الدخول أولاً.');
            const wsName = await getWorkspaceName(state.page, state.email);
            const insertedId = await dbRun('INSERT INTO workspaces (chat_id, name, email, password, url2fa, profile_dir) VALUES (?, ?, ?, ?, ?, ?)', [chatId, wsName, state.email, state.password || '', state.url2fa || '', state.profileDir]);
            if (insertedId) { activeContexts[insertedId] = state.context; state.context.on('close', () => { delete activeContexts[insertedId]; }); }
            await state.page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 });
            await sleep(1600); const members = await extractAllEmails(state.page);
            await state.page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 45000 });
            await sleep(1600); const invites = await extractAllEmails(state.page);
            const uniqueOldEmails = [...new Set([...members, ...invites])];
            for (const e of uniqueOldEmails) { const exists = await dbGet('SELECT id FROM allowed_emails WHERE ws_id = ? AND email = ?', [insertedId, normalizeEmail(e)]); if (!exists) await dbRun('INSERT INTO allowed_emails (ws_id, email) VALUES (?, ?)', [insertedId, normalizeEmail(e)]); }
            const p = path.join(os.tmpdir(), `manual_done_${Date.now()}.png`); await state.page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(1200); await state.page.screenshot({ path: p });
            state.currentWsId = insertedId; state.currentTab = 'members';
            await bot.sendPhoto(chatId, p, { caption: `✅ تمت إضافة المساحة بنجاح!\nالمساحة: ${wsName}`, reply_markup: { inline_keyboard: getDashboardKeyboard(state, chatId) } });
            fs.unlinkSync(p); await state.page.close().catch(() => {}); state.page = null; state.context = null; state.step = null;
        } catch (e) { bot.sendMessage(chatId, `❌ فشل الإنهاء اليدوي: ${e.message}`); }
        return;
    }

    if (data === 'ws_sync_whitelist') {
        const wsId = state.currentWsId; if (!wsId) return;
        let statusMsg = await bot.sendMessage(chatId, '⏳ جاري عمل مسح عميق لتوثيق جميع الأعضاء والدعوات الحالية...');
        try {
            const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
            const context = await getContext(ws.id, ws.profile_dir); const page = await context.newPage();
            await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(1500);
            let members = await extractAllEmails(page);
            await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(1500);
            let invites = await extractAllEmails(page);
            const allEmails = [...new Set([...members, ...invites])]; let addedCount = 0;
            for (let e of allEmails) { const exists = await dbGet('SELECT id FROM allowed_emails WHERE ws_id = ? AND email = ?', [ws.id, normalizeEmail(e)]); if (!exists) { await dbRun('INSERT INTO allowed_emails (ws_id, email) VALUES (?, ?)', [ws.id, normalizeEmail(e)]); addedCount++; } }
            await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
            await bot.sendMessage(chatId, `✅ تمت الحماية بنجاح!\nتم فحص وإضافة (${addedCount}) عضو/دعوة جديدة إلى القائمة البيضاء.`);
            await page.close();
        } catch (e) { bot.sendMessage(chatId, `❌ خطأ أثناء التوثيق: ${e.message}`); }
        return;
    }

    if (data === 'ws_back') { state.step = null; state.currentWsId = null; return sendMainMenu(chatId); }
    if (data === 'ws_add_person') { state.step = 'ws_awaiting_add_person'; return bot.sendMessage(chatId, `واجهة اضافة عضو\nارسل الإيميل المطلوب دعوته:`); }
    if (data === 'ws_change_name') { if (await isTrader(chatId)) return; state.step = 'ws_awaiting_change_name'; return bot.sendMessage(chatId, `واجهة تغيير اسم المساحة\nارسل الاسم الجديد للمساحة:`); }
    if (data === 'ws_revoke_invite') { state.step = 'ws_awaiting_revoke_invite'; return bot.sendMessage(chatId, `واجهة إلغاء دعوة\nارسل الإيميل الذي تريد إلغاء دعوته:`); }
    if (data === 'ws_remove_member') { state.step = 'ws_awaiting_remove_member'; return bot.sendMessage(chatId, `واجهة إزالة عضو\nارسل الإيميل الذي تريد إزالته نهائياً:`); }

    if (data === 'ws_fetch_emails') {
        const wsId = state.currentWsId; if (!wsId) return;
        let statusMsg = await bot.sendMessage(chatId, '⏳ جاري الاستخراج العميق للإيميلات...');
        try {
            const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
            const context = await getContext(ws.id, ws.profile_dir); const page = await context.newPage();
            await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded' }); await sleep(1500);
            const members = await extractAllEmails(page);
            await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded' }); await sleep(1500);
            const invites = await extractAllEmails(page);
            await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
            await bot.sendMessage(chatId, `ايميلات الدعوات المعلقة:\n${invites.length ? invites.join('\\n') : '-'}\n\nايميلات المتوفرين حاليا:\n${members.length ? members.join('\\n') : '-'}`);
            await page.close();
        } catch(e) { bot.sendMessage(chatId, `❌ خطأ: ${e.message}`); }
        return;
    }

    if (data === 'ws_delete') {
        if (await isTrader(chatId)) return;
        return bot.sendMessage(chatId, '⚠️ متأكد من تسجيل الخروج وإزالة المساحة؟', { reply_markup: { inline_keyboard: [[{ text: '✅ نعم', callback_data: `confirm_ws_del` }], [{ text: '❌ إلغاء', callback_data: `ws_back` }]] } });
    }
    if (data === 'confirm_ws_del') {
        const wsId = state.currentWsId; const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
        if (ws) {
            if (activeContexts[wsId]) { await activeContexts[wsId].close().catch(() => {}); delete activeContexts[wsId]; }
            try { fs.rmSync(ws.profile_dir, { recursive: true, force: true }); } catch (e) {}
            await dbRun('DELETE FROM workspaces WHERE id = ?', [wsId]); await dbRun('DELETE FROM allowed_emails WHERE ws_id = ?', [wsId]);
            state.currentWsId = null; return sendMainMenu(chatId, '🗑️ تم إزالة المساحة.');
        }
    }
});

// ================= معالجة النصوص والدخول التلقائي والعمليات =================
bot.on('message', async (msg) => {
    const chatId = msg.chat.id.toString();
    const text = msg.text?.trim();
    if (!text || text.startsWith('/')) return;
    if (!sessions[chatId]) sessions[chatId] = { step: null, currentTab: 'members' };
    const state = sessions[chatId];

    if (state.step === 'trader_awaiting_real_name') {
        await dbRun('UPDATE user_access SET real_name = ? WHERE chat_id = ?', [text, chatId]);
        state.step = null;
        return sendMainMenu(chatId, '✅ تم حفظ اسمك الحقيقي.');
    }
    if (isAdmin(chatId) && state.step === 'admin_enable_manager') {
        await dbRun('INSERT OR REPLACE INTO user_access (chat_id, role, can_refresh, trader_price_iqd, approved_by, approved_at) VALUES (?, ?, ?, ?, ?, ?)', [text, 'manager', 0, DEFAULT_TRADER_PRICE, chatId, nowIso()]);
        state.step = null; await bot.sendMessage(text, '✅ تم تفعيل البوت لديك كمدير مساحات. أرسل /start'); return bot.sendMessage(chatId, '✅ تم التفعيل.');
    }
    if (isAdmin(chatId) && state.step === 'admin_enable_trader') {
        await dbRun('INSERT OR REPLACE INTO user_access (chat_id, role, can_refresh, trader_price_iqd, approved_by, approved_at) VALUES (?, ?, ?, ?, ?, ?)', [text, 'trader', 0, DEFAULT_TRADER_PRICE, chatId, nowIso()]);
        sessions[text] = sessions[text] || { step: null, currentTab: 'members' }; sessions[text].step = 'trader_awaiting_real_name';
        await bot.sendMessage(text, '✅ تم تفعيل البوت لديك كتاجر. ارسل اسمك الحقيقي الآن.'); state.step = null; return bot.sendMessage(chatId, '✅ تم التفعيل.');
    }
    if (isAdmin(chatId) && state.step === 'admin_remove_access') { await dbRun('DELETE FROM user_access WHERE chat_id = ?', [text]); state.step = null; return bot.sendMessage(chatId, '✅ تم إزالة الصلاحية.'); }
    if (isAdmin(chatId) && state.step === 'admin_set_payment') { await setSetting('payment_method', /^skip$/i.test(text) ? '' : text); state.step = null; return bot.sendMessage(chatId, '✅ تم تحديث طريقة الدفع.'); }
    if (isAdmin(chatId) && state.step === 'admin_set_trader_price') { const val = Number(text.replace(/[^\d]/g, '')); if (!val) return bot.sendMessage(chatId, 'ارسل رقم صحيح.'); await setSetting('trader_price_iqd', val); state.step = null; return bot.sendMessage(chatId, `✅ تم تحديث السعر إلى ${val} د.ع`); }
    if (isAdmin(chatId) && state.step === 'admin_clear_trader_debt') { await dbRun(`UPDATE member_additions SET settled = 1 WHERE actor_chat_id = ? AND actor_role = 'trader'`, [text]); state.step = null; await bot.sendMessage(text, '✅ تم تصفير ديونك.'); return bot.sendMessage(chatId, '✅ تم تصفير الديون.'); }

    if (state.step === 'awaiting_credentials') {
        const parsed = extractCredentialsSmart(text);
        if (!parsed.email || !parsed.password) return bot.sendMessage(chatId, '⚠️ لم أستطع فهم الإيميل والباسورد. أرسل البيانات بشكل أوضح.');
        if (state.mode === 'auto' && !parsed.url2fa) return bot.sendMessage(chatId, '⚠️ في الوضع التلقائي يجب وجود رابط 2FA.');
        state.email = parsed.email; state.password = parsed.password; state.url2fa = parsed.url2fa || ''; state.step = 'processing';
        try {
            if (state.context && !Object.values(activeContexts).includes(state.context)) await state.context.close().catch(() => {});
            state.context = null;
            const profileDir = path.join(DATA_DIR, `ws_profile_${Date.now()}`); fs.mkdirSync(profileDir, { recursive: true });
            const context = await chromium.launchPersistentContext(profileDir, { headless: true, args: ['--no-sandbox','--disable-setuid-sandbox','--disable-blink-features=AutomationControlled','--disable-dev-shm-usage'], userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36', viewport: { width: 1366, height: 768 } });
            state.context = context; state.profileDir = profileDir; state.page = await context.newPage(); const pages = context.pages(); if (pages.length > 1) await pages[0].close().catch(() => {});
            if (state.mode === 'manual') { state.step = 'manual_idle'; return sendInteractiveMenu(chatId, '✅ تم فتح الجلسة اليدوية.'); }
            let statusMsg = await bot.sendMessage(chatId, `⏳ جاري تنفيذ مسار الدخول بسرعة ${CURRENT_SPEED_PERCENT}% ...`);
            const updateStatus = async (t) => { try { await bot.editMessageText(`⚡ ${t}`, { chat_id: chatId, message_id: statusMsg.message_id }); } catch (e) {} };
            try {
                await updateStatus('فتح صفحة الدخول...');
                await completeLoginFlow(state.page, state, chatId);
                await updateStatus('جاري حفظ المساحة...');
                const wsName = await getWorkspaceName(state.page, state.email);
                const insertedId = await dbRun('INSERT INTO workspaces (chat_id, name, email, password, url2fa, profile_dir) VALUES (?, ?, ?, ?, ?, ?)', [chatId, wsName, state.email, state.password, state.url2fa, state.profileDir]);
                if (insertedId) { activeContexts[insertedId] = context; context.on('close', () => { delete activeContexts[insertedId]; }); }
                await state.page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(1600); let members = await extractAllEmails(state.page);
                await state.page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(1600); let invites = await extractAllEmails(state.page);
                let uniqueOldEmails = [...new Set([...members, ...invites])];
                for (let e of uniqueOldEmails) { const exists = await dbGet('SELECT id FROM allowed_emails WHERE ws_id = ? AND email = ?', [insertedId, normalizeEmail(e)]); if (!exists) await dbRun('INSERT INTO allowed_emails (ws_id, email) VALUES (?, ?)', [insertedId, normalizeEmail(e)]); }
                await state.page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(1200);
                await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
                const p = path.join(os.tmpdir(), `admin_${Date.now()}.png`); await state.page.screenshot({ path: p });
                state.currentWsId = insertedId; state.currentTab = 'members';
                await bot.sendPhoto(chatId, p, { caption: `✅ تمت إضافة المساحة بنجاح!
🛡️ تم توثيق وحماية (${uniqueOldEmails.length}) عضو شرعي.

المساحة: ${wsName}
القسم الحالي: 👥 الأعضاء النشطين`, reply_markup: { inline_keyboard: getDashboardKeyboard(state, chatId, await getUserAccess(chatId)) }});
                fs.unlinkSync(p); await state.page.close().catch(() => {}); state.page = null; state.context = null; state.step = null;
            } catch (autoError) { await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {}); bot.sendMessage(chatId, `⚠️ فشل الدخول التلقائي: ${autoError.message}`); state.context = null; }
        } catch (error) { bot.sendMessage(chatId, `❌ خطأ فادح: ${error.message}`); }
        return;
    }

    if (state.currentWsId && state.step) {
        const textInput = text.trim(); const wsId = state.currentWsId;
        if (state.step === 'ws_awaiting_add_person') {
            state.step = 'processing'; let statusMsg = await bot.sendMessage(chatId, '⏳ جاري البحث عن مساحة مناسبة ثم الإضافة...');
            try {
                if (await isTrader(chatId) && await traderDebtBlocked(chatId)) { state.step = null; await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {}); return bot.sendMessage(chatId, '❌ لا يمكنك إضافة المزيد حتى سداد الدين.'); }
                const ownerChatId = await getOwnerChatForAction(chatId);
                const actorRole = isAdmin(chatId) ? 'admin' : ((await isTrader(chatId)) ? 'trader' : 'manager');
                const result = await addEmailIntoAvailableWorkspace(ownerChatId, textInput, { actorChatId: chatId, actorRole });
                await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
                if (result.ok) {
                    bot.sendMessage(chatId, `✅ تمت إضافة هذا الإيميل: ${result.email}
🏢 في هذه المساحة: ${result.workspaceName}`);
                    if (await isTrader(chatId)) await sendTraderDebtCard(chatId);
                } else {
                    bot.sendMessage(chatId, '❌ لم أجد مساحة متاحة أقل من 6 أعضاء أو أن الإيميل موجود مسبقًا في كل المساحات.');
                }
            } catch (error) { bot.sendMessage(chatId, `❌ خطأ: ${error.message}`); }
            state.step = null; return;
        }
        if (state.step === 'ws_awaiting_change_name') {
            if (await isTrader(chatId)) { state.step = null; return bot.sendMessage(chatId, '❌ هذا الخيار غير متاح للتاجر.'); }
            state.step = 'processing'; let statusMsg = await bot.sendMessage(chatId, '⏳ جاري تغيير الاسم...');
            try {
                const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]); const context = await getContext(wsId, ws.profile_dir); const page = await context.newPage();
                await page.goto('https://chatgpt.com/admin/settings', { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(async () => { await page.goto('https://chatgpt.com/admin', { waitUntil: 'domcontentloaded', timeout: 45000 }); }); await sleep(2200);
                const inputCoords = await page.evaluate(() => { const inputs = Array.from(document.querySelectorAll('input[type="text"], input[name="name"], input[name="workspace_name"]')).filter(el => { const rect = el.getBoundingClientRect(); const ph = String(el.placeholder || '').toLowerCase(); return rect.width > 0 && rect.height > 0 && !ph.includes('search'); }); if (inputs.length > 0) { const rect = inputs[0].getBoundingClientRect(); return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 }; } return null; });
                if (!inputCoords) throw new Error('لم أتمكن من العثور على حقل الاسم في الشاشة.');
                await page.mouse.click(inputCoords.x, inputCoords.y, { clickCount: 3 }); await page.keyboard.press('Backspace'); await page.keyboard.type(textInput, { delay: typeDelay(45) }); await sleep(400); await page.keyboard.press('Enter').catch(() => {}); await sleep(1200); await dbRun('UPDATE workspaces SET name = ? WHERE id = ?', [textInput, wsId]); await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {}); await bot.sendMessage(chatId, '✅ تم وضع الاسم الجديد وحفظه.'); await page.close();
            } catch (error) { await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {}); bot.sendMessage(chatId, `❌ خطأ: ${error.message}`); }
            state.step = null; return;
        }
        if (state.step === 'ws_awaiting_revoke_invite') {
            state.step = 'processing'; let statusMsg = await bot.sendMessage(chatId, '⏳ جاري إلغاء الدعوة...');
            try {
                if (await isTrader(chatId)) { const own = await canTraderManageEmail(chatId, textInput); if (!own) { state.step = null; await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {}); return bot.sendMessage(chatId, '❌ لا يمكنك إلغاء دعوة لم تضفها أنت.'); } }
                const ownerChatId = await getOwnerChatForAction(chatId); const visible = await dbAll('SELECT * FROM workspaces WHERE chat_id = ?', [ownerChatId]); let ok = false;
                for (const ws of visible) { const context = await getContext(ws.id, ws.profile_dir); const page = await context.newPage(); await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(1500); if (await dynamicGeometryAction(page, textInput, 'revoke')) { ok = true; await dbRun('DELETE FROM allowed_emails WHERE ws_id = ? AND email = ?', [ws.id, normalizeEmail(textInput)]); await page.close().catch(() => {}); break; } await page.close().catch(() => {}); }
                await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {}); if (ok) { if (await isTrader(chatId)) await markTraderEmailStatus(chatId, textInput, 'revoked'); bot.sendMessage(chatId, `✅ تم إلغاء الدعوة بنجاح لـ: ${textInput}`); } else bot.sendMessage(chatId, '❌ لم يتم العثور على الإيميل في الدعوات المعلقة أو فشل الإجراء.');
            } catch (error) { bot.sendMessage(chatId, `❌ خطأ: ${error.message}`); }
            state.step = null; return;
        }
        if (state.step === 'ws_awaiting_remove_member') {
            state.step = 'processing'; let statusMsg = await bot.sendMessage(chatId, '⏳ جاري إزالة العضو...');
            try {
                if (await isTrader(chatId)) { const own = await canTraderManageEmail(chatId, textInput); if (!own) { state.step = null; await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {}); return bot.sendMessage(chatId, '❌ لا يمكنك إزالة عضو لم تضفه أنت.'); } }
                const ownerChatId = await getOwnerChatForAction(chatId); const visible = await dbAll('SELECT * FROM workspaces WHERE chat_id = ?', [ownerChatId]); let ok = false;
                for (const ws of visible) { const context = await getContext(ws.id, ws.profile_dir); const page = await context.newPage(); await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(1500); if (await dynamicGeometryAction(page, textInput, 'remove')) { ok = true; await dbRun('DELETE FROM allowed_emails WHERE ws_id = ? AND email = ?', [ws.id, normalizeEmail(textInput)]); await page.close().catch(() => {}); break; } await page.close().catch(() => {}); }
                await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {}); if (ok) { if (await isTrader(chatId)) await markTraderEmailStatus(chatId, textInput, 'removed'); bot.sendMessage(chatId, `✅ تمت إزالة العضو نهائياً: ${textInput}`); } else bot.sendMessage(chatId, '❌ لم يتم العثور على الإيميل في قائمة الأعضاء النشطين أو فشل الإجراء.');
            } catch (error) { bot.sendMessage(chatId, `❌ خطأ: ${error.message}`); }
            state.step = null; return;
        }
    }
});
// =========================================================================
// 🛡️ الحارس الليلي (المنطق الهندسي الفيزيائي العام)
// =========================================================================
let currentWatcherIndex = 0;

setInterval(async () => {
    try {
        const workspaces = await dbAll('SELECT * FROM workspaces');
        if (workspaces.length === 0) return;

        if (currentWatcherIndex >= workspaces.length) currentWatcherIndex = 0;
        const ws = workspaces[currentWatcherIndex];
        currentWatcherIndex++;

        if (!fs.existsSync(ws.profile_dir)) return;

        const allowedRows = await dbAll('SELECT email FROM allowed_emails WHERE ws_id = ?', [ws.id]);
        const context = await getContext(ws.id, ws.profile_dir);
        const page = await context.newPage();
        await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
        await sleep(1200);
        if (!(await pageLooksLoggedIn(page))) {
            const moved = await relocateRecentMembersFromWorkspace(ws).catch(() => []);
            const targets = new Set([ADMIN_ID, String(ws.chat_id), ...moved.map(m => String(m.actorChatId))]);
            const movedText = moved.length ? `
تمت اضافة الاعضاء الجدد في مساحات: ${[...new Set(moved.map(m => m.workspaceName))].join(' ، ')}` : '';
            for (const target of targets) await bot.sendMessage(target, `🚨 تمت تعطيل المساحة: ${ws.name}${movedText}`).catch(() => {});
            await page.close().catch(() => {});
            return;
        }

        if (allowedRows.length === 0) {
            await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 });
            await sleep(2500);
            let members = await extractAllEmails(page);
            await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 45000 });
            await sleep(2500);
            let invites = await extractAllEmails(page);
            let all = [...new Set([...members, ...invites])];
            for (let e of all) await dbRun('INSERT INTO allowed_emails (ws_id, email) VALUES (?, ?)', [ws.id, normalizeEmail(e)]);
            await page.close().catch(() => {});
            return;
        }

        const allowedEmails = new Set(allowedRows.map(r => normalizeEmail(r.email)));
        allowedEmails.add(normalizeEmail(ws.email));

        await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 });
        await sleep(2500);
        let foundEmails = await extractAllEmails(page);
        for (const email of foundEmails) {
            const norm = normalizeEmail(email);
            if (!allowedEmails.has(norm)) {
                if (await dynamicGeometryAction(page, norm, 'remove')) {
                    bot.sendMessage(ws.chat_id, `🚨 نظام الحماية (الحارس):\nتم طرد إيميل دخيل أضيف من خارج البوت!\nالإيميل: ${norm}\nالمساحة: (${ws.name})`);
                }
            }
        }

        await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 45000 });
        await sleep(2500);
        let pendingEmails = await extractAllEmails(page);
        for (const email of pendingEmails) {
            const norm = normalizeEmail(email);
            if (!allowedEmails.has(norm)) {
                if (await dynamicGeometryAction(page, norm, 'revoke')) {
                    bot.sendMessage(ws.chat_id, `🚨 نظام الحماية (الحارس):\nتم إلغاء دعوة غريبة أُرسلت من خارج البوت!\nالإيميل: ${norm}\nالمساحة: (${ws.name})`);
                }
            }
        }
        await page.close().catch(() => {});
    } catch (e) {}
}, WATCH_INTERVAL_MS);

process.on('uncaughtException', () => {});
process.on('unhandledRejection', () => {});
