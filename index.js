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
const { HumanizedComputer, ScriptRecorder } = require('./ands');

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
const WATCH_INTERVAL_MS = Number(process.env.WATCH_INTERVAL_MS || 10000);

function resolveDataDir() {
    const candidates = [
        process.env.RAILWAY_VOLUME_MOUNT_PATH,
        '/data',
        path.join(process.cwd(), 'data'),
        path.join(__dirname, 'data')
    ].filter(Boolean);
    for (const dir of candidates) {
        try {
            fs.mkdirSync(dir, { recursive: true });
            fs.accessSync(dir, fs.constants.W_OK);
            return dir;
        } catch (e) {}
    }
    const fallback = path.join(__dirname, 'data');
    fs.mkdirSync(fallback, { recursive: true });
    return fallback;
}

const DATA_DIR = resolveDataDir();

function resolveDbPath() {
    const preferred = path.join(DATA_DIR, 'workspace_bot.db');
    if (fs.existsSync(preferred)) return preferred;
    const oldNames = ['workspace_bot.db', 'workspaces_v12.db', 'workspaces_v13.db'];
    for (const name of oldNames) {
        const p = path.join(DATA_DIR, name);
        if (fs.existsSync(p)) {
            if (p !== preferred) {
                try { fs.copyFileSync(p, preferred); return preferred; } catch (e) { return p; }
            }
            return p;
        }
    }
    return preferred;
}

const bot = new TelegramBot(BOT_TOKEN, { polling: true });
const sessions = {};
const activeContexts = {};
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, scaledMs(ms)));

const dbPath = resolveDbPath();
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
    db.run(`CREATE TABLE IF NOT EXISTS payment_receipts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trader_chat_id TEXT,
        trader_name TEXT DEFAULT '',
        amount_iqd INTEGER DEFAULT 0,
        method TEXT DEFAULT '',
        status TEXT DEFAULT 'pending',
        created_at TEXT,
        photo_file_id TEXT DEFAULT ''
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
    db.run(`CREATE TABLE IF NOT EXISTS workspace_stats (
        ws_id INTEGER PRIMARY KEY,
        member_count INTEGER DEFAULT 0,
        invite_count INTEGER DEFAULT 0,
        last_known_emails TEXT DEFAULT '',
        last_known_invites TEXT DEFAULT '',
        last_synced_at TEXT DEFAULT '',
        status TEXT DEFAULT 'ok'
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS trader_seat_reservations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trader_chat_id TEXT,
        workspace_id INTEGER,
        email TEXT,
        expires_at TEXT,
        created_at TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS customer_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_chat_id TEXT,
        customer_name TEXT DEFAULT '',
        username TEXT DEFAULT '',
        order_type TEXT DEFAULT 'normal',
        email TEXT,
        amount_iqd INTEGER DEFAULT 0,
        method TEXT DEFAULT '',
        status TEXT DEFAULT 'awaiting_payment',
        created_at TEXT,
        expires_at TEXT DEFAULT '',
        next_reminder_at TEXT DEFAULT '',
        reminder_count INTEGER DEFAULT 0,
        photo_file_id TEXT DEFAULT '',
        workspace_id INTEGER
    )`);
});

const dbRun = (query, params = []) => new Promise((resolve, reject) => {
    db.run(query, params, function(err) { err ? reject(err) : resolve(this.lastID); });
});
const dbGet = (query, params = []) => new Promise((resolve, reject) => db.get(query, params, (err, row) => err ? reject(err) : resolve(row)));
const dbAll = (query, params = []) => new Promise((resolve, reject) => db.all(query, params, (err, rows) => err ? reject(err) : resolve(rows)));


async function runMigrations() {
    try {
        const cols = await dbAll("PRAGMA table_info(workspaces)");
        const names = new Set(cols.map(c => c.name));
        if (!names.has('owner_chat_id')) await dbRun('ALTER TABLE workspaces ADD COLUMN owner_chat_id TEXT');
        if (!names.has('created_by_chat_id')) await dbRun('ALTER TABLE workspaces ADD COLUMN created_by_chat_id TEXT');
    } catch (e) {}
}
setTimeout(() => { runMigrations().catch(() => {}); }, 0);

async function cleanupExpiredReservations() {
    try {
        await dbRun('DELETE FROM trader_seat_reservations WHERE expires_at != "" AND expires_at <= ?', [nowIso()]);
    } catch (e) {}
}

async function getWorkspaceStats(wsId) {
    let row = await dbGet('SELECT * FROM workspace_stats WHERE ws_id = ?', [wsId]);
    if (!row) {
        await dbRun('INSERT OR REPLACE INTO workspace_stats (ws_id, member_count, invite_count, last_synced_at, status) VALUES (?, ?, ?, ?, ?)', [wsId, 0, 0, '', 'unknown']);
        row = await dbGet('SELECT * FROM workspace_stats WHERE ws_id = ?', [wsId]);
    }
    return row;
}

async function setWorkspaceStats(wsId, payload = {}) {
    const current = await getWorkspaceStats(wsId);
    const next = {
        member_count: payload.member_count != null ? payload.member_count : Number(current.member_count || 0),
        invite_count: payload.invite_count != null ? payload.invite_count : Number(current.invite_count || 0),
        last_known_emails: payload.last_known_emails != null ? payload.last_known_emails : String(current.last_known_emails || ''),
        last_known_invites: payload.last_known_invites != null ? payload.last_known_invites : String(current.last_known_invites || ''),
        last_synced_at: payload.last_synced_at != null ? payload.last_synced_at : nowIso(),
        status: payload.status != null ? payload.status : String(current.status || 'ok')
    };
    await dbRun('INSERT OR REPLACE INTO workspace_stats (ws_id, member_count, invite_count, last_known_emails, last_known_invites, last_synced_at, status) VALUES (?, ?, ?, ?, ?, ?, ?)', [wsId, next.member_count, next.invite_count, next.last_known_emails, next.last_known_invites, next.last_synced_at, next.status]);
}

async function syncWorkspaceStatsFromPage(wsId, members, invites, status = 'ok') {
    const memberList = [...new Set((members || []).map(normalizeEmail).filter(Boolean))];
    const inviteList = [...new Set((invites || []).map(normalizeEmail).filter(Boolean))];
    await setWorkspaceStats(wsId, {
        member_count: memberList.length,
        invite_count: inviteList.length,
        last_known_emails: memberList.join('\n'),
        last_known_invites: inviteList.join('\n'),
        last_synced_at: nowIso(),
        status
    });
}

async function reserveTraderSeat(traderChatId, workspaceId, email, hours = 40) {
    const expires = new Date(Date.now() + hours * 60 * 60 * 1000).toISOString();
    await dbRun('INSERT INTO trader_seat_reservations (trader_chat_id, workspace_id, email, expires_at, created_at) VALUES (?, ?, ?, ?, ?)', [String(traderChatId), workspaceId, normalizeEmail(email), expires, nowIso()]);
}

async function getReservedSeatsForWorkspace(workspaceId) {
    await cleanupExpiredReservations();
    const rows = await dbAll('SELECT * FROM trader_seat_reservations WHERE workspace_id = ? AND expires_at > ?', [workspaceId, nowIso()]);
    return rows;
}

async function getReservedSeatsByTrader(traderChatId) {
    await cleanupExpiredReservations();
    return await dbAll('SELECT * FROM trader_seat_reservations WHERE trader_chat_id = ? AND expires_at > ? ORDER BY id DESC', [String(traderChatId), nowIso()]);
}

async function getFastCandidateWorkspaces(actorChatId, actorRole, excludeWorkspaceId = null) {
    const list = await getCandidateWorkspacesForActor(actorChatId, actorRole, excludeWorkspaceId);
    const reservations = actorRole === 'trader' ? await getReservedSeatsByTrader(actorChatId) : [];
    const reservedWorkspaceIds = new Set(reservations.map(r => String(r.workspace_id)));
    const prepared = [];
    for (const ws of list) {
        const stats = await getWorkspaceStats(ws.id);
        const activeReservations = await getReservedSeatsForWorkspace(ws.id);
        const reservedByOthers = actorRole === 'trader'
            ? activeReservations.filter(r => String(r.trader_chat_id) !== String(actorChatId)).length
            : activeReservations.length;
        const memberCount = Number(stats.member_count || 0);
        const inviteCount = Number(stats.invite_count || 0);
        const used = memberCount + inviteCount + reservedByOthers;
        prepared.push({
            ws,
            stats,
            reservedByOthers,
            used,
            remaining: Math.max(0, MEMBER_LIMIT - used),
            reservedForTrader: reservedWorkspaceIds.has(String(ws.id))
        });
    }
    prepared.sort((a, b) => {
        if (a.reservedForTrader && !b.reservedForTrader) return -1;
        if (!a.reservedForTrader && b.reservedForTrader) return 1;
        return a.used - b.used;
    });
    return prepared;
}

async function getTraderAddedEmails(chatId) {
    return await dbAll("SELECT email, workspace_id, status, added_at FROM member_additions WHERE actor_chat_id = ? AND actor_role = 'trader' ORDER BY id DESC", [String(chatId)]);
}

async function canInviteUsingWorkspace(wsId, actorRole, actorChatId) {
    const stats = await getWorkspaceStats(wsId);
    const reservations = await getReservedSeatsForWorkspace(wsId);
    const reservedByOthers = actorRole === 'trader'
        ? reservations.filter(r => String(r.trader_chat_id) !== String(actorChatId)).length
        : reservations.length;
    const used = Number(stats.member_count || 0) + Number(stats.invite_count || 0) + reservedByOthers;
    return used < MEMBER_LIMIT;
}


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
        const pay = await dbGet('SELECT value FROM settings WHERE key = ?', ['payment_methods']);
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
    return !!row && String(row.role || '').startsWith('manager');
}

async function isManagerOwn(chatId) {
    const row = await getUserAccess(chatId);
    return !!row && row.role === 'manager_own';
}

async function isManagerFull(chatId) {
    const row = await getUserAccess(chatId);
    return !!row && row.role === 'manager_full';
}

async function canUseRefresh(chatId) {
    if (isAdmin(chatId)) return true;
    const row = await getUserAccess(chatId);
    return !!row && Number(row.can_refresh || 0) === 1;
}

async function getVisibleWorkspaces(chatId) {
    if (isAdmin(chatId)) return await dbAll('SELECT id, name FROM workspaces ORDER BY id DESC');
    const row = await getUserAccess(chatId);
    if (!row) return [];
    if (row.role === 'trader') return await dbAll('SELECT id, name FROM workspaces WHERE chat_id = ? ORDER BY id DESC', [ADMIN_ID]);
    if (row.role === 'manager_full') return await dbAll('SELECT id, name FROM workspaces ORDER BY id DESC');
    return await dbAll('SELECT id, name FROM workspaces WHERE chat_id = ? ORDER BY id DESC', [String(chatId)]);
}

async function sendMainMenu(chatId, headerText) {
    const accessRow = await getUserAccess(chatId);
    if (!(await hasBotAccess(chatId))) {
        return await bot.sendMessage(chatId, `${headerText || '♕مرحبا ببوت مساحة العمل♕'}

اختر الخدمة المناسبة:`, {
            reply_markup: { inline_keyboard: [
                [{ text: '🛒 شراء دعوة', callback_data: 'customer_buy_normal' }],
                [{ text: '⚡ شراء دعوة مستعجل', callback_data: 'customer_buy_urgent' }],
                [{ text: '🧾 طلب صلاحية تاجر/مدير', callback_data: 'request_access_menu' }]
            ] }
        });
    }

    if (accessRow && accessRow.role === 'trader') {
        const debtCountRow = await dbGet(`SELECT COUNT(*) AS c FROM member_additions WHERE actor_chat_id = ? AND actor_role = 'trader' AND settled = 0 AND status IN ('pending','member','migrated')`, [String(chatId)]);
        const count = Number((debtCountRow && debtCountRow.c) || 0);
        const price = Number((accessRow && accessRow.trader_price_iqd) || DEFAULT_TRADER_PRICE || 0);
        const lines = [
            headerText || `مرحبا يا أيها التاجر`,
            'يمكنك اضافة عضو وازالة عضو والغاء دعوة'
        ];
        if (count >= 7) {
            lines.push(`لقد اضفت ${count} دعوات`);
            lines.push(`المبلغ المراد دفعه هو: ${count * price} د.ع`);
        }
        return await bot.sendMessage(chatId, lines.join('\\n'), {
            reply_markup: { inline_keyboard: [
                [{ text: 'اضافة عضو', callback_data: 'ws_add_person' }],
                [{ text: 'إزالة عضو', callback_data: 'ws_remove_member' }, { text: 'إلغاء دعوة', callback_data: 'ws_revoke_invite' }],
                [{ text: 'ملاحظة', callback_data: 'trader_note' }],
                ...(count >= 7 ? [[{ text: '💳 تسديد الديون', callback_data: 'trader_show_payment' }]] : [])
            ] }
        });
    }

    const text = headerText || 'مرحبا ببوت ادارة مساحات العمل';
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
    const methodsRaw = await getSetting('payment_methods', '');
    const methods = methodsRaw.split('\n').map(v => v.trim()).filter(Boolean);
    const price = Number((row && row.trader_price_iqd) || DEFAULT_TRADER_PRICE || 0);
    const debtCountRow = await dbGet(`SELECT COUNT(*) AS c FROM member_additions WHERE actor_chat_id = ? AND actor_role = 'trader' AND settled = 0 AND status IN ('pending','member','migrated')`, [String(chatId)]);
    const count = Number((debtCountRow && debtCountRow.c) || 0);
    if (count < 7) return null;
    const total = count * price;
    const buttons = methods.length ? methods.map((m, i) => ([{ text: `طريقة ${i+1}`, callback_data: `trader_pick_method_${i}` }])) : [];
    buttons.push([{ text: '💳 تسديد الديون', callback_data: 'trader_show_payment' }]);
    return await bot.sendMessage(chatId, `لقد اضفت ${count} دعوات
المبلغ المراد دفعه هو: ${total} د.ع`, {
        reply_markup: { inline_keyboard: buttons }
    });
}

async function getOwnerChatForAction(chatId) {
    if (isAdmin(chatId)) return ADMIN_ID;
    const row = await getUserAccess(chatId);
    if (!row) return String(chatId);
    if (row.role === 'trader') return ADMIN_ID;
    if (row.role === 'manager_full') return ADMIN_ID;
    return String(chatId);
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
        const result = await addEmailIntoAvailableWorkspace(row.actor_chat_id, row.email, { actorChatId: row.actor_chat_id, actorRole: row.actor_role, excludeWorkspaceId: ws.id, silentLog: false });
        if (result && result.ok) {
            await dbRun('UPDATE member_additions SET status = ?, migrated_to_workspace_id = ? WHERE id = ?', ['migrated', result.workspaceId, row.id]);
            moved.push({ email: row.email, workspaceName: result.workspaceName, actorChatId: row.actor_chat_id });
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


async function getActorRoleValue(chatId) {
    if (isAdmin(chatId)) return 'admin';
    const row = await getUserAccess(chatId);
    return row ? row.role : 'manager_own';
}

async function getCandidateWorkspacesForActor(actorChatId, actorRole, excludeWorkspaceId = null) {
    let rows = [];
    if (actorRole === 'admin' || actorRole === 'manager_full') rows = await dbAll('SELECT * FROM workspaces ORDER BY id DESC');
    else if (actorRole === 'trader') rows = await dbAll('SELECT * FROM workspaces WHERE chat_id = ? ORDER BY id DESC', [ADMIN_ID]);
    else rows = await dbAll('SELECT * FROM workspaces WHERE chat_id = ? ORDER BY id DESC', [String(actorChatId)]);
    if (excludeWorkspaceId) rows = rows.filter(r => String(r.id) !== String(excludeWorkspaceId));
    return rows;
}

async function emailExistsInWorkspace(page, email) {
    const all = await extractAllEmails(page);
    return all.map(normalizeEmail).includes(normalizeEmail(email));
}

async function inviteEmailOnPage(page, email) {
    const inviteBtn = page.locator('button:has-text("Invite member"), button:has-text("Invite members")').first();
    if (!(await inviteBtn.isVisible({ timeout: 6000 }).catch(() => false))) throw new Error('زر الدعوة غير ظاهر');
    await inviteBtn.click({ force: true });
    await sleep(900);
    const input = page.locator('input[type="email"], textarea[placeholder*="email" i], input[placeholder*="email" i]').first();
    if (!(await input.isVisible({ timeout: 5000 }).catch(() => false))) throw new Error('حقل الإيميل غير ظاهر');
    await input.click({ force: true });
    await page.keyboard.type(email, { delay: typeDelay(30) });
    await sleep(350);
    const sendBtn = page.locator('button:has-text("Send invites"), button:has-text("Send invite")').first();
    if (!(await sendBtn.isVisible({ timeout: 5000 }).catch(() => false))) throw new Error('زر الإرسال غير ظاهر');
    await sendBtn.click({ force: true });
    await sleep(1600);
}

async function addEmailIntoAvailableWorkspace(actorChatId, email, opts = {}) {
    await cleanupExpiredReservations();
    const actorRole = opts.actorRole || await getActorRoleValue(actorChatId);
    const actorChatIdNormalized = String(opts.actorChatId || actorChatId);
    const norm = normalizeEmail(email);
    const prepared = await getFastCandidateWorkspaces(actorChatIdNormalized, actorRole, opts.excludeWorkspaceId || null);

    const filtered = [];
    for (const item of prepared) {
        const ws = item.ws;
        const stats = item.stats || await getWorkspaceStats(ws.id);
        const knownMembers = String(stats.last_known_emails || '').split('\n').map(normalizeEmail).filter(Boolean);
        const knownInvites = String(stats.last_known_invites || '').split('\n').map(normalizeEmail).filter(Boolean);
        const dbKnown = new Set([...knownMembers, ...knownInvites]);

        if (dbKnown.has(norm)) continue;
        if (item.used >= MEMBER_LIMIT) continue;
        if (!fs.existsSync(ws.profile_dir)) continue;

        filtered.push({
            ...item,
            stats,
            knownMembers,
            knownInvites,
            score: item.reservedForTrader ? -1000 : item.used
        });
    }

    filtered.sort((a, b) => a.score - b.score || a.ws.id - b.ws.id);

    for (const item of filtered) {
        const ws = item.ws;
        const context = await getContext(ws.id, ws.profile_dir);
        const page = await context.newPage();
        try {
            await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 });
            await sleep(900);
            await inviteEmailOnPage(page, norm);

            const exists = await dbGet('SELECT id FROM allowed_emails WHERE ws_id = ? AND email = ?', [ws.id, norm]);
            if (!exists) await dbRun('INSERT INTO allowed_emails (ws_id, email) VALUES (?, ?)', [ws.id, norm]);
            if (!opts.silentLog) await logMemberAddition(actorChatIdNormalized, actorRole, ws.id, norm, 'pending');

            const newInvites = [...new Set([...(item.knownInvites || []), norm])];
            await setWorkspaceStats(ws.id, {
                member_count: item.knownMembers.length,
                invite_count: newInvites.length,
                last_known_emails: item.knownMembers.join('\n'),
                last_known_invites: newInvites.join('\n'),
                last_synced_at: nowIso(),
                status: 'ok'
            });

            if (actorRole === 'trader') {
                await dbRun('DELETE FROM trader_seat_reservations WHERE trader_chat_id = ? AND workspace_id = ? AND expires_at > ?', [actorChatIdNormalized, ws.id, nowIso()]).catch(() => {});
            }

            await page.close().catch(() => {});
            return {
                ok: true,
                email: norm,
                workspaceId: ws.id,
                workspaceName: ws.name,
                remaining: Math.max(0, MEMBER_LIMIT - (item.knownMembers.length + newInvites.length + item.reservedByOthers))
            };
        } catch (e) {
            try {
                await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 });
                await sleep(800);
                const freshMembers = await extractAllEmails(page);
                await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 45000 });
                await sleep(800);
                const freshInvites = await extractAllEmails(page);
                await syncWorkspaceStatsFromPage(ws.id, freshMembers, freshInvites, 'desynced_recovered');
            } catch (syncErr) {
                await setWorkspaceStats(ws.id, { status: 'login_failed', last_synced_at: nowIso() });
            }
            await page.close().catch(() => {});
        }
    }
    return { ok: false, email: norm };
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
    const role = isAdmin(chatId) ? 'admin' : (accessRow ? accessRow.role : 'manager_own');
    const isTraderRole = role === 'trader';
    const isOwnManager = role === 'manager_own';
    if (isAdmin(chatId) || Number((accessRow && accessRow.can_refresh) || 0) === 1) {
        keyboard.push([{ text: '🔁 تحديث الشاشة (صورتين)', callback_data: 'ws_toggle' }]);
    }
    if (isAdmin(chatId)) {
        keyboard.push([{ text: '🛡️ توثيق الأعضاء (حماية من الحارس)', callback_data: 'ws_sync_whitelist' }]);
        keyboard.push([{ text: '🖥 وضع الكمبيوتر / التسجيل اليدوي', callback_data: 'admin_open_computer' }]);
    }
    if (isTraderRole && false) {}
    keyboard.push([{ text: 'اضافة عضو', callback_data: 'ws_add_person' }]);
    keyboard.push([{ text: 'إزالة عضو', callback_data: 'ws_remove_member' }, { text: 'إلغاء دعوة', callback_data: 'ws_revoke_invite' }]);
    if (!isTraderRole) {
        keyboard.push([{ text: 'جلب الإيميلات', callback_data: 'ws_fetch_emails' }]);
        keyboard.push([{ text: 'تغيير اسم المساحة', callback_data: 'ws_change_name' }]);
        if (!isOwnManager) keyboard.push([{ text: '❌ إزالة المساحة (تسجيل الخروج)', callback_data: 'ws_delete' }]);
    }
    if (isTraderRole) keyboard.unshift([{ text: '💳 تسديد الديون', callback_data: 'trader_show_payment' }]);
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
        [{ text: '💳 اضافة طريقة دفع', callback_data: 'admin_add_payment_method' }, { text: '📋 عرض طرق الدفع', callback_data: 'admin_list_payment_methods' }],
        [{ text: '📧 جلب ايميلات التجار', callback_data: 'admin_get_trader_emails' }],
        [{ text: '💰 تحديد سعر التاجر', callback_data: 'admin_set_trader_price' }],
        [{ text: '💸 تصفير دين تاجر', callback_data: 'admin_clear_trader_debt' }],
        [{ text: '🔙 رجوع', callback_data: 'ws_back' }]
    ]};
}

function getSpeedKeyboard() {
    return { inline_keyboard: [[{ text: '🔙 رجوع للاعدادات', callback_data: 'admin_settings' }]] };
}

bot.on('callback_query', async (query) => {
    const chatId = query.message.chat.id.toString();
    bot.answerCallbackQuery(query.id).catch(() => {});
    if (!sessions[chatId]) sessions[chatId] = { step: null, currentTab: 'members' };
    const state = sessions[chatId];
    const data = query.data;

    if (data === 'noop') return;
    if (data === 'request_access_menu') {
        return bot.sendMessage(chatId, 'اختر نوع الصلاحية التي تريد طلبها:', { reply_markup: { inline_keyboard: [
            [{ text: '🛒 أنا تاجر', callback_data: 'request_role_trader' }],
            [{ text: '👥 أنا مدير مساحات', callback_data: 'request_role_manager' }]
        ] } });
    }

    if (data === 'customer_buy_normal' || data === 'customer_buy_urgent') {
        state.customerOrderType = data === 'customer_buy_urgent' ? 'urgent' : 'normal';
        state.step = data === 'customer_buy_urgent' ? 'customer_awaiting_email_urgent' : 'customer_awaiting_email_normal';
        return bot.sendMessage(chatId, data === 'customer_buy_urgent' ? '⚡ ارسل الايميل الخاص بك للدعوة المستعجلة.' : 'ارسل الايميل الخاص بك لشراء الدعوة.');
    }

    if (data === 'trader_note') {
        return bot.sendMessage(chatId, `أيها التاجر إن قمت بإضافة شخص ومر عليه يومان ثم قمت بإزالته أو استبداله سوف يتم احتساب اليومين عليك.

أي أنك تتحمل المسؤولية.
سوف يتم إزالة الإيميل بنجاح ✅
لكن عندما تقوم بإضافة إيميل ثاني سوف يتم إضافة الإيميل الثاني بمكانه ويأخذ 28 يوم بدل 30 يوم.`);
    }


    if (data === 'request_role_trader' || data === 'request_role_manager') {
        if (await hasBotAccess(chatId)) return bot.sendMessage(chatId, '✅ لديك صلاحية بالفعل.');
        const role = data === 'request_role_trader' ? 'trader' : 'manager_own';
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
            await bot.sendMessage(req.chat_id, '✅ تمت الموافقة عليك كمدير مساحات لمساحاتك فقط. أرسل /start');
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
    if (data === 'admin_speed_menu' && isAdmin(chatId)) { state.step = 'admin_speed_input'; return bot.sendMessage(chatId, 'ارسل رقم السرعة فقط من 1 الى 100. مثال: 10', { reply_markup: getSpeedKeyboard() }); }
    if (data === 'admin_enable_manager' && isAdmin(chatId)) { state.step = 'admin_enable_manager'; return bot.sendMessage(chatId, 'ارسل ايدي الشخص على تيليجرام لتفعيل المدير.'); }
    if (data === 'admin_enable_trader' && isAdmin(chatId)) { state.step = 'admin_enable_trader'; return bot.sendMessage(chatId, 'ارسل ID تيليجرام للتاجر الذي تريد تفعيله.'); }
    if (data === 'admin_remove_access' && isAdmin(chatId)) { state.step = 'admin_remove_access'; return bot.sendMessage(chatId, 'ارسل ID تيليجرام الذي تريد إزالة صلاحيته.'); }
    if (data === 'admin_add_payment_method' && isAdmin(chatId)) { state.step = 'admin_add_payment_method'; return bot.sendMessage(chatId, 'ارسل طريقة دفع جديدة أو ارسل skip لمسح الجميع.'); }
    if (data === 'admin_set_trader_price' && isAdmin(chatId)) { state.step = 'admin_set_trader_price'; return bot.sendMessage(chatId, 'ارسل سعر الدعوة للتاجر بالدينار العراقي.'); }
    if (data === 'admin_clear_trader_debt' && isAdmin(chatId)) { state.step = 'admin_clear_trader_debt'; return bot.sendMessage(chatId, 'ارسل ID التاجر لتصفير ديونه.'); }
    
    if (data === 'admin_manager_full' && isAdmin(chatId)) {
        const targetId = state.pendingTargetId;
        if (!targetId) return bot.sendMessage(chatId, '❌ لا يوجد ايدي محفوظ.');
        await dbRun('INSERT OR REPLACE INTO user_access (chat_id, role, can_refresh, trader_price_iqd, approved_by, approved_at) VALUES (?, ?, ?, ?, ?, ?)', [String(targetId), 'manager_full', 1, DEFAULT_TRADER_PRICE, chatId, nowIso()]);
        state.step = null; state.pendingTargetId = null;
        await bot.sendMessage(String(targetId), '✅ تم تفعيل البوت لديك مع صلاحيات مدير كاملة. أرسل /start');
        return bot.sendMessage(chatId, '✅ تم منح كل الصلاحيات.');
    }
    if (data === 'admin_manager_own' && isAdmin(chatId)) {
        const targetId = state.pendingTargetId;
        if (!targetId) return bot.sendMessage(chatId, '❌ لا يوجد ايدي محفوظ.');
        await dbRun('INSERT OR REPLACE INTO user_access (chat_id, role, can_refresh, trader_price_iqd, approved_by, approved_at) VALUES (?, ?, ?, ?, ?, ?)', [String(targetId), 'manager_own', 0, DEFAULT_TRADER_PRICE, chatId, nowIso()]);
        state.step = null; state.pendingTargetId = null;
        await bot.sendMessage(String(targetId), '✅ تم تفعيل البوت لديك لادارة مساحاتك فقط. أرسل /start');
        return bot.sendMessage(chatId, '✅ تم منح ادارة المساحات الخاصة به فقط.');
    }
    if (data === 'admin_list_payment_methods' && isAdmin(chatId)) {
        const methods = String(await getSetting('payment_methods', '') || '').split('\n').map(v => v.trim()).filter(Boolean);
        return bot.sendMessage(chatId, methods.length ? methods.map((m, i) => `${i+1}) ${m}`).join('\n') : 'لا توجد طرق دفع مضافة.');
    }
    if (data === 'admin_get_trader_emails' && isAdmin(chatId)) {
        const rows = await dbAll("SELECT actor_chat_id, email, workspace_id, status, added_at FROM member_additions WHERE actor_role = 'trader' ORDER BY id DESC LIMIT 200");
        if (!rows.length) return bot.sendMessage(chatId, 'لا توجد ايميلات مضافة من التجار بعد.');
        const lines = rows.map(r => `التاجر: ${r.actor_chat_id} | ${r.email} | مساحة: ${r.workspace_id} | الحالة: ${r.status}`);
        return bot.sendMessage(chatId, lines.join('\n').slice(0, 3900));
    }

    if (/^customer_pick_method_\d+$/.test(data)) {
        const methods = String(await getSetting('payment_methods', '') || '').split('\n').map(v => v.trim()).filter(Boolean);
        const idx = Number(data.split('_').pop());
        const method = methods[idx];
        if (!method) return bot.sendMessage(chatId, '❌ طريقة الدفع غير موجودة.');
        const amount = Number(await getSetting(state.customerOrderType === 'urgent' ? 'customer_urgent_price_iqd' : 'customer_normal_price_iqd', String(DEFAULT_TRADER_PRICE || 0)));
        state.selectedPaymentMethod = method;
        state.expectedPaymentAmount = amount;
        state.step = 'customer_waiting_payment_photo';
        return bot.sendMessage(chatId, `${method}

قم بأرسال المبلغ (${amount} د.ع)

ثم ارسل صورة التحويل هنا للتحقق منها.`);
    }

    if (/^trader_pick_method_\d+$/.test(data)) {
        const methods = String(await getSetting('payment_methods', '') || '').split('\n').map(v => v.trim()).filter(Boolean);
        const idx = Number(data.split('_').pop());
        const method = methods[idx];
        if (!method) return bot.sendMessage(chatId, '❌ طريقة الدفع غير موجودة.');
        const row = await getUserAccess(chatId);
        const price = Number((row && row.trader_price_iqd) || DEFAULT_TRADER_PRICE || 0);
        const debtCountRow = await dbGet(`SELECT COUNT(*) AS c FROM member_additions WHERE actor_chat_id = ? AND actor_role = 'trader' AND settled = 0 AND status IN ('pending','member','migrated')`, [String(chatId)]);
        const count = Number((debtCountRow && debtCountRow.c) || 0);
        const total = count * price;
        state.selectedPaymentMethod = method;
        state.expectedPaymentAmount = total;
        state.step = 'trader_waiting_payment_photo';
        return bot.sendMessage(chatId, `${method}

قم بأرسال (${total} د.ع) ثم ارسل صورة التحويل`);
    }
    if (data === 'trader_show_payment') {
        const methods = String(await getSetting('payment_methods', '') || '').split('\n').map(v => v.trim()).filter(Boolean);
        const row = await getUserAccess(chatId);
        const price = Number((row && row.trader_price_iqd) || DEFAULT_TRADER_PRICE || 0);
        const debtCountRow = await dbGet(`SELECT COUNT(*) AS c FROM member_additions WHERE actor_chat_id = ? AND actor_role = 'trader' AND settled = 0 AND status IN ('pending','member','migrated')`, [String(chatId)]);
        const count = Number((debtCountRow && debtCountRow.c) || 0);
        const total = count * price;
        if (count < 7) return bot.sendMessage(chatId, '✅ لا يوجد عليك حد ديون مانع حالياً.');
        if (!methods.length) return bot.sendMessage(chatId, 'لا توجد طرق دفع مضافة حالياً.');
        const kb = methods.map((m, i) => ([{ text: `اختيار طريقة ${i+1}`, callback_data: `trader_pick_method_${i}` }]));
        return bot.sendMessage(chatId, `المبلغ المطلوب: ${total} د.ع
اختر طريقة الدفع:`, { reply_markup: { inline_keyboard: kb } });
    }

    if (/^approve_receipt_\d+$/.test(data) && isAdmin(chatId)) {
        const id = Number(data.split('_').pop());
        const row = await dbGet('SELECT * FROM payment_receipts WHERE id = ?', [id]);
        if (!row) return bot.sendMessage(chatId, '❌ الطلب غير موجود.');
        await dbRun('UPDATE payment_receipts SET status = ? WHERE id = ?', ['approved', id]);
        await dbRun(`UPDATE member_additions SET settled = 1 WHERE actor_chat_id = ? AND actor_role = 'trader' AND settled = 0`, [row.trader_chat_id]);
        await bot.sendMessage(row.trader_chat_id, '✅ تمت الموافقة على التسديد وتم تصفير الديون.');
        return bot.sendMessage(chatId, '✅ تمت الموافقة على السداد.');
    }
    if (/^reject_receipt_\d+$/.test(data) && isAdmin(chatId)) {
        const id = Number(data.split('_').pop());
        const row = await dbGet('SELECT * FROM payment_receipts WHERE id = ?', [id]);
        if (!row) return bot.sendMessage(chatId, '❌ الطلب غير موجود.');
        await dbRun('UPDATE payment_receipts SET status = ? WHERE id = ?', ['rejected', id]);
        await bot.sendMessage(row.trader_chat_id, '❌ تم رفض صورة التسديد. يرجى إعادة المحاولة بصورة أوضح.');
        return bot.sendMessage(chatId, '✅ تم رفض السداد.');
    }

    if (/^approve_customer_order_\d+$/.test(data) && isAdmin(chatId)) {
        const id = Number(data.split('_').pop());
        const row = await dbGet('SELECT * FROM customer_orders WHERE id = ?', [id]);
        if (!row) return bot.sendMessage(chatId, '❌ الطلب غير موجود.');
        const result = await addEmailIntoAvailableWorkspace(ADMIN_ID, row.email, { actorChatId: ADMIN_ID, actorRole: 'admin' });
        if (!result.ok) return bot.sendMessage(chatId, '❌ لم أجد مساحة مناسبة حالياً لهذه الدعوة.');
        await dbRun('UPDATE customer_orders SET status = ?, workspace_id = ? WHERE id = ?', ['approved', result.workspaceId, id]);
        await bot.sendMessage(row.customer_chat_id, `تم التفعيل ✅

ادخل للـ Chat GPT

بعدها لاعدادات الچات

بعدها اكو خانة
 اسمها ( مساحة العمل) اضغط عليها

ثم اضغط على ( اسم المساحة )`);
        return bot.sendMessage(chatId, `✅ تمت الموافقة على الطلب وإرسال الدعوة إلى ${row.email} في مساحة ${result.workspaceName}`);
    }
    if (/^reject_customer_order_\d+$/.test(data) && isAdmin(chatId)) {
        const id = Number(data.split('_').pop());
        const row = await dbGet('SELECT * FROM customer_orders WHERE id = ?', [id]);
        if (!row) return bot.sendMessage(chatId, '❌ الطلب غير موجود.');
        await dbRun('UPDATE customer_orders SET status = ? WHERE id = ?', ['rejected', id]);
        await bot.sendMessage(row.customer_chat_id, '❌ تم رفض التحويل. يمكنك إعادة الإرسال بصورة أوضح.');
        return bot.sendMessage(chatId, '✅ تم رفض طلب الزبون.');
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
        const ws = await dbGet('SELECT * FROM workspaces WHERE id = ?', [wsId]);
        const context = await getContext(wsId, ws.profile_dir);
        const page = await context.newPage();
        await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
        await sleep(1000);
        state.context = context;
        state.page = page;
        state.profileDir = ws.profile_dir;
        state.step = 'manual_idle';
        state.codeGen = new PlaywrightCodeGenerator(path.join(DATA_DIR, 'recordings'));
        state.human = new HumanizedComputer(page, { recorder: state.codeGen });
        state.codeGen.addStep('بدء جلسة التحكم اليدوي');
        state.codeGen.addCommand(`await human.gotoHuman('https://chatgpt.com/admin/members?tab=members');`);
        return await sendInteractiveMenu(chatId, '🖥 تم فتح وضع الكمبيوتر للأدمن. كل حركة ستُنفذ بشكل بشري وتُسجَّل داخل سكربت.');
    }
    if (data === 'int_goto_url') {
        if (!isAdmin(chatId) || !state.page || !state.human) return;
        state.step = 'manual_awaiting_url';
        return bot.sendMessage(chatId, 'ارسل الرابط الذي تريد فتحه الآن.');
    }
    if (data === 'int_type_text') {
        if (!isAdmin(chatId) || !state.page || !state.human) return;
        state.step = 'manual_awaiting_text';
        return bot.sendMessage(chatId, 'ارسل النص الذي تريد كتابته كتابة حقيقية داخل الصفحة الحالية.');
    }
    if (data === 'int_press_enter') {
        if (!isAdmin(chatId) || !state.page || !state.human) return;
        await state.human.note('ضغط زر Enter');
        await state.human.pressKeyHuman('Enter');
        return bot.sendMessage(chatId, '✅ تم ضغط Enter بشكل بشري.');
    }
    if (data === 'int_show_grid') {
        if (!isAdmin(chatId) || !state.page || !state.human) return;
        const p = path.join(os.tmpdir(), `manual_grid_${Date.now()}.png`);
        state.lastGrid = await state.human.captureGridScreenshot(p, { cols: 6, rows: 8 });
        state.step = 'manual_awaiting_grid_cell';
        await bot.sendPhoto(chatId, p, { caption: '🖱️ هذه شبكة الشاشة. ارسل رقم المربع، وسأحرك الماوس من أسفل الشاشة إليه بشكل تدريجي وبشري.' });
        fs.unlinkSync(p);
        return;
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
            await syncWorkspaceStatsFromPage(insertedId, members, invites);
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
            await syncWorkspaceStatsFromPage(ws.id, members, invites);
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
            await syncWorkspaceStatsFromPage(ws.id, members, invites);
            await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
            await bot.sendMessage(chatId, `ايميلات: الدعوات المعلقة:\n${invites.length ? invites.join('\n') : '-'}\n\nايميلات المتوفرين حاليا:\n${members.length ? members.join('\n') : '-'}`);
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
        state.pendingTargetId = String(text).trim();
        state.step = 'admin_enable_manager_choose';
        return bot.sendMessage(chatId, 'اختر نوع صلاحية المدير:', { reply_markup: { inline_keyboard: [
            [{ text: 'منحه كل الصلاحيات', callback_data: 'admin_manager_full' }],
            [{ text: 'منحه ادارة مساحات العمل الخاصات به', callback_data: 'admin_manager_own' }]
        ] } });
    }
    if (isAdmin(chatId) && state.step === 'admin_enable_trader') {
        await dbRun('INSERT OR REPLACE INTO user_access (chat_id, role, can_refresh, trader_price_iqd, approved_by, approved_at) VALUES (?, ?, ?, ?, ?, ?)', [text, 'trader', 0, DEFAULT_TRADER_PRICE, chatId, nowIso()]);
        sessions[text] = sessions[text] || { step: null, currentTab: 'members' }; sessions[text].step = 'trader_awaiting_real_name';
        await bot.sendMessage(text, '✅ تم تفعيل البوت لديك كتاجر. ارسل اسمك الحقيقي الآن.'); state.step = null; return bot.sendMessage(chatId, '✅ تم التفعيل.');
    }
    if (isAdmin(chatId) && state.step === 'admin_remove_access') { await dbRun('DELETE FROM user_access WHERE chat_id = ?', [text]); state.step = null; return bot.sendMessage(chatId, '✅ تم إزالة الصلاحية.'); }
    if (isAdmin(chatId) && state.step === 'admin_add_payment_method') {
        const current = String(await getSetting('payment_methods', '') || '');
        let methods = current.split('\n').map(v => v.trim()).filter(Boolean);
        if (/^skip$/i.test(text)) { methods = []; } else { methods.push(text); }
        await setSetting('payment_methods', methods.join('\n'));
        state.step = null;
        return bot.sendMessage(chatId, '✅ تم تحديث طرق الدفع.');
    }
    if (isAdmin(chatId) && state.step === 'admin_speed_input') { const val = Number(String(text).replace(/[^\d]/g, '')); if (!val || val < 1 || val > 100) return bot.sendMessage(chatId, 'ارسل رقماً من 1 الى 100.'); CURRENT_SPEED_PERCENT = val; await setSetting('speed_percent', String(val)); state.step = null; return bot.sendMessage(chatId, `✅ تم ضبط السرعة على ${val}%`); }
    if (isAdmin(chatId) && state.step === 'admin_set_trader_price') { const val = Number(text.replace(/[^\d]/g, '')); if (!val) return bot.sendMessage(chatId, 'ارسل رقم صحيح.'); await setSetting('trader_price_iqd', val); state.step = null; return bot.sendMessage(chatId, `✅ تم تحديث السعر إلى ${val} د.ع`); }
    if (isAdmin(chatId) && state.step === 'admin_clear_trader_debt') { await dbRun(`UPDATE member_additions SET settled = 1 WHERE actor_chat_id = ? AND actor_role = 'trader'`, [text]); state.step = null; await bot.sendMessage(text, '✅ تم تصفير ديونك.'); return bot.sendMessage(chatId, '✅ تم تصفير الديون.'); }

    if (state.step === 'customer_awaiting_email_normal' || state.step === 'customer_awaiting_email_urgent') {
        const emailMatch = String(text || '').match(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/);
        if (!emailMatch) return bot.sendMessage(chatId, 'ارسل ايميلاً صحيحاً.');
        state.customerEmail = normalizeEmail(emailMatch[0]);
        const methods = String(await getSetting('payment_methods', '') || '').split('\n').map(v => v.trim()).filter(Boolean);
        if (!methods.length) return bot.sendMessage(chatId, 'لا توجد طرق دفع مضافة حالياً من الادمن.');
        if (state.step === 'customer_awaiting_email_urgent') {
            const fast = await addEmailIntoAvailableWorkspace(ADMIN_ID, state.customerEmail, { actorChatId: ADMIN_ID, actorRole: 'admin', silentLog: true });
            if (!fast.ok) { state.step = null; return bot.sendMessage(chatId, '❌ لا توجد مساحة متاحة حالياً لهذه الدعوة المستعجلة.'); }
            state.customerUrgentWorkspaceId = fast.workspaceId;
            state.customerUrgentOrderCreatedAt = nowIso();
            await bot.sendMessage(chatId, `✅ تم إرسال الدعوة المستعجلة إلى ${state.customerEmail} لمدة متابعة الدفع.
اختر طريقة الدفع الآن لإكمال الطلب خلال ساعة واحدة.`);
        }
        state.step = 'customer_choose_payment_method';
        return bot.sendMessage(chatId, 'اختر طريقة الدفع:', { reply_markup: { inline_keyboard: methods.map((m, i) => ([{ text: `طريقة ${i+1}`, callback_data: `customer_pick_method_${i}` }])) } });
    }

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
            if (state.mode === 'manual') {
                state.step = 'manual_idle';
                state.codeGen = new PlaywrightCodeGenerator(path.join(DATA_DIR, 'recordings'));
                state.human = new HumanizedComputer(state.page, { recorder: state.codeGen });
                state.codeGen.addStep('بدء جلسة يدوية جديدة');
                return sendInteractiveMenu(chatId, '✅ تم فتح الجلسة اليدوية. الأوامر الآن ستستخدم كتابة وحركة بشرية وتُحفظ كسكربت.');
            }
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
                await syncWorkspaceStatsFromPage(insertedId, members, invites);
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

    if (state.step === 'manual_awaiting_url') {
        if (!state.page || !state.human) { state.step = null; return bot.sendMessage(chatId, '❌ لا توجد جلسة يدوية فعالة.'); }
        state.step = 'manual_idle';
        try {
            await state.human.note('فتح رابط جديد');
            await state.human.gotoHuman(text);
            return sendInteractiveMenu(chatId, `✅ تم فتح الرابط:
${text}`);
        } catch (e) {
            return bot.sendMessage(chatId, `❌ فشل فتح الرابط: ${e.message}`);
        }
    }

    if (state.step === 'manual_awaiting_text') {
        if (!state.page || !state.human) { state.step = null; return bot.sendMessage(chatId, '❌ لا توجد جلسة يدوية فعالة.'); }
        state.step = 'manual_idle';
        try {
            await state.human.note('كتابة نص بشكل بشري');
            await state.human.typeHuman(text);
            return sendInteractiveMenu(chatId, '✅ تمت الكتابة بشكل تدريجي وحقيقي.');
        } catch (e) {
            return bot.sendMessage(chatId, `❌ فشلت الكتابة: ${e.message}`);
        }
    }

    if (state.step === 'manual_awaiting_grid_cell') {
        if (!state.page || !state.human || !state.lastGrid) { state.step = null; return bot.sendMessage(chatId, '❌ لا توجد شبكة نشطة حالياً.'); }
        const cellNumber = Number(String(text).replace(/[^\d]/g, ''));
        if (!cellNumber) return bot.sendMessage(chatId, 'ارسل رقم مربع صحيح.');
        state.step = 'manual_idle';
        try {
            await state.human.note(`التحرك إلى المربع رقم ${cellNumber}`);
            const target = await state.human.moveToGridCellFromBottom(cellNumber, state.lastGrid, { click: false });
            return sendInteractiveMenu(chatId, `✅ تحرك الماوس إلى المربع ${target.cellNumber} عند الإحداثيات التقريبية (${Math.round(target.x)}, ${Math.round(target.y)}).`);
        } catch (e) {
            return bot.sendMessage(chatId, `❌ فشل التحرك إلى المربع: ${e.message}`);
        }
    }

    if (state.step) {
        const textInput = text.trim(); const wsId = state.currentWsId;
        if (state.step === 'ws_awaiting_add_person') {
            state.step = 'processing'; let statusMsg = await bot.sendMessage(chatId, '⏳ جاري البحث عن مساحة مناسبة ثم الإضافة...');
            try {
                if (await isTrader(chatId) && await traderDebtBlocked(chatId)) { state.step = null; await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {}); return bot.sendMessage(chatId, '❌ لا يمكنك إضافة المزيد حتى سداد الدين.'); }
                const actorRole = await getActorRoleValue(chatId);
                const result = await addEmailIntoAvailableWorkspace(chatId, textInput, { actorChatId: chatId, actorRole });
                await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {});
                if (result.ok) {
                    bot.sendMessage(chatId, `✅ تمت إضافة هذا الإيميل: ${result.email}
🏢 في هذه المساحة: ${result.workspaceName}
📌 كم دعوة متبقية: ${result.remaining}`);
                    await bot.sendMessage(chatId, `تم التفعيل ✅

ادخل للـ Chat GPT

بعدها لاعدادات الچات

بعدها اكو خانة
 اسمها ( مساحة العمل) اضغط عليها

ثم اضغط على ( اسم المساحة )`);
                    if (await isTrader(chatId)) await sendTraderDebtCard(chatId);
                } else {
                    bot.sendMessage(chatId, '❌ لم أجد مساحة متاحة أقل من 6 أعضاء أو أن الإيميل موجود مسبقًا في كل المساحات.');
                }
            } catch (error) { bot.sendMessage(chatId, `❌ خطأ: ${error.message}`); }
            state.step = null; return;
        }
        if (state.step === 'ws_awaiting_change_name') {
            if (!wsId) { state.step = null; return bot.sendMessage(chatId, '❌ افتح مساحة أولاً.'); }
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
                for (const ws of visible) { const context = await getContext(ws.id, ws.profile_dir); const page = await context.newPage(); await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(900); if (await dynamicGeometryAction(page, textInput, 'revoke')) { ok = true; await dbRun('DELETE FROM allowed_emails WHERE ws_id = ? AND email = ?', [ws.id, normalizeEmail(textInput)]); const stats = await getWorkspaceStats(ws.id); const invites = String(stats.last_known_invites || '').split('\n').map(normalizeEmail).filter(v => v && v !== normalizeEmail(textInput)); await setWorkspaceStats(ws.id, { invite_count: Math.max(0, invites.length), last_known_invites: invites.join('\n'), last_synced_at: nowIso() }); await page.close().catch(() => {}); break; } await page.close().catch(() => {}); }
                await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {}); if (ok) { if (await isTrader(chatId)) await markTraderEmailStatus(chatId, textInput, 'revoked'); bot.sendMessage(chatId, `✅ تم إلغاء الدعوة بنجاح لـ: ${textInput}`); } else bot.sendMessage(chatId, '❌ لم يتم العثور على الإيميل في الدعوات المعلقة أو فشل الإجراء.');
            } catch (error) { bot.sendMessage(chatId, `❌ خطأ: ${error.message}`); }
            state.step = null; return;
        }
        if (state.step === 'ws_awaiting_remove_member') {
            state.step = 'processing'; let statusMsg = await bot.sendMessage(chatId, '⏳ جاري إزالة العضو...');
            try {
                if (await isTrader(chatId)) { const own = await canTraderManageEmail(chatId, textInput); if (!own) { state.step = null; await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {}); return bot.sendMessage(chatId, '❌ لا يمكنك إزالة عضو لم تضفه أنت.'); } }
                const ownerChatId = await getOwnerChatForAction(chatId); const visible = await dbAll('SELECT * FROM workspaces WHERE chat_id = ?', [ownerChatId]); let ok = false;
                for (const ws of visible) { const context = await getContext(ws.id, ws.profile_dir); const page = await context.newPage(); await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(900); if (await dynamicGeometryAction(page, textInput, 'remove')) { ok = true; await dbRun('DELETE FROM allowed_emails WHERE ws_id = ? AND email = ?', [ws.id, normalizeEmail(textInput)]); const stats = await getWorkspaceStats(ws.id); const members = String(stats.last_known_emails || '').split('\n').map(normalizeEmail).filter(v => v && v !== normalizeEmail(textInput)); await setWorkspaceStats(ws.id, { member_count: Math.max(0, members.length), last_known_emails: members.join('\n'), last_synced_at: nowIso() }); if (await isTrader(chatId)) { const own = await canTraderManageEmail(chatId, textInput); if (own && own.added_at && (Date.now() - new Date(own.added_at).getTime()) >= (40 * 60 * 60 * 1000)) { await reserveTraderSeat(chatId, ws.id, textInput, 40); } } await page.close().catch(() => {}); break; } await page.close().catch(() => {}); }
                await bot.deleteMessage(chatId, statusMsg.message_id).catch(() => {}); if (ok) { if (await isTrader(chatId)) await markTraderEmailStatus(chatId, textInput, 'removed'); bot.sendMessage(chatId, `✅ تمت إزالة العضو نهائياً: ${textInput}`); } else bot.sendMessage(chatId, '❌ لم يتم العثور على الإيميل في قائمة الأعضاء النشطين أو فشل الإجراء.');
            } catch (error) { bot.sendMessage(chatId, `❌ خطأ: ${error.message}`); }
            state.step = null; return;
        }
    }
});


bot.on('photo', async (msg) => {
    const chatId = String(msg.chat.id);
    if (!sessions[chatId]) sessions[chatId] = { step: null, currentTab: 'members' };
    const state = sessions[chatId];
    const fileId = msg.photo && msg.photo.length ? msg.photo[msg.photo.length - 1].file_id : '';
    if (!fileId) return;

    if (state.step === 'trader_waiting_payment_photo') {
        const row = await getUserAccess(chatId);
        const amount = Number(state.expectedPaymentAmount || 0);
        const method = String(state.selectedPaymentMethod || 'غير محددة');
        const name = (row && row.real_name) || msg.from?.first_name || '';
        const username = msg.from?.username ? '@' + msg.from.username : 'لا يوجد';
        const receiptId = await dbRun('INSERT INTO payment_receipts (trader_chat_id, trader_name, amount_iqd, method, status, created_at, photo_file_id) VALUES (?, ?, ?, ?, ?, ?, ?)', [chatId, name, amount, method, 'pending', nowIso(), fileId]);
        state.step = null;
        await bot.sendMessage(chatId, '✅ تم إرسال صورة التحويل إلى الأدمن للمراجعة.');
        return await bot.sendPhoto(ADMIN_ID, fileId, {
            caption: `طلب تسديد جديد
الوقت: ${new Date().toLocaleString('ar-IQ')}
طريقة الدفع: ${method}
الاسم: ${name}
المعرف: ${msg.from?.username ? '@' + msg.from.username : 'لا يوجد'}
الايدي: ${chatId}
كم دفع: ${amount} د.ع`,
            reply_markup: { inline_keyboard: [
                [{ text: '✅ موافق', callback_data: `approve_receipt_${receiptId}` }, { text: '❌ رفض', callback_data: `reject_receipt_${receiptId}` }]
            ] }
        });
    }

    if (state.step === 'customer_waiting_payment_photo') {
        const amount = Number(state.expectedPaymentAmount || 0);
        const method = String(state.selectedPaymentMethod || 'غير محددة');
        const orderType = state.customerOrderType === 'urgent' ? 'urgent' : 'normal';
        const expiresAt = orderType === 'urgent' ? new Date(Date.now() + 60 * 60 * 1000).toISOString() : '';
        const nextReminderAt = orderType === 'urgent' ? new Date(Date.now() + 15 * 60 * 1000).toISOString() : '';
        const orderId = await dbRun('INSERT INTO customer_orders (customer_chat_id, customer_name, username, order_type, email, amount_iqd, method, status, created_at, expires_at, next_reminder_at, reminder_count, photo_file_id, workspace_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', [
            chatId,
            msg.from?.first_name || '',
            msg.from?.username ? '@' + msg.from.username : '',
            orderType,
            normalizeEmail(state.customerEmail || ''),
            amount,
            method,
            'pending_admin_review',
            nowIso(),
            expiresAt,
            nextReminderAt,
            0,
            fileId,
            state.customerUrgentWorkspaceId || null
        ]);
        state.step = null;
        await bot.sendMessage(chatId, '✅ تم إرسال التحويل إلى الأدمن للمراجعة.');
        return await bot.sendPhoto(ADMIN_ID, fileId, {
            caption: `طلب شراء دعوة
الوقت: ${new Date().toLocaleString('ar-IQ')}
النوع: ${orderType === 'urgent' ? 'مستعجل' : 'عادي'}
طريقة الدفع: ${method}
الاسم: ${msg.from?.first_name || ''}
المعرف: ${msg.from?.username ? '@' + msg.from.username : 'لا يوجد'}
الايدي: ${chatId}
الايميل: ${normalizeEmail(state.customerEmail || '')}
كم دفع: ${amount} د.ع`,
            reply_markup: { inline_keyboard: [
                [{ text: '✅ موافق', callback_data: `approve_customer_order_${orderId}` }, { text: '❌ رفض', callback_data: `reject_customer_order_${orderId}` }]
            ] }
        });
    }
});

// =========================================================================
// 🛡️ الحارس الليلي (سريع ومتكيف مع السرعة)
// =========================================================================
let watcherRunning = false;

async function watcherTick() {
    if (watcherRunning) return;
    watcherRunning = true;
    try {
        const workspaces = await dbAll('SELECT * FROM workspaces');
        for (const ws of workspaces) {
            try {
                if (!fs.existsSync(ws.profile_dir)) continue;
                const allowedRows = await dbAll('SELECT email FROM allowed_emails WHERE ws_id = ?', [ws.id]);
                const context = await getContext(ws.id, ws.profile_dir);
                const page = await context.newPage();
                if (allowedRows.length === 0) {
                    await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(1000);
                    let members = await extractAllEmails(page);
                    await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(1000);
                    let invites = await extractAllEmails(page);
                    let all = [...new Set([...members, ...invites])];
                    for (let e of all) await dbRun('INSERT INTO allowed_emails (ws_id, email) VALUES (?, ?)', [ws.id, normalizeEmail(e)]);
                    await syncWorkspaceStatsFromPage(ws.id, members, invites);
                    await page.close().catch(() => {});
                    continue;
                }
                const allowedEmails = new Set(allowedRows.map(r => normalizeEmail(r.email)));
                allowedEmails.add(normalizeEmail(ws.email));
                await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(900);
                let foundEmails = await extractAllEmails(page);
                for (const email of foundEmails) {
                    const norm = normalizeEmail(email);
                    if (!allowedEmails.has(norm)) {
                        if (await dynamicGeometryAction(page, norm, 'remove')) {
                            await bot.sendMessage(ws.chat_id, `🚨 نظام الحماية:
تم طرد إيميل دخيل أضيف من خارج البوت!
الإيميل: ${norm}
المساحة: (${ws.name})`);
                        }
                    }
                }
                const latestMembers = await extractAllEmails(page).catch(() => foundEmails);
                await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 45000 }); await sleep(900);
                let pendingEmails = await extractAllEmails(page);
                for (const email of pendingEmails) {
                    const norm = normalizeEmail(email);
                    if (!allowedEmails.has(norm)) {
                        if (await dynamicGeometryAction(page, norm, 'revoke')) {
                            await bot.sendMessage(ws.chat_id, `🚨 نظام الحماية:
تم إلغاء دعوة غريبة أُرسلت من خارج البوت!
الإيميل: ${norm}
المساحة: (${ws.name})`);
                        }
                    }
                }
                await syncWorkspaceStatsFromPage(ws.id, latestMembers || foundEmails, pendingEmails);
                await page.close().catch(() => {});
            } catch (e) {}
        }

        const pendingUrgent = await dbAll("SELECT * FROM customer_orders WHERE order_type = 'urgent' AND status IN ('pending_admin_review') AND expires_at != ''");
        for (const ord of pendingUrgent) {
            try {
                if (ord.next_reminder_at && ord.next_reminder_at <= nowIso()) {
                    await bot.sendMessage(ord.customer_chat_id, '⏰ تذكير: يجب إرسال المبلغ قبل انتهاء الوقت وإلا سيتم إلغاء اشتراكك.', {
                        reply_markup: { inline_keyboard: [[{ text: '💳 تسديد المبلغ', callback_data: 'customer_buy_urgent' }]] }
                    }).catch(() => {});
                    const nextAt = new Date(Date.now() + 15 * 60 * 1000).toISOString();
                    await dbRun('UPDATE customer_orders SET next_reminder_at = ?, reminder_count = COALESCE(reminder_count, 0) + 1 WHERE id = ?', [nextAt, ord.id]);
                }
                if (ord.expires_at && ord.expires_at <= nowIso() && ord.status !== 'approved') {
                    const ws = ord.workspace_id ? await dbGet('SELECT * FROM workspaces WHERE id = ?', [ord.workspace_id]) : null;
                    if (ws && fs.existsSync(ws.profile_dir)) {
                        const context = await getContext(ws.id, ws.profile_dir);
                        const page = await context.newPage();
                        await page.goto('https://chatgpt.com/admin/members?tab=invites', { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
                        await sleep(700);
                        if (!(await dynamicGeometryAction(page, ord.email, 'revoke'))) {
                            await page.goto('https://chatgpt.com/admin/members?tab=members', { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
                            await sleep(700);
                            await dynamicGeometryAction(page, ord.email, 'remove').catch(() => {});
                        }
                        await page.close().catch(() => {});
                    }
                    await dbRun("UPDATE customer_orders SET status = 'expired' WHERE id = ?", [ord.id]);
                    await bot.sendMessage(ord.customer_chat_id, '⌛ انتهت مهلة الدعوة المستعجلة وتم إلغاء الطلب لعدم التسديد.').catch(() => {});
                }
            } catch (e) {}
        }
    } catch (e) {}
    watcherRunning = false;
    setTimeout(watcherTick, Math.max(4000, scaledMs(WATCH_INTERVAL_MS)));
}

setTimeout(watcherTick, 5000);

process.on('uncaughtException', () => {});
process.on('unhandledRejection', () => {});
