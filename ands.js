
const fs = require('fs');
const path = require('path');

function clamp(v, min, max) {
    return Math.max(min, Math.min(max, v));
}

function rand(min, max) {
    return Math.random() * (max - min) + min;
}

function randInt(min, max) {
    return Math.floor(rand(min, max + 1));
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function easeInOut(t) {
    return t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
}

function bezierPoint(p0, p1, p2, p3, t) {
    const mt = 1 - t;
    return {
        x: Math.pow(mt, 3) * p0.x + 3 * Math.pow(mt, 2) * t * p1.x + 3 * mt * Math.pow(t, 2) * p2.x + Math.pow(t, 3) * p3.x,
        y: Math.pow(mt, 3) * p0.y + 3 * Math.pow(mt, 2) * t * p1.y + 3 * mt * Math.pow(t, 2) * p2.y + Math.pow(t, 3) * p3.y
    };
}

class ScriptRecorder {
    constructor(outputDir = process.cwd()) {
        this.outputDir = outputDir;
        this.lines = [];
        this.stepCounter = 1;
        this.ensureDir();
    }

    ensureDir() {
        fs.mkdirSync(this.outputDir, { recursive: true });
    }

    addComment(comment) {
        this.lines.push(`\n  // ${String(this.stepCounter).padStart(2, '0')} - ${comment}`);
        this.stepCounter += 1;
    }

    addRaw(line) {
        this.lines.push(`  ${line}`);
    }

    build() {
        return `const { chromium } = require('playwright');
const { HumanizedComputer } = require('./ands');

(async () => {
  const browser = await chromium.launch({ headless: false });
  const context = await browser.newContext({ viewport: { width: 1366, height: 768 } });
  const page = await context.newPage();
  const human = new HumanizedComputer(page, { recordScript: false });
${this.lines.join('\n')}
})();\n`;
    }

    save(fileName = `manual_record_${Date.now()}.js`) {
        this.ensureDir();
        const full = path.join(this.outputDir, fileName);
        fs.writeFileSync(full, this.build(), 'utf8');
        return full;
    }
}

class HumanizedComputer {
    constructor(page, options = {}) {
        this.page = page;
        this.viewport = options.viewport || { width: 1366, height: 768 };
        this.pointer = {
            x: rand(this.viewport.width * 0.35, this.viewport.width * 0.65),
            y: rand(this.viewport.height * 0.80, this.viewport.height * 0.94)
        };
        this.recordScript = options.recordScript !== false;
        this.recorder = options.recorder || null;
        this.defaultTypingDelay = options.defaultTypingDelay || [45, 140];
        this.defaultMoveDuration = options.defaultMoveDuration || [500, 1800];
        this.mistakeChance = options.mistakeChance == null ? 0.06 : options.mistakeChance;
        this.keyboardLayout = 'qwertyuiopasdfghjklzxcvbnm';
    }

    setRecorder(recorder) {
        this.recorder = recorder;
    }

    setPage(page) {
        this.page = page;
    }

    note(comment) {
        if (this.recordScript && this.recorder) this.recorder.addComment(comment);
    }

    raw(line) {
        if (this.recordScript && this.recorder) this.recorder.addRaw(line);
    }

    currentPoint() {
        return { x: this.pointer.x, y: this.pointer.y };
    }

    async syncViewport() {
        try {
            const vp = this.page.viewportSize();
            if (vp) this.viewport = vp;
        } catch (e) {}
    }

    async moveMouseHuman(targetX, targetY, opts = {}) {
        await this.syncViewport();
        const start = this.currentPoint();
        const target = {
            x: clamp(targetX, 1, this.viewport.width - 1),
            y: clamp(targetY, 1, this.viewport.height - 1)
        };
        const dx = target.x - start.x;
        const dy = target.y - start.y;
        const distance = Math.hypot(dx, dy);
        const duration = opts.duration || clamp(distance * rand(1.8, 3.4), this.defaultMoveDuration[0], this.defaultMoveDuration[1] + distance * 1.4);
        const steps = opts.steps || clamp(Math.round(distance / rand(7, 13)), 18, 130);
        const spread = clamp(distance * rand(0.10, 0.22), 25, 180);

        const p1 = { x: start.x + dx * rand(0.18, 0.32) + rand(-spread, spread), y: start.y + dy * rand(0.18, 0.32) + rand(-spread, spread) };
        const p2 = { x: start.x + dx * rand(0.66, 0.84) + rand(-spread, spread), y: start.y + dy * rand(0.66, 0.84) + rand(-spread, spread) };

        for (let i = 1; i <= steps; i++) {
            const t = easeInOut(i / steps);
            const point = bezierPoint(start, p1, p2, target, t);
            const jitterScale = i > steps - 4 ? 0.45 : 1;
            const x = clamp(point.x + rand(-1.4, 1.4) * jitterScale, 1, this.viewport.width - 1);
            const y = clamp(point.y + rand(-1.4, 1.4) * jitterScale, 1, this.viewport.height - 1);
            await this.page.mouse.move(x, y);
            await sleep(Math.max(5, duration / steps + rand(-3, 7)));
        }

        this.pointer = target;
        if (opts.record !== false) {
            this.raw(`await human.moveMouseHuman(${target.x.toFixed(2)}, ${target.y.toFixed(2)});`);
        }
    }

    async clickHuman(x, y, opts = {}) {
        await this.moveMouseHuman(x, y, { duration: opts.moveDuration, steps: opts.steps, record: opts.record });
        await sleep(randInt(40, 140));
        await this.page.mouse.down();
        await sleep(randInt(35, 120));
        await this.page.mouse.up();
        await sleep(randInt(60, 180));
        if (opts.record !== false) {
            this.raw(`await human.clickHuman(${Number(x).toFixed(2)}, ${Number(y).toFixed(2)});`);
        }
    }

    async pressKeyHuman(key, opts = {}) {
        await sleep(randInt(35, 110));
        await this.page.keyboard.down(key);
        await sleep(randInt(25, 90));
        await this.page.keyboard.up(key);
        await sleep(randInt(50, 140));
        if (opts.record !== false) {
            this.raw(`await human.pressKeyHuman(${JSON.stringify(key)});`);
        }
    }

    randomNeighborChar(ch) {
        const lower = String(ch || '').toLowerCase();
        if (!/[a-z]/.test(lower)) return lower || 'a';
        const idx = this.keyboardLayout.indexOf(lower);
        if (idx === -1) return 'a';
        const offset = Math.random() > 0.5 ? 1 : -1;
        return this.keyboardLayout[clamp(idx + offset, 0, this.keyboardLayout.length - 1)];
    }

    async typeHuman(text, opts = {}) {
        const str = String(text || '');
        for (const ch of str) {
            if (/[a-zA-Z]/.test(ch) && Math.random() < (opts.mistakeChance ?? this.mistakeChance)) {
                const wrong = this.randomNeighborChar(ch);
                await this.page.keyboard.type(wrong, { delay: randInt(35, 90) });
                await sleep(randInt(40, 110));
                await this.page.keyboard.press('Backspace');
                await sleep(randInt(60, 120));
            }
            const delay = randInt(opts.minDelay || this.defaultTypingDelay[0], opts.maxDelay || this.defaultTypingDelay[1]);
            await this.page.keyboard.type(ch, { delay });
            if (/[\s,.;:!?]/.test(ch)) {
                await sleep(randInt(30, 140));
            } else if (Math.random() < 0.08) {
                await sleep(randInt(35, 120));
            }
        }
        if (opts.record !== false) {
            this.raw(`await human.typeHuman(${JSON.stringify(str)});`);
        }
    }

    async scrollHuman(amount = 700, opts = {}) {
        const parts = opts.parts || clamp(Math.round(Math.abs(amount) / 260), 2, 8);
        const base = amount / parts;
        for (let i = 0; i < parts; i++) {
            const delta = base + rand(-40, 40);
            await this.page.mouse.wheel(0, delta);
            await sleep(randInt(80, 220));
        }
        if (opts.record !== false) {
            this.raw(`await human.scrollHuman(${Math.round(amount)});`);
        }
    }

    async gotoHuman(url, opts = {}) {
        await this.page.goto(url, { waitUntil: opts.waitUntil || 'domcontentloaded', timeout: opts.timeout || 45000 });
        await sleep(randInt(700, 1600));
        if (opts.record !== false) {
            this.raw(`await human.gotoHuman(${JSON.stringify(url)});`);
        }
    }

    async moveToGridCellFromBottom(cellNumber, gridInfo, opts = {}) {
        const cols = gridInfo.cols;
        const rows = gridInfo.rows;
        const cellWidth = gridInfo.cellWidth;
        const cellHeight = gridInfo.cellHeight;
        const total = cols * rows;
        const normalizedCell = clamp(Number(cellNumber) || 1, 1, total);
        const index = normalizedCell - 1;
        const col = index % cols;
        const row = Math.floor(index / cols);
        const x = (col * cellWidth) + cellWidth / 2;
        const y = (row * cellHeight) + cellHeight / 2;
        const startX = this.viewport.width / 2 + rand(-60, 60);
        const startY = this.viewport.height - rand(12, 28);
        await this.moveMouseHuman(startX, startY, { duration: randInt(300, 700), record: false });
        await sleep(randInt(100, 260));
        await this.moveMouseHuman(x, y, { duration: opts.duration || randInt(1200, 2600), record: false });
        if (opts.click) {
            await this.clickHuman(x, y, { record: false });
        }
        if (opts.record !== false) {
            this.raw(`await human.moveToGridCellFromBottom(${normalizedCell}, ${JSON.stringify(gridInfo)}, ${JSON.stringify({ click: !!opts.click })});`);
        }
        return { x, y, row, col, cellNumber: normalizedCell };
    }

    async clickElementCenter(selector, opts = {}) {
        const box = await this.page.locator(selector).first().boundingBox();
        if (!box) throw new Error(`Element not found for selector: ${selector}`);
        const x = box.x + box.width / 2;
        const y = box.y + box.height / 2;
        await this.clickHuman(x, y, { record: false });
        if (opts.record !== false) this.raw(`await human.clickElementCenter(${JSON.stringify(selector)});`);
        return { x, y };
    }

    async captureGridScreenshot(filePath, opts = {}) {
        await this.syncViewport();
        const cols = opts.cols || 6;
        const rows = opts.rows || 8;
        const gridInfo = {
            cols,
            rows,
            cellWidth: this.viewport.width / cols,
            cellHeight: this.viewport.height / rows,
            viewport: { ...this.viewport }
        };

        await this.page.evaluate(({ cols, rows }) => {
            const old = document.getElementById('__human_grid_overlay__');
            if (old) old.remove();
            const overlay = document.createElement('div');
            overlay.id = '__human_grid_overlay__';
            overlay.style.position = 'fixed';
            overlay.style.inset = '0';
            overlay.style.zIndex = '2147483647';
            overlay.style.pointerEvents = 'none';
            overlay.style.fontFamily = 'Arial, sans-serif';
            overlay.style.backgroundImage = `linear-gradient(to right, rgba(255,0,0,.35) 1px, transparent 1px), linear-gradient(to bottom, rgba(255,0,0,.35) 1px, transparent 1px)`;
            overlay.style.backgroundSize = `${100 / cols}% ${100 / rows}%`;
            document.body.appendChild(overlay);
            let counter = 1;
            for (let r = 0; r < rows; r++) {
                for (let c = 0; c < cols; c++) {
                    const badge = document.createElement('div');
                    badge.textContent = String(counter++);
                    badge.style.position = 'fixed';
                    badge.style.left = `calc(${(c * 100) / cols}% + 8px)`;
                    badge.style.top = `calc(${(r * 100) / rows}% + 8px)`;
                    badge.style.color = '#fff';
                    badge.style.background = 'rgba(220,0,0,.85)';
                    badge.style.padding = '2px 6px';
                    badge.style.borderRadius = '999px';
                    badge.style.fontSize = '14px';
                    badge.style.fontWeight = '700';
                    badge.style.boxShadow = '0 1px 4px rgba(0,0,0,.35)';
                    overlay.appendChild(badge);
                }
            }
        }, { cols, rows });

        await this.page.screenshot({ path: filePath, fullPage: false });
        await this.page.evaluate(() => document.getElementById('__human_grid_overlay__')?.remove()).catch(() => {});
        return gridInfo;
    }
}

module.exports = {
    HumanizedComputer,
    ScriptRecorder,
    sleep,
    rand,
    randInt,
    clamp
};
