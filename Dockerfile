# استخدام النسخة الرسمية من Playwright التي تحتوي على نظام تشغيل مجهز للمتصفحات
FROM mcr.microsoft.com/playwright:v1.42.1-jammy

# تحديد مجلد العمل داخل السيرفر
WORKDIR /app

# نسخ ملفات الإعدادات وتثبيت المكتبات البرمجية
COPY package*.json ./
RUN npm install

# نسخ كود البوت وبقية الملفات إلى السيرفر
COPY . .

# تثبيت متصفح Chromium مع كافة تعريفات النظام المطلوبة
RUN npx playwright install chromium --with-deps

# الأمر النهائي لتشغيل البوت فور تشغيل السيرفر
CMD ["node", "index.js"]

