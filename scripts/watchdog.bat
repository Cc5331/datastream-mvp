@echo off
rem 双击即可启动看门狗（常驻：每 30s 探一次 /api/health，连续 3 次失败自动重启后端）
rem 演示前预检：watchdog.bat -Preflight
powershell -ExecutionPolicy Bypass -NoProfile -File "%~dp0watchdog.ps1" %*
