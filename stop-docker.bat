@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo 正在停止 DataStream MVP 容器...
docker compose down
echo.
echo 已停止。数据卷已保留，再次启动运行 start-docker.bat 即可。
echo 如需连数据一起清除，请执行: docker compose down -v
pause
