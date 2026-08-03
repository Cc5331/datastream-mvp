@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
echo ============================================
echo  DataStream MVP - Docker 一键部署
echo ============================================

where docker >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] 未检测到 Docker，请先安装并启动 Docker Desktop
    echo         https://www.docker.com/products/docker-desktop/
    pause
    exit /b 1
)

echo [1/4] 准备 Flink 附加 jar（UDF/连接器）...
call "%~dp0docker\flink\prepare-flink-jars.bat" || exit /b 1

echo [2/4] 构建并启动全部容器（首次构建较慢）...
docker compose up -d --build
if %ERRORLEVEL% neq 0 (
    echo [ERROR] docker compose up 失败，请确认 Docker Desktop 已启动
    pause
    exit /b 1
)

echo [3/4] 等待服务就绪（约 20 秒）...
timeout /t 20 /nobreak >nul

echo [4/4] 服务状态：
docker compose ps

echo.
echo ============================================
echo  启动完成！访问地址：
echo    前端页面:      http://localhost:3000
echo    后端 API:      http://localhost:8080
echo    Flink Web UI:  http://localhost:8081
echo    SQL Gateway:   http://localhost:8083
echo    MySQL(本机):   localhost:3307   root / root123
echo    Kafka(本机):   localhost:29092
echo ============================================
echo.
echo  容器内作业节点参数（在页面上填写）：
echo    MySQL 输出 URL:  jdbc:mysql://mysql:3306/flink_demo
echo    Kafka bootstrap: kafka:9092
echo    CSV 输入路径:    /data/xxx.csv      （对应项目根目录 data）
echo    CSV 输出路径:    /output/xxx.csv    （对应项目根目录 output）
echo.
pause
