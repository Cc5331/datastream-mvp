@echo off
chcp 65001 >nul
echo ============================================
echo  DataStream MVP - Startup Script
echo ============================================

set FLINK_HOME=D:\code\flink-1.18.1
set FLINK_CONF_DIR=%FLINK_HOME%\conf
set BACKEND_DIR=D:\code\??\2026?????\backend

echo [1/4] Checking Flink cluster...
curl -s -o nul http://localhost:8081/overview
if %ERRORLEVEL% neq 0 (
    echo Starting Flink cluster...
    call "%FLINK_HOME%\bin\start-cluster.bat"
    timeout /t 5 /nobreak >nul
) else (
    echo Flink cluster is RUNNING
)

echo [2/4] Starting SQL Gateway...
cd "%FLINK_HOME%"
set FLINK_CONF_DIR=%FLINK_HOME%\conf
start "Flink SQL Gateway" /B java -cp "%FLINK_HOME%\lib\*" org.apache.flink.table.gateway.SqlGateway -D sql-gateway.endpoint.rest.address=localhost -D sql-gateway.endpoint.rest.port=8083
timeout /t 3 /nobreak >nul
echo SQL Gateway started on port 8083

echo [3/4] Starting Backend...
cd "%BACKEND_DIR%"
start "DataStream Backend" cmd /c "mvn spring-boot:run"
echo Backend starting on http://localhost:8080

echo [4/4] Starting Frontend...
start "DataStream Frontend" cmd /c "npx serve D:\code\??\2026?????\frontend\public -l 3000"

echo.
echo ============================================
echo  System Starting...
echo  Backend:  http://localhost:8080
echo  Frontend: http://localhost:3000
echo  Flink UI: http://localhost:8081
echo ============================================
echo.
echo Press any key to view status...
pause >nul
echo.
echo To check Flink jobs:
echo   curl http://localhost:8081/jobs/overview
echo.
echo To check SQL Gateway:
echo   curl http://localhost:8083/v1/info
echo.
echo To check MySQL data:
echo   mysql -u root -pYOUR_MYSQL_PASSWORD -e "SELECT * FROM flink_demo.user_data;"
