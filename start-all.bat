@echo off
chcp 65001 >nul
setlocal
echo ============================================
echo  DataStream MVP - Startup Script
echo ============================================

rem Resolve project root from this script location
set "PROJECT_DIR=%~dp0"
set "BACKEND_DIR=%PROJECT_DIR%backend"
set "FRONTEND_DIR=%PROJECT_DIR%frontend\public"
set "FLINK_HOME=D:\code\flink-1.18.1"
set "FLINK_CONF_DIR=%FLINK_HOME%\conf"

rem Load root .env (key=value, # comments skipped) into process environment
rem so backend picks up MYSQL_*/DEEPSEEK_*/SMTP_* without manual setup
if exist "%PROJECT_DIR%.env" (
    for /f "usebackq tokens=1,* delims== eol=#" %%A in ("%PROJECT_DIR%.env") do (
        if not "%%A"=="" set "%%A=%%B"
    )
)
rem Force UTF-8 for every JVM started from this shell (backend / gateway / Flink)
set "MAVEN_OPTS=-Dfile.encoding=UTF-8"
if not defined JAVA_TOOL_OPTIONS set "JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8"

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
set "FLINK_CONF_DIR=%FLINK_HOME%\conf"
start "Flink SQL Gateway" /B java -cp "%FLINK_HOME%\lib\*" org.apache.flink.table.gateway.SqlGateway -D sql-gateway.endpoint.rest.address=localhost -D sql-gateway.endpoint.rest.port=8083
timeout /t 3 /nobreak >nul
echo SQL Gateway started on port 8083

echo [3/4] Starting Backend...
cd "%BACKEND_DIR%"
start "DataStream Backend" cmd /c "mvn spring-boot:run"
echo Backend starting on http://localhost:8080

echo [4/4] Starting Frontend...
start "DataStream Frontend" cmd /c "npx serve %FRONTEND_DIR% -l 3000"

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
echo To check MySQL data (password in .env MYSQL_PASSWORD):
echo   mysql -u root -p -e "SELECT * FROM flink_demo.user_data;"
