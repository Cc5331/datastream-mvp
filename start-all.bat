@echo off
chcp 65001 >nul
setlocal
echo ============================================
echo  DataStream MVP - Startup Script
echo ============================================

rem Resolve project root from this script location
set "PROJECT_DIR=%~dp0"
set "BACKEND_DIR=%PROJECT_DIR%backend"
set "FRONTEND_DIR=%PROJECT_DIR%frontend"
set "FLINK_HOME=D:\code\flink-1.18.1"
set "FLINK_CONF_DIR=%FLINK_HOME%\conf"
set "BACKEND_PORT=18080"
set "FLINK_PORT=18081"
set "SQL_GATEWAY_PORT=18083"
set "FRONTEND_PORT=3000"

rem Load root .env (key=value, # comments skipped) into process environment
rem so backend picks up MYSQL_*/DEEPSEEK_*/SMTP_* without manual setup.
rem NOTE: use delayed expansion + escape so values containing & (e.g. JDBC URLs with
rem &serverTimezone=...) are NOT interpreted by cmd as command separators.
setlocal EnableDelayedExpansion
if exist "%PROJECT_DIR%.env" (
    for /f "usebackq tokens=1,* delims== eol=#" %%A in ("%PROJECT_DIR%.env") do (
        if not "%%A"=="" (
            set "_EVAL=%%B"
            set "_EVAL=!_EVAL:&=^&!"
            set "%%A=!_EVAL!"
        )
    )
)

rem Force UTF-8 for every JVM started from this shell (backend / gateway / Flink)
set "MAVEN_OPTS=-Dfile.encoding=UTF-8"
if not defined JAVA_TOOL_OPTIONS set "JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8"

rem Resolve Java executable (JAVA_HOME first, then known fallback, then PATH)
set "JAVA_EXE=%JAVA_HOME%\bin\java.exe"
if not exist "%JAVA_EXE%" set "JAVA_EXE=E:\java\jdk-17.0.16+8\bin\java.exe"
if not exist "%JAVA_EXE%" set "JAVA_EXE=java"

rem ========== [1/5] Kafka (%KAFKA_PORT%) ==========
set "KAFKA_PORT=29092"
echo [1/5] Checking Kafka...
netstat -ano | findstr /C:":%KAFKA_PORT% " | findstr /C:"LISTENING" >nul
if %ERRORLEVEL% neq 0 (
    docker info >nul 2>&1
    if errorlevel 1 (
        echo   Kafka is DOWN and Docker Desktop is not running.
        echo   Start Docker Desktop, then run this script again for Kafka visualization.
    ) else (
        echo   Kafka is DOWN, starting Docker service on %KAFKA_PORT%...
        docker compose -f "%PROJECT_DIR%docker-compose.yml" up -d kafka
        ping -n 31 127.0.0.1 >nul
        netstat -ano | findstr /C:":%KAFKA_PORT% " | findstr /C:"LISTENING" >nul && echo   Kafka started on %KAFKA_PORT% || echo   Kafka FAILED - check Docker Desktop
    )
) else (
    echo   Kafka already RUNNING on %KAFKA_PORT%
)

rem ========== [2/5] Flink cluster (%FLINK_PORT%) ==========
echo [2/5] Checking Flink cluster...
netstat -ano | findstr /C:":%FLINK_PORT% " | findstr /C:"LISTENING" >nul
if %ERRORLEVEL% neq 0 (
    echo   Flink cluster is DOWN, starting on %FLINK_PORT%...
    start "Flink JobManager" /MIN "%JAVA_EXE%" -cp "%FLINK_HOME%\lib\*" -Dlog.file="%FLINK_HOME%\log\flink-jobmanager.log" org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint --configDir "%FLINK_CONF_DIR%" -Drest.port=%FLINK_PORT%
    ping -n 9 127.0.0.1 >nul
    start "Flink TaskManager" /MIN "%JAVA_EXE%" -cp "%FLINK_HOME%\lib\*" -Dlog.file="%FLINK_HOME%\log\flink-taskmanager.log" org.apache.flink.runtime.taskexecutor.TaskManagerRunner --configDir "%FLINK_CONF_DIR%"
) else (
    echo   Flink cluster already RUNNING on %FLINK_PORT%
)

rem ========== [3/5] SQL Gateway (%SQL_GATEWAY_PORT%) ==========
echo [3/5] Checking SQL Gateway...
netstat -ano | findstr /C:":%SQL_GATEWAY_PORT% " | findstr /C:"LISTENING" >nul
if %ERRORLEVEL% neq 0 (
    echo   SQL Gateway is DOWN, starting...
    start "Flink SQL Gateway" /MIN "%JAVA_EXE%" -cp "%FLINK_HOME%\lib\*" -Dlog.file="%FLINK_HOME%\log\flink-sql-gateway.log" org.apache.flink.table.gateway.SqlGateway -Dsql-gateway.endpoint.rest.port=%SQL_GATEWAY_PORT% -Drest.address=localhost -Drest.port=%FLINK_PORT%
    ping -n 11 127.0.0.1 >nul
    netstat -ano | findstr /C:":%SQL_GATEWAY_PORT% " | findstr /C:"LISTENING" >nul && echo   SQL Gateway started on %SQL_GATEWAY_PORT% || echo   SQL Gateway FAILED - check log\flink-sql-gateway.log
) else (
    echo   SQL Gateway already RUNNING on %SQL_GATEWAY_PORT%
)

rem ========== [4/5] Backend (%BACKEND_PORT%) ==========
echo [4/5] Checking Backend...
netstat -ano | findstr /C:":%BACKEND_PORT% " | findstr /C:"LISTENING" >nul
if %ERRORLEVEL%==0 goto backend_running
echo   Backend is DOWN, starting...
set "SERVER_PORT=%BACKEND_PORT%"
set "FLINK_CLUSTER_PORT=%FLINK_PORT%"
set "FLINK_SQL_GATEWAY_PORT=%SQL_GATEWAY_PORT%"
cd /d "%BACKEND_DIR%"
start "DataStream Backend" /MIN "%JAVA_EXE%" -Xmx1536m -jar "target\mvp-backend-1.0.0.jar"
cd /d "%PROJECT_DIR%"
set /a tries=0
:wait_backend
ping -n 3 127.0.0.1 >nul
netstat -ano | findstr /C:":%BACKEND_PORT% " | findstr /C:"LISTENING" >nul
if %ERRORLEVEL%==0 goto backend_up
set /a tries+=1
if %tries% lss 40 goto wait_backend
echo   Backend did not respond within 2 min - check the Backend window
goto backend_done
:backend_running
echo   Backend already RUNNING on %BACKEND_PORT%
goto backend_done
:backend_up
echo   Backend started on %BACKEND_PORT%
:backend_done

rem ========== [5/5] Frontend (%FRONTEND_PORT%) ==========
echo [5/5] Checking Frontend...
netstat -ano | findstr /C:":%FRONTEND_PORT% " | findstr /C:"LISTENING" >nul
if %ERRORLEVEL% neq 0 (
    echo   Frontend is DOWN, starting...
    cd "%FRONTEND_DIR%"
    start "DataStream Frontend" cmd /c "node serve.js"
    cd "%PROJECT_DIR%"
    ping -n 6 127.0.0.1 >nul
    netstat -ano | findstr /C:":%FRONTEND_PORT% " | findstr /C:"LISTENING" >nul && echo   Frontend started on %FRONTEND_PORT% || echo   Frontend FAILED - check serve window
) else (
    echo   Frontend already RUNNING on %FRONTEND_PORT%
)

echo.
echo ============================================
echo  Startup summary:
netstat -ano | findstr /C:":%BACKEND_PORT% " | findstr /C:"LISTENING" >nul && echo   Backend  :%BACKEND_PORT% OK || echo   Backend  :%BACKEND_PORT% DOWN
netstat -ano | findstr /C:":%FLINK_PORT% " | findstr /C:"LISTENING" >nul && echo   Flink    :%FLINK_PORT% OK || echo   Flink    :%FLINK_PORT% DOWN
netstat -ano | findstr /C:":%SQL_GATEWAY_PORT% " | findstr /C:"LISTENING" >nul && echo   SQL GW   :%SQL_GATEWAY_PORT% OK || echo   SQL GW   :%SQL_GATEWAY_PORT% DOWN
netstat -ano | findstr /C:":%KAFKA_PORT% " | findstr /C:"LISTENING" >nul && echo   Kafka    :%KAFKA_PORT% OK || echo   Kafka    :%KAFKA_PORT% DOWN
netstat -ano | findstr /C:":%FRONTEND_PORT% " | findstr /C:"LISTENING" >nul && echo   Frontend :%FRONTEND_PORT% OK || echo   Frontend :%FRONTEND_PORT% DOWN
echo ============================================
echo.
echo  Frontend: http://localhost:%FRONTEND_PORT%
echo  Backend:  http://localhost:%BACKEND_PORT%
echo  Flink UI: http://localhost:%FLINK_PORT%
echo.
if not "%1"=="-nopause" pause >nul
