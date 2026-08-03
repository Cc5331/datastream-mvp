@echo off
REM =====================================================
REM ?? Flink Standalone Cluster (Windows)
REM =====================================================
setlocal

set FLINK_HOME=D:\code\flink-1.18.1
set FLINK_CONF_DIR=%FLINK_HOME%\conf
set FLINK_LIB_DIR=%FLINK_HOME%\lib
set FLINK_LOG_DIR=%FLINK_HOME%\log
set FLINK_PID_DIR=%FLINK_HOME%\pid

if not exist "%FLINK_LOG_DIR%" mkdir "%FLINK_LOG_DIR%"
if not exist "%FLINK_PID_DIR%" mkdir "%FLINK_PID_DIR%"

set CLASS_PATH=%FLINK_LIB_DIR%\*;%FLINK_CONF_DIR%

echo ===================================================
echo  ?? Flink Standalone Cluster
echo  FLINK_HOME: %FLINK_HOME%
echo  Conf: %FLINK_CONF_DIR%
echo ===================================================

REM ?? JobManager
echo ???? JobManager ...
start "Flink-JobManager" /B /MIN java -cp "%CLASS_PATH%" ^
    -Dlog.file="%FLINK_LOG_DIR%\jobmanager.log" ^
    -Dlogback.configurationFile="%FLINK_CONF_DIR%\logback.xml" ^
    -Dlog4j.configurationFile="%FLINK_CONF_DIR%\log4j.properties" ^
    org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint ^
    --configDir "%FLINK_CONF_DIR%"

REM ?? JobManager ???
timeout /t 8 /nobreak >nul

REM ?? TaskManager
echo ???? TaskManager ...
start "Flink-TaskManager" /B /MIN java -cp "%CLASS_PATH%" ^
    -Dlog.file="%FLINK_LOG_DIR%\taskmanager.log" ^
    -Dlogback.configurationFile="%FLINK_CONF_DIR%\logback.xml" ^
    -Dlog4j.configurationFile="%FLINK_CONF_DIR%\log4j.properties" ^
    org.apache.flink.runtime.taskexecutor.TaskManagerRunner ^
    --configDir "%FLINK_CONF_DIR%"

REM ?? TaskManager ???
timeout /t 5 /nobreak >nul

REM ?? SQL Gateway
echo ???? SQL Gateway ...
start "Flink-SQL-Gateway" /B /MIN java -cp "%CLASS_PATH%" ^
    -Dlog.file="%FLINK_LOG_DIR%\sql-gateway.log" ^
    -Dlogback.configurationFile="%FLINK_CONF_DIR%\logback.xml" ^
    -Dlog4j.configurationFile="%FLINK_CONF_DIR%\log4j.properties" ^
    org.apache.flink.table.gateway.SqlGateway ^
    --configDir "%FLINK_CONF_DIR%"

echo.
echo Flink ???????
echo Web UI: http://localhost:8081
echo SQL Gateway: http://localhost:8083
echo.
echo ????: tasklist /FI "IMAGENAME eq java.exe"
echo ????: stop-flink-cluster.bat
