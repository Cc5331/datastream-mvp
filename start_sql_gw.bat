@echo off
rem Flink SQL Gateway 单独启动（端口与 flink-conf.yaml 对齐：gateway 18083 / JobManager REST 18081）
setlocal
if not defined FLINK_HOME set "FLINK_HOME=D:\code\flink-1.18.1"
set "JAVA_EXE=%JAVA_HOME%\bin\java.exe"
if not exist "%JAVA_EXE%" set "JAVA_EXE=java"
start "Flink SQL Gateway" /MIN "%JAVA_EXE%" -cp "%FLINK_HOME%\lib\*" -Dlog.file="%FLINK_HOME%\log\flink-sql-gateway.log" org.apache.flink.table.gateway.SqlGateway -Dsql-gateway.endpoint.rest.port=18083 -Drest.address=localhost -Drest.port=18081
echo SQL Gateway starting on http://localhost:18083
