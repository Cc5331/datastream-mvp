@echo off
setlocal enabledelayedexpansion

set FLINK_HOME=%~dp0..\
set FLINK_CONF_DIR=%FLINK_HOME%conf
set CLASS_PATH=%FLINK_HOME%lib\*

set JAVA_OPTS=-Xms256m -Xmx1024m

echo Starting Flink SQL Gateway on port 8083...

"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp "%CLASS_PATH%" -Dlog.file="%FLINK_HOME%log\flink-sql-gateway.log" -Dlog4j.configurationFile="file:/%FLINK_CONF_DIR%/log4j.properties" -Dlogback.configurationFile="file:/%FLINK_CONF_DIR%/logback.xml" org.apache.flink.table.gateway.SqlGateway --port 8083

endlocal