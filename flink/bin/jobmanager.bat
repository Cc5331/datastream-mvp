@echo off
setlocal enabledelayedexpansion

set FLINK_HOME=%~dp0..\
set FLINK_CONF_DIR=%FLINK_HOME%conf
set CLASS_PATH=%FLINK_HOME%lib\*

set JAVA_OPTS=-Xms512m -Xmx4096m

echo Starting Flink JobManager...
echo FLINK_HOME: %FLINK_HOME%
echo Config: %FLINK_CONF_DIR%

"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp "%CLASS_PATH%" -Dlog.file="%FLINK_HOME%log\flink-jobmanager.log" -Dlog4j.configurationFile="file:/%FLINK_CONF_DIR%/log4j.properties" -Dlogback.configurationFile="file:/%FLINK_CONF_DIR%/logback.xml" org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint --configDir "%FLINK_CONF_DIR%"

endlocal