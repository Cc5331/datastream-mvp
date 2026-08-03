@echo off
setlocal enabledelayedexpansion

set FLINK_HOME=%~dp0..\
set FLINK_CONF_DIR=%FLINK_HOME%conf
set CLASS_PATH=%FLINK_HOME%lib\*

set JAVA_OPTS=-Xms256m -Xmx2048m

echo Starting Flink TaskManager...
echo FLINK_HOME: %FLINK_HOME%
echo Config: %FLINK_CONF_DIR%

"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp "%CLASS_PATH%" -Dlog.file="%FLINK_HOME%log\flink-taskmanager.log" -Dlog4j.configurationFile="file:/%FLINK_CONF_DIR%/log4j.properties" -Dlogback.configurationFile="file:/%FLINK_CONF_DIR%/logback.xml" org.apache.flink.runtime.taskexecutor.TaskManagerRunner --configDir "%FLINK_CONF_DIR%"

endlocal