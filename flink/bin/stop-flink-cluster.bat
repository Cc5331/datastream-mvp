@echo off
REM ?? Flink Standalone Cluster
echo ???? Flink ?? ...
taskkill /F /FI "WINDOWTITLE eq Flink-JobManager" 2>nul
taskkill /F /FI "WINDOWTITLE eq Flink-TaskManager" 2>nul
taskkill /F /FI "WINDOWTITLE eq Flink-SQL-Gateway" 2>nul
echo Flink ?????
