@echo off
chcp 65001 >nul
REM ==========================================
REM  停止 Flink Standalone 集群
REM ==========================================

set FLINK_DIR=D:\code\flink-1.18.1
set BASH="C:\Program Files\Git\bin\bash.exe"

echo Stopping Flink Cluster...
cd /d %FLINK_DIR%
%BASH% -c "./bin/stop-cluster.sh"

echo Flink Cluster stopped.
pause
