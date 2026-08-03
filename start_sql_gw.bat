@echo off
start /B "" "E:\java\jdk-17.0.16+8\bin\java.exe" -cp "D:\code\flink-1.18.1\lib\*" org.apache.flink.table.gateway.SqlGateway --port 8083
