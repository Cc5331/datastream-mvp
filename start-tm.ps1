$jars = (Get-ChildItem "D:\code\flink-1.18.1\lib" -Filter "*.jar" | Select-Object -ExpandProperty FullName) -join ";"
$cp = "$jars;D:\code\flink-1.18.1\conf"
Write-Host "Starting Flink TaskManager..." -NoNewline
$p = Start-Process -WindowStyle Hidden -FilePath "java" -ArgumentList @("-cp",$cp,"-Dlog.file=D:\code\flink-1.18.1\log\flink-taskmanager.log","org.apache.flink.taskexecutor.TaskManagerRunner","--configDir","D:\code\flink-1.18.1\conf") -PassThru
Start-Sleep -Seconds 5
if ($p.HasExited) {
    Write-Host " FAILED (exited with code $($p.ExitCode))"
} else {
    Write-Host " OK (PID: $($p.Id))"
}
