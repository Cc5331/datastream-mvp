param(
    [string]$mysqlPwd = "",
    [string]$mysqlUser = "root",
    [string]$mysqlHost = "localhost:3306",
    [string]$dbName = "flink_demo"
)

$mysql = "C:\MySQL\MySQL Server 8.1\bin\mysql.exe"
if (-not (Test-Path $mysql)) { $mysql = "mysql" }

Write-Output "=========================================="
Write-Output "数据流任务管理系统 - MySQL 全流程测试脚本"
Write-Output "=========================================="
Write-Output ""

# ===== Step 1: 设置 MySQL =====
Write-Output "[Step 1/5] 设置 MySQL 数据库和表..."
$setupSql = @"
CREATE DATABASE IF NOT EXISTS $dbName DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE $dbName;
CREATE TABLE IF NOT EXISTS user_data (
  id      INT         NOT NULL,
  name    VARCHAR(255) DEFAULT NULL,
  age     INT          DEFAULT NULL,
  salary  DOUBLE       DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"@

$sqlFile = "$env:TEMP\mysql_setup.sql"
$setupSql | Out-File -FilePath $sqlFile -Encoding utf8

$pw = if ($mysqlPwd) { "-p""$mysqlPwd""" } else { "" }
$result = & $mysql -u $mysqlUser -e $setupSql.Replace("`n"," ") 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Output "  ⚠️  MySQL setup may have failed. You may need to create the table manually."
    Write-Output "  Error: $result"
    Write-Output "  File saved at: $sqlFile"
} else {
    Write-Output "  ✅  Database '$dbName' and table 'user_data' ready"
}

# ===== Step 2: 启动 Flink =====
Write-Output "[Step 2/5] 启动 Flink 集群..."
$flinkHome = "D:\code\flink-1.18.1"
if ((Get-Process -Name "java" -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -match "flink" } | Measure-Object).Count -gt 0) {
    Write-Output "  ✅  Flink cluster already running"
} else {
    Write-Output "  Starting Flink cluster..."
    $startResult = & "$flinkHome\bin\start-cluster.bat" 2>&1
    Start-Sleep -Seconds 5
    Write-Output "  Flink cluster started (check http://localhost:8081)"
}

# ===== Step 3: 启动后端 =====
Write-Output "[Step 3/5] 启动后端..."
$backendDir = "D:\code\比赛\2026省服务外包\backend"
Write-Output "  Check if backend is already running on port 8080..."
$testConn = Test-NetConnection -ComputerName localhost -Port 8080 -WarningAction SilentlyContinue 2>$null
if ($testConn.TcpTestSucceeded) {
    Write-Output "  ✅  Backend already running"
} else {
    Write-Output "  Starting backend (mvn spring-boot:run)..."
    Write-Output "  ⏳ This may take 30-60 seconds..."
    Start-Process -FilePath "cmd" -ArgumentList "/c cd /d $backendDir && mvn spring-boot:run > backend.log 2>&1" -WindowStyle Hidden
    Start-Sleep -Seconds 15
    Write-Output "  Backend starting... (check progress with: Get-Content backend.log -Tail 5)"
}

# ===== Step 4: 创建并提交作业 =====
Write-Output "[Step 4/5] 创建并提交 DAG 作业..."
$dagJson = @"
{"jobName":"Datagen`r`nMySQL`r`nTest","parallelism":1,"nodes":[{"id":"datagen_in","type":"datagen_input","label":"Datagen`r`nInput","params":{"rowsPerSecond":"3","fieldsConfig":"[{\`"name\`":\`"id\`",\`"type\`":\`"INT\`",\`"kind\`":\`"sequence\`",\`"start\`":\`"1\`",\`"end\`":\`"500\`"},{\`"name\`":\`"name\`",\`"type\`":\`"STRING\`",\`"length\`":\`"8\`"},{\`"name\`":\`"age\`",\`"type\`":\`"INT\`",\`"min\`":\`"18\`",\`"max\`":\`"80\`"},{\`"name\`":\`"salary\`",\`"type\`":\`"DOUBLE\`",\`"min\`":\`"5000\`",\`"max\`":\`"50000\`"}]"},"x":100,"y":250},{"id":"mysql_out","type":"mysql_output","label":"MySQL`r`nOutput","params":{"url":"jdbc:mysql://$mysqlHost/$dbName","table":"user_data","username":"$mysqlUser","password":"$mysqlPwd"},"x":450,"y":250}],"edges":[{"id":"e1","source":"datagen_in","target":"mysql_out","sourcePort":"out","targetPort":"in"}]}
"@

# Create job via API
$createResp = Invoke-RestMethod -Uri "http://localhost:8080/api/jobs" -Method Post -ContentType "application/json" -Body $dagJson -ErrorAction SilentlyContinue
if ($createResp -and $createResp.id) {
    $jobId = $createResp.id
    Write-Output "  ✅  Job created: ID = $jobId"
    
    # Submit job
    Start-Sleep -Seconds 2
    $submitResp = Invoke-RestMethod -Uri "http://localhost:8080/api/jobs/$jobId/submit" -Method Post -ErrorAction SilentlyContinue
    if ($submitResp) {
        Write-Output "  ✅  Job submitted! Status: $($submitResp.status)"
        Write-Output "     Flink Job ID: $($submitResp.flinkJobId)"
    } else {
        Write-Output "  ⚠️  Submit may have failed. Check logs."
    }
} else {
    Write-Output "  ⚠️  Failed to create job. Backend may not be ready yet."
    Write-Output "     Response: $createResp"
}

# ===== Step 5: 验证数据 =====
Write-Output "[Step 5/5] 验证数据..."
Start-Sleep -Seconds 10

$query = "SELECT COUNT(*) AS total FROM $dbName.user_data;"
$verifyResult = & $mysql -u $mysqlUser $pw -e $query 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Output "  ✅  MySQL query OK"
    $verifyResult
} else {
    Write-Output "  ⚠️  Could not query MySQL."
    Write-Output "     您可以在 Flink Web UI (http://localhost:8081) 查看作业状态"
    Write-Output "     然后在 MySQL 中查询: SELECT * FROM flink_demo.user_data LIMIT 20;"
}

Write-Output ""
Write-Output "=========================================="
Write-Output "测试完成!"
Write-Output "=========================================="
Write-Output "有用链接:"
Write-Output "  前端:         http://localhost:3000"
Write-Output "  Flink UI:     http://localhost:8081"
Write-Output "  后端 API:     http://localhost:8080"
Write-Output "  MySQL:        mysql -u $mysqlUser -p -h $mysqlHost"
Write-Output "  DAG 模板:     D:\code\比赛\2026省服务外包\test-resources\dags\08-datagen-to-mysql.json"
