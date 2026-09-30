# ============================================================
# MySQL 输出链路诊断：确认「运行中后端缺 MYSQL_PASSWORD」类故障
# 用法：pwsh -File scripts\diag-mysql-output.ps1
# 只读诊断，不改任何配置、不重启后端
# ============================================================
$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $root '.env'

Write-Host '=== 1. .env 凭据解析（与 start-all.bat 同逻辑：KEY=VALUE，# 注释） ==='
$envMap = @{}
if (Test-Path $envFile) {
    foreach ($line in Get-Content $envFile -Encoding UTF8) {
        if ($line -match '^\s*#' -or $line -notmatch '=') { continue }
        $k, $v = $line.Split('=', 2)
        # .env 写作 "KEY = VALUE"，两侧都要 trim（尾空格会让 KEY 失配）
        $envMap[$k.Trim()] = $v.Trim().Trim('"').Trim("'")
    }
    Write-Host ("  .env            : {0}" -f $envFile)
} else {
    Write-Host "  .env 不存在：$envFile"
}
$user = if ($envMap['MYSQL_USERNAME']) { $envMap['MYSQL_USERNAME'] } else { 'root' }
$pass = $envMap['MYSQL_PASSWORD']
Write-Host ("  MYSQL_USERNAME  : {0}" -f $user)
Write-Host ("  MYSQL_PASSWORD  : {0}" -f $(if ([string]::IsNullOrEmpty($pass)) { '<空！>' } else { ('已配置（{0} 位）' -f $pass.Length) }))

Write-Host ''
Write-Host '=== 2. 后端进程环境变量（后端若未加载 .env，这里为空） ==='
foreach ($n in 'MYSQL_USERNAME', 'MYSQL_PASSWORD') {
    $v = [Environment]::GetEnvironmentVariable($n, 'Process')
    Write-Host ("  当前 shell 进程 {0} : {1}" -f $n, $(if ([string]::IsNullOrEmpty($v)) { '<空>' } else { '已设置' }))
    $mv = [Environment]::GetEnvironmentVariable($n, 'Machine')
    $uv = [Environment]::GetEnvironmentVariable($n, 'User')
    Write-Host ("  机器级/用户级 {0} : {1} / {2}" -f $n, $(if ([string]::IsNullOrEmpty($mv)) { '空' } else { '已设置' }), $(if ([string]::IsNullOrEmpty($uv)) { '空' } else { '已设置' }))
}

Write-Host ''
Write-Host '=== 3. MySQL 目标库/端口探测 ==='
$mysqlExe = 'C:\MySQL\MySQL Server 8.1\bin\mysql.exe'
$metaUrl = $envMap['MYSQL_META_URL']
$db = 'flink_demo'
if ($metaUrl -match '//[^/]+/([^?;]+)') { Write-Host ("  元数据库        : {0}" -f $Matches[1]) }
Write-Host ("  控件默认目标库  : {0}（app.mysql.default-url）" -f $db)
foreach ($p in 3306, 3307) {
    $ok = Test-NetConnection -ComputerName 127.0.0.1 -Port $p -InformationLevel Quiet -WarningAction SilentlyContinue
    Write-Host ("  127.0.0.1:{0}   : {1}" -f $p, $(if ($ok) { '存活' } else { '未监听' }))
}

if (Test-Path $mysqlExe) {
    Write-Host ''
    Write-Host '=== 4. 凭据实测（用 .env 里的密码连 3306） ==='
    $sql = "SELECT CONCAT('OK ', VERSION(), ' / ', CURRENT_USER());"
    $out = & $mysqlExe --host=127.0.0.1 --port=3306 --user=$user --password=$pass --connect-timeout=5 -N -B -e $sql 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host ("  认证结果        : {0}" -f ($out -join ' '))
        $tbl = & $mysqlExe --host=127.0.0.1 --port=3306 --user=$user --password=$pass --connect-timeout=5 -N -B -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$db';" 2>&1
        Write-Host ("  {0} 库中表数量 : {1}" -f $db, ($tbl -join ' '))
    } else {
        Write-Host ("  认证失败        : {0}" -f (($out | Where-Object { $_ -notmatch 'Warning' }) -join ' '))
    }
} else {
    Write-Host ''
    Write-Host "  跳过实测：找不到 $mysqlExe"
}

Write-Host ''
Write-Host '=== 结论判读 ==='
Write-Host '  · 第 4 步认证 OK 且后端报 "using password: NO" → 后端进程没拿到 MYSQL_PASSWORD，重启时须经 start-all.bat（或先加载 .env）'
Write-Host '  · 第 4 步认证失败 → 密码本身不对，改 .env 的 MYSQL_PASSWORD'
