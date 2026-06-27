# Mata TODOS os python app.py e sobe um Flask na porta 5001.
$procs = Get-CimInstance Win32_Process -Filter "name='python.exe'" |
    Where-Object { $_.CommandLine -like '*app.py*' }

foreach ($p in $procs) {
    Write-Host "Encerrando PID $($p.ProcessId)..."
    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}

Start-Sleep -Seconds 2

$still = Get-NetTCPConnection -LocalPort 5001 -State Listen -ErrorAction SilentlyContinue
if ($still) {
    Write-Warning "Porta 5001 ainda em uso (PID $($still.OwningProcess)). Encerrando..."
    Stop-Process -Id $still.OwningProcess -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
}

$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root
Write-Host "Iniciando Flask em $root ..."
python app.py
