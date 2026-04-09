# Mata todos os processos que estiverem escutando na porta 5001
$pids = (netstat -ano | Select-String ":5001\s" | ForEach-Object {
    ($_ -split '\s+')[-1]
} | Sort-Object -Unique)

foreach ($p in $pids) {
    if ($p -match '^\d+$' -and $p -ne '0') {
        Write-Host "Encerrando processo PID $p (porta 5001)..."
        Stop-Process -Id ([int]$p) -Force -ErrorAction SilentlyContinue
    }
}

if ($pids) { Start-Sleep -Seconds 1 }

Write-Host "Iniciando servidor Flask..."
& ".\venv\Scripts\python.exe" app.py
