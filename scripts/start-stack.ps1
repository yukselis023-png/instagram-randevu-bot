$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

docker compose up -d --build

Write-Host 'Stack başlatıldı, servis sağlıkları bekleniyor...' -ForegroundColor Cyan

for ($i = 0; $i -lt 60; $i++) {
  try {
    $api = Invoke-RestMethod -Uri 'http://localhost:18000/health' -TimeoutSec 5
    if ($api.status -eq 'ok') { break }
  } catch {}
  Start-Sleep -Seconds 2
}

Write-Host 'booking-api sağlık durumu:' -ForegroundColor Green
Invoke-RestMethod -Uri 'http://localhost:18000/health' -TimeoutSec 10 | ConvertTo-Json -Depth 5

Write-Host ''
Write-Host 'n8n sağlık durumu:' -ForegroundColor Green
Invoke-WebRequest -Uri 'http://localhost:5678/healthz' -UseBasicParsing -TimeoutSec 10 | Select-Object StatusCode, Content | Format-List
