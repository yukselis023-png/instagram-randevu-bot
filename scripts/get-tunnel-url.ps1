$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

$logs = docker compose logs cloudflared --no-color 2>$null
$url = ($logs | Select-String -Pattern 'https://[-a-z0-9]+\.trycloudflare\.com' -AllMatches | ForEach-Object { $_.Matches.Value } | Select-Object -Unique | Select-Object -Last 1)

if (-not $url) {
  Write-Host 'Tunnel URL bulunamadı. cloudflared loglarını kontrol et.' -ForegroundColor Yellow
  exit 1
}

Write-Output $url
