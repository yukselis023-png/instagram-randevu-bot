$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

$projectId = 'yrdGcfOKRFjTUkX8'

docker compose exec -T n8n sh -lc "n8n import:workflow --input=/files/workflows/instagram-webhook-verify.json --projectId=$projectId"
docker compose exec -T n8n sh -lc "n8n import:workflow --input=/files/workflows/instagram-message-bot.json --projectId=$projectId"

$verifyId = (docker exec ig-randevu-postgres psql -U n8n -d n8n -t -A -c "SELECT id FROM workflow_entity WHERE name='Instagram Webhook Verify' ORDER BY \"createdAt\" DESC LIMIT 1;").Trim()
$messageId = (docker exec ig-randevu-postgres psql -U n8n -d n8n -t -A -c "SELECT id FROM workflow_entity WHERE name='Instagram Message Bot' ORDER BY \"createdAt\" DESC LIMIT 1;").Trim()

if (-not $verifyId) { throw 'Verify workflow ID bulunamadı.' }
if (-not $messageId) { throw 'Message workflow ID bulunamadı.' }

docker compose exec -T n8n n8n update:workflow --id=$verifyId --active=true
docker compose exec -T n8n n8n update:workflow --id=$messageId --active=true
docker compose restart n8n

Write-Host "Workflow import ve aktivasyon tamamlandı. Verify ID: $verifyId | Message ID: $messageId" -ForegroundColor Green
