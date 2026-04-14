$ErrorActionPreference = 'Stop'

$body = @{
  sender_id = '17890000000000000'
  message_text = 'Merhaba, yarın saat 14:00 için saç kesimi randevusu almak istiyorum. Ben Doel. Numaram 05551234567'
  instagram_username = 'doel_test'
  raw_event = @{
    source = 'local_test'
    sender = @{ id = '17890000000000000' }
    recipient = @{ id = '17840000000000000' }
    message = @{ text = 'Merhaba, yarın saat 14:00 için saç kesimi randevusu almak istiyorum. Ben Doel. Numaram 05551234567' }
  }
} | ConvertTo-Json -Depth 10

Invoke-RestMethod -Uri 'http://localhost:5678/webhook/instagram/ai-router' -Method Post -ContentType 'application/json' -Body $body | ConvertTo-Json -Depth 10
