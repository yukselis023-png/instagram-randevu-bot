$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$BASE = 'https://instagram-randevu-bot.onrender.com'
$RUN_TS = (Get-Date -Format 'yyyyMMddHHmmss')
$OUT_FILE = 'C:\Users\oyunc\Desktop\instagram-randevu-bot\data\e2e_full_2026-06-07.json'

function Send-DM {
    param(
        [string]$SenderId,
        [string]$Username,
        [string]$Text,
        [string]$MsgId
    )
    if (-not $MsgId) { $MsgId = "mid_${SenderId}_$([guid]::NewGuid().ToString('N').Substring(0,12))" }
    $body = @{
        sender_id          = $SenderId
        message_text       = $Text
        instagram_username = $Username
        raw_event          = @{
            source     = 'e2e_full_test'
            platform   = 'instagram_dm'
            message_id = $MsgId
            trace_id   = "e2e_${SenderId}_$MsgId"
            message    = @{ text = $Text }
            sender     = @{ id = $SenderId }
        }
    } | ConvertTo-Json -Depth 12 -Compress

    $started = Get-Date
    try {
        $resp = Invoke-RestMethod -Uri "$BASE/api/process-instagram-message" -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 120
        return [pscustomobject]@{
            ok       = $true
            ms       = [int]((Get-Date) - $started).TotalMilliseconds
            response = $resp
            msg_id   = $MsgId
            error    = $null
        }
    }
    catch {
        return [pscustomobject]@{
            ok       = $false
            ms       = [int]((Get-Date) - $started).TotalMilliseconds
            response = $null
            msg_id   = $MsgId
            error    = $_.Exception.Message
        }
    }
}

function Get-AppointmentsForSender {
    param([string]$SenderId)
    try {
        $all = Invoke-RestMethod -Uri "$BASE/api/appointments?limit=200" -Method Get -TimeoutSec 60
        $items = if ($all.items) { $all.items } elseif ($all.appointments) { $all.appointments } else { $all }
        $mine = @($items | Where-Object {
            ($_.instagram_user_id -eq $SenderId) -or ($_.sender_id -eq $SenderId) -or ($_.user_id -eq $SenderId)
        })
        return $mine
    }
    catch {
        return @()
    }
}

function Run-Journey {
    param(
        [string]$Id,
        [string]$Name,
        [string[]]$Messages,
        [string]$Username
    )
    $sender = "TEST_E2E_${Id}_$RUN_TS"
    Write-Host "`n=== $Id : $Name (sender=$sender) ==="
    $steps = @()
    $i = 0
    foreach ($m in $Messages) {
        $i++
        Write-Host "  -> [$i] $m"
        $r = Send-DM -SenderId $sender -Username $Username -Text $m
        $reply = $null; $state = $null; $dp = $null; $handoff = $null; $shouldReply = $null
        if ($r.ok -and $r.response) {
            $reply = $r.response.reply_text
            if (-not $reply) { $reply = $r.response.outbound_text }
            $state = $r.response.conversation_state
            $dp = $r.response.decision_path
            $handoff = $r.response.handoff
            $shouldReply = $r.response.should_reply
        }
        $steps += [pscustomobject]@{
            i             = $i
            msg           = $m
            ok            = $r.ok
            ms            = $r.ms
            state         = $state
            should_reply  = $shouldReply
            handoff       = $handoff
            decision_path = $dp
            reply         = if ($reply) { $reply.Substring(0, [Math]::Min(280, $reply.Length)) } else { $null }
            error         = $r.error
        }
        Start-Sleep -Milliseconds 600
    }
    Start-Sleep -Seconds 2
    $appts = Get-AppointmentsForSender -SenderId $sender
    $finalState = if ($steps.Count -gt 0) { $steps[-1].state } else { $null }
    return [pscustomobject]@{
        id                  = $Id
        name                = $Name
        sender_id           = $sender
        messages            = $Messages.Count
        final_state         = $finalState
        appointments        = $appts
        steps               = $steps
    }
}

function Run-DuplicateJourney {
    param([string]$Id, [string]$Username)
    $sender = "TEST_E2E_${Id}_$RUN_TS"
    $msg = 'merhaba duplicate testi'
    $fixedMid = "mid_dup_${sender}"
    Write-Host "`n=== $Id : duplicate_guard (sender=$sender) ==="
    Write-Host "  -> [1] $msg (msg_id=$fixedMid)"
    $r1 = Send-DM -SenderId $sender -Username $Username -Text $msg -MsgId $fixedMid
    Start-Sleep -Milliseconds 800
    Write-Host "  -> [2] $msg (SAME msg_id=$fixedMid)"
    $r2 = Send-DM -SenderId $sender -Username $Username -Text $msg -MsgId $fixedMid

    $dp1 = if ($r1.response) { $r1.response.decision_path } else { $null }
    $dp2 = if ($r2.response) { $r2.response.decision_path } else { $null }
    $steps = @(
        [pscustomobject]@{
            i=1; msg=$msg; ok=$r1.ok; ms=$r1.ms;
            state=$r1.response.conversation_state; should_reply=$r1.response.should_reply;
            handoff=$r1.response.handoff; decision_path=$dp1;
            reply= if ($r1.response.reply_text) { $r1.response.reply_text.Substring(0,[Math]::Min(280,$r1.response.reply_text.Length)) } else { $null };
            error=$r1.error
        },
        [pscustomobject]@{
            i=2; msg=$msg; ok=$r2.ok; ms=$r2.ms;
            state=$r2.response.conversation_state; should_reply=$r2.response.should_reply;
            handoff=$r2.response.handoff; decision_path=$dp2;
            reply= if ($r2.response.reply_text) { $r2.response.reply_text.Substring(0,[Math]::Min(280,$r2.response.reply_text.Length)) } else { $null };
            error=$r2.error
        }
    )
    Start-Sleep -Seconds 2
    $appts = Get-AppointmentsForSender -SenderId $sender
    return [pscustomobject]@{
        id           = $Id
        name         = 'duplicate_guard'
        sender_id    = $sender
        messages     = 2
        final_state  = $r2.response.conversation_state
        appointments = $appts
        steps        = $steps
        duplicate_marker_present = ($dp2 -join ',' -match 'duplicate')
    }
}

$results = @()

$results += Run-Journey -Id 'J1' -Name 'happy_path' -Username 'test_j1' -Messages @(
    'merhaba',
    'web tasarım yaptırmak istiyorum',
    'Ahmet Yılmaz',
    '05551234567',
    'yarın 14:00'
)

$results += Run-Journey -Id 'J2' -Name 'full_info_one_message' -Username 'test_j2' -Messages @(
    'Yarın saat 14:00 için web tasarım görüşmesi almak istiyorum. İsmim Mehmet Kaya, numaram 05559876543'
)

$results += Run-Journey -Id 'J3' -Name 'pricing_question' -Username 'test_j3' -Messages @(
    'Web tasarım paketi ne kadar?'
)

$results += Run-Journey -Id 'J4' -Name 'service_not_in_catalog' -Username 'test_j4' -Messages @(
    'mobil uygulama yaptırmak istiyorum'
)

$results += Run-Journey -Id 'J5' -Name 'cancel_existing' -Username 'test_j5' -Messages @(
    'merhaba',
    'web tasarım yaptırmak istiyorum',
    'Ayşe Demir',
    '05553334455',
    'yarın 15:00',
    'randevumu iptal etmek istiyorum'
)

$results += Run-Journey -Id 'J6' -Name 'reschedule' -Username 'test_j6' -Messages @(
    'merhaba',
    'web tasarım yaptırmak istiyorum',
    'Burak Kara',
    '05556667788',
    'yarın 16:00',
    'randevumu 2 gün sonraya alabilir miyim',
    'evet uygun'
)

$results += Run-Journey -Id 'J7' -Name 'handoff_to_human' -Username 'test_j7' -Messages @(
    'operatöre bağla lütfen'
)

$results += Run-Journey -Id 'J8' -Name 'greeting_only' -Username 'test_j8' -Messages @(
    'selam'
)

$results += Run-Journey -Id 'J9' -Name 'topic_switch_after_booking' -Username 'test_j9' -Messages @(
    'merhaba',
    'web tasarım yaptırmak istiyorum',
    'Cem Aydın',
    '05557778899',
    'yarın 11:00',
    'aslında fiyat da öğrenebilir miyim'
)

$results += Run-DuplicateJourney -Id 'J10' -Username 'test_j10'

$summary = @()
$apptsCreated = @()
$passCount = 0; $failCount = 0
foreach ($r in $results) {
    $status = 'FAIL'
    $reason = $null
    switch ($r.id) {
        'J1' {
            $hasAppt = $r.appointments.Count -gt 0
            $okState = $r.final_state -in @('completed','confirmed','booked','appointment_confirmed','closed')
            if ($hasAppt -and $okState) { $status = 'PASS' } else { $reason = "appt=$hasAppt state=$($r.final_state)" }
        }
        'J2' {
            $hasAppt = $r.appointments.Count -gt 0
            if ($hasAppt) { $status = 'PASS' } else { $reason = "no_appt state=$($r.final_state)" }
        }
        'J3' {
            $replyText = ($r.steps.reply -join ' ').ToLower()
            $hasPrice = $replyText -match '12\.?900|12900'
            $noAppt = $r.appointments.Count -eq 0
            if ($hasPrice -and $noAppt) { $status = 'PASS' } else { $reason = "price=$hasPrice noAppt=$noAppt reply='$($r.steps[0].reply)'" }
        }
        'J4' {
            $replyText = ($r.steps.reply -join ' ').ToLower()
            $rejects = ($replyText -match 'hizmet|bulun|sunmu|yapm|maalesef|destek|kapsam')
            if ($rejects -and $r.appointments.Count -eq 0) { $status = 'PASS' } else { $reason = "rejects=$rejects appts=$($r.appointments.Count) reply='$($r.steps[0].reply)'" }
        }
        'J5' {
            $cancelStep = $r.steps[-1]
            $replyText = ($cancelStep.reply + '').ToLower()
            $cancelled = ($replyText -match 'iptal|kapat') -or ($r.appointments | Where-Object { $_.status -in @('cancelled','canceled','iptal') }).Count -gt 0
            if ($cancelled) { $status = 'PASS' } else { $reason = "no_cancel_indicator reply='$($cancelStep.reply)'" }
        }
        'J6' {
            $rescheduleStep = $r.steps[5]
            $replyText = ($rescheduleStep.reply + '').ToLower()
            $reschOffered = ($replyText -match 'erteleme|değiştir|yeni|alternatif|uygun|saat|tarih')
            if ($reschOffered) { $status = 'PASS' } else { $reason = "no_reschedule_offer reply='$($rescheduleStep.reply)'" }
        }
        'J7' {
            $s = $r.steps[0]
            if ($s.handoff -eq $true -or $s.state -in @('human_handoff','handoff')) { $status = 'PASS' } else { $reason = "handoff=$($s.handoff) state=$($s.state)" }
        }
        'J8' {
            $s = $r.steps[0]
            $hasReply = $s.reply -and $s.reply.Length -gt 0
            if ($hasReply -and $s.should_reply -eq $true) { $status = 'PASS' } else { $reason = "reply=$hasReply should_reply=$($s.should_reply)" }
        }
        'J9' {
            $hasAppt = $r.appointments.Count -gt 0
            $lastReply = ($r.steps[-1].reply + '').ToLower()
            $priceMentioned = $lastReply -match '12\.?900|12900|fiyat|ücret|tl'
            if ($hasAppt -and $priceMentioned) { $status = 'PASS' } else { $reason = "appt=$hasAppt price_in_last=$priceMentioned" }
        }
        'J10' {
            if ($r.duplicate_marker_present) { $status = 'PASS' } else { $reason = "dp2=$($r.steps[1].decision_path -join ',')" }
        }
    }
    if ($status -eq 'PASS') { $passCount++ } else { $failCount++ }

    foreach ($a in $r.appointments) {
        $apptsCreated += [pscustomobject]@{
            journey        = $r.id
            appointment_id = $(@($a.id, $a.appointment_id) | Where-Object { $_ } | Select-Object -First 1)
            service        = $(@($a.service_name, $a.service, $a.service_label) | Where-Object { $_ } | Select-Object -First 1)
            date           = $(@($a.appointment_date, $a.date) | Where-Object { $_ } | Select-Object -First 1)
            time           = $(@($a.appointment_time, $a.time) | Where-Object { $_ } | Select-Object -First 1)
            status         = $a.status
        }
    }

    $summary += [pscustomobject]@{
        id                   = $r.id
        name                 = $r.name
        status               = $status
        reason               = $reason
        messages             = $r.messages
        final_state          = $r.final_state
        appointment_created  = ($r.appointments.Count -gt 0)
        appointments_count   = $r.appointments.Count
        sender_id            = $r.sender_id
        steps                = $r.steps
    }
}

$verdict = if ($failCount -eq 0) { 'ALL_PASS' } elseif ($passCount -eq 0) { 'ALL_FAIL' } else { 'MIXED' }

$fixes = @()
foreach ($s in $summary | Where-Object { $_.status -eq 'FAIL' }) {
    $fixes += "$($s.id) ($($s.name)) FAIL: $($s.reason)"
}

$report = [ordered]@{
    test_id              = "E2E_FULL_260607_$RUN_TS"
    agent                = 'E2E Full Agent'
    deploy_commit        = '12d41ad'
    base_url             = $BASE
    run_at               = (Get-Date -Format 'o')
    total_journeys       = $summary.Count
    passed               = $passCount
    failed               = $failCount
    journeys             = $summary
    appointments_created = $apptsCreated
    verdict              = $verdict
    fix_recommendations  = $fixes
}

$json = $report | ConvertTo-Json -Depth 20
Set-Content -LiteralPath $OUT_FILE -Value $json -Encoding UTF8

Write-Host "`n========================================"
Write-Host "VERDICT: $verdict ($passCount/$($summary.Count) passed)"
Write-Host "REPORT: $OUT_FILE"
Write-Host "========================================"
foreach ($s in $summary) {
    $mark = if ($s.status -eq 'PASS') { 'OK' } else { 'XX' }
    $rsn = if ($s.reason) { $s.reason } else { '' }
    Write-Host ("  [{0}] {1} {2,-30} state={3} appt={4} {5}" -f $mark, $s.id, $s.name, $s.final_state, $s.appointment_created, $rsn)
}
