# Local failure exercise: briefly stops Kafka and always starts it again.
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/demo.ps1" -BaseUrl 'http://127.0.0.1:8080'
$apiContainer = docker compose ps -q api
$apiRestarts = docker inspect --format '{{.RestartCount}}' $apiContainer
try {
    docker compose stop kafka
    if ($LASTEXITCODE -ne 0) { throw 'Could not stop Kafka' }
    $until = [DateTime]::UtcNow.AddSeconds(30)
    do {
        $status = Invoke-RestMethod "$BaseUrl/status" -TimeoutSec 5
        if ($status.consumer.state -eq 'retrying') { break }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $until)
    if ($status.consumer.state -ne 'retrying') { throw 'Consumer did not report retrying' }
    $bidder['Idempotency-Key'] = [guid]::NewGuid().ToString()
    $accepted = Invoke-RestMethod -Method Post -Uri $bidUrl -Headers $bidder -ContentType 'application/json' -Body '{"amount":1150000}' -TimeoutSec 5
    $ready = Invoke-RestMethod "$BaseUrl/health/ready" -TimeoutSec 5
    $status = Invoke-RestMethod "$BaseUrl/status" -TimeoutSec 5
    if ($accepted.amount -ne 1150000 -or $ready.status -ne 'ready' -or $status.outbox.pending -lt 1) { throw 'Expected an accepted bid, a ready API, and a queued event while Kafka was stopped' }
    Write-Output "Kafka down: REST ready, bid accepted, pending=$($status.outbox.pending), consumer=$($status.consumer.state)"
}
finally {
    docker compose start kafka
    if ($LASTEXITCODE -ne 0) { throw 'Could not restart Kafka; run docker compose start kafka' }
}
$until = [DateTime]::UtcNow.AddSeconds(90)
do {
    $status = Invoke-RestMethod "$BaseUrl/status" -TimeoutSec 5
    if ($status.consumer.state -eq 'running' -and $status.outbox.pending -eq 0) { break }
    Start-Sleep -Seconds 1
} while ([DateTime]::UtcNow -lt $until)
if ($status.consumer.state -ne 'running' -or $status.outbox.pending -ne 0) { throw 'Recovery timed out' }
if ((docker compose ps -q api) -ne $apiContainer) { throw 'API container changed during outage' }
if ((docker inspect --format '{{.RestartCount}}' $apiContainer) -ne $apiRestarts) { throw 'API process restarted during outage' }
Write-Output "Recovered: consumer running, outbox empty, restarts=$($status.consumer.restarts), API container unchanged"
