param([string]$BaseUrl = 'http://localhost:8080')
$ErrorActionPreference = 'Stop'
$suffix = [guid]::NewGuid().ToString('N')
function New-DemoSession([string]$Role) {
    $credentials = @{ email = "$Role-$suffix@example.com"; password = 'demo-password-12345' } | ConvertTo-Json
    Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/auth/register" -ContentType 'application/json' -Body $credentials | Out-Null
    $session = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/auth/login" -ContentType 'application/json' -Body $credentials
    return @{ Authorization = "Bearer $($session.token)" }
}
$owner = New-DemoSession 'owner'
$bidder = New-DemoSession 'bidder'
$inputBody = @{
    title = 'Demo lot'
    description = 'Amounts are in kopecks'
    starting_price = 1000000
    min_increment = 50000
    starts_at = [DateTime]::UtcNow.AddMinutes(-1).ToString('o')
    ends_at = [DateTime]::UtcNow.AddMinutes(10).ToString('o')
} | ConvertTo-Json
$auction = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/auctions" -Headers $owner -ContentType 'application/json' -Body $inputBody
$bidder['Idempotency-Key'] = [guid]::NewGuid().ToString()
$bidUrl = "$BaseUrl/api/v1/auctions/$($auction.id)/bids"
$bid = Invoke-RestMethod -Method Post -Uri $bidUrl -Headers $bidder -ContentType 'application/json' -Body '{"amount":1100000}'
$replay = Invoke-RestMethod -Method Post -Uri $bidUrl -Headers $bidder -ContentType 'application/json' -Body '{"amount":1100000}'
if ($bid.id -ne $replay.id) { throw 'Retry returned a different bid ID' }
Invoke-RestMethod -Uri "$BaseUrl/api/v1/auctions/$($auction.id)" | ConvertTo-Json
Write-Output "Bid accepted and replayed with the same ID: $($bid.id)"
Write-Output "WebSocket: $($BaseUrl.Replace('http', 'ws'))/api/v1/auctions/$($auction.id)/ws"
