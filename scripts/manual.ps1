# Dot-source this script: . ./scripts/manual.ps1
# Keep the demo variables and Send-Bid in the caller's PowerShell session.
if ($MyInvocation.InvocationName -ne '.') {
    throw 'Load this script with a leading dot: . ./scripts/manual.ps1'
}

. "$PSScriptRoot/demo.ps1"

function Send-Bid {
    param(
        [Parameter(Mandatory)]
        [long]$Amount,
        [Parameter(Mandatory)]
        [string]$Key,
        [hashtable]$Headers = $bidder,
        [string]$Url = $bidUrl
    )

    if (-not $Headers -or -not $Url) {
        throw 'Initialize this session first: . ./scripts/manual.ps1'
    }

    $requestHeaders = @{} + $Headers
    $requestHeaders['Idempotency-Key'] = $Key

    try {
        $response = Invoke-WebRequest -UseBasicParsing `
            -Method Post -Uri $Url `
            -Headers $requestHeaders `
            -ContentType 'application/json' `
            -Body (@{ amount = $Amount } | ConvertTo-Json)

        [pscustomobject]@{
            Status = [int]$response.StatusCode
            Body = $response.Content
        }
    }
    catch {
        if (-not $_.Exception.Response) {
            throw
        }
        [pscustomobject]@{
            Status = [int]$_.Exception.Response.StatusCode
            Body = $_.ErrorDetails.Message
        }
    }
}

Write-Output 'Ready. Try: Send-Bid -Amount 1110000 -Key too-low'
