[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("up", "status", "demo", "verify", "failure-drill", "down", "reset", "test")]
    [string]$Command = "status",
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$EnvFile = Join-Path $RepoRoot ".env"
$EnvTemplate = Join-Path $RepoRoot ".env.example"

function Set-EnvValue {
    param([string]$Name, [string]$Value)
    $lines = [System.Collections.Generic.List[string]](Get-Content -LiteralPath $EnvFile)
    $matched = $false
    for ($index = 0; $index -lt $lines.Count; $index++) {
        if ($lines[$index] -match "^$([regex]::Escape($Name))=") {
            $lines[$index] = "$Name=$Value"
            $matched = $true
            break
        }
    }
    if (-not $matched) { $lines.Add("$Name=$Value") }
    Set-Content -LiteralPath $EnvFile -Value $lines -Encoding utf8
}

function Initialize-Environment {
    if (-not (Test-Path -LiteralPath $EnvFile)) {
        Copy-Item -LiteralPath $EnvTemplate -Destination $EnvFile
        Set-EnvValue "MINIO_ROOT_PASSWORD" ([guid]::NewGuid().ToString("N"))
        Set-EnvValue "POSTGRES_PASSWORD" ([guid]::NewGuid().ToString("N"))
        Set-EnvValue "POLARIS_CLIENT_SECRET" ([guid]::NewGuid().ToString("N"))
        Set-EnvValue "GRAFANA_ADMIN_PASSWORD" ([guid]::NewGuid().ToString("N"))
        Write-Host "Created ignored .env with generated local credentials."
    }
}

function Invoke-Docker {
    param([Parameter(Mandatory)][string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Compose-Arguments {
    param([string[]]$Tail)
    return @("compose", "--env-file", $EnvFile) + $Tail
}

function Wait-Http {
    param([string]$Url, [int]$TimeoutSeconds = 300)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 5
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) { return }
        } catch {
            Start-Sleep -Seconds 3
        }
    }
    throw "Timed out waiting for $Url"
}

function Wait-FlinkCheckpointAfter {
    param([string]$ManifestPath, [int]$TimeoutSeconds = 300)
    if (-not (Test-Path -LiteralPath $ManifestPath)) {
        throw "Simulator manifest not found: $ManifestPath"
    }
    $runStart = [int64](Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json).start_time_ms
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $overview = Invoke-RestMethod -Uri "http://127.0.0.1:8081/jobs/overview" -TimeoutSec 5
        $job = $overview.jobs | Where-Object { $_.state -eq "RUNNING" } | Select-Object -First 1
        if ($null -ne $job) {
            $checkpoints = Invoke-RestMethod `
                -Uri "http://127.0.0.1:8081/jobs/$($job.jid)/checkpoints" -TimeoutSec 5
            $latest = $checkpoints.latest.completed
            if ($null -ne $latest -and [int64]$latest.trigger_timestamp -gt $runStart) {
                Write-Host "Checkpoint $($latest.id) committed after the simulator run."
                return
            }
        }
        Start-Sleep -Seconds 5
    }
    throw "Timed out waiting for a Flink checkpoint newer than $runStart"
}

function Start-Platform {
    Initialize-Environment
    Invoke-Docker (Compose-Arguments @("up", "-d", "--build"))
    Wait-Http "http://127.0.0.1:8081/overview"
    Wait-Http "http://127.0.0.1:8080/v1/info"
    Write-Host "Cold-chain platform is ready."
}

function Show-Status {
    Initialize-Environment
    Invoke-Docker (Compose-Arguments @("ps"))
    Write-Host "Flink:      http://127.0.0.1:8081"
    Write-Host "Trino:      http://127.0.0.1:8080"
    Write-Host "MinIO:      http://127.0.0.1:9001"
    Write-Host "Prometheus: http://127.0.0.1:9090"
    Write-Host "Grafana:    http://127.0.0.1:3000"
}

function Run-Verification {
    Initialize-Environment
    Invoke-Docker (Compose-Arguments @(
        "--profile", "tools", "run", "--rm", "--entrypoint", "python",
        "simulator", "-m", "tools.verify"
    ))
}

function Run-Demo {
    Start-Platform
    Invoke-Docker (Compose-Arguments @(
        "--profile", "tools", "run", "--rm", "simulator",
        "--profile", "demo", "--manifest", "data/manifest.json"
    ))
    Wait-FlinkCheckpointAfter (Join-Path $RepoRoot "data/manifest.json")
    Run-Verification
    Write-Host "Demo complete. Open Grafana at http://127.0.0.1:3000."
}

function Run-FailureDrill {
    Start-Platform
    & docker rm -f coldchain-failure-producer 2>$null | Out-Null
    $producerArguments = Compose-Arguments @(
        "--profile", "tools", "run", "-d", "--name", "coldchain-failure-producer",
        "simulator", "--profile", "benchmark", "--manifest", "data/failure-manifest.json"
    )
    $producerId = & docker @producerArguments
    if ($LASTEXITCODE -ne 0) { throw "Could not start failure-drill producer" }
    Start-Sleep -Seconds 20
    Invoke-Docker (Compose-Arguments @("kill", "-s", "SIGKILL", "taskmanager"))
    Invoke-Docker (Compose-Arguments @("up", "-d", "taskmanager"))
    & docker wait $producerId.Trim() | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Failure-drill producer failed" }
    Wait-FlinkCheckpointAfter (Join-Path $RepoRoot "data/failure-manifest.json")
    Invoke-Docker (Compose-Arguments @(
        "--profile", "tools", "run", "--rm", "-e",
        "MANIFEST_PATH=data/failure-manifest.json", "--entrypoint", "python",
        "simulator", "-m", "tools.verify"
    ))
    Invoke-Docker @("rm", "coldchain-failure-producer")
    Write-Host "Failure drill passed: TaskManager recovered and reconciliation succeeded."
}

function Run-Tests {
    Initialize-Environment
    Invoke-Docker (Compose-Arguments @("build", "simulator"))
    Invoke-Docker (Compose-Arguments @(
        "--profile", "tools", "run", "--rm", "--entrypoint", "python",
        "simulator", "-m", "pytest", "tests/python"
    ))
    Invoke-Docker @(
        "run", "--rm", "--volume", "${RepoRoot}:/workspace",
        "--volume", "coldchain-maven-cache:/root/.m2", "--workdir", "/workspace/flink-job",
        "maven:3.9.11-eclipse-temurin-17", "mvn", "-B", "test"
    )
}

Set-Location $RepoRoot
switch ($Command) {
    "up" { Start-Platform }
    "status" { Show-Status }
    "demo" { Run-Demo }
    "verify" { Run-Verification }
    "failure-drill" { Run-FailureDrill }
    "down" {
        Initialize-Environment
        Invoke-Docker (Compose-Arguments @("stop"))
        Write-Host "Services stopped; persistent volumes and bootstrap state were retained."
    }
    "reset" {
        Initialize-Environment
        if (-not $Force) {
            $confirmation = Read-Host "Type RESET to delete all local lakehouse volumes and manifests"
            if ($confirmation -cne "RESET") { throw "Reset cancelled" }
        }
        Invoke-Docker (Compose-Arguments @("down", "--volumes", "--remove-orphans"))
        Remove-Item -LiteralPath (Join-Path $RepoRoot "data/manifest.json") -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath (Join-Path $RepoRoot "data/failure-manifest.json") -Force -ErrorAction SilentlyContinue
        Write-Host "Deleted local Docker volumes and generated manifests; this cannot be recovered."
    }
    "test" { Run-Tests }
}
