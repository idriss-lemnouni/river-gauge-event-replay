param(
    [string]$Snapshot = 'data/raw/potomac-2024-01',
    [string]$RunId = ('replay-' + [Guid]::NewGuid().ToString('N').Substring(0, 12))
)

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

if ($RunId -notmatch '^[a-zA-Z0-9_-]{1,45}$') {
    throw 'RunId must be letters, digits, hyphens or underscores and at most 45 characters.'
}
$run = "runs/$RunId"
if (Test-Path -LiteralPath $run) { throw "Run already exists: $run" }

$source = Join-Path $Snapshot 'observations.jsonl'
$manifest = Join-Path $Snapshot 'manifest.json'
if (-not (Test-Path -LiteralPath $source -PathType Leaf) -or
    -not (Test-Path -LiteralPath $manifest -PathType Leaf)) {
    throw 'Provide an acquired USGS JSONL snapshot and its manifest.'
}
New-Item -ItemType Directory -Path $run | Out-Null

function Invoke-Checked {
    param([scriptblock]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "Command failed with status ${LASTEXITCODE}: $Command" }
}

$shared = @(
    'compose', 'run', '--rm', '-T', 'spark', '--master', 'local[2]',
    '--driver-memory', '1g', '--conf', 'spark.ui.enabled=false'
)
$connector = @(
    '--conf', 'spark.jars.ivy=/workspace/runs/ivy-cache',
    '--conf', 'spark.jars.ivySettings=/workspace/ivysettings.xml',
    '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6'
)
$topic = "usgs-$RunId"
$bronze = "/workspace/$run/bronze"
$checkpoint = "/workspace/$run/checkpoint"

Invoke-Checked { docker compose up -d --wait --wait-timeout 120 kafka }
foreach ($phase in @('first', 'second')) {
    Write-Output "Replay $phase into $topic"
    Invoke-Checked {
        python replay.py --source $source --manifest $manifest --topic $topic --replay-id "$RunId-$phase" |
            Tee-Object -FilePath "$run/$phase-producer.json"
    }
    Invoke-Checked {
        docker @shared @connector stream_job.py ingest --topic $topic --bronze $bronze --checkpoint $checkpoint `
            --progress "/workspace/$run/progress-$phase.json"
    }
    Invoke-Checked {
        docker @shared stream_job.py aggregate --bronze $bronze --out "/workspace/$run/$phase"
    }
}
Invoke-Checked { python verify_replay.py --run $run --manifest $manifest }
