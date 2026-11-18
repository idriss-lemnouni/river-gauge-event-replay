param([string]$RunId = 'potomac-validation-03')

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

if ($RunId -notmatch '^[a-zA-Z0-9_-]{1,45}$') { throw 'Invalid run ID.' }
$run = "runs/$RunId"
if (-not (Test-Path -LiteralPath "$run/verification.json" -PathType Leaf)) {
    throw 'Run the Kafka/Spark replay and supply its validated RunId.'
}

docker compose run --rm -T spark --master 'local[2]' --driver-memory 1g --conf 'spark.ui.enabled=false' `
    stream_job.py export-hourly --hourly "/workspace/$run/second/hourly" --out "/workspace/$run/hourly.jsonl"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker compose --profile serving up -d --wait --wait-timeout 180 cassandra redis
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

python serve_snapshot.py --hourly "$run/hourly.jsonl" --replay-proof "$run/verification.json" `
    --out "$run/serving-verification.json"
exit $LASTEXITCODE
