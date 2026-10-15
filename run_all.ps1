param(
    [ValidatePattern('^[A-Za-z0-9_]+$')]
    [string]$RunId = ('run' + (Get-Date -Format 'yyyyMMddHHmmss'))
)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

function Invoke-DockerStep {
    param([string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker step failed with exit code $LASTEXITCODE" }
}

Invoke-DockerStep -Arguments @('compose','up','-d','--build')
Invoke-DockerStep -Arguments @('compose','exec','-T','tools','python','-u','scripts/bootstrap.py')
Invoke-DockerStep -Arguments @('compose','exec','-T','tools','python','-u','scripts/benchmark.py','--run-id',$RunId)
foreach ($script in @('pipeline.py','demos.py','duckdb_demo.py')) {
    Invoke-DockerStep -Arguments @('compose','exec','-T','tools','python','-u',"scripts/$script")
}
Invoke-DockerStep -Arguments @('compose','exec','-T','tools','python','-u','scripts/storage_audit.py','--run-id',$RunId)
Invoke-DockerStep -Arguments @('compose','exec','-T','tools','python','-u','scripts/finalize_evidence.py')

$dockerInfo = & docker info --format '{{json .}}' | ConvertFrom-Json
$environmentRecord = [ordered]@{
    captured_utc = [DateTimeOffset]::UtcNow.ToString('o')
    client_timezone = 'Europe/Moscow'
    docker_version = $dockerInfo.ServerVersion
    docker_os = $dockerInfo.OperatingSystem
    kernel = $dockerInfo.KernelVersion
    logical_cpus = $dockerInfo.NCPU
    docker_memory_bytes = $dockerInfo.MemTotal
    architecture = $dockerInfo.Architecture
    storage_driver = $dockerInfo.Driver
}
$environmentRecord | ConvertTo-Json | Set-Content -LiteralPath 'results/environment.json' -Encoding utf8
& docker compose ps -a | Set-Content -LiteralPath 'results/docker_ps.txt' -Encoding utf8
& docker compose exec -T tools pip freeze | Set-Content -LiteralPath 'results/python_packages.txt' -Encoding utf8
& docker compose exec -T trino cat /proc/cpuinfo | Select-String 'model name' | Select-Object -First 1 | Set-Content -LiteralPath 'results/cpu_model.txt' -Encoding utf8
& docker image inspect postgres:17.4 quay.io/lakekeeper/catalog:v0.14.0 pgsty/silo:RELEASE.2026-09-03T13-18-01Z trinodb/trino:483 voyaga-lakehouse-tools:1.0 --format '{{json .RepoTags}} {{json .RepoDigests}} {{.Id}}' | Set-Content -LiteralPath 'results/image_digests.txt' -Encoding utf8
Invoke-DockerStep -Arguments @('compose','exec','-T','tools','python','-u','scripts/verify_results.py','--run-id',$RunId)
Write-Output "Completed additional benchmark $RunId. Samples: $PSScriptRoot/results/benchmark_$RunId.json"
Write-Output 'The submitted report is retained; its conclusions use the main, repeat1 and review1 sessions.'
