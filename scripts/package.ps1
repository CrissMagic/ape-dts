param(
    [Parameter(Position = 0)]
    [string]$TargetEnvironment,

    [Parameter(Position = 1)]
    [string]$OutputDirectory,

    [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Show-Usage {
    @"
Usage:
  .\scripts\package.ps1 <target> [output-directory]

Targets:
  centos7-x86_64  Linux x86_64, glibc 2.17 (CentOS/RHEL 7)
  linux-x86_64    Linux x86_64, glibc 2.31 or newer
  linux-arm64     Linux ARM64, glibc 2.31 or newer

Output:
  ape-dts-<version>-<target>.tar.gz
  ape-dts-<version>-<target>.tar.gz.sha256

The package contains ape-dts, ape-dts.sh, and log4rs.yaml. Task configuration is not included.
"@
}

if ($Help) {
    Show-Usage
    exit 0
}

if ([string]::IsNullOrWhiteSpace($TargetEnvironment)) {
    Show-Usage
    exit 1
}

$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$targetTriple = ""
$crossConfig = ""

switch ($TargetEnvironment) {
    "centos7-x86_64" {
        $targetTriple = "x86_64-unknown-linux-gnu"
        $crossConfig = Join-Path $repoRoot "scripts\cross\centos7-x86_64.toml"
    }
    "linux-x86_64" {
        $targetTriple = "x86_64-unknown-linux-gnu"
        $crossConfig = Join-Path $repoRoot "Cross.toml"
    }
    "linux-arm64" {
        $targetTriple = "aarch64-unknown-linux-gnu"
        $crossConfig = Join-Path $repoRoot "Cross.toml"
    }
    default {
        throw "unsupported target '$TargetEnvironment'; run .\scripts\package.ps1 -Help"
    }
}

$buildTargetDirectory = "target/package/$TargetEnvironment"

foreach ($command in @("cargo", "cross", "docker")) {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "required command '$command' was not found"
    }
}

& docker info --format "{{.ServerVersion}}" *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker is installed but the daemon is not available"
}

if (-not (Test-Path -LiteralPath $crossConfig -PathType Leaf)) {
    throw "cross config not found: $crossConfig"
}

$manifest = Get-Content -Raw -LiteralPath (Join-Path $repoRoot "dt-main\Cargo.toml")
$versionMatch = [regex]::Match($manifest, '(?m)^version\s*=\s*"([^"]+)"')
if (-not $versionMatch.Success) {
    throw "failed to read dt-main version"
}
$version = $versionMatch.Groups[1].Value

if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    $outputPath = Join-Path $repoRoot "dist"
} elseif ([System.IO.Path]::IsPathRooted($OutputDirectory)) {
    $outputPath = [System.IO.Path]::GetFullPath($OutputDirectory)
} else {
    $outputPath = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $OutputDirectory))
}

New-Item -ItemType Directory -Force -Path $outputPath | Out-Null
$stageName = ".package-$([guid]::NewGuid().ToString('N'))"
$stagePath = Join-Path $outputPath $stageName
New-Item -ItemType Directory -Path $stagePath | Out-Null

$archiveName = "ape-dts-$version-$TargetEnvironment.tar.gz"
$archivePath = Join-Path $outputPath $archiveName
$checksumPath = "$archivePath.sha256"
$hadCrossConfig = Test-Path Env:CROSS_CONFIG
$previousCrossConfig = if ($hadCrossConfig) { $env:CROSS_CONFIG } else { $null }
$previousLocation = Get-Location

try {
    Set-Location $repoRoot
    $env:CROSS_CONFIG = $crossConfig

    & cross build --locked --release --target $targetTriple --target-dir $buildTargetDirectory -p dt-main --bin dt-main --features metrics
    if ($LASTEXITCODE -ne 0) {
        throw "cross build failed for $TargetEnvironment"
    }

    $binaryPath = Join-Path $repoRoot "target\package\$TargetEnvironment\$targetTriple\release\dt-main"
    if (-not (Test-Path -LiteralPath $binaryPath -PathType Leaf)) {
        throw "build succeeded but binary was not found: $binaryPath"
    }

    Copy-Item -LiteralPath $binaryPath -Destination (Join-Path $stagePath "ape-dts")
    Copy-Item -LiteralPath (Join-Path $repoRoot "scripts\ape-dts.sh") -Destination $stagePath
    Copy-Item -LiteralPath (Join-Path $repoRoot "log4rs.yaml") -Destination $stagePath

    foreach ($path in @($archivePath, $checksumPath)) {
        if (Test-Path -LiteralPath $path) {
            Remove-Item -LiteralPath $path -Force
        }
    }

    $distMount = "type=bind,source=$outputPath,target=/dist"
    & docker run --rm --mount $distMount --env "STAGE_NAME=$stageName" --env "ARCHIVE_NAME=$archiveName" busybox:1.36.1-musl@sha256:3c6ae8008e2c2eedd141725c30b20d9c36b026eb796688f88205845ef17aa213 sh -ec 'chmod 0755 "/dist/$STAGE_NAME/ape-dts" "/dist/$STAGE_NAME/ape-dts.sh"; chmod 0644 "/dist/$STAGE_NAME/log4rs.yaml"; tar -czf "/dist/$ARCHIVE_NAME" -C "/dist/$STAGE_NAME" ape-dts ape-dts.sh log4rs.yaml'
    if ($LASTEXITCODE -ne 0) {
        throw "failed to create package archive"
    }

    $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $archivePath).Hash.ToLowerInvariant()
    Set-Content -LiteralPath $checksumPath -Encoding ascii -NoNewline -Value "$hash  $archiveName"

    Write-Output "package: $archivePath"
    Write-Output "sha256: $checksumPath"
} finally {
    Set-Location $previousLocation
    if ($hadCrossConfig) {
        $env:CROSS_CONFIG = $previousCrossConfig
    } else {
        Remove-Item Env:CROSS_CONFIG -ErrorAction SilentlyContinue
    }
    if (Test-Path -LiteralPath $stagePath) {
        Remove-Item -LiteralPath $stagePath -Recurse -Force
    }
}
