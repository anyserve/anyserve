# AnyServe CLI installer for Windows (x86_64).
# Usage:
#   irm https://anyserve.github.io/anyserve/install.ps1 | iex
# Parameters / Environment overrides:
#   -Version / $env:ANYSERVE_VERSION          Pin a release tag (e.g. v0.6.0 or 0.6.0)
#   -InstallDir / $env:ANYSERVE_INSTALL_DIR   Install directory (default: %LOCALAPPDATA%\anyserve\bin)
#   $env:ANYSERVE_REPO                        GitHub repo slug (default: anyserve/anyserve)
#   $env:ANYSERVE_BASE_URL                    Custom release download base URL

[CmdletBinding()]
param(
    [string]$Version = $env:ANYSERVE_VERSION,
    [string]$InstallDir = $env:ANYSERVE_INSTALL_DIR
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if ([System.Net.ServicePointManager]::SecurityProtocol -notmatch 'Tls12') {
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
}

$Repo = if ($env:ANYSERVE_REPO) { $env:ANYSERVE_REPO } else { 'anyserve/anyserve' }
if (-not $InstallDir) {
    $InstallDir = Join-Path $env:LOCALAPPDATA 'anyserve\bin'
}

$Arch = $env:PROCESSOR_ARCHITECTURE
if ($Arch -notin @('AMD64', 'x86_64', 'X64')) {
    throw "Unsupported Windows architecture '$Arch'. Prebuilt binaries currently support x86_64 (AMD64)."
}
$Target = 'x86_64-pc-windows-msvc'

function Resolve-LatestTag {
    param([string]$RepoSlug)
    $headers = @{ 'User-Agent' = 'anyserve-installer' }
    $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$RepoSlug/releases/latest" -Headers $headers
    if (-not $release.tag_name) {
        throw "Failed to resolve latest release tag for $RepoSlug. Specify -Version vX.Y.Z."
    }
    return [string]$release.tag_name
}

if (-not $Version) {
    $Version = Resolve-LatestTag -RepoSlug $Repo
}
$Tag = if ($Version.StartsWith('v')) { $Version } else { "v$Version" }

$Archive = "anyserve-$Tag-$Target.zip"
$Checksums = "anyserve-$Tag-SHA256SUMS.txt"
$BaseUrl = if ($env:ANYSERVE_BASE_URL) { $env:ANYSERVE_BASE_URL.TrimEnd('/') } else { "https://github.com/$Repo/releases/download/$Tag" }

$TempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("anyserve-install-" + [System.Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $TempDir -Force | Out-Null

try {
    Write-Host "Installing AnyServe $Tag ($Target)..."
    $ArchivePath = Join-Path $TempDir $Archive
    $ChecksumsPath = Join-Path $TempDir $Checksums

    Invoke-WebRequest -Uri "$BaseUrl/$Archive" -OutFile $ArchivePath -UseBasicParsing
    Invoke-WebRequest -Uri "$BaseUrl/$Checksums" -OutFile $ChecksumsPath -UseBasicParsing

    $Expected = $null
    foreach ($line in Get-Content -Path $ChecksumsPath) {
        $trimmed = $line.Trim()
        if ($trimmed -match '^([0-9a-fA-F]{64})\s+\*?(.+)$') {
            if ($Matches[2].Trim() -eq $Archive) {
                $Expected = $Matches[1].ToLowerInvariant()
                break
            }
        }
    }
    if (-not $Expected) {
        throw "Checksum entry for $Archive not found in $Checksums."
    }

    $Actual = (Get-FileHash -Path $ArchivePath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($Actual -ne $Expected) {
        throw "SHA-256 checksum mismatch for $Archive (expected $Expected, got $Actual)."
    }
    Write-Host "Verified SHA-256 checksum."

    $ExtractDir = Join-Path $TempDir 'extract'
    Expand-Archive -Path $ArchivePath -DestinationPath $ExtractDir -Force

    $BinSrc = Join-Path $ExtractDir "anyserve-$Tag-$Target\anyserve.exe"
    if (-not (Test-Path -Path $BinSrc -PathType Leaf)) {
        $BinSrc = Join-Path $ExtractDir 'anyserve.exe'
    }
    if (-not (Test-Path -Path $BinSrc -PathType Leaf)) {
        throw "Expected anyserve.exe not found inside $Archive."
    }

    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    $Dest = Join-Path $InstallDir 'anyserve.exe'
    Copy-Item -Path $BinSrc -Destination $Dest -Force
    Write-Host "Installed AnyServe to $Dest"

    $UserPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    $Segments = if ($UserPath) { $UserPath.Split(';') | Where-Object { $_ } } else { @() }
    if ($Segments -notcontains $InstallDir) {
        $NewUserPath = if ($UserPath) { "$UserPath;$InstallDir" } else { $InstallDir }
        [Environment]::SetEnvironmentVariable('Path', $NewUserPath, 'User')
        Write-Host "Added $InstallDir to your User PATH (open a new terminal if 'anyserve' is not found)."
    }
    if (($env:Path -split ';') -notcontains $InstallDir) {
        $env:Path = "$InstallDir;$env:Path"
    }

    Write-Host ""
    Write-Host "Quick start:"
    Write-Host "  anyserve serve --console-listen 127.0.0.1:3001 --auto-token"
}
finally {
    if (Test-Path -Path $TempDir) {
        Remove-Item -Path $TempDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}
