param(
    [string]$Version = $env:ANYSERVE_VERSION,
    [string]$InstallDir = $env:ANYSERVE_INSTALL_DIR,
    [string]$Repo = $(if ($env:ANYSERVE_INSTALL_REPO) { $env:ANYSERVE_INSTALL_REPO } else { "anyserve/anyserve" })
)

$ErrorActionPreference = "Stop"

if (-not $Version) {
    $Version = "latest"
}

if (-not $InstallDir) {
    $InstallDir = Join-Path $env:LOCALAPPDATA "Programs\AnyServe\bin"
}

function Get-TargetTriple {
    $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture

    switch ($arch) {
        "X64" { return "x86_64-pc-windows-msvc" }
        "Arm64" { throw "Prebuilt Windows arm64 binaries are not published; build from source instead." }
        default { throw "Unsupported Windows architecture: $arch" }
    }
}

function Resolve-VersionTag {
    param([string]$RequestedVersion, [string]$Repository)

    if ($RequestedVersion -ne "latest") {
        return $RequestedVersion
    }

    $release = Invoke-RestMethod -Headers @{
        Accept = "application/vnd.github+json"
        "User-Agent" = "anyserve-install"
    } -Uri "https://api.github.com/repos/$Repository/releases/latest"

    if (-not $release.tag_name) {
        throw "Failed to resolve latest release tag from GitHub."
    }

    return $release.tag_name
}

function Ensure-PathContains {
    param([string]$PathEntry)

    $currentUserPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $entries = @()
    if ($currentUserPath) {
        $entries = $currentUserPath -split ';' | Where-Object { $_ }
    }

    if ($entries -contains $PathEntry) {
        return
    }

    $updated = @($entries + $PathEntry) -join ';'
    [Environment]::SetEnvironmentVariable("Path", $updated, "User")
    $env:Path = "$PathEntry;$env:Path"
    Write-Host "Added $PathEntry to the user PATH."
}

$target = Get-TargetTriple
$versionTag = Resolve-VersionTag -RequestedVersion $Version -Repository $Repo
$archiveName = "anyserve-$versionTag-$target.zip"
$checksumName = "anyserve-$versionTag-SHA256SUMS.txt"
$archiveRoot = "anyserve-$versionTag-$target"
$downloadBase = "https://github.com/$Repo/releases/download/$versionTag"

$tempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("anyserve-install-" + [System.Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tempDir | Out-Null

try {
    $archivePath = Join-Path $tempDir $archiveName
    $checksumPath = Join-Path $tempDir $checksumName
    $extractDir = Join-Path $tempDir "extract"

    Write-Host "Installing AnyServe $versionTag for $target"
    Invoke-WebRequest -Headers @{ "User-Agent" = "anyserve-install" } -Uri "$downloadBase/$archiveName" -OutFile $archivePath
    Invoke-WebRequest -Headers @{ "User-Agent" = "anyserve-install" } -Uri "$downloadBase/$checksumName" -OutFile $checksumPath

    $expected = Select-String -Path $checksumPath -Pattern ([regex]::Escape($archiveName) + '$') | Select-Object -First 1
    if (-not $expected) {
        throw "Checksum entry for $archiveName not found."
    }

    $expectedHash = ($expected.Line -split '\s+')[0].ToLowerInvariant()
    $actualHash = (Get-FileHash -Path $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($expectedHash -ne $actualHash) {
        throw "Checksum verification failed for $archiveName."
    }

    Expand-Archive -Path $archivePath -DestinationPath $extractDir -Force

    $binaryPath = Join-Path $extractDir "$archiveRoot\anyserve.exe"
    if (-not (Test-Path $binaryPath)) {
        throw "Release archive did not contain $archiveRoot\anyserve.exe"
    }

    New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
    Copy-Item -Path $binaryPath -Destination (Join-Path $InstallDir "anyserve.exe") -Force
    Ensure-PathContains -PathEntry $InstallDir

    Write-Host "Installed anyserve to $(Join-Path $InstallDir 'anyserve.exe')"
    & (Join-Path $InstallDir "anyserve.exe") --version
}
finally {
    if (Test-Path $tempDir) {
        Remove-Item -Recurse -Force $tempDir
    }
}
