# VERSION=2
# https://github.com/ageorge95/ag95_repo_base

<#
.SYNOPSIS
    Generic updater: updates the packages of this repository's existing
    Python virtual environment.

.DESCRIPTION
    Copy this file next to install.ps1 (repository root or an '_install' or
    '_install_templates' folder inside it) and keep the CONFIGURATION section
    in sync with install.ps1.

    Steps:
      1. Optionally update Git submodules.
      2. Optionally recreate directory/file links.
      3. Copy pip.ini into the virtual environment, upgrade pip/uv and upgrade
         the requirements with uv (using the indexes from pip.ini).
      4. Optionally reinstall the repository itself in editable mode.

    Run install.ps1 first if the virtual environment does not exist yet.

.PARAMETER NoPause
    Do not wait for Enter before exiting (useful for CI / scripting).

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File update.ps1
#>
[CmdletBinding()]
param(
    [switch]$NoPause
)

$ErrorActionPreference = 'Stop'

# The scripts can live in the repository root or in an '_install' or
# '_install_templates' sub-folder. In the latter cases the repository root is
# one level up.
$INSTALL_DIR_NAMES = @('_install', '_install_templates')
$REPO_ROOT = $PSScriptRoot
if ($INSTALL_DIR_NAMES -contains (Split-Path -Leaf $PSScriptRoot)) { $REPO_ROOT = Split-Path -Parent $PSScriptRoot }
Set-Location -LiteralPath $REPO_ROOT

# ============================================================
# CONFIGURATION (keep in sync with install.ps1)
# ============================================================

# Name shown in messages. Defaults to the repository folder name.
$APP_NAME = Split-Path -Leaf $REPO_ROOT

# Virtual environment folder, relative to the repository root.
$VENV_DIR = 'venv'

# Python versions that must NOT be used. Only used here to warn if the
# existing virtual environment was built with an excluded version.
#   '3.12'   excludes every 3.12.x
#   '3.12.1' excludes only 3.12.1
$INCOMPATIBLE_PYTHON = @()

# Set to $true to run 'git submodule update --init --recursive --force'.
$UPDATE_SUBMODULES = $false

# Restrict the submodule update to these paths. Empty = all submodules.
# Example: $SUBMODULE_PATHS = @('batmon-ha', 'libs\other')
$SUBMODULE_PATHS = @()

# Links recreated after the submodule update, relative to the repository root.
# Directories get a junction and files get a hard link.
# Examples:
#   $LINKS = @(
#       @{ Link = 'bmslib';         Target = 'batmon-ha\bmslib' }
#       @{ Link = 'workers\module'; Target = 'some\other\module' }
#   )
$LINKS = @()

# Package index configuration, looked up next to this script first and then in
# the repository root. It is copied
# into the venv (so plain 'pip' uses it) and its index-url, extra-index-url and
# trusted-host entries are passed to uv. Add package servers there.
$PIP_CONFIG = 'pip.ini'

# Requirement files upgraded with 'uv pip install --upgrade -r', in order.
# Missing files are skipped with a warning.
$REQUIREMENTS_FILES = @('requirements.txt')

# Set to $true to also re-run 'pip install -e .'.
$INSTALL_EDITABLE = $false

# ============================================================
# HELPERS (no changes needed below this line)
# ============================================================

function Write-Step {
    param([string]$Message)
    Write-Host ''
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Exit-Script {
    param([int]$Code)
    if (-not $NoPause) {
        Write-Host ''
        Read-Host 'Press Enter to exit' | Out-Null
    }
    exit $Code
}

function Stop-Script {
    param([string]$Message)
    Write-Host ''
    Write-Host "ERROR: $Message" -ForegroundColor Red
    Write-Host ''
    Write-Host '!!!!! UPDATE FAILED !!!!!' -ForegroundColor Red
    Exit-Script 1
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed (exit code $LASTEXITCODE): $FilePath $($Arguments -join ' ')"
    }
}

function Get-PythonVersion {
    param([string]$Exe)
    try {
        $out = @(& $Exe -c "import sys; print('%d.%d.%d' % sys.version_info[:3])" 2>$null)
        if ($LASTEXITCODE -eq 0 -and $out.Count -gt 0 -and "$($out[-1])" -match '^\d+\.\d+\.\d+$') {
            return [version]"$($out[-1])"
        }
    }
    catch { }
    return $null
}

function Test-PythonExcluded {
    param([version]$Version)
    $short = "$($Version.Major).$($Version.Minor)"
    $full = "$short.$($Version.Build)"
    foreach ($entry in $INCOMPATIBLE_PYTHON) {
        if ("$entry" -eq $short -or "$entry" -eq $full) { return $true }
    }
    return $false
}

function Update-Submodules {
    if (-not $UPDATE_SUBMODULES) { return }
    Write-Step 'Updating Git submodules...'
    if (-not (Test-Path -LiteralPath '.gitmodules')) {
        Write-Host 'No .gitmodules file found, skipping.' -ForegroundColor Yellow
        return
    }
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        throw 'git was not found in PATH.'
    }
    $gitArgs = @('submodule', 'update', '--progress', '--init', '--recursive', '--force')
    if (@($SUBMODULE_PATHS).Count -gt 0) { $gitArgs += @('--') + @($SUBMODULE_PATHS) }
    Invoke-Checked -FilePath 'git' -Arguments $gitArgs
}

function New-RepoLink {
    param([string]$Link, [string]$Target)

    $linkPath = Join-Path $REPO_ROOT $Link
    $targetPath = Join-Path $REPO_ROOT $Target

    if (-not (Test-Path -LiteralPath $targetPath)) {
        throw "Link target '$Target' was not found. Is the submodule missing? (set `$UPDATE_SUBMODULES = `$true)"
    }

    # Remove an existing link, but never a real directory.
    $existing = Get-Item -LiteralPath $linkPath -Force -ErrorAction SilentlyContinue
    if ($existing) {
        $isReparse = ($existing.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0
        if ($existing.PSIsContainer) {
            if (-not $isReparse) {
                throw "'$Link' exists and is a real directory, refusing to replace it. Remove it manually."
            }
            # Deletes only the junction itself, not the contents of its target.
            [IO.Directory]::Delete($linkPath)
        }
        else {
            Remove-Item -LiteralPath $linkPath -Force
        }
    }

    $parent = Split-Path -Parent $linkPath
    if (-not (Test-Path -LiteralPath $parent)) { New-Item -ItemType Directory -Path $parent | Out-Null }

    Write-Host "Linking $Link -> $Target"
    if (Test-Path -LiteralPath $targetPath -PathType Container) {
        New-Item -ItemType Junction -Path $linkPath -Target $targetPath | Out-Null
    }
    else {
        New-Item -ItemType HardLink -Path $linkPath -Target $targetPath | Out-Null
    }
}

function New-RepoLinks {
    if (@($LINKS).Count -eq 0) { return }
    Write-Step 'Recreating links...'
    foreach ($entry in $LINKS) { New-RepoLink -Link $entry.Link -Target $entry.Target }
}

function Get-PipConfigPath {
    # Next to this script first, then in the repository root.
    foreach ($dir in @($PSScriptRoot, $REPO_ROOT)) {
        $path = Join-Path $dir $PIP_CONFIG
        if (Test-Path -LiteralPath $path -PathType Leaf) { return $path }
    }
    throw "Pip configuration '$PIP_CONFIG' was not found. Copy the default pip.ini next to this script."
}

# Reads index-url, extra-index-url and trusted-host from a pip.ini file
# (any section, multi-line values supported) and returns the matching uv options.
function Get-UvIndexArgs {
    param([string]$ConfigPath)

    $values = @{ 'index-url' = @(); 'extra-index-url' = @(); 'trusted-host' = @() }
    $key = $null
    foreach ($line in Get-Content -LiteralPath $ConfigPath) {
        if ($line -match '^\s*([#;].*)?$') { continue }                  # blank line or comment
        if ($line -match '^\s*\[.*\]\s*$') { $key = $null; continue }    # [section]
        if ($line -match '^\s+(\S.*)$') {                                 # continuation line
            if ($key) { $values[$key] += @($Matches[1].Trim() -split '\s+') }
            continue
        }
        if ($line -match '^([^=:]+?)\s*[=:]\s*(.*)$') {                   # key = value
            $name = $Matches[1].Trim().ToLower().Replace('_', '-')
            $key = if ($values.ContainsKey($name)) { $name } else { $null }
            if ($key -and $Matches[2].Trim()) { $values[$key] += @($Matches[2].Trim() -split '\s+') }
        }
    }

    $uvArgs = @()
    if ($values['index-url'].Count -gt 0) { $uvArgs += @('--index-url', $values['index-url'][0]) }
    foreach ($url in $values['extra-index-url']) { $uvArgs += @('--extra-index-url', $url) }
    foreach ($h in $values['trusted-host']) { $uvArgs += @('--allow-insecure-host', $h) }
    # Like pip: pick the best matching version across all indexes.
    if ($values['extra-index-url'].Count -gt 0) { $uvArgs += @('--index-strategy', 'unsafe-best-match') }
    return , $uvArgs
}

# Copies pip.ini into the venv, so plain 'pip' inside the venv uses the same indexes.
function Copy-PipConfig {
    param([string]$VenvPath)
    Copy-Item -LiteralPath (Get-PipConfigPath) -Destination (Join-Path $VenvPath 'pip.ini') -Force
}

function Install-Tools {
    param([string]$VenvPython)
    Write-Step 'Upgrading pip and installing uv...'
    Invoke-Checked -FilePath $VenvPython -Arguments @('-m', 'pip', 'install', '--upgrade', 'pip', 'uv')
}

function Invoke-UvPip {
    param([string]$VenvPython, [string[]]$Arguments)
    $uvArgs = @('-m', 'uv', 'pip', 'install', '--python', $VenvPython) + $Arguments +
        (Get-UvIndexArgs (Get-PipConfigPath))
    Invoke-Checked -FilePath $VenvPython -Arguments $uvArgs
}

function Install-Requirements {
    param([string]$VenvPython, [switch]$Upgrade)
    foreach ($req in $REQUIREMENTS_FILES) {
        if (-not (Test-Path -LiteralPath $req -PathType Leaf)) {
            Write-Host "Requirements file '$req' not found, skipping." -ForegroundColor Yellow
            continue
        }
        Write-Step "Upgrading $req..."
        $reqArgs = @()
        if ($Upgrade) { $reqArgs += '--upgrade' }
        Invoke-UvPip -VenvPython $VenvPython -Arguments ($reqArgs + @('-r', $req))
    }
}

function Install-Editable {
    param([string]$VenvPython)
    if (-not $INSTALL_EDITABLE) { return }
    if (-not ((Test-Path -LiteralPath 'setup.py') -or (Test-Path -LiteralPath 'pyproject.toml'))) {
        throw 'INSTALL_EDITABLE is enabled but neither setup.py nor pyproject.toml was found.'
    }
    Write-Step "Reinstalling $APP_NAME in editable mode..."
    Invoke-UvPip -VenvPython $VenvPython -Arguments @('-e', '.')
}

# ============================================================
# MAIN
# ============================================================

try {
    Write-Host "Updating $APP_NAME" -ForegroundColor Green

    $venvPython = Join-Path $REPO_ROOT (Join-Path $VENV_DIR 'Scripts\python.exe')
    if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
        throw "Virtual environment not found ($VENV_DIR). Run install.ps1 first."
    }

    $version = Get-PythonVersion $venvPython
    if (-not $version) {
        throw "The virtual environment's Python is not working. Run install.ps1 to recreate it."
    }
    Write-Host "Virtual environment Python $version"
    if (Test-PythonExcluded $version) {
        Write-Host "WARNING: Python $version is excluded for $APP_NAME. Run install.ps1 to recreate the virtual environment." -ForegroundColor Yellow
    }

    Update-Submodules
    New-RepoLinks

    Copy-PipConfig -VenvPath (Join-Path $REPO_ROOT $VENV_DIR)
    Install-Tools -VenvPython $venvPython
    Install-Requirements -VenvPython $venvPython -Upgrade
    Install-Editable -VenvPython $venvPython
}
catch {
    Stop-Script $_.Exception.Message
}

Write-Host ''
Write-Host "$APP_NAME update complete." -ForegroundColor Green

Exit-Script 0
