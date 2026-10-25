# VERSION=2
# https://github.com/ageorge95/ag95_repo_base

<#
.SYNOPSIS
    Generic installer: creates a Python virtual environment for this repository
    and installs its requirements.

.DESCRIPTION
    Copy this file (together with update.ps1 and pip.ini) into the root of a
    repository or into an '_install' or '_install_templates' folder inside it,
    and adapt the CONFIGURATION section below. Nothing else should need
    changing.

    Steps:
      1. Find the highest installed Python that is not excluded
         (or use the one given with -Python).
      2. Optionally update Git submodules.
      3. Optionally create directory/file links.
      4. (Re)create the virtual environment, copy pip.ini into it and install
         the requirements with uv (using the indexes from pip.ini).
      5. Optionally install the repository itself in editable mode.

.PARAMETER Python
    Python interpreter to use instead of auto-detection (path or command name).
    The exclusion list still applies.

.PARAMETER NoPause
    Do not wait for Enter before exiting (useful for CI / scripting).

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File install.ps1
    powershell -ExecutionPolicy Bypass -File install.ps1 -Python C:\Python311\python.exe -NoPause
#>
[CmdletBinding()]
param(
    [string]$Python = '',
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
# CONFIGURATION
# ============================================================

# Name shown in messages. Defaults to the repository folder name.
$APP_NAME = Split-Path -Leaf $REPO_ROOT

# Virtual environment folder, relative to the repository root.
$VENV_DIR = 'venv'

# Python versions that must NOT be used. The highest remaining version is used.
#   '3.12'   excludes every 3.12.x
#   '3.12.1' excludes only 3.12.1
# Examples:
#   $INCOMPATIBLE_PYTHON = @('3.8', '3.9')
#   $INCOMPATIBLE_PYTHON = @('3.13', '3.14', '3.15')
$INCOMPATIBLE_PYTHON = @()

# Set to $true to run 'git submodule update --init --recursive --force'.
$UPDATE_SUBMODULES = $false

# Restrict the submodule update to these paths. Empty = all submodules.
# Example: $SUBMODULE_PATHS = @('batmon-ha', 'libs\other')
$SUBMODULE_PATHS = @()

# Links created after the submodule update, relative to the repository root.
# Windows doesn't allow symlinks without special privileges, so directories
# get a junction and files get a hard link.
#   Link   - path of the link to create
#   Target - path the link points to
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

# Requirement files installed with 'uv pip install -r', in order.
# Missing files are skipped with a warning.
$REQUIREMENTS_FILES = @('requirements.txt')

# Set to $true to also run 'pip install -e .' (needs setup.py or pyproject.toml).
# This registers the repository in the venv so it can be imported from anywhere,
# and 'git pull's are picked up without reinstalling.
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
    Write-Host '!!!!! INSTALLATION FAILED !!!!!' -ForegroundColor Red
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

function Test-InsideFolder {
    param([string]$Path, [string]$Folder)
    if (-not $Folder) { return $false }
    $f = [IO.Path]::GetFullPath($Folder).TrimEnd('\') + '\'
    return [IO.Path]::GetFullPath($Path).StartsWith($f, [StringComparison]::OrdinalIgnoreCase)
}

function Find-Python {
    # Collect candidate interpreters from the py launcher and from PATH.
    $paths = New-Object System.Collections.Generic.List[string]

    $launcher = Get-Command py.exe -CommandType Application -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($launcher) {
        try {
            foreach ($line in @(& $launcher.Source --list-paths 2>$null)) {
                if ("$line" -match '([A-Za-z]:\\.*?\.exe)') { $paths.Add($Matches[1]) }
            }
        }
        catch { }
    }
    foreach ($cmd in @(Get-Command python.exe, python3.exe -All -CommandType Application -ErrorAction SilentlyContinue)) {
        $paths.Add($cmd.Source)
    }

    $seen = @{}
    $found = foreach ($p in $paths) {
        if (-not (Test-Path -LiteralPath $p -PathType Leaf)) { continue }
        $full = [IO.Path]::GetFullPath($p)
        if ($seen.ContainsKey($full)) { continue }
        $seen[$full] = $true

        # Never use a virtual environment's interpreter (especially the one we are about to delete).
        if ((Test-InsideFolder $full (Join-Path $REPO_ROOT $VENV_DIR)) -or
            (Test-InsideFolder $full $env:VIRTUAL_ENV) -or
            (Test-Path -LiteralPath (Join-Path (Split-Path -Parent (Split-Path -Parent $full)) 'pyvenv.cfg'))) { continue }

        $version = Get-PythonVersion $full
        if (-not $version) { continue }

        [PSCustomObject]@{
            Version  = $version
            Excluded = Test-PythonExcluded $version
            Path     = $full
        }
    }

    $found = @($found | Sort-Object Version -Descending)
    foreach ($f in $found) {
        $status = if ($f.Excluded) { 'excluded' } else { 'ok' }
        Write-Host ("  {0,-8} {1,-9} {2}" -f $f.Version, $status, $f.Path)
    }
    return $found | Where-Object { -not $_.Excluded } | Select-Object -First 1
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
    Write-Step 'Creating links...'
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
        Write-Step "Installing $req..."
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
    Write-Step "Installing $APP_NAME in editable mode..."
    Invoke-UvPip -VenvPython $VenvPython -Arguments @('-e', '.')
}

# ============================================================
# MAIN
# ============================================================

try {
    Write-Host "Installing $APP_NAME" -ForegroundColor Green
    if (@($INCOMPATIBLE_PYTHON).Count -gt 0) {
        Write-Host "Excluded Python versions: $($INCOMPATIBLE_PYTHON -join ', ')"
    }
    $null = Get-PipConfigPath   # fail early, before anything is changed

    # --- Python selection ---
    if ($Python) {
        Write-Step "Checking the requested Python ($Python)..."
        $cmd = Get-Command $Python -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $cmd) { throw "Python '$Python' was not found." }
        $version = Get-PythonVersion $cmd.Source
        if (-not $version) { throw "'$($cmd.Source)' does not look like a working Python interpreter." }
        if (Test-PythonExcluded $version) {
            throw "Python $version is excluded for $APP_NAME (excluded: $($INCOMPATIBLE_PYTHON -join ', '))."
        }
        $PYTHON_EXE = $cmd.Source
    }
    else {
        Write-Step 'Searching for a compatible Python installation...'
        $selected = Find-Python
        if (-not $selected) {
            throw 'No compatible Python installation was found. Install a supported Python version or pass -Python <path>.'
        }
        $version = $selected.Version
        $PYTHON_EXE = $selected.Path
    }
    Write-Host "Selected Python $version : $PYTHON_EXE" -ForegroundColor Green

    # --- Submodules and links ---
    Update-Submodules
    New-RepoLinks

    # --- Virtual environment ---
    $venvPath = Join-Path $REPO_ROOT $VENV_DIR
    if (Test-Path -LiteralPath $venvPath) {
        Write-Step "Removing the existing virtual environment ($VENV_DIR)..."
        if ($env:VIRTUAL_ENV -and (Test-InsideFolder $env:VIRTUAL_ENV $venvPath)) {
            throw "The virtual environment is active in this shell. Run 'deactivate' first."
        }
        Remove-Item -LiteralPath $venvPath -Recurse -Force
    }

    Write-Step "Creating the virtual environment ($VENV_DIR)..."
    Invoke-Checked -FilePath $PYTHON_EXE -Arguments @('-m', 'venv', $VENV_DIR)

    $venvPython = Join-Path $venvPath 'Scripts\python.exe'
    if (-not (Test-Path -LiteralPath $venvPython)) { throw "Virtual environment Python not found: $venvPython" }

    # activate.bat in the repository root (hard link, symlinks need privileges).
    # activate.bat stores the venv path itself, so a link works.
    $activateBat = Join-Path $REPO_ROOT 'activate.bat'
    if (Get-Item -LiteralPath $activateBat -Force -ErrorAction SilentlyContinue) {
        Remove-Item -LiteralPath $activateBat -Force
    }
    New-Item -ItemType HardLink -Path $activateBat -Target (Join-Path $venvPath 'Scripts\activate.bat') | Out-Null

    # activate.ps1 in the repository root. A link cannot be used here:
    # Activate.ps1 derives the venv from its own location, so a link in the
    # repository root would look for the venv one level above the repository.
    # Use a small wrapper that dot-sources the real script instead.
    $activatePs1 = Join-Path $REPO_ROOT 'activate.ps1'
    if (Get-Item -LiteralPath $activatePs1 -Force -ErrorAction SilentlyContinue) {
        Remove-Item -LiteralPath $activatePs1 -Force
    }
    Set-Content -LiteralPath $activatePs1 -Encoding utf8 -Value @(
        '# Activates the virtual environment created by install.ps1.',
        ". (Join-Path `$PSScriptRoot '$VENV_DIR\Scripts\Activate.ps1')"
    )

    # --- Packages ---
    Copy-PipConfig -VenvPath $venvPath
    Install-Tools -VenvPython $venvPython
    Install-Requirements -VenvPython $venvPython
    Install-Editable -VenvPython $venvPython
}
catch {
    Stop-Script $_.Exception.Message
}

# ============================================================
# POST-INSTALLATION
# ============================================================

Write-Host ''
Write-Host '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
Write-Host ''
Write-Host "$APP_NAME installation complete." -ForegroundColor Green
Write-Host ''
Write-Host "Activate $APP_NAME's Python virtual environment with:"
Write-Host "  cmd        : activate"
Write-Host "  PowerShell : .\activate.ps1"
Write-Host "and run 'deactivate' to leave it."
Write-Host ''
Write-Host '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'

Exit-Script 0
