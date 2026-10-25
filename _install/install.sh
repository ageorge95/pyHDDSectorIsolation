#!/usr/bin/env bash
# VERSION=2
# https://github.com/ageorge95/ag95_repo_base
#
# Generic installer: creates a Python virtual environment for this repository
# and installs its requirements.
#
# Copy this file (together with update.sh and pip.ini) into the root of a
# repository or into an '_install' or '_install_templates' folder inside it,
# and adapt the CONFIGURATION section below. Nothing else should need changing.
#
# Steps:
#   1. Find the highest installed Python that is not excluded
#      (or use the one given with --python).
#   2. Optionally update Git submodules.
#   3. Optionally create symlinks.
#   4. (Re)create the virtual environment, copy pip.ini into it and install
#      the requirements with uv (using the indexes from pip.ini).
#   5. Optionally install the repository itself in editable mode.
#
# Usage:
#   ./install.sh [--python <path-or-command>] [--no-pause]
#
#   --python    Python interpreter to use instead of auto-detection.
#               The exclusion list still applies.
#   --no-pause  Do not wait for Enter before exiting (CI / scripting).

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The scripts can live in the repository root or in an '_install' or
# '_install_templates' sub-folder. In the latter cases the repository root is
# one level up.
REPO_ROOT="$SCRIPT_DIR"
case "$(basename "$SCRIPT_DIR" | tr '[:upper:]' '[:lower:]')" in
    _install|_install_templates) REPO_ROOT="$(dirname "$SCRIPT_DIR")" ;;
esac
cd "$REPO_ROOT"

# ============================================================
# CONFIGURATION
# ============================================================

# Name shown in messages. Defaults to the repository folder name.
APP_NAME="$(basename "$REPO_ROOT")"

# Virtual environment folder, relative to the repository root.
VENV_DIR="venv"

# Python versions that must NOT be used. The highest remaining version is used.
#   "3.12"   excludes every 3.12.x
#   "3.12.1" excludes only 3.12.1
# Examples:
#   INCOMPATIBLE_PYTHON=("3.8" "3.9")
#   INCOMPATIBLE_PYTHON=("3.13" "3.14" "3.15")
INCOMPATIBLE_PYTHON=()

# Set to true to run 'git submodule update --init --recursive --force'.
UPDATE_SUBMODULES=false

# Restrict the submodule update to these paths. Empty = all submodules.
# Example: SUBMODULE_PATHS=("batmon-ha" "libs/other")
SUBMODULE_PATHS=()

# Symlinks created after the submodule update, relative to the repository root.
# Format: "link:target"
# Examples:
#   LINKS=(
#       "bmslib:batmon-ha/bmslib"
#       "workers/module:some/other/module"
#   )
LINKS=()

# Package index configuration, looked up next to this script first and then in
# the repository root. It is copied
# into the venv (so plain 'pip' uses it) and its index-url, extra-index-url and
# trusted-host entries are passed to uv. Add package servers there.
PIP_CONFIG="pip.ini"

# Requirement files installed with 'uv pip install -r', in order.
# Missing files are skipped with a warning.
REQUIREMENTS_FILES=("requirements.txt")

# Set to true to also run 'pip install -e .' (needs setup.py or pyproject.toml).
# This registers the repository in the venv so it can be imported from anywhere,
# and 'git pull's are picked up without reinstalling.
INSTALL_EDITABLE=false

# ============================================================
# HELPERS (no changes needed below this line)
# ============================================================

PYTHON_ARG=""
NO_PAUSE=false

while [ $# -gt 0 ]; do
    case "$1" in
        --python)    PYTHON_ARG="${2:-}"; shift 2 || { echo "ERROR: --python needs a value" >&2; exit 2; } ;;
        --python=*)  PYTHON_ARG="${1#*=}"; shift ;;
        --no-pause)  NO_PAUSE=true; shift ;;
        -h|--help)   sed -n '4,26p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)           echo "ERROR: unknown argument '$1' (see --help)" >&2; exit 2 ;;
    esac
done

if [ -t 1 ]; then
    C_RED=$'\033[31m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_CYAN=$'\033[36m'; C_OFF=$'\033[0m'
else
    C_RED=""; C_GREEN=""; C_YELLOW=""; C_CYAN=""; C_OFF=""
fi

step() { echo ""; echo "${C_CYAN}==> $*${C_OFF}"; }
warn() { echo "${C_YELLOW}$*${C_OFF}"; }
die()  { echo ""; echo "${C_RED}ERROR: $*${C_OFF}" >&2; exit 1; }

pause() {
    if [ "$NO_PAUSE" != true ] && [ -t 0 ]; then
        echo ""
        read -r -p "Press Enter to exit..." _ || true
    fi
}

on_exit() {
    local code=$?
    if [ $code -ne 0 ]; then
        echo ""
        echo "${C_RED}!!!!! INSTALLATION FAILED !!!!!${C_OFF}"
    fi
    pause
    exit $code
}
trap on_exit EXIT

# Prints "major.minor.micro" of the given interpreter, or nothing.
python_version() {
    "$1" -c "import sys; print('%d.%d.%d' % sys.version_info[:3])" 2>/dev/null | tail -n 1 | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' || true
}

is_excluded() {
    local full="$1" short="${1%.*}" entry
    for entry in ${INCOMPATIBLE_PYTHON[@]+"${INCOMPATIBLE_PYTHON[@]}"}; do
        if [ "$entry" = "$short" ] || [ "$entry" = "$full" ]; then return 0; fi
    done
    return 1
}

abs_path() {
    # Absolute path of an existing file, symlinks in the directory part resolved.
    echo "$(cd "$(dirname "$1")" && pwd -P)/$(basename "$1")"
}

# Searches PATH for python, python3 and python3.X; prints the best one.
find_python() {
    local IFS_SAVE="$IFS" dir exe ver venv_abs list="" best=""
    venv_abs="$REPO_ROOT/$VENV_DIR"

    IFS=":"
    for dir in $PATH; do
        IFS="$IFS_SAVE"
        [ -d "$dir" ] || continue
        for exe in "$dir"/python "$dir"/python3 "$dir"/python3.[0-9]*; do
            [ -f "$exe" ] && [ -x "$exe" ] || continue
            case "$exe" in *-config|*m-config|*-dbg) continue ;; esac
            # Never use a virtual environment's interpreter.
            case "$exe" in "$venv_abs"/*) continue ;; esac
            if [ -n "${VIRTUAL_ENV:-}" ]; then
                case "$exe" in "$VIRTUAL_ENV"/*) continue ;; esac
            fi
            [ -f "$(dirname "$dir")/pyvenv.cfg" ] && continue
            ver="$(python_version "$exe")"
            [ -n "$ver" ] || continue
            list="${list}${ver} ${exe}"$'\n'
        done
        IFS=":"
    done
    IFS="$IFS_SAVE"

    [ -n "$list" ] || return 0

    # Sort by version (highest first), print the table, pick the first allowed one.
    while read -r ver exe; do
        [ -n "$ver" ] || continue
        if is_excluded "$ver"; then
            printf '  %-8s %-9s %s\n' "$ver" "excluded" "$exe" >&2
        else
            printf '  %-8s %-9s %s\n' "$ver" "ok" "$exe" >&2
            [ -n "$best" ] || best="$exe"
        fi
    done < <(printf '%s' "$list" | sort -u | sort -t. -k1,1nr -k2,2nr -k3,3nr)

    echo "$best"
}

update_submodules() {
    [ "$UPDATE_SUBMODULES" = true ] || return 0
    step "Updating Git submodules..."
    if [ ! -f .gitmodules ]; then
        warn "No .gitmodules file found, skipping."
        return 0
    fi
    command -v git >/dev/null 2>&1 || die "git was not found in PATH."
    if [ ${#SUBMODULE_PATHS[@]} -gt 0 ]; then
        git submodule update --progress --init --recursive --force -- "${SUBMODULE_PATHS[@]}"
    else
        git submodule update --progress --init --recursive --force
    fi
}

create_links() {
    [ ${#LINKS[@]} -gt 0 ] || return 0
    step "Creating links..."
    local entry link target link_dir rel
    for entry in "${LINKS[@]}"; do
        link="${entry%%:*}"
        target="${entry#*:}"
        [ -n "$link" ] && [ -n "$target" ] && [ "$link" != "$entry" ] || die "Invalid LINKS entry '$entry' (expected 'link:target')."
        [ -e "$target" ] || die "Link target '$target' was not found. Is the submodule missing? (set UPDATE_SUBMODULES=true)"

        # Remove an existing link, but never a real directory or file.
        if [ -L "$link" ]; then
            rm -f "$link"
        elif [ -e "$link" ]; then
            die "'$link' exists and is not a symlink, refusing to replace it. Remove it manually."
        fi

        link_dir="$(dirname "$link")"
        mkdir -p "$link_dir"

        # Use a relative symlink so the repository can be moved.
        if rel="$(realpath --relative-to="$link_dir" "$target" 2>/dev/null)"; then :; else
            rel="$REPO_ROOT/$target"
        fi
        echo "Linking $link -> $rel"
        ln -s "$rel" "$link"
    done
}

# Finds the pip configuration (next to this script first, then in the
# repository root) and stores its path in PIP_CONFIG_PATH.
PIP_CONFIG_PATH=""
check_pip_config() {
    local dir
    for dir in "$SCRIPT_DIR" "$REPO_ROOT"; do
        if [ -f "$dir/$PIP_CONFIG" ]; then PIP_CONFIG_PATH="$dir/$PIP_CONFIG"; return 0; fi
    done
    die "Pip configuration '$PIP_CONFIG' was not found. Copy the default pip.ini next to this script."
}

# Reads index-url, extra-index-url and trusted-host from the pip.ini file
# (any section, multi-line values supported) and fills the UV_INDEX_ARGS array.
UV_INDEX_ARGS=()
read_uv_index_args() {
    local kind value has_extra=false
    UV_INDEX_ARGS=()
    while read -r kind value; do
        case "$kind" in
            index-url)       UV_INDEX_ARGS+=(--index-url "$value") ;;
            extra-index-url) UV_INDEX_ARGS+=(--extra-index-url "$value"); has_extra=true ;;
            trusted-host)    UV_INDEX_ARGS+=(--allow-insecure-host "$value") ;;
        esac
    done < <(tr -d '\r' < "$PIP_CONFIG_PATH" | awk '
        /^[ \t]*([#;].*)?$/   { next }                       # blank line or comment
        /^[ \t]*\[.*\][ \t]*$/ { key = ""; next }            # [section]
        /^[ \t]/              { if (key != "") for (i = 1; i <= NF; i++) print key, $i; next }
        {
            pos = match($0, /[=:]/); if (!pos) { key = ""; next }
            name = tolower(substr($0, 1, pos - 1)); gsub(/[ \t]/, "", name); gsub(/_/, "-", name)
            if (name != "index-url" && name != "extra-index-url" && name != "trusted-host") { key = ""; next }
            key = name; n = split(substr($0, pos + 1), parts, /[ \t]+/)
            for (i = 1; i <= n; i++) if (parts[i] != "") { print key, parts[i]; if (key == "index-url") key = "" }
        }')
    # Like pip: pick the best matching version across all indexes.
    if [ "$has_extra" = true ]; then UV_INDEX_ARGS+=(--index-strategy unsafe-best-match); fi
}

# Copies pip.ini into the venv, so plain 'pip' inside the venv uses the same indexes.
# pip reads pip.conf on Linux/macOS and pip.ini on Windows (Git Bash).
copy_pip_config() {
    local venv="$1"
    if [ -d "$venv/Scripts" ]; then cp -f "$PIP_CONFIG_PATH" "$venv/pip.ini"; else cp -f "$PIP_CONFIG_PATH" "$venv/pip.conf"; fi
}

install_tools() {
    step "Upgrading pip and installing uv..."
    "$1" -m pip install --upgrade pip uv
}

uv_pip_install() {
    local venv_python="$1"; shift
    read_uv_index_args
    "$venv_python" -m uv pip install --python "$venv_python" "$@" ${UV_INDEX_ARGS[@]+"${UV_INDEX_ARGS[@]}"}
}

install_requirements() {
    local venv_python="$1" upgrade="$2" req
    for req in ${REQUIREMENTS_FILES[@]+"${REQUIREMENTS_FILES[@]}"}; do
        if [ ! -f "$req" ]; then
            warn "Requirements file '$req' not found, skipping."
            continue
        fi
        step "Installing $req..."
        if [ "$upgrade" = true ]; then
            uv_pip_install "$venv_python" --upgrade -r "$req"
        else
            uv_pip_install "$venv_python" -r "$req"
        fi
    done
}

install_editable() {
    local venv_python="$1"
    [ "$INSTALL_EDITABLE" = true ] || return 0
    [ -f setup.py ] || [ -f pyproject.toml ] || die "INSTALL_EDITABLE is enabled but neither setup.py nor pyproject.toml was found."
    step "Installing $APP_NAME in editable mode..."
    uv_pip_install "$venv_python" -e .
}

# ============================================================
# MAIN
# ============================================================

echo "${C_GREEN}Installing $APP_NAME${C_OFF}"
if [ ${#INCOMPATIBLE_PYTHON[@]} -gt 0 ]; then
    echo "Excluded Python versions: ${INCOMPATIBLE_PYTHON[*]}"
fi
check_pip_config   # fail early, before anything is changed

# --- Python selection ---
if [ -n "$PYTHON_ARG" ]; then
    step "Checking the requested Python ($PYTHON_ARG)..."
    PYTHON_EXE="$(command -v "$PYTHON_ARG" 2>/dev/null || true)"
    [ -n "$PYTHON_EXE" ] || die "Python '$PYTHON_ARG' was not found."
    PY_VERSION="$(python_version "$PYTHON_EXE")"
    [ -n "$PY_VERSION" ] || die "'$PYTHON_EXE' does not look like a working Python interpreter."
    if is_excluded "$PY_VERSION"; then
        die "Python $PY_VERSION is excluded for $APP_NAME (excluded: ${INCOMPATIBLE_PYTHON[*]})."
    fi
else
    step "Searching for a compatible Python installation..."
    PYTHON_EXE="$(find_python)"
    [ -n "$PYTHON_EXE" ] || die "No compatible Python installation was found. Install a supported Python version or pass --python <path>."
    PY_VERSION="$(python_version "$PYTHON_EXE")"
fi
echo "${C_GREEN}Selected Python $PY_VERSION : $PYTHON_EXE${C_OFF}"

# --- Submodules and links ---
update_submodules
create_links

# --- Virtual environment ---
if [ -e "$VENV_DIR" ]; then
    step "Removing the existing virtual environment ($VENV_DIR)..."
    if [ -n "${VIRTUAL_ENV:-}" ] && [ "$(abs_path "$VIRTUAL_ENV")" = "$(abs_path "$VENV_DIR")" ]; then
        die "The virtual environment is active in this shell. Run 'deactivate' first."
    fi
    rm -rf "$VENV_DIR"
fi

step "Creating the virtual environment ($VENV_DIR)..."
if ! "$PYTHON_EXE" -m venv "$VENV_DIR"; then
    die "Could not create the virtual environment. On Debian/Ubuntu install the venv package first, e.g. 'sudo apt install python${PY_VERSION%.*}-venv'."
fi

# Linux/macOS use bin/, Python on Windows (Git Bash) uses Scripts/.
if [ -x "$VENV_DIR/bin/python" ]; then
    VENV_BIN="$VENV_DIR/bin"
    VENV_PYTHON="$VENV_BIN/python"
elif [ -f "$VENV_DIR/Scripts/python.exe" ]; then
    VENV_BIN="$VENV_DIR/Scripts"
    VENV_PYTHON="$VENV_BIN/python.exe"
else
    die "Virtual environment Python not found in $VENV_DIR."
fi

# activate.sh in the repository root (symlink to the venv's activate script).
# The activate script stores the venv path itself, so sourcing the symlink works.
rm -f activate.sh
ln -s "$VENV_BIN/activate" activate.sh 2>/dev/null || cp "$VENV_BIN/activate" activate.sh

# --- Packages ---
copy_pip_config "$VENV_DIR"
install_tools "$VENV_PYTHON"
install_requirements "$VENV_PYTHON" false
install_editable "$VENV_PYTHON"

# ============================================================
# POST-INSTALLATION
# ============================================================

echo ""
echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
echo ""
echo "${C_GREEN}$APP_NAME installation complete.${C_OFF}"
echo ""
echo "Run 'source activate.sh' to activate $APP_NAME's Python virtual environment"
echo "and 'deactivate' to leave it."
echo ""
echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"

exit 0
