@echo off
REM VERSION=2
REM https://github.com/ageorge95/ag95_repo_base

cd /d "%~dp0"
call activate.bat
python -m pip install --upgrade pyinstaller
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
pyinstaller pyHDDSectorIsolation.spec --noconfirm
pause
