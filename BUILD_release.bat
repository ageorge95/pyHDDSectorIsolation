@echo off
REM VERSION=2
REM https://github.com/ageorge95/ag95_repo_base

call activate.bat
python -m pip install --upgrade pyinstaller
rem pyinstaller --windowed --icon=icon.ico main.py
pyinstaller main.spec
pause