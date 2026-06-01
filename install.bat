@echo off
mode con: cols=68 lines=14
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m playwright install chromium
pause
