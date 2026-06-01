@echo off
mode con: cols=68 lines=14
set WISP_PORT=5220
python api\app.py
pause
