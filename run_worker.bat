@echo off
mode con: cols=68 lines=14
python -m workers.crawl_worker
pause
