@echo off
chcp 65001 >nul
pip install -q -r requirements.txt
python main.py
