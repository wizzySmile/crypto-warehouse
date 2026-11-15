@echo off
cd /d C:\projects\crypto-warehouse
call .venv\Scripts\activate
streamlit run crypto_dashboard.py
pause