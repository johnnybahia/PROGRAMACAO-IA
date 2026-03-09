@echo off
chcp 65001 >nul
title Otimizador de Producao

set URL_PLANILHA="https://docs.google.com/spreadsheets/d/1TP1rN4V8nz2d7pTqPXXzK4I75ROkxDv-0GMQIx6R9SU/edit?usp=drive_link"
set CREDENCIAIS="credenciais.json"

echo Instalando dependencias...
pip install -r requirements.txt --quiet

echo.
echo Iniciando otimizador...
echo.

python otimizador.py %URL_PLANILHA% %CREDENCIAIS%

echo.
pause
