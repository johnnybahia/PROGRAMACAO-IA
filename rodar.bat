@echo off
chcp 65001 >nul
title Otimizador de Producao

set URL_PLANILHA="https://docs.google.com/spreadsheets/d/COLE_O_ID_DA_PLANILHA_AQUI"
set CREDENCIAIS="credenciais.json"

echo Instalando dependencias...
pip install -r requirements.txt --quiet

echo.
echo Iniciando otimizador...
echo.

python otimizador.py %URL_PLANILHA% %CREDENCIAIS%

echo.
pause
