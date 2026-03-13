@echo off
chcp 65001 >nul
title Otimizador de Producao — VERBOSE

set URL_PLANILHA="https://docs.google.com/spreadsheets/d/1dSOzgv3GS7f7_1loh1xlB5NxLwaPgMAiM0hoiK8ezxE/edit?usp=sharing"
set CREDENCIAIS="credenciais.json"

echo Instalando dependencias...
pip install -r requirements.txt --quiet

echo.
echo Iniciando otimizador (modo verbose — passo a passo detalhado)...
echo.

python otimizador.py %URL_PLANILHA% %CREDENCIAIS% --verbose

echo.
pause
