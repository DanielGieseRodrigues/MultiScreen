@echo off
title MultiScreen - servidor local
cd /d "%~dp0"

echo ==============================================
echo   MultiScreen
echo ==============================================
echo.
echo Iniciando o servidor e abrindo o Chrome em:
echo   http://localhost:8000
echo.
echo Deixe esta janela aberta enquanto usa o app.
echo Para PARAR: feche esta janela ou aperte Ctrl+C.
echo.

REM Abre o Chrome ~2s depois, ja com o servidor no ar.
REM (Forcamos o Chrome porque o navegador padrao deste PC e o Internet Explorer.)
start "" cmd /c "timeout /t 2 >nul & start chrome http://localhost:8000"

REM Inicia o servidor. Tenta o launcher "py"; se nao existir, usa "python".
py server.py 8000
if errorlevel 1 python server.py 8000

echo.
echo Servidor encerrado.
pause
