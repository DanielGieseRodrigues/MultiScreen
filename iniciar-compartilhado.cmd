@echo off
title MultiScreen - servidor local
REM ============================================================
REM  Lancador para uma segunda conta do mesmo PC.
REM
REM  Nao instala nada: usa o Python, as dependencias (yt-dlp,
REM  numpy, onnxruntime, ffmpeg) e o cache de modelos que ja
REM  existem no perfil do outro usuario. Sao ~3,7 GB que NAO
REM  sao duplicados em disco.
REM ============================================================

set "PYEXE=C:\Users\softo\AppData\Local\Programs\Python\Python312\python.exe"
set "APPDIR=C:\Users\softo\Desktop\Repositorios\Repositorios\MultiScreen"
set "MULTISCREEN_CACHE=C:\Users\softo\.cache\multiscreen"
set "PORTA=8000"

echo ==============================================
echo   MultiScreen
echo ==============================================
echo.

if not exist "%PYEXE%" (
  echo [x] Python nao encontrado em:
  echo     %PYEXE%
  echo.
  echo     O outro usuario pode ter movido ou desinstalado.
  pause
  exit /b 1
)

if not exist "%APPDIR%\server.py" (
  echo [x] O app nao foi encontrado em:
  echo     %APPDIR%
  pause
  exit /b 1
)

REM Uma leitura de teste diz na hora se falta permissao, em vez de
REM deixar o servidor subir e falhar so na primeira busca.
"%PYEXE%" -c "open(r'%APPDIR%\server.py','rb').read(1)" 2>nul
if errorlevel 1 (
  echo [x] Sem permissao para ler o app.
  echo     Peca para o outro usuario rodar, na conta dele:
  echo       %APPDIR%\permitir-outro-usuario.ps1
  pause
  exit /b 1
)

echo Abrindo o Chrome em http://localhost:%PORTA%
echo Deixe esta janela aberta enquanto usa o app.
echo Para PARAR: feche esta janela ou aperte Ctrl+C.
echo.

REM O navegador padrao desta maquina e o Internet Explorer, que nao
REM roda o app; por isso o Chrome e chamado pelo nome.
start "" cmd /c "timeout /t 3 >nul & start chrome http://localhost:%PORTA%"

cd /d "%APPDIR%"
"%PYEXE%" server.py %PORTA%

echo.
echo Servidor encerrado.
pause
