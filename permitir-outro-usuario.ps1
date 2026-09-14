# Verifica - e so entao libera, se faltar - o acesso que uma segunda conta do
# mesmo PC precisa para rodar o MultiScreen sem instalar nada.
#
# Sao tres pastas, e so essas tres:
#   Python + dependencias (yt-dlp, numpy, onnxruntime, ffmpeg)   leitura
#   a pasta do app                                               leitura
#   o cache de modelos e indices do CLIP                         leitura/escrita
#
# Compartilhar em vez de duplicar poupa ~3,7 GB, e a escrita no cache faz os
# dois usuarios aproveitarem o trabalho um do outro: video indexado por um nao
# e reindexado pelo outro.
#
# Conferir primeiro importa: numa conta de administrador o acesso costuma ja
# existir por heranca, e nesse caso mexer na ACL so acrescenta entulho.
#
# Rode na conta dona das pastas.

param(
    [string]$Conta = "pOK",
    [switch]$Aplicar          # sem isto, o script apenas informa
)

$ErrorActionPreference = "Stop"

$alvos = @(
    @{ Caminho = "C:\Users\softo\AppData\Local\Programs\Python\Python312"; Direito = "RX"; O_que = "Python e dependencias" },
    @{ Caminho = "C:\Users\softo\Desktop\Repositorios\Repositorios\MultiScreen"; Direito = "RX"; O_que = "o app" },
    @{ Caminho = "C:\Users\softo\.cache\multiscreen"; Direito = "M"; O_que = "cache de modelos e indices" }
)

function Ja-Tem-Acesso($caminho, $conta, $precisaEscrever) {
    $aces = (Get-Acl $caminho).Access | Where-Object {
        $_.AccessControlType -eq "Allow" -and
        ($_.IdentityReference.Value -split "\\")[-1] -eq $conta
    }
    foreach ($ace in $aces) {
        $r = $ace.FileSystemRights.ToString()
        if ($r -match "FullControl") { return $true }
        if ($precisaEscrever) {
            if ($r -match "Modify|Write") { return $true }
        } elseif ($r -match "ReadAndExecute|Read") {
            return $true
        }
    }
    return $false
}

$falta = @()
Write-Host "Conta: $Conta`n"
foreach ($alvo in $alvos) {
    if (-not (Test-Path $alvo.Caminho)) {
        Write-Host ("  [nao existe] " + $alvo.Caminho) -ForegroundColor Yellow
        continue
    }
    if (Ja-Tem-Acesso $alvo.Caminho $Conta ($alvo.Direito -eq "M")) {
        Write-Host ("  [ja tem] " + $alvo.O_que)
    } else {
        Write-Host ("  [FALTA]  " + $alvo.O_que + " (" + $alvo.Direito + ")") -ForegroundColor Yellow
        $falta += $alvo
    }
}

if (-not $falta) {
    Write-Host "`nNada a fazer: a conta ja alcanca tudo que precisa."
    Write-Host "Se mesmo assim o app nao abre nela, o problema nao e permissao -"
    Write-Host "e o Python nao estar no PATH dessa conta, e o atalho da area de"
    Write-Host "trabalho resolve isso (ele chama o python.exe pelo caminho inteiro)."
    exit 0
}

if (-not $Aplicar) {
    Write-Host "`nRode de novo com -Aplicar para liberar o que falta."
    exit 0
}

foreach ($alvo in $falta) {
    # (OI)(CI) = vale tambem para o que houver dentro, inclusive o que for criado depois.
    $regra = "{0}:(OI)(CI){1}" -f $Conta, $alvo.Direito
    icacls $alvo.Caminho /grant $regra /C /Q | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host ("  [ok] " + $alvo.O_que)
    } else {
        Write-Host ("  [falhou] " + $alvo.Caminho) -ForegroundColor Red
    }
}
Write-Host "`nPronto."
