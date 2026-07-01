# MultiScreen

Assista **vários vídeos ao mesmo tempo**, dividindo a tela em N players — no estilo do [multivideo.io](https://multivideo.io/).

Cole a URL de qualquer vídeo e ele aparece num player da grade. Funciona com arquivos diretos, streams, os principais sites de vídeo e — via um pequeno servidor local com [yt-dlp](https://github.com/yt-dlp/yt-dlp) — **~1800 sites**.

## Recursos

- Grade que se reorganiza sozinha (ou layout fixo de 1–4 colunas).
- Fontes suportadas:
  - Arquivos diretos: `.mp4`, `.webm`, `.mov`, etc.
  - Streams HLS `.m3u8` (via [hls.js](https://github.com/video-dev/hls.js)).
  - Embeds de **YouTube**, **Vimeo**, **Twitch** (canal/VOD/clip) e **Dailymotion**.
  - Qualquer outro site: resolvido pelo servidor com **yt-dlp**.
- **Tela cheia** da grade inteira ou de um player individual.
- **Salvar / Carregar** a grade atual (fica no navegador via `localStorage`).
- **Recarregar** um player individual (útil quando o stream trava no meio).
- Botão de **mudo** global.

## Como rodar

**Requisitos:** [Python 3](https://www.python.org/) e [yt-dlp](https://github.com/yt-dlp/yt-dlp) (`pip install yt-dlp`).

### Windows (mais fácil)

Dê um duplo clique em **`iniciar.bat`**. Ele sobe o servidor e abre o navegador em `http://localhost:8000`.

### Qualquer sistema

```bash
python server.py        # porta 8000 (ou: python server.py 8080)
```

Depois acesse **http://localhost:8000** no navegador (use um navegador moderno — Chrome, Edge, Firefox).

> Por que via servidor e não abrindo o `index.html` direto? Vários embeds (ex.: YouTube) só funcionam sob `http://`, e os ~1800 sites extras dependem do back-end (yt-dlp + proxy).

## Como funciona

- **`index.html`** — front-end (grade, players, controles). Reconhece localmente arquivos diretos, HLS e os embeds. Qualquer outro link é enviado ao back-end.
- **`server.py`** — servidor Python (só `stdlib` + `yt-dlp`):
  - `GET /api/resolve?url=` — usa o yt-dlp pra descobrir o stream real da página.
  - `GET /api/proxy?p=` — repassa o vídeo com os cabeçalhos corretos (Referer/User-Agent), libera CORS e reescreve playlists HLS para tocarem no navegador.

## Limitações

- **DRM** (Netflix, Prime Video, Disney+, etc.): não é suportado.
- Sites que exigem **login** só funcionam passando cookies ao yt-dlp.
- Para **uso pessoal** — respeite os termos de uso de cada site.
