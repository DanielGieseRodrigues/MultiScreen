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
- **🗂 Tabs** — adiciona **todas as guias abertas do navegador** de uma vez (requer a extensão companheira, veja abaixo).

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

## Adicionar todas as guias abertas (extensão)

Uma página web não consegue ler as URLs das outras guias (restrição de segurança do navegador), então o repositório inclui uma mini-extensão em **`extension/`** que faz essa ponte.

**Instalação (uma vez só, Chrome ou Edge):**

1. Abra `chrome://extensions` (ou `edge://extensions`).
2. Ative o **Modo do desenvolvedor** (canto superior direito).
3. Clique em **Carregar sem compactação** e escolha a pasta **`extension/`** deste projeto.

**Uso:**

- No MultiScreen, clique em **🗂 Tabs** — todas as guias abertas (todas as janelas) entram na grade de uma vez.
- Ou clique no **ícone da extensão** de qualquer guia: ele acha (ou abre) o MultiScreen e joga tudo lá, sem recarregar os vídeos que já estão tocando.

Guias repetidas e as que já estão na grade são ignoradas; guias que não são vídeo mostram um erro no tile — é só fechar.

**Ela também resolve links `blob:`.** Player moderno (hls.js, dash.js…) monta o vídeo em JavaScript e entrega ao `<video>` um endereço tipo `blob:https://site.com/uuid`: isso é um ponteiro para a memória daquela guia, não um link — ninguém fora dela consegue abrir, e ele morre junto com a guia. A extensão observa quais streams cada guia baixa (`.m3u8`/`.mpd`/`.mp4`) e entrega o endereço real. Por isso ela pede acesso a todos os sites: só as URLs de mídia são guardadas, na memória da sessão do navegador, e somem quando ele fecha.

Sem a extensão, o caminho é colar a **URL da página** (a da barra de endereços): o servidor procura o manifesto do player dentro do HTML — e, se não achar, abre os `<iframe>` da página e procura lá dentro, que é onde o player quase sempre mora.

## Vídeo em `blob:`

Se o "Copiar endereço do vídeo" te deu algo assim, ele **não é um link** e nenhum programa fora daquela aba consegue abrir. Três saídas, da melhor pra pior:

1. **Cole os dois juntos, na mesma linha** — é o jeito mais confiável:

   ```
   blob:https://tubevid.site/f8501e71-df78  https://meusite.com/watch/123
   ```

   O `blob:` não vira player nenhum, mas a origem dele (`tubevid.site`) diz **qual iframe** da página segura o vídeo, e o servidor vai direto nele. Várias linhas = vários vídeos, um par por linha.

2. **Só a URL da página** — funciona quando o manifesto está no HTML ou num iframe achável.

3. **Só o `blob:`** — o app pergunta à extensão qual stream aquela aba baixou. Depende de a aba ter sido carregada com a extensão ativa: se não achar, o aviso diz exatamente o que ela viu.

## Como funciona

- **`index.html`** — front-end (grade, players, controles). Reconhece localmente arquivos diretos, HLS e os embeds. Qualquer outro link é enviado ao back-end.
- **`server.py`** — servidor Python (só `stdlib` + `yt-dlp`):
  - `GET /api/resolve?url=` — usa o yt-dlp pra descobrir o stream real da página.
  - `GET /api/scan?url=[&origin=]` — acha os `<video>` da página; se todos forem `blob:`, o `.m3u8`/`.mpd` escondido no HTML do player; se nem isso, abre os iframes da página e repete lá dentro (`origin` = de qual iframe começar).
  - `GET /api/wrap?url=&ref=` — embrulha no proxy um stream que o navegador já achou (o que a extensão descobre atrás de um `blob:`).
  - `GET /api/proxy?p=` — repassa o vídeo com os cabeçalhos corretos (Referer/User-Agent), libera CORS e reescreve playlists HLS para tocarem no navegador.

## Limitações

- **DRM** (Netflix, Prime Video, Disney+, etc.): não é suportado.
- Sites que exigem **login** só funcionam passando cookies ao yt-dlp.
- Para **uso pessoal** — respeite os termos de uso de cada site.
