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

## 🔗 Related — mais um vídeo parecido com os que já estão na tela

Ninguém expõe uma API de "vídeos relacionados" que sirva pra vários sites. Mas toda página
de vídeo já lista as recomendações **do próprio site** — então a parede é a consulta:

**Onda 1 — o que a página oferece.** O servidor busca o HTML de uma das páginas de onde os
tiles vieram e separa duas coisas:

- **Links de vídeo**: só os que têm **a mesma forma de URL** da página onde foram achados.
  É isso que distingue vídeo de categoria sem saber nada do site: `/video/<dígitos>/<slug>/`
  é outro vídeo, `/tags/<nome>/` não é. Segmento com `:` é namespace (`Category:`), não título.
- **Links de taxonomia**: `/tags/`, `/models/`, `/categories/`, `/characters/`, `/game/`,
  `/studio/`, `/tagged/`… — ou seja, **como o site classificou aquele vídeo**.

**Onda 2 — o tema da parede.** Cada rótulo é pontuado por **quantos títulos na tela ele
nomeia**: a atriz, o personagem ou o jogo que fica voltando ganha. Com isso o servidor busca
a **página-índice dos dois rótulos mais fortes** — em `/tags/shadowheart/` *todo* vídeo é do
tema pelo critério do próprio site, o que é melhor do que qualquer palpite por palavra.

**Ranqueamento.** Cada palavra vale quantos títulos da parede a contêm (o nome recorrente
pesa mais que o que apareceu uma vez), pares de palavras adjacentes valem bônus, e vídeo que
veio da página-índice do tema leva o bônus maior. Cada sugestão vem com o **motivo** —
`filed under tags "Shadowheart"` ou `shares orin, bhaal` — e o motivo aparece no aviso.

Cada clique adiciona um tile. As páginas ficam 10 min em cache (e as que falham, 2 min, pra
não insistir numa tag bloqueada a cada clique), então só o primeiro clique paga requisição —
e como o que já está na parede é excluído, os seguintes trazem coisas novas. Se nenhum tile
atual tiver página pra raspar (parede só de arquivos locais, links diretos ou embeds), ele
cai pras páginas de sites que você já usou.

## Pacote (.zip) e o tamanho do tile

**📦 Pack** baixa todos os vídeos da grade num único `.zip` (com o `multiscreen.json`
dentro), pra reabrir depois sem re-resolver nada: sem rede, sem yt-dlp, sem link morto.

Só que guardar os arquivos originais não resolve travamento — travamento é **decode**,
não download. Um tile 4K60 custa ~500 Mpixel/s pra decodificar, uma RTX 3060 tem **um**
motor NVDEC (satura com dois desses), e cada quadro decodificado é jogado fora pelo
scaler ao entrar numa célula de ~400px. Dez tiles 4K60 é uma parede que não toca, não
importa de onde vêm os bytes.

Por isso o pacote é **re-encodado uma vez, no tamanho em que o tile aparece**:

- **Tile size** (no modal do Pack) — `Auto` mede a célula nesta tela e neste número de
  tiles e escolhe a faixa (360p…1080p); `Original` desliga o re-encode.
- Sempre no máximo **30 fps** e **8-bit yuv420p** — 60 fps dobra o custo de decode de um
  quadradinho, e fonte 10-bit/HDR não tem decoder de hardware no navegador.
- Nunca aumenta: fonte menor que a faixa passa intacta.
- **Shrink heavy videos when loading a package** faz o mesmo **ao carregar** um `.zip`
  antigo — conserta os pacotes que você já tem, sem baixar nada de novo.

Quem re-encoda é o servidor, com a GPU (`h264_nvenc`, ~3,5× tempo real numa fonte 4K60;
cai pra `libx264` se a GPU recusar). Vinte tiles a 768x432/30 somam menos decode que um
único stream 4K60 — e os arquivos ficam ~15× menores, o que também deixa cada reinício
de loop 100% local.

## Como funciona

- **`index.html`** — front-end (grade, players, controles). Reconhece localmente arquivos diretos, HLS e os embeds. Qualquer outro link é enviado ao back-end.
- **`server.py`** — servidor Python (só `stdlib` + `yt-dlp`):
  - `GET /api/resolve?url=` — usa o yt-dlp pra descobrir o stream real da página.
  - `GET /api/scan?url=[&origin=]` — acha os `<video>` da página; se todos forem `blob:`, o `.m3u8`/`.mpd` escondido no HTML do player; se nem isso, abre os iframes da página e repete lá dentro (`origin` = de qual iframe começar).
  - `GET /api/wrap?url=&ref=` — embrulha no proxy um stream que o navegador já achou (o que a extensão descobre atrás de um `blob:`).
  - `GET /api/proxy?p=` — repassa o vídeo com os cabeçalhos corretos (Referer/User-Agent), libera CORS e reescreve playlists HLS para tocarem no navegador.
  - `POST /api/related` — recebe `{seeds, have, titles}` e devolve `{items, themes}`:
    raspa as páginas dos próprios tiles, deduz o tema pelas tags do site e busca a
    página-índice do tema pra achar mais do mesmo.
  - `POST /api/shrink?id=&h=&fps=` — re-encoda um vídeo já enviado (`/api/upload`) para
    caber num tile de `h` px de altura; devolve um `job_id`, e `GET /api/shrink/status?id=`
    dá o progresso e a URL final. Arquivo que já cabe volta na hora, sem encodar.

## Limitações

- **DRM** (Netflix, Prime Video, Disney+, etc.): não é suportado.
- Sites que exigem **login** só funcionam passando cookies ao yt-dlp.
- Para **uso pessoal** — respeite os termos de uso de cada site.
