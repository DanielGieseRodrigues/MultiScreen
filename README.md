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
- **↗ Link original** — cada tile tem um botão que abre, em outra guia, a página de onde aquele vídeo veio.
- Botão de **mudo** global.
- **🗂 Tabs** — adiciona **todas as guias abertas do navegador** de uma vez (requer a extensão companheira, veja abaixo).
- **🎬 Compile** — corta trechos de qualquer tile e exporta tudo como um `.mp4` só. O **REC** de cada tile só aparece com o mouse em cima dele (e continua visível enquanto grava), pra não poluir a grade.
- **🎯 Moments** — acha sozinho os momentos interessantes de cada tile (corte de cena, movimento e volume, medidos por ffmpeg aqui na máquina) e transforma em compilação.
- **🔎 Find** — aba separada: você digita o nome de uma atriz e vem uma grade de fotos e
  thumbs de vídeo de vários sites, cada card abrindo a página de origem em outra guia.
- **🧠 Search** — dentro do Moments: você escreve o que procura (`kiss, explosion, close-up of a face`) e o **CLIP roda local** pra achar os segundos parecidos, em cada tile. Grátis, offline, sem chave.
- **📡 Cast** — espelha a parede na TV pela rede local **sem levar o som** (o stream não tem faixa de áudio; o fone continua sendo o padrão do Windows). Só pra TV pareada, conferida por UUID + MAC antes de cada start.

## Como rodar

**Requisitos:** [Python 3](https://www.python.org/) e [yt-dlp](https://github.com/yt-dlp/yt-dlp) (`pip install yt-dlp`).

**Opcional:** `pip install edge-tts` — libera ~160 vozes neurais (femininas e masculinas, 142 idiomas) no modo Trance, de graça e sem chave. Sem isso o Trance usa as vozes que o navegador já tem instaladas.

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

## 🔎 Find — digita um nome, vem a web em miniaturas

A aba **Find** (ao lado de **Wall**, no topo) é a única parte do MultiScreen que não começa
por uma URL: ela começa por um **nome**. Um buscador escolhe as páginas, e o servidor abre
cada uma e fica com as miniaturas.

**Sem lista de sites e sem regra por site.** O que define um card é uma regra só: um `<a>`
com um `<img>` dentro, apontando pro **mesmo site** da página (link pra fora, ali, é
anúncio). Isso pega de uma vez a grade de um tube, a página de uma performer e um índice de
galeria, sem saber qual das três está lendo — e deixa de fora menu, sidebar e banner, que
não são foto linkando pra dentro do site.

**Vídeo ou foto** sai do caminho da URL (`/gallery/`, `/photos/` de um lado; `/video/`,
`/watch/`, `/scene/` do outro) e, quando a URL não diz, o card herda o que a página é. O
tempo (`18:43`) vem do texto do próprio card, quando o site mostra.

**O que fica de fora:** link de categoria/tag/perfil (`/category/anal`, `/pornstars/fulana`)
— vem com imagem própria e viraria um card chamado "Anal"; `<script>`/`<style>`, que
carregam âncoras que não existem na página; imagem menor que 90 px, sprite, avatar, logo; e
o "thumb" que na verdade é o `.mp4` do preview de hover, que um `<img>` não sabe mostrar.
Miniatura que mesmo assim não carrega (403, link morto) some da grade: quem avisa é o
próprio navegador, no `error` da imagem.

**Miniatura passa pelo proxy.** Quase todo site desses bloqueia hotlink; pedida direto pela
página ela dá 403, pedida pelo `/api/proxy` com a **página de origem como `Referer`** ela
vem. É o mesmo proxy dos vídeos, então elas também entram no cache de RAM.

**Buscador: uma corrente, não uma escolha.** DuckDuckGo HTML → Brave → SearXNG, todos sem
chave e cada um com o botão de safe search desligado (`kp=-2`, `safesearch=off`,
`safesearch=0`), porque sem isso a busca por uma performer volta vazia. Eles limitam por IP
com frequência: quem devolve a página de robô (o "anomaly") conta como recusa, o próximo da
fila assume e o que recusou fica **10 min** de castigo. Aqui a **impersonation vem primeiro**
— ao contrário da regra do resto do servidor — porque o cliente comum é reconhecido pelo
handshake TLS e leva a página de robô toda vez; e ela é pedida **sem nenhum header nosso**,
já que um `User-Agent` escrito à mão do lado de um handshake do Chrome é o próprio denúncia.

**Ritmo.** São 3 consultas (o nome, `"nome" videos`, `"nome" photos gallery`), até **30
páginas** (no máximo 4 por site, pra um site grande não comer o orçamento), **8 em paralelo**.
Os cards aparecem conforme cada página responde — os primeiros em ~2 s, o resto até ~15 s —
e dentro de cada página os que **nomeiam a busca** vêm na frente. A barra em cima mostra
quantas páginas já foram lidas.

**Cache em disco** em `~/.cache/multiscreen/find`: resposta do buscador por 3 h, página
minerada por 6 h (inclusive as que falharam, pra não insistir o dia inteiro). Repetir o mesmo
nome pinta na hora e não custa nada aos sites. O botão **↻ Fresh** ignora tudo isso.

**Filtros:** `All / 🎬 Videos / 🖼 Photos` e **Name match**, que deixa só os cards cujo
título ou link carrega o nome buscado — útil quando uma página traz junto a grade genérica
do site.

Clicar num card **abre o site de origem em outra guia**: o Find é um índice, não um player.
Pra assistir, cole a URL na aba **Wall** como sempre.

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

## 📡 Cast — a parede na TV, o som no fone

**📡 Cast** manda a tela inteira pra TV pela rede local — e o som **não vai junto**. Não
por algum malabarismo de rotear áudio: o stream que a TV recebe simplesmente **não tem
faixa de áudio** (`-an`). Os `<video>` continuam tocando no navegador como sempre, no
dispositivo de som padrão do Windows, que continua sendo o seu fone.

Na TV não se instala nada. Uma Samsung (e a maioria das TVs) já expõe um *MediaRenderer*
DLNA; o servidor entrega a ela uma URL HLS por UPnP `AVTransport` e ela toca. Do lado do
PC, o `ffmpeg` captura o monitor pela GPU (`ddagrab`, Desktop Duplication), codifica em
`h264_nvenc` a 1080p/30 (a TV faz o upscale; 4K quadruplicaria o custo enquanto 40 tiles
decodificam) e escreve segmentos HLS de 1s. Clicar **▶ Start casting** também põe a grade
em tela cheia — a TV mostra exatamente a parede, sem barra de tarefas.

### A trava: só a sua TV, nunca outra

Existe uma segunda Samsung nesta rede, e ela também responde como *MediaRenderer*. Por
isso nada aqui "escolhe a primeira TV que aparecer":

- **Pareamento explícito, uma vez.** Em *Pair a TV…* o app lista os renderers da rede,
  você escolhe o seu e confirma. Fica gravado em `~/.cache/multiscreen/cast_device.json`.
- **Identidade = UUID + MAC.** Nome é editável pelo controle remoto, e uma Samsung expõe
  **UUIDs diferentes por serviço** — então o UUID do *MediaRenderer* e o MAC (lido da
  tabela ARP) precisam bater, os dois, **antes de cada start**. Nome mudou, MAC mudou, dois
  aparelhos casando, nenhum casando: recusa e diz por quê. Não há fallback.
- **O stream só é servido à TV pareada.** Os arquivos HLS saem de um listener próprio,
  preso ao IP da interface da LAN (não `0.0.0.0`), na porta `principal + 50`, que responde
  **só ao IP da TV pareada** — qualquer outro cliente, inclusive o próprio PC, leva 403. O
  servidor principal, com proxy e arquivos locais, continua só em `127.0.0.1`.

Isso é o núcleo da feature, então são **várias travas independentes**, e todas têm que
passar, ao vivo, antes de cada entrega do stream (start, re-push, retomada):

1. **UUID** do *MediaRenderer* igual ao pareado.
2. **MAC** igual (lido da tabela ARP do IP que respondeu).
3. **Nome** igual ao pareado.
4. **Marca de dono no nome** — por padrão `dani`, case-insensitive (`MULTISCREEN_CAST_OWNER`
   aceita uma lista separada por vírgula). Uma TV sem a marca não pode nem ser **pareada**.
5. **Modelo** igual.
6. **Exatamente um** aparelho casando; dois ou nenhum é recusa.
7. **Control URL** apontando pro próprio IP do aparelho.
8. **Eco da URL**: depois do `SetAVTransportURI`, `GetMediaInfo` tem que devolver exatamente a
   nossa URL — a NU7100 devolve — senão `Stop` e abortar.
9. **Listener** servindo só a quem tem o **IP e o MAC** da TV pareada (ARP, cache de 30 s).
10. **Teste obrigatório**: o cast da tela só sai se um 🧪 Test pattern chegou a `PLAYING`
    **nessa mesma TV** (UUID+MAC) nos últimos 30 min e desde que este servidor subiu
    (`MULTISCREEN_CAST_REQUIRE_TEST_MINUTES`, `0` desliga). O botão Start fica desabilitado
    até isso acontecer.

Cada push, recusa, pareamento e stop vai pra `~/.cache/multiscreen/cast_audit.log`, com
hora, nome, UUID, MAC, IP e fonte. O modal lista as travas que o último start passou.

`python server.py --cast-selftest` prova a trava na rede real: a TV pareada tem que
passar e todo outro renderer tem que ser recusado. Rode antes de mexer no cast.

**🧪 Test pattern** é a prova pelos olhos: manda barras coloridas com um contador —
geradas pelo ffmpeg, não a sua tela — pela **mesma** verificação, pelo mesmo listener e
pelo mesmo push. Se as barras aparecerem na TV certa (e em nenhuma outra), o caminho
inteiro está provado antes de qualquer vídeo real ir pro ar. Depois é Stop e Start.

### Enquanto toca

O modal mostra o que **a própria TV reporta** (`PLAYING` e a posição, via
`GetTransportInfo`), o fps e a velocidade do encoder, e quantos segmentos já foram
servidos. Se o encoder cair, é reiniciado e o stream re-entregue; se a TV parar (ela às
vezes larga um stream ao vivo), é empurrado de novo — sempre passando pela mesma trava.
Se a TV **sumir** (desligou, trocou de entrada, reiniciou), o encoder fica aquecido e o
servidor bate na porta dela a cada 10 s por até 3 min, e retoma sozinho quando ela
volta — de novo pela mesma verificação de UUID + MAC; nunca por "apareceu uma TV". Se a
TV aceitar o stream mas nunca vier buscar, o aviso aponta o firewall do Windows (porta
TCP `principal + 50` pro `python.exe`).

### O som, atrasado pra encontrar a imagem

A TV mostra a tela **10–12 s depois** (é o buffer HLS da Samsung, e varia), então o fone
correria na frente. Dois pedaços resolvem isso:

- **Medir o atraso sem relógio na tela.** A TV conta a posição (`RelTime`, com
  milissegundos) a partir do primeiro segmento que pegou, e o servidor sabe quando esse
  segmento foi escrito: `atraso = agora − (escrita do segmento inicial + RelTime)`. Sai
  contínuo, suavizado, e segue a TV quando ela rebufferiza. Aparece no modal como
  `TV delay ≈ 10.4 s`. **🕒 Clock in the corner** continua lá pra conferir a olho.
- **Um gêmeo de áudio por tile com som.** `createMediaElementSource` silencia mídia
  cross-origin (o ducking do Trance já esbarrou nisso), então `DelayNode` está fora. Em
  vez disso, cada `<video>` que está com som ganha um gêmeo escondido da mesma fonte,
  tocando `atraso` segundos atrás, enquanto o visível fica com volume zero. Só quem tem
  som paga o segundo decode (numa parede de 40, um ou dois). Deriva lenta é corrigida
  com `playbackRate` 0,96–1,04, salto com um seek. O slider **±3 s** ajusta no ouvido; o
  toggle **🔈 Delay the sound…** desliga tudo e devolve o volume.

Onde não chega: iframes (YouTube/Vimeo/Twitch) ficam de fora, como no mudo global; a
precisão prática é ~±0,3–0,5 s; e na virada do loop o gêmeo ainda está no fim da volta
anterior, então dá um soluço por volta.

### Detalhes que custaram uma tarde

- Depois do `SetAVTransportURI`, a NU7100 entra em `TRANSITIONING` e **começa a tocar
  sozinha**. Um `Play` mandado nesse instante recebe UPnP **701 "Transition not
  available"** — parece que a TV recusou o stream, mas ela só estava ocupada aceitando.
  O servidor espera a transição acabar, pula o `Play` se ela já toca e repete um 701 por
  alguns segundos.
- Handlers POST em HTTP/1.1 precisam **ler o corpo** mesmo quando não o usam: com
  keep-alive, um `{}` não lido vira o começo da próxima requisição da mesma conexão
  (`Unsupported method ('{}GET')`).

Requisito: um `ffmpeg` com `h264_nvenc` e `ddagrab` — o do `winget install Gyan.FFmpeg`
tem os dois; sem GPU NVIDIA cai pra `gdigrab` + `libx264`, que também segura 1080p30.

## 🎯 Moments — o servidor acha os momentos

Marcar uma compilação na mão significa assistir tudo antes. O botão **🎯 Moments** mede
cada tile e marca os picos — **sem IA, sem serviço, sem nada pago**.

São **duas passadas de nada** dentro de um único comando de ffmpeg, com o vídeo decodificado
a 4 fps e 160 px de largura:

- `scdet` devolve, por quadro, o `mafd` (o quanto a imagem mudou desde o quadro anterior =
  **movimento**) e o score de **corte de cena**;
- `ebur128` devolve o **volume momentâneo** (LUFS) do áudio.

Pico de volume e de movimento é onde acontece alguma coisa; corte de cena é onde um clipe
pode começar e terminar sem cortar um plano no meio. O servidor devolve só as curvas — um
valor por segundo — e **quem escolhe os picos é o navegador**, então mudar a duração do
clipe ou o equilíbrio áudio/movimento re-marca a parede inteira na hora, sem decodificar
nada de novo.

No painel:

- **Clip length**, **Per video** e **Look for** (áudio, equilibrado ou movimento) re-marcam tudo instantaneamente.
- Cada tile tem uma **linha do tempo**: área roxa = movimento, linha azul = volume, riscos = cortes de cena, blocos = os momentos marcados.
- **Clique** na linha do tempo pula o vídeo pra lá; **arraste** pra marcar um trecho seu (fica amarelo e sobrevive a qualquer re-marcação).
- **＋ Send to 🎬 Compile** joga tudo que está marcado na lista de cortes do Compile, que gera o `.mp4` como sempre.

A medição é o custo: é uma decodificação inteira do vídeo (rápida, mas inteira), e por isso
duas rodam por vez e o resultado fica **em cache no disco** — reabrir o painel, ou o mesmo
vídeo semana que vem, sai de graça. Tile que já é arquivo local (upload, pacote `.zip`,
shrink) é lido direto do disco, sem rede e sem yt-dlp.

### 🧠 Search — descrever o momento em palavras

O botão **🧠 Search**, dentro do Moments, responde a outra pergunta: não "onde acontece
alguma coisa", e sim "onde acontece **isto**". É o [CLIP](https://openai.com/research/clip)
da OpenAI rodando **na sua máquina** via `onnxruntime`:

1. um quadro por segundo do vídeo vira um vetor (encoder de imagem) — o quadro **inteiro**,
   encaixado no quadrado com barras cinza, e não o corte central, que jogaria fora 44% de
   um frame 16:9;
2. cada frase que você digitou vira um vetor pelo mesmo modelo (encoder de texto), passando
   por quatro moldes (`"{}"`, `"a photo of {}"`, …) cuja média é um alvo mais estável;
3. os dois se encontram num softmax contra frases-controle, e o resultado é a **fatia do
   casamento** daquele segundo, de 0 a 1.

Importante: com frases ativas a busca é **só imagem**. Volume e movimento são o outro motor
(o 🔎 Analyze) e ficam de fora.

Nada sai da máquina, não tem chave, não tem custo por requisição — só dois arquivos ONNX
(~600 MB) baixados uma vez do Hugging Face, e a CPU ou a GPU que já estão aí. O painel
oferece o download quando você usa a busca pela primeira vez.

- Uma passada **por frase**: pedir `kiss, explosion, dancing` marca os melhores momentos de
  cada uma, e não só os da frase mais forte — é assim que a compilação sai com **todos os
  tipos** que você listou. Cada frase ganha uma cor, na linha do tempo e nos chips.
- **Fit to the match**: o clipe dura o que a cena dura. O pico vira semente e as bordas
  crescem enquanto o casamento segura metade do valor do pico (critério de meia-altura),
  até 12× o "Clip length". É o que faz um trecho contínuo de 70 s sair como 70 s, e não
  como um pedaço fixo de 8 s no meio dele. Desmarcado, volta a cortar no tamanho fixo.
- **Confidence**: o número no chip (`94%`) é a fatia do casamento — cada segundo é
  disputado entre as **suas** frases e uma dúzia de frases comuns ("a random video frame",
  "an empty room", "text on a screen"…), e o que aparece é quanto a sua ganhou. Isso é uma
  escala absoluta: distância bruta do CLIP não é. Frase que não está no vídeo fica em ~0 e
  o painel diz **nothing matched "…"** em vez de marcar o segundo menos ruim.
- **Model**: `B/32` (rápido), `B/16` (quadra a resolução dos patches, acha coisa menor) e
  `L/14` (o bom, e vários vezes mais lento). Cada um baixa uma vez, na primeira vez que
  você escolhe. Trocar de modelo invalida o índice — é outro espaço vetorial.
- **Example** (o mais preciso): em vez de descrever, aponte **um tile que já mostra o tipo
  de momento que você quer** — se for um edit pronto, melhor ainda. Cada segundo é comparado
  com **todos os quadros** do exemplo e fica com os três melhores; a média deles é a nota.
  (A primeira versão fazia a média dos quadros do exemplo primeiro — numa compilação com
  várias cenas isso vira um borrão que não se parece com nenhuma. Medido nos vídeos reais,
  a forma por quadro escolhe segundos claramente mais parecidos: 0.933 contra 0.922 de
  semelhança com o exemplo, e 0.883 contra 0.855 entre si, sobre uma linha de base de
  0.865/0.769 para segundos aleatórios.)
- **`gap`** aparece ao lado de cada tile: é o espaço entre o segundo comum e o melhor
  segundo daquele vídeo, em distância bruta do CLIP. É a medida honesta de se o modelo
  conseguiu separar alguma coisa. Em material com a mesma pessoa, no mesmo cenário, o
  **B/32 satura**: tudo fica entre 0.85 e 0.96 e o gap cai pra ~0.03. Aí a escala não é o
  problema, é o encoder — suba pra **B/16** ou **L/14**.
- **Margin**: guarda alguns segundos antes e depois do que foi achado, porque clipe que
  começa exatamente no acontecimento parece corte seco. Padrão ±5 s.
- **Per phrase = 1** é o padrão: normalmente é um momento por vídeo.
- **⚡ Auto-edit**: um clique faz tudo — indexa/mede cada tile, fica com o que passa da
  confiança escolhida, manda pro Compile e já exporta o `.mp4`.

> **Uma frase por vez.** As frases dividem o mesmo softmax, então uma ampla ("a couple in
> bed", 0.9 no vídeo inteiro) abafa a específica que você quer (0.05). Para um momento
> preciso, busque uma frase de cada vez — ou use o Example.

### 📺 A página inteira de uma atriz, sem abrir um vídeo

A parede nunca foi o ponto da compilação — era só onde as URLs estavam. Cinquenta players
tocando ao mesmo tempo é o que faz o navegador travar, e **nada disso é necessário**: dada a
página da atriz, o yt-dlp lista os vídeos, o preset acha o momento em cada um, o ffmpeg junta.
Nenhum player envolvido.

No botão **📺 Channel**: cola a página (`pornhub.com/model/…`, canal, playlist), escolhe
quantos vídeos e a resolução, e **⚡ Build the compilation**. A barra mostra o que está
acontecendo — lendo a página, procurando no vídeo N de M, cortando o clipe N de M, juntando —
e os clipes achados vão aparecendo com o tempo e a confiança de cada um enquanto rodam.

O `Check` lê só a listagem (`extract_flat`), então são ~1 segundo para 60 vídeos: dá pra
conferir que a página é a certa antes de começar o trabalho pesado.

Detalhe que mudou por causa dessa escala: o `.mp4` pronto agora vai **direto do servidor pro
disco** pelo próprio navegador. Antes ele era montado inteiro na memória da aba
(`resp.blob()`), o que com cinquenta cortes é 1–2 GB numa aba já sobrecarregada.

A escolha do trecho, que morava no JavaScript, foi portada para o servidor
(`clip_pick_moment`) — o modo canal não tem navegador para rodá-la. O porte reproduz a
calibração segundo a segundo: mesmos 3/4, zero falso positivo, mesmos tempos.

### Um clique: compilação de um tipo de cena

O caso real do Moments não é "buscar qualquer coisa", é **montar uma compilação de um tipo
de cena só**, a partir dos vídeos que estão na parede. Para isso existe o seletor **preset**,
e ele vem escolhido por padrão:

1. escolha o preset (ex.: `Cumshot compilation`);
2. **⚡ Auto-edit**.

Pronto — cada tile é indexado, marcado, e o `.mp4` sai. Nada de digitar frase.

Um preset é um par de bancos: as maneiras de **dizer** a cena e as cenas que **se confundem**
com ela (sexo, oral, conversa, créditos, marca d'água). O segundo banco é o que importa: uma
frase sozinha, disputando com alternativas genéricas, não separa o final de uma cena do resto
da mesma cena. Cada segundo vira um softmax entre o protótipo positivo, os vizinhos
confundíveis e o fundo comum — e o que sai é uma probabilidade, comparável entre vídeos.

O preset também traz **onde procurar**. Medido nos seis vídeos de teste, com o preset
`cumshot`:

| trecho buscado | resultado |
|---|---|
| vídeo inteiro | picos em 0:23, 2:00, 2:27 — com 0.95 de confiança, e errados |
| último terço | todo pico no fim, e a confiança separa: 0.93, 0.80, 0.77 contra 0.51, 0.48, 0.04 |

Por isso o seletor **Where** existe e o preset já o posiciona. O `0.04` do último vídeo é a
resposta certa para "esse aqui não termina assim" — e aparece como `nothing matched`.

Para escrever um preset novo, é o dicionário `CLIP_PRESETS` no `server.py`: nome, janela,
lista positiva, lista negativa. O painel lê a lista do servidor sozinho.

#### Segunda rodada: clipes mais curtos e mais exigentes

Depois de uma compilação real (60 vídeos, 43 clipes, 42 min), duas queixas: clipes longos
demais e com trechos não relacionados. Cada uma tem seu parafuso, e um deles é de graça:

| até onde o clipe cresce | acerto | duração dos clipes |
|---|---|---|
| 50% do pico | 3/4, 0 falso pos. | 77 s, 63 s, 25 s |
| **70% do pico** | **3/4, 0 falso pos.** | **20 s, 30 s, 23 s** |

Crescer só até 70% do pico corta o clipe a um terço **sem perder nenhum acerto** — o que
sobrava era justamente o "não relacionado" nas bordas. Esse é `"grow"` no preset.

Já subir o corte de confiança custa, e o custo está medido: dos 43 clipes da rodada real, 22
ficavam entre 0.35 e 0.60; mas no conjunto com gabarito, o acerto que vale 0.41 se perde e o
placar vai de 3/4 para 2/4. O preset ficou em `0.6` porque uma compilação que alguém vende
paga mais caro por um clipe errado do que por um faltando — e o seletor **confidence** no
painel do canal move isso sem tocar em código.

**Uma ideia que os números recusaram:** procurar direto no fim (últimos 20% em vez de 40%)
parece economizar, mas mede pior — 2/4 em vez de 3/4 — *e* devolve clipes maiores (77 s
contra 63 s). O algoritmo precisa de trecho comum antes do momento para saber o que é normal
naquele vídeo; sem contexto, tudo parece alto e a região se espalha. Todos os momentos do
gabarito começam entre 92% e 96% do vídeo, então a tentação é grande — mas a janela ficou nos
40%. A economia de tempo veio de outro lugar: baixar em **360p** em vez de 480p, já que o
CLIP encaixa tudo num quadrado de 224 px antes de olhar.

### Uma segunda conta no mesmo PC

`iniciar-compartilhado.cmd` + um atalho na área de trabalho da outra conta. Ele chama o
`python.exe` pelo caminho inteiro (a outra conta não tem Python no PATH — que costuma ser o
problema real, não permissão) e aponta `MULTISCREEN_CACHE` para o cache já existente. Assim
os ~3,7 GB de Python, dependências e modelos são **compartilhados, não duplicados**, e um
vídeo indexado por uma conta não é reindexado pela outra.

`permitir-outro-usuario.ps1` confere o acesso e só libera o que faltar — e apenas três
pastas, não o perfil inteiro. Numa conta de administrador ele normalmente responde "nada a
fazer", porque o acesso já existe por herança.

#### Como o preset foi calibrado (e por que "o último", não "o maior")

Seis vídeos com a resposta conhecida — quatro têm o momento, com o segundo exato; dois não
têm nenhum. Todo ajuste abaixo foi decidido nesse conjunto, não no olho:

| escolha | acertos | falsos positivos |
|---|---|---|
| pico global da curva | 2/4 | 0 |
| **último trecho forte** (≥50% do pico) | **3/4** | **0** |

A diferença é a invariante do formato: essa cena **fecha** o vídeo. Alguma coisa mais cedo
quase sempre parece mais com as palavras do que a coisa real, então pegar o pico global erra.
Pegar o último trecho que ainda pontua perto do topo acerta — e de quebra deixa os dois
vídeos negativos ainda mais quietos (0.34 → 0.21), o que abriu espaço para baixar o corte de
confiança para 25% com folga dos dois lados: negativos em 0.04 e 0.21, positivos em 0.35,
0.72 e 0.78.

Isso vive no preset (`"prefer": "last"`, `"gate": 0.25`), não no código do seletor — outro
tipo de cena, com outra distribuição, traz os seus próprios números.

Duas medidas que também saíram daí, e que valem para quem for escrever um preset novo:

- **Banco largo demais piora.** Somar frases de corpo e de interno ao banco de rosto/boca
  levou um dos negativos a disparar (0.47) sem consertar nenhum dos erros. Frase a mais não é
  cobertura a mais, é diluição do protótipo.
- **O erro que sobrou é de percepção, não de escolha.** No vídeo que ainda falha, o trecho
  certo pontua 0.12 contra 0.45 do lugar errado: o B/32 não vê o que está lá. Isso não se
  conserta com palavra.
- **Mas modelo maior não é automaticamente melhor.** O B/16 enxerga justamente esse vídeo
  (0.32 onde o B/32 lê 0.11) e fecharia em 4/4 — só que inventa um momento num vídeo que não
  tem nenhum, e inventa com **0.56**, acima do verdadeiro mais fraco que ele resgatou. Não
  existe corte que separe os dois. Para uma compilação, corte errado no arquivo final custa
  mais que corte faltando, então o padrão continua no B/32: **3/4, zero falso positivo**. O
  seletor de modelo está lá para quem preferir o contrário.

### Quando o site começa a desconfiar

Depois de uma rodada longa — dezenas de vídeos indexados e logo em seguida os mesmos
segundos pedidos de novo para cortar — o site passa a responder com página de desafio em vez
de vídeo. Isso chega como uma mensagem que parece outra coisa:

```
ERROR: [PornHub] …: PhantomJS not found, Please download it from https://phantomjs.org/…
```

Não falta programa nenhum: é o extrator do yt-dlp sendo recusado. Três coisas mudaram por
causa disso, e as três valem para qualquer site que aperte:

- **O corte tenta o stream resolvido antes do yt-dlp.** É o mesmo caminho que a indexação já
  usa e que continuava funcionando enquanto o download falhava — sem extração nova, sem
  baixar o arquivo inteiro. Medido: 43 s pedidos, 43 s entregues, em 7 segundos.
- **A listagem lê o HTML da página quando o extrator é recusado.** O `_fetch_page_html` do
  projeto já se comporta como navegador; os links estão na marcação. Em produção:
  `CHANNEL extractor refused … reading the page instead` → 60 vídeos com títulos.
- **O trecho que a indexação baixa não é mais descartado** (`multiscreen_spans`, teto de 8 GB,
  limpeza por idade). Pedir os mesmos segundos duas vezes era metade do motivo de o site
  endurecer.

### Quatro caracteres que custam uma hora

O índice é arquivado pelo hash do endereço, e o mesmo vídeo chega escrito de formas
diferentes: o yt-dlp devolve `http://`, a marcação da página `https://`. Hash diferente,
cache perdido, tudo medido de novo — sessenta vídeos, uma hora e meia. Agora as grafias
conhecidas são procuradas antes de decidir que falta índice, e o novo é gravado sempre na
forma canônica. A verificação que motivou isso:

```
dos 60 videos da pagina, 59 ja tem indice aproveitavel
```

### A ingestão: de onde vinham os índices pela metade

Ler o stream assinado direto no ffmpeg era a origem de quase todo erro que parecia erro de
modelo — o socket morre, o ffmpeg para, e os quadros que chegaram parecem o vídeo inteiro.
Agora o vídeo é **baixado antes**, por yt-dlp, numa rendição pequena (480p; o CLIP olha 224
px, baixar 1080p é desperdício), em três degraus: só a janela pedida, senão o arquivo
inteiro, e só em último caso o stream ao vivo. Cada degrau confere o que recebeu contra o que
foi pedido — e o que foi pedido nunca é medido pelo arquivo que chegou, senão um download
cortado rebaixaria a própria régua.

Há um relógio de parede em cima disso (`CLIP_FETCH_TIMEOUT`, 420 s): uma fonte que falha no
ritmo certo — toda resposta cortada, toda retentativa avançando um pouco — mantém um
downloader ocupado para sempre, e indexar é coisa que alguém está esperando.

### Leitura curta: o erro que parece resposta errada

Stream remoto assinado cai no meio da leitura o tempo todo. Antes, o ffmpeg saía, os quadros
que já tinham vindo pareciam o vídeo completo, e um filme de 14 min ficava indexado até
**0:30** — a busca então respondia com toda a confiança sobre o começo do vídeo, e parecia
que o modelo é que estava errando.

Agora as duas passadas (curvas e CLIP) **conferem o que leram** contra a duração que o
próprio ffmpeg reporta, tentam de novo com o stream re-resolvido (o motivo quase sempre é o
token expirado) e, se ainda ficar curto, **falham dizendo até onde deu** em vez de gravar
cache pela metade. Índices curtos gravados por versões antigas são recusados na leitura e
refeitos sozinhos. O painel também mostra `searched 12:34 of 21:46 ⚠` quando é o caso, e
`-reconnect` foi ligado nas leituras http.

O servidor agora registra cada busca no console — o que foi pedido, sobre quanto de vídeo,
e onde ficou o pico:

```
  CLIP search [b32] 21:46 of video: cum on her face=0.84@9:02
  CLIP like [b32] 17:58 of video, best 0.99 at 12:41
  CLIP short read https://… : 0:30 of 14:03 (rc=1) - retrying
```

- **Group by phrase** (no rodapé) monta o `.mp4` em capítulos: todos os `kiss` de todos os
  tiles, depois todos os `explosion`.
- Apagar a caixa devolve a marcação pro modo corte de cena + movimento + volume.
- Frases em **inglês** funcionam bem melhor — o CLIP foi treinado assim.

O índice de um vídeo (um vetor por segundo, `float16`) fica em `~/.cache/multiscreen/clipidx`
— ~3,7 MB por hora de vídeo — então buscar outra frase depois é instantâneo: só o texto é
reprocessado. O modelo em si fica em `~/.cache/multiscreen/clip`.

**Velocidade:** ~38 quadros/s no B/32 só na CPU desta máquina (um vídeo de 1 h indexa em
~1,5 min); B/16 e L/14 são ~4× e ~10× mais lentos, e é aí que a GPU passa a valer.
Pra usar a GPU: `pip install onnxruntime-directml` (ou `onnxruntime-gpu` com CUDA) e
reiniciar — o servidor escolhe sozinho o melhor provider disponível e mostra qual está
usando no painel.

**Requisitos:** `pip install numpy onnxruntime`. Sem isso a busca por texto some do
caminho e o resto do Moments continua funcionando normalmente.

## Como funciona

- **`index.html`** — front-end (grade, players, controles). Reconhece localmente arquivos diretos, HLS e os embeds. Qualquer outro link é enviado ao back-end.
- **`server.py`** — servidor Python (só `stdlib` + `yt-dlp`):
  - `GET /api/resolve?url=` — usa o yt-dlp pra descobrir o stream real da página.
  - `GET /api/scan?url=[&origin=]` — acha os `<video>` da página; se todos forem `blob:`, o `.m3u8`/`.mpd` escondido no HTML do player; se nem isso, abre os iframes da página e repete lá dentro (`origin` = de qual iframe começar).
  - `GET /api/wrap?url=&ref=` — embrulha no proxy um stream que o navegador já achou (o que a extensão descobre atrás de um `blob:`).
  - `GET /api/proxy?p=` — repassa o vídeo com os cabeçalhos corretos (Referer/User-Agent), libera CORS e reescreve playlists HLS para tocarem no navegador.
  - `GET /api/voices?lang=&gender=` — lista as vozes neurais do Trance (edge-tts). O gênero vem
    declarado pelo serviço, não adivinhado pelo primeiro nome. Devolve 503 se o edge-tts não
    estiver instalado, e aí o front cai nas vozes do navegador.
  - `GET /api/tts?text=&voice=&rate=&pitch=` — a frase em MP3, guardada em cache: um mantra em
    loop só vai à rede na primeira vez que é falado.
  - `POST /api/related` — recebe `{seeds, have, titles}` e devolve `{items, themes}`:
    raspa as páginas dos próprios tiles, deduz o tema pelas tags do site e busca a
    página-índice do tema pra achar mais do mesmo.
  - `POST /api/shrink?id=&h=&fps=` — re-encoda um vídeo já enviado (`/api/upload`) para
    caber num tile de `h` px de altura; devolve um `job_id`, e `GET /api/shrink/status?id=`
    dá o progresso e a URL final. Arquivo que já cabe volta na hora, sem encodar.
  - `POST /api/compile` — recebe `{cuts:[{url,start,end}], resolution}`, baixa só os trechos
    pedidos, normaliza cada um e concatena; `GET /api/compile/status?id=` acompanha e
    `GET /api/compile/result?id=` baixa o `.mp4`.
  - `POST /api/analyze` — recebe `{url}` e mede o vídeo com ffmpeg (corte de cena, movimento
    e volume), devolvendo um valor por segundo. Com `{probe:true}` só responde se a curva já
    estiver em cache — abrir o painel numa parede de 40 tiles não dispara 40 medições.
    `GET /api/analyze/status?id=` dá o progresso e `GET /api/analyze/result?id=` as curvas.
  - `GET /api/clip/status` — o que a busca por texto consegue fazer agora: dependências,
    modelo baixado, provider (CPU/DirectML/CUDA). `POST /api/clip/setup` baixa o modelo.
  - `POST /api/clip/index` — passa o vídeo pelo encoder de imagem do CLIP e guarda um vetor
    por segundo; `GET /api/clip/index/status?id=` acompanha.
  - `POST /api/clip/similar` — recebe `{url, ref, model}` e devolve a curva de semelhança
    com o tile de exemplo (softmax entre o exemplo e o quadro médio do próprio vídeo).
  - `POST /api/clip/search` — recebe `{url, prompts:[…], model}` e devolve, por frase, a
    fatia do casamento de cada segundo (softmax contra as frases-controle). Só multiplicação
    de matriz em cima do índice: milissegundos, então trocar de frase é instantâneo.

## Limitações

- **DRM** (Netflix, Prime Video, Disney+, etc.): não é suportado.
- Sites que exigem **login** só funcionam passando cookies ao yt-dlp.
- Para **uso pessoal** — respeite os termos de uso de cada site.
