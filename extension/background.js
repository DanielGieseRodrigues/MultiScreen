// MultiScreen Tabs — background service worker.
//
// Two entry points:
//   1. The MultiScreen page asks for the open-tab list through content.js —
//      that's what the 🗂 Tabs button in the app uses.
//   2. Clicking the extension icon collects every tab and delivers the URLs
//      to a MultiScreen tab (found or newly opened) via the #add= hash.
//
// It also watches what the tabs actually stream. Most video sites today use
// MSE (hls.js, dash.js, shaka…): the player creates a MediaSource, hands the
// <video> element a blob: URL and pushes segments into it from JS. That blob:
// URL is a handle into that tab's memory — it can't be fetched from another
// page or from the server, and it dies with the tab, so pasting it into
// MultiScreen can never work. What IS usable is the manifest the page fetched
// to feed it, so we remember one per tab and hand it over on request.

const APP_URL = "http://localhost:8000/";

function isAppTab(tab) {
  try {
    const u = new URL(tab.url || "");
    return (u.hostname === "localhost" || u.hostname === "127.0.0.1") &&
           (u.pathname === "/" || u.pathname === "/index.html");
  } catch (e) {
    return false;
  }
}

/* ---------- what each tab is streaming ---------- */

const MANIFEST_RE = /\.(m3u8|mpd)(\?|#|$)/i;
const FILE_RE = /\.(mp4|webm|m4v|mov|ogv)(\?|#|$)/i;
const MAX_PER_TAB = 6;
const MAX_FRAMES_PER_TAB = 12;

const mediaByTab = new Map();  // tabId -> [{ url, manifest }]
// Origins of the <iframe>s a tab loaded. A blob: URL carries the origin of
// the frame that created it, and on most sites that is the embedded player
// (tubevid.site & co.), NOT the page in the address bar — so matching a blob
// against tab.url alone finds nothing. This is what makes the match work.
const framesByTab = new Map(); // tabId -> [origin, …]

// The service worker is evicted after ~30s idle and takes the maps with it, so
// mirror them to session storage (wiped when the browser closes) and read them
// back on the next wake-up. Debounced, because playback fires a segment
// request every few seconds and each one would otherwise be a write.
let hydrated = null;
let flushTimer = null;

function hydrate() {
  if (!hydrated) {
    hydrated = chrome.storage.session.get(["media", "frames"]).then((stored) => {
      for (const [id, list] of Object.entries(stored.media || {})) {
        const tabId = Number(id);
        const merged = mediaByTab.get(tabId) || [];
        for (const entry of list) {
          if (!merged.some(m => m.url === entry.url)) merged.push(entry);
        }
        merged.sort((a, b) => (b.manifest ? 1 : 0) - (a.manifest ? 1 : 0));
        mediaByTab.set(tabId, merged.slice(0, MAX_PER_TAB));
      }
      for (const [id, origins] of Object.entries(stored.frames || {})) {
        const tabId = Number(id);
        const merged = framesByTab.get(tabId) || [];
        for (const o of origins) if (!merged.includes(o)) merged.push(o);
        framesByTab.set(tabId, merged.slice(0, MAX_FRAMES_PER_TAB));
      }
    }).catch(() => {});
  }
  return hydrated;
}

function scheduleFlush() {
  if (flushTimer) return;
  flushTimer = setTimeout(() => {
    flushTimer = null;
    chrome.storage.session.set({
      media: Object.fromEntries(mediaByTab),
      frames: Object.fromEntries(framesByTab),
    }).catch(() => {});
  }, 1000);
}

function originOf(url) {
  try { const u = new URL(url); return u.protocol + "//" + u.host; }
  catch (e) { return ""; }
}

async function remember(tabId, url, manifest) {
  if (tabId == null || tabId < 0) return;
  await hydrate();
  let list = mediaByTab.get(tabId);
  if (!list) mediaByTab.set(tabId, list = []);
  if (list.some(m => m.url === url)) return;
  if (manifest) {
    // Manifests keep their arrival order at the front of the list — the first
    // one is the master playlist, the later ones its quality variants — and
    // they always outrank plain files, which a page can fire by the hundred.
    const after = list.filter(m => m.manifest).length;
    list.splice(after, 0, { url, manifest: true });
  } else if (list.length < MAX_PER_TAB) {
    list.push({ url, manifest: false });
  }
  if (list.length > MAX_PER_TAB) list.length = MAX_PER_TAB;
  scheduleFlush();
}

async function rememberFrame(tabId, url) {
  if (tabId == null || tabId < 0) return;
  const origin = originOf(url);
  if (!origin) return;
  await hydrate();
  let list = framesByTab.get(tabId);
  if (!list) framesByTab.set(tabId, list = []);
  if (list.includes(origin)) return;
  list.push(origin);
  if (list.length > MAX_FRAMES_PER_TAB) list.shift();
  scheduleFlush();
}

chrome.webRequest.onBeforeRequest.addListener((d) => {
  if (d.type === "main_frame") {
    // The tab is going somewhere else; its old streams go with it.
    mediaByTab.delete(d.tabId);
    framesByTab.delete(d.tabId);
    scheduleFlush();
    return;
  }
  const url = d.url || "";
  if (!/^https?:/i.test(url)) return;
  if (d.type === "sub_frame") rememberFrame(d.tabId, url);
}, { urls: ["http://*/*", "https://*/*"] });

// Streams are remembered only once the server ANSWERS, and only on success.
// Pages probe URLs that don't work for them either — eporner's player fires
// a request at its SEO decoy (gvideo.*/id.mp4, always 403) before loading
// the real CDN URL — and onBeforeRequest happily recorded that dead URL as
// the tab's first (thus preferred) stream, so the fallback tile died too.
chrome.webRequest.onResponseStarted.addListener((d) => {
  if (d.type === "main_frame" || d.type === "sub_frame") return;
  const url = d.url || "";
  if (!/^https?:/i.test(url)) return;
  if (d.statusCode >= 400) return;
  const manifest = MANIFEST_RE.test(url);
  const file = FILE_RE.test(url) && (d.type === "media" || d.type === "other");
  if (manifest || file) remember(d.tabId, url, manifest);
}, { urls: ["http://*/*", "https://*/*"] });

chrome.tabs.onRemoved.addListener((tabId) => {
  mediaByTab.delete(tabId);
  framesByTab.delete(tabId);
  scheduleFlush();
});

// Every open http(s) tab across all windows, minus MultiScreen itself
// (and anything else on localhost), deduped by URL — each with whatever
// stream we saw it load.
async function collectTabs() {
  await hydrate();
  const tabs = await chrome.tabs.query({});
  const seen = new Set();
  const out = [];
  for (const t of tabs) {
    const url = t.url || "";
    if (!/^https?:\/\//i.test(url)) continue; // chrome://, about:, extensions…
    let host = "";
    try { host = new URL(url).hostname; } catch (e) { continue; }
    if (host === "localhost" || host === "127.0.0.1") continue;
    if (seen.has(url)) continue;
    seen.add(url);
    out.push({
      url,
      title: t.title || "",
      media: (mediaByTab.get(t.id) || []).map(m => m.url),
    });
  }
  return out;
}

// site.com from www.cdn.site.com — enough to tell "the same site's CDN" from
// "some other site". Two labels is wrong for co.uk & friends, but the cost is
// only a slightly looser match on a list we already scored.
function siteOf(urlOrHost) {
  let host = String(urlOrHost || "");
  try { host = new URL(host).hostname; } catch (e) { /* already a host */ }
  return host.toLowerCase().replace(/^https?:\/\//, "").split("/")[0]
             .split(".").slice(-2).join(".");
}

// Rescues a pasted blob: URL. All it carries is the origin of the FRAME that
// created it, so a tab qualifies when that origin is its own, one of the
// iframes it loaded, or the site its streams are being pulled from.
async function findMedia(origin) {
  await hydrate();
  const want = String(origin || "").toLowerCase().replace(/\/+$/, "");
  if (!want) return { ok: false, reason: "no origin", media: [] };
  const site = siteOf(want);

  const tabs = await chrome.tabs.query({});
  const candidates = [];
  let watched = 0;
  for (const t of tabs) {
    const list = mediaByTab.get(t.id) || [];
    if (!list.length) continue;
    watched++;
    const frames = framesByTab.get(t.id) || [];
    // Best evidence first: the tab itself is on that origin, then it embedded
    // a frame from it, then its streams merely come from the same site.
    let score = 0;
    if ((t.url || "").toLowerCase().startsWith(want)) score = 3;
    else if (frames.some(f => f.toLowerCase() === want)) score = 2;
    else if (site && frames.some(f => siteOf(f) === site)) score = 2;
    else if (site && list.some(m => siteOf(m.url) === site)) score = 1;
    if (!score) continue;
    if (list[0].manifest) score += 0.5; // a real playlist beats loose files
    candidates.push({ score, pageUrl: t.url, title: t.title || "", list });
  }

  candidates.sort((a, b) => b.score - a.score);
  let best = candidates[0];
  let guess = false;
  // Nothing matched but exactly one tab in the whole browser is streaming
  // anything: that is almost certainly the one, and saying so beats a
  // "couldn't find it" the user can do nothing about.
  if (!best && watched === 1) {
    for (const t of tabs) {
      const list = mediaByTab.get(t.id) || [];
      if (list.length) { best = { pageUrl: t.url, title: t.title || "", list }; guess = true; }
    }
  }
  if (!best) {
    return { ok: false, media: [], watched, tabs: tabs.length,
             reason: watched ? "no tab matches " + want
                             : "no tab is streaming anything yet" };
  }
  return {
    ok: true, guess, pageUrl: best.pageUrl, title: best.title,
    media: best.list.map(m => m.url),
  };
}

// Path 1: requests relayed by content.js from the app.
chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg && msg.type === "ms-get-tabs") {
    collectTabs().then(tabs => sendResponse({ ok: true, tabs }));
    return true; // keep the channel open for the async response
  }
  if (msg && msg.type === "ms-find-media") {
    findMedia(msg.origin).then(sendResponse);
    return true;
  }
});

// Path 2: extension icon click — hand the URLs over via #add= and focus the
// app. Changing only the hash doesn't reload the page; the app listens for
// hashchange, so videos already playing keep playing.
chrome.action.onClicked.addListener(async () => {
  const tabs = await collectTabs();
  // Only the first stream per tab travels in the hash: it's the one the app
  // would fall back to, and 40 tabs' worth of signed CDN URLs is a lot of URL.
  const payload = tabs.map(t => ({ url: t.url, media: t.media.slice(0, 1) }));
  const hash = "#add=" + encodeURIComponent(JSON.stringify(payload));
  const existing = (await chrome.tabs.query({})).find(isAppTab);
  if (existing) {
    await chrome.tabs.update(existing.id, {
      url: existing.url.split("#")[0] + hash,
      active: true,
    });
    await chrome.windows.update(existing.windowId, { focused: true });
  } else {
    await chrome.tabs.create({ url: APP_URL + hash });
  }
});
