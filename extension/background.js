// MultiScreen Tabs — background service worker.
//
// Two entry points:
//   1. The MultiScreen page asks for the open-tab list through content.js —
//      that's what the 🗂 Tabs button in the app uses.
//   2. Clicking the extension icon collects every tab and delivers the URLs
//      to a MultiScreen tab (found or newly opened) via the #add= hash.

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

// Every open http(s) tab across all windows, minus MultiScreen itself
// (and anything else on localhost), deduped by URL.
async function collectTabs() {
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
    out.push({ url, title: t.title || "" });
  }
  return out;
}

// Path 1: request relayed by content.js from the app's 🗂 Tabs button.
chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg && msg.type === "ms-get-tabs") {
    collectTabs().then(tabs => sendResponse({ ok: true, tabs }));
    return true; // keep the channel open for the async response
  }
});

// Path 2: extension icon click — hand the URLs over via #add= and focus the
// app. Changing only the hash doesn't reload the page; the app listens for
// hashchange, so videos already playing keep playing.
chrome.action.onClicked.addListener(async () => {
  const tabs = await collectTabs();
  const hash = "#add=" + encodeURIComponent(JSON.stringify(tabs.map(t => t.url)));
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
