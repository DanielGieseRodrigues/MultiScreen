// Bridge between the MultiScreen page and the extension. The page can't read
// other browser tabs itself, so it posts a request here and we relay it to
// the background service worker (which has the "tabs" permission).

window.addEventListener("message", (e) => {
  if (e.source !== window || !e.data || e.data.source !== "multiscreen-page") return;
  if (e.data.type === "get-tabs") {
    chrome.runtime.sendMessage({ type: "ms-get-tabs" }, (resp) => {
      window.postMessage({
        source: "multiscreen-ext",
        type: "tabs",
        tabs: (resp && resp.tabs) || [],
      }, "*");
    });
  } else if (e.data.type === "ping") {
    window.postMessage({ source: "multiscreen-ext", type: "pong" }, "*");
  }
});

// Announce the bridge to the page (covers either load order).
window.postMessage({ source: "multiscreen-ext", type: "ready" }, "*");
