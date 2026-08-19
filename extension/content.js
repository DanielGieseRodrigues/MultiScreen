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
  } else if (e.data.type === "find-media") {
    // Someone pasted a blob: URL. Only its origin survives, so the worker
    // looks for a tab on that origin and reports the stream it saw it load.
    const origin = e.data.origin;
    chrome.runtime.sendMessage({ type: "ms-find-media", origin }, (resp) => {
      // Pass the worker's answer through as-is (media, but also why it found
      // nothing) and pin the origin so the page can match it to its request.
      window.postMessage(Object.assign(
        { media: [], reason: chrome.runtime.lastError ? "extension error" : "" },
        resp || {},
        { source: "multiscreen-ext", type: "media", origin }), "*");
    });
  } else if (e.data.type === "ping") {
    window.postMessage({ source: "multiscreen-ext", type: "pong" }, "*");
  }
});

// Announce the bridge to the page (covers either load order).
window.postMessage({ source: "multiscreen-ext", type: "ready" }, "*");
