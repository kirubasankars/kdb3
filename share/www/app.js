(function () {
"use strict";

/* ═══════════════════════════════════════════════════
   Utils
═══════════════════════════════════════════════════ */

function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g,"&amp;").replace(/</g,"&lt;")
    .replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

function enc(s) { return encodeURIComponent(s); }

function jsonHighlight(str) {
  if (!str) return "";
  return esc(str).replace(
    /("(?:\\u[\da-fA-F]{4}|\\[^u]|[^\\"])*"(?:\s*:)?|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
    function(m) {
      if (/:$/.test(m)) return '<span class="jk">'+ m +'</span>';
      if (/^"/.test(m)) return '<span class="js">'+ m +'</span>';
      if (/true|false/.test(m)) return '<span class="jb">'+ m +'</span>';
      if (/null/.test(m)) return '<span class="jn">'+ m +'</span>';
      return '<span class="ji">'+ m +'</span>';
    }
  );
}

function prettyJSON(str) {
  try { return JSON.stringify(JSON.parse(str), null, 2); }
  catch(e) { return str; }
}

function copyText(text) {
  navigator.clipboard && navigator.clipboard.writeText(text).catch(function(){});
}

function docPreview(data) {
  if (!data) return "";
  try {
    var o = typeof data === "string" ? JSON.parse(data) : data;
    var parts = [];
    for (var k in o) {
      if (k.startsWith("_")) continue;
      var v = o[k];
      if (typeof v === "object") v = "{…}";
      parts.push(k + ": " + String(v).slice(0, 24));
      if (parts.length >= 2) break;
    }
    return parts.join(" · ");
  } catch(e) { return ""; }
}

function dbInitials(name) {
  var s = String(name || "");
  if (s.length <= 2) return s;
  var parts = s.split(/[^A-Za-z0-9]+/).filter(Boolean);
  if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
  return s.slice(0, 2).toUpperCase();
}

var IS_MAC = /Mac|iPhone|iPad|iPod/.test(navigator.platform || "") ||
  (navigator.userAgentData && navigator.userAgentData.platform === "macOS");

function saveShortcutLabel() {
  return IS_MAC ? "Cmd+S" : "Ctrl+S";
}

function emptyHTML(msg, pad) {
  return '<div class="empty' + (pad ? " empty-pad" : "") + '">' + msg + "</div>";
}

function setHidden(el, hidden) {
  if (!el) return;
  el.classList.toggle("is-hidden", !!hidden);
}

function bounceStage() {
  var stage = document.getElementById("stage");
  if (!stage) return;
  stage.classList.remove("stage-enter");
  void stage.offsetWidth;
  stage.classList.add("stage-enter");
}

/* ═══════════════════════════════════════════════════
   Toast / Modal
═══════════════════════════════════════════════════ */

var toastContainer = document.getElementById("toasts");

function toast(msg, type, duration) {
  type = type || "info";
  duration = duration == null ? 3500 : duration;
  var el = document.createElement("div");
  el.className = "toast " + type;
  el.innerHTML = '<span class="toast-msg">'+ esc(msg) +'</span>' +
    '<button class="toast-close" aria-label="close">&times;</button>';
  el.querySelector(".toast-close").addEventListener("click", function() { remove(); });
  toastContainer.appendChild(el);
  var timer = duration > 0 ? setTimeout(remove, duration) : null;
  function remove() {
    clearTimeout(timer);
    el.style.opacity = "0";
    el.style.transform = "translateX(8px)";
    el.style.transition = "opacity .2s, transform .2s";
    setTimeout(function() { el.parentNode && el.parentNode.removeChild(el); }, 220);
  }
}

function showConfirm(title, message, opts) {
  opts = opts || {};
  return new Promise(function(resolve) {
    var root = document.getElementById("modal-root");
    var okLabel = opts.okLabel || "Confirm";
    var okClass = opts.danger ? "btn-danger" : "btn-primary";
    root.innerHTML =
      '<div class="modal-overlay" id="modal-overlay">' +
        '<div class="modal">' +
          '<h3>'+ esc(title) +'</h3>' +
          '<p>'+ esc(message) +'</p>' +
          '<div class="modal-actions">' +
            '<button class="btn-ghost" id="modal-cancel">Cancel</button>' +
            '<button class="'+ okClass +'" id="modal-ok">'+ esc(okLabel) +'</button>' +
          '</div>' +
        '</div>' +
      '</div>';
    function close(val) {
      root.innerHTML = "";
      resolve(val);
    }
    root.querySelector("#modal-cancel").addEventListener("click", function(){ close(false); });
    root.querySelector("#modal-ok").addEventListener("click", function(){ close(true); });
    root.querySelector("#modal-overlay").addEventListener("click", function(e){
      if (e.target === e.currentTarget) close(false);
    });
  });
}

function showPrompt(title, message, opts) {
  opts = opts || {};
  return new Promise(function(resolve) {
    var root = document.getElementById("modal-root");
    var okLabel = opts.okLabel || "Create";
    var placeholder = opts.placeholder || "";
    var initial = opts.value || "";
    root.innerHTML =
      '<div class="modal-overlay" id="modal-overlay">' +
        '<div class="modal">' +
          '<h3>'+ esc(title) +'</h3>' +
          '<p>'+ esc(message) +'</p>' +
          '<input id="modal-input" class="modal-input" placeholder="'+ esc(placeholder) +'" value="'+ esc(initial) +'" />' +
          '<div class="modal-actions">' +
            '<button class="btn-ghost" id="modal-cancel">Cancel</button>' +
            '<button class="btn-primary" id="modal-ok">'+ esc(okLabel) +'</button>' +
          '</div>' +
        '</div>' +
      '</div>';
    var input = root.querySelector("#modal-input");
    function close(val) {
      root.innerHTML = "";
      resolve(val);
    }
    function submit() {
      var v = (input.value || "").trim();
      if (!v) { input.focus(); return; }
      close(v);
    }
    root.querySelector("#modal-cancel").addEventListener("click", function(){ close(null); });
    root.querySelector("#modal-ok").addEventListener("click", submit);
    input.addEventListener("keydown", function(e) {
      if (e.key === "Enter") { e.preventDefault(); submit(); }
      if (e.key === "Escape") close(null);
    });
    root.querySelector("#modal-overlay").addEventListener("click", function(e){
      if (e.target === e.currentTarget) close(null);
    });
    setTimeout(function(){ input.focus(); input.select(); }, 0);
  });
}

/* ═══════════════════════════════════════════════════
   API client
═══════════════════════════════════════════════════ */

var TOKEN_KEY = "kdb3_token";

var tokenInput = document.getElementById("token");
var tokenPanel = document.getElementById("token-panel");
var tokenToggle = document.getElementById("token-toggle");

tokenInput.value = localStorage.getItem(TOKEN_KEY) || "";
tokenInput.addEventListener("change", function() {
  var v = this.value.trim();
  if (v) localStorage.setItem(TOKEN_KEY, v);
  else localStorage.removeItem(TOKEN_KEY);
});

tokenToggle.addEventListener("click", function(e) {
  e.stopPropagation();
  var open = tokenPanel.classList.toggle("is-hidden");
  tokenToggle.classList.toggle("open", !open);
  tokenToggle.setAttribute("aria-expanded", open ? "false" : "true");
  if (!open) tokenInput.focus();
});

document.addEventListener("click", function(e) {
  if (tokenPanel.classList.contains("is-hidden")) return;
  if (tokenPanel.contains(e.target) || tokenToggle.contains(e.target)) return;
  tokenPanel.classList.add("is-hidden");
  tokenToggle.classList.remove("open");
  tokenToggle.setAttribute("aria-expanded", "false");
});

function request(path, init) {
  init = init || {};
  var headers = new Headers(init.headers || {});
  var token = localStorage.getItem(TOKEN_KEY) || "";
  if (token) headers.set("Authorization", "Bearer " + token);
  if (init.body && !headers.has("Content-Type"))
    headers.set("Content-Type", "application/json");
  return fetch(path, Object.assign({}, init, { headers: headers })).then(function(res) {
    return res.text().then(function(text) {
      var data = null;
      try { data = text ? JSON.parse(text) : null; } catch(e) { data = text; }
      if (!res.ok) {
        var msg = (data && (data.reason || data.error)) ||
          "HTTP " + res.status + (text ? ": " + text : "");
        var err = new Error(msg);
        err.status = res.status;
        err.data = data;
        throw err;
      }
      return data;
    });
  });
}

var api = {
  info:       function()          { return request("/"); },
  listDbs:    function()          { return request("/_cat/dbs"); },
  createDb:   function(n)         { return request("/"+enc(n), {method:"PUT"}); },
  deleteDb:   function(n)         { return request("/"+enc(n), {method:"DELETE"}); },
  dbStat:     function(n)         { return request("/"+enc(n)); },
  allDocs:    function(n,pg,lim)  { return request("/"+enc(n)+"/_all_docs?page="+(pg||1)+"&limit="+(lim||50)); },
  listDesignDocs: function(db) {
    return request("/"+enc(db)+"/_all_docs?page=1&limit=200&startkey="+enc("_design/")+"&endkey="+enc("_design0"));
  },
  getDoc:     function(db,id)     { return request("/"+enc(db)+"/"+enc(id)); },
  putDoc:     function(db,id,b)   { return request("/"+enc(db)+"/"+enc(id), {method:"PUT",body:b}); },
  postDoc:    function(db,b)      { return request("/"+enc(db), {method:"POST",body:b}); },
  deleteDoc:  function(db,id,rev) { return request("/"+enc(db)+"/"+enc(id)+"?rev="+rev, {method:"DELETE"}); },
  getDesign:  function(db,id)     { return request("/"+enc(db)+"/_design/"+enc(id)); },
  putDesign:  function(db,id,b)   { return request("/"+enc(db)+"/_design/"+enc(id), {method:"PUT",body:b}); },
  deleteDesign: function(db,id,rev) {
    return request("/"+enc(db)+"/_design/"+enc(id)+"?rev="+rev, {method:"DELETE"});
  },
  selectView: function(db,dd,v,s,params) {
    var p = "/"+enc(db)+"/_design/"+enc(dd)+"/"+enc(v);
    if (s && s !== "default") p += "/"+enc(s);
    var q = [];
    if (params) {
      Object.keys(params).forEach(function(k) {
        if (params[k] != null && params[k] !== "") q.push(enc(k)+"="+enc(String(params[k])));
      });
    }
    if (q.length) p += "?" + q.join("&");
    return request(p);
  },
  viewStatus: function(db,dd,v) {
    return request("/"+enc(db)+"/_design/"+enc(dd)+"/"+enc(v)+"/_status");
  },
  dryRunView: function(db,dd,v,body) {
    return request("/"+enc(db)+"/_design/"+enc(dd)+"/"+enc(v)+"/_dry_run", {
      method: "POST",
      body: JSON.stringify(body || {})
    });
  },
  changes:    function(db,since,lim) { return request("/"+enc(db)+"/_changes?since="+(since||0)+"&limit="+(lim||200)); },
  vacuum:     function(db)        { return request("/"+enc(db)+"/_vacuum", {method:"POST"}); },
};

/** Continuous _changes SSE (feed=eventsource). Supports Bearer via fetch stream. */
function followChanges(opts, onChange) {
  var db = opts.db;
  var since = opts.since != null ? Number(opts.since) : 0;
  var limit = opts.limit || 1000;
  var onError = typeof opts.onError === "function" ? opts.onError : null;
  var onStatus = typeof opts.onStatus === "function" ? opts.onStatus : null;
  var backoffMs = opts.backoffMs != null ? opts.backoffMs : 1000;
  var maxBackoffMs = opts.maxBackoffMs != null ? opts.maxBackoffMs : 15000;
  var aborted = false;
  var controller = null;
  var currentBackoff = backoffMs;

  function buildURL() {
    return "/"+enc(db)+"/_changes?feed=eventsource&since="+enc(String(since))+"&limit="+enc(String(limit));
  }

  function headers() {
    var h = { Accept: "text/event-stream" };
    var token = localStorage.getItem(TOKEN_KEY) || "";
    if (token) h.Authorization = "Bearer " + token;
    return h;
  }

  function handleDataLine(line) {
    if (line.indexOf("data:") !== 0) return;
    var payload = line.slice(5).replace(/^\s+/, "");
    if (!payload) return;
    var change;
    try { change = JSON.parse(payload); }
    catch (e) { if (onError) onError(e); return; }
    if (change && typeof change.update_seq === "number" && change.update_seq > since) {
      since = change.update_seq;
    }
    onChange(change);
  }

  function processBuffer(buf) {
    var parts = buf.split("\n");
    var rest = parts.pop();
    for (var i = 0; i < parts.length; i++) {
      var line = parts[i].replace(/\r$/, "");
      if (!line || line.charAt(0) === ":") continue;
      handleDataLine(line);
    }
    return rest || "";
  }

  function sleep(ms) {
    return new Promise(function(resolve) { setTimeout(resolve, ms); });
  }

  async function run() {
    while (!aborted) {
      controller = typeof AbortController !== "undefined" ? new AbortController() : null;
      try {
        if (onStatus) onStatus("connecting");
        var res = await fetch(buildURL(), {
          headers: headers(),
          signal: controller ? controller.signal : undefined
        });
        if (!res.ok) {
          var body = "";
          try { body = await res.text(); } catch (_) {}
          throw new Error("HTTP " + res.status + (body ? ": " + body : ""));
        }
        if (!res.body || !res.body.getReader) {
          throw new Error("ReadableStream not supported");
        }
        currentBackoff = backoffMs;
        if (onStatus) onStatus("live");
        var reader = res.body.getReader();
        var decoder = new TextDecoder("utf-8");
        var pending = "";
        while (!aborted) {
          var chunk = await reader.read();
          if (chunk.done) break;
          pending += decoder.decode(chunk.value, { stream: true });
          pending = processBuffer(pending);
        }
        if (!aborted && onStatus) onStatus("reconnecting");
      } catch (err) {
        if (aborted) return;
        if (onStatus) onStatus("reconnecting");
        if (onError) onError(err);
      }
      if (aborted) return;
      await sleep(currentBackoff);
      currentBackoff = Math.min(currentBackoff * 2, maxBackoffMs);
    }
  }

  run();
  return {
    abort: function() {
      aborted = true;
      if (controller) {
        try { controller.abort(); } catch (_) {}
      }
      if (onStatus) onStatus("stopped");
    },
    since: function() { return since; }
  };
}

function extractSelectParams(sql) {
  var params = [];
  var seen = {};
  var re = /\$\{(.*?)\}/g;
  var m;
  var text = sql || "";
  while ((m = re.exec(text)) !== null) {
    if (!seen[m[1]]) {
      seen[m[1]] = true;
      params.push(m[1]);
    }
  }
  return params;
}

function parseSQLErrorReason(reason) {
  var text = String(reason || "");
  var m = text.match(/^(setup|run|select)\[(\d+)\]:\s*(.*)$/);
  if (m) return { phase: m[1], index: parseInt(m[2], 10), message: m[3] };
  if (/invalid keyword:/i.test(text)) return { phase: "keyword", index: 0, message: text };
  return null;
}

function fillServerIdentity() {
  var el = document.getElementById("server-identity");
  if (!el) return;
  api.info().then(function(info) {
    var name = (info && info.name) || "kdb";
    var sqlite = info && info.version && info.version.sqlite;
    el.textContent = sqlite ? name + " · sqlite " + sqlite : name;
    el.title = el.textContent;
  }).catch(function() {
    el.textContent = "offline";
    el.title = "offline";
  });
}

/* ═══════════════════════════════════════════════════
   Shell helpers
═══════════════════════════════════════════════════ */

var indexEl = document.getElementById("index");
var stageEl = document.getElementById("stage");
var topbarEl = document.getElementById("topbar");
var workBody = document.querySelector(".work-body");
var railHome = document.getElementById("rail-home");

function setIndexVisible(visible) {
  workBody.classList.toggle("no-index", !visible);
}

function setTopbar(html) {
  topbarEl.innerHTML = html || "";
}

function refreshRail(activeDb) {
  api.listDbs().then(function(list) {
    var el = document.getElementById("rail-dbs");
    if (!list || !list.length) {
      el.innerHTML = "";
      return;
    }
    el.innerHTML = list.map(function(db) {
      var active = db === activeDb ? " active" : "";
      return '<a class="rail-db'+ active +'" href="#/db/'+ enc(db) +'/docs" title="'+ esc(db) +'">'+
        esc(dbInitials(db)) +'</a>';
    }).join("");
  }).catch(function(){});
  railHome.classList.toggle("active", !activeDb);
}

function dbTopbar(dbName, tab, metaHtml) {
  var tabs = [
    ["docs", "Documents"],
    ["design", "Design"],
    ["changes", "Changes"],
    ["vacuum", "Vacuum"]
  ];
  return '<div class="crumb">' +
      '<a href="#/">Databases</a><span class="sep">/</span>' +
      '<span class="cur" title="'+ esc(dbName) +'">'+ esc(dbName) +'</span>' +
    '</div>' +
    (metaHtml || '<div class="db-meta" id="db-meta">…</div>') +
    '<nav class="tabs">' +
      tabs.map(function(t) {
        var active = t[0] === tab ? " active" : "";
        return '<a class="tab-link'+ active +'" href="#/db/'+ enc(dbName) +"/"+ t[0] +'">'+ t[1] +'</a>';
      }).join("") +
    '</nav>' +
    '<button type="button" class="btn-ghost btn-small index-toggle" id="index-toggle">Index</button>' +
    '<button type="button" class="btn-danger btn-small" id="del-db-btn">Delete</button>';
}

function bindDeleteDb(dbName) {
  var btn = document.getElementById("del-db-btn");
  if (!btn) return;
  btn.addEventListener("click", function() {
    showConfirm("Delete database", 'Delete "' + dbName + '" permanently?', {
      okLabel: "Delete", danger: true
    }).then(function(ok) {
      if (!ok) return;
      api.deleteDb(dbName).then(function() {
        toast('Deleted "' + dbName + '"', "success");
        refreshRail(null);
        location.hash = "#/";
      }).catch(function(e) { toast(e.message, "error"); });
    });
  });
}

function bindIndexToggle() {
  var btn = document.getElementById("index-toggle");
  if (!btn) return;
  btn.addEventListener("click", function() {
    indexEl.classList.toggle("open");
  });
}

function loadDbMeta(dbName) {
  var el = document.getElementById("db-meta");
  if (!el) return;
  api.dbStat(dbName).then(function(s) {
    var docs = s.doc_count != null ? s.doc_count : "?";
    var seq = s.update_seq != null ? s.update_seq : "?";
    var del = s.deleted_doc_count != null ? s.deleted_doc_count : null;
    el.textContent = docs + " docs · seq " + seq + (del != null ? " · " + del + " deleted" : "");
  }).catch(function() {
    el.textContent = "";
  });
}

function hrefDoc(db, id) {
  return "#/db/" + enc(db) + "/docs/" + enc(id);
}

function hrefDesign(db, ddoc, view) {
  var h = "#/db/" + enc(db) + "/design";
  if (ddoc) h += "/" + enc(ddoc);
  if (view) h += "/" + enc(view);
  return h;
}

/* ═══════════════════════════════════════════════════
   Router
═══════════════════════════════════════════════════ */

var liveDocs = null;
var liveDesign = null;
var liveChanges = null;
var routeSig = "";

function parseRoute() {
  var hash = location.hash.replace(/^#/, "") || "/";
  var parts = hash.split("/").filter(Boolean).map(function(p) {
    try { return decodeURIComponent(p); } catch(e) { return p; }
  });
  if (!parts.length) return { name: "home" };
  if (parts[0] === "db" && parts[1]) {
    var db = parts[1];
    var tab = parts[2] || "docs";
    if (["docs", "design", "changes", "vacuum"].indexOf(tab) < 0) tab = "docs";
    if (!parts[2]) {
      location.replace("#/db/" + enc(db) + "/docs");
      return { name: "redirect" };
    }
    var route = { name: "db", db: db, tab: tab };
    if (tab === "docs" && parts[3]) route.docId = parts.slice(3).join("/");
    if (tab === "design") {
      if (parts[3]) route.ddoc = parts[3];
      if (parts[4]) route.view = parts[4];
    }
    return route;
  }
  return { name: "home" };
}

function shellSig(r) {
  if (r.name === "home") return "home";
  return "db:" + r.db + ":" + r.tab;
}

function route() {
  var r = parseRoute();
  if (r.name === "redirect") return;

  // Soft-update: same DB tab, only selection changed
  if (r.name === "db" && shellSig(r) === routeSig) {
    if (r.tab === "docs" && liveDocs) {
      if (r.docId) liveDocs.open(r.docId, true);
      else liveDocs.newDoc(true);
      return;
    }
    if (r.tab === "design" && liveDesign) {
      liveDesign.applyRoute(r.ddoc, r.view);
      return;
    }
  }

  if (liveChanges) {
    liveChanges.abort();
    liveChanges = null;
  }
  liveDocs = null;
  liveDesign = null;
  routeSig = shellSig(r);

  if (r.name === "home") {
    refreshRail(null);
    renderHome();
  } else {
    refreshRail(r.db);
    renderDatabase(r);
  }
}

window.addEventListener("hashchange", route);

document.getElementById("rail-home").addEventListener("click", function() {
  location.hash = "#/";
});

/* ═══════════════════════════════════════════════════
   Home — library catalog
═══════════════════════════════════════════════════ */

function renderHome() {
  setIndexVisible(false);
  setTopbar("");
  indexEl.innerHTML = "";
  bounceStage();

  stageEl.innerHTML =
    '<div class="home-hero">' +
      '<div class="home-brand">kdb<span>3</span></div>' +
      '<p class="home-lede">Document atelier — browse databases, edit JSON, and shape SQL views on the page.</p>' +
      '<div class="home-create">' +
        '<input id="new-db-name" placeholder="New database name" autocomplete="off" />' +
        '<button class="btn-primary" id="create-db-btn">Create</button>' +
      '</div>' +
      '<div class="catalog">' +
        '<div class="catalog-head">' +
          '<h2>Databases</h2>' +
          '<input class="catalog-filter" id="db-search" placeholder="Filter…" />' +
        '</div>' +
        '<div id="catalog-list">'+ emptyHTML('<span class="spinner"></span>') +'</div>' +
      '</div>' +
    '</div>';

  var allDbs = [];
  var stats = {};

  function renderCatalog() {
    var q = (document.getElementById("db-search") || {}).value || "";
    var filtered = q
      ? allDbs.filter(function(d){ return d.toLowerCase().indexOf(q.toLowerCase()) >= 0; })
      : allDbs;
    var list = document.getElementById("catalog-list");
    if (!list) return;
    if (!filtered.length) {
      list.innerHTML = emptyHTML(allDbs.length ? "No databases match." : "No databases yet.");
      return;
    }
    list.innerHTML = filtered.map(function(db) {
      var s = stats[db] || {};
      var docs = s.doc_count != null ? s.doc_count + " docs" : "…";
      var seq = s.update_seq != null ? "seq " + s.update_seq : "";
      return '<div class="catalog-row">' +
          '<a class="catalog-name" href="#/db/'+ enc(db) +'/docs">'+ esc(db) +'</a>' +
          '<span class="catalog-stat">'+ esc(docs) + (seq ? " · " + esc(seq) : "") +'</span>' +
          '<span class="catalog-actions">' +
            '<button type="button" class="btn-danger btn-small" data-del="'+ esc(db) +'">Delete</button>' +
          '</span>' +
        '</div>';
    }).join("");
  }

  function load() {
    Promise.all([api.listDbs(), api.info()]).then(function(r) {
      allDbs = r[0] || [];
      fillServerIdentity();
      renderCatalog();
      allDbs.forEach(function(db) {
        api.dbStat(db).then(function(s) {
          stats[db] = s;
          renderCatalog();
        }).catch(function(){});
      });
    }).catch(function(e) { toast(e.message, "error"); });
  }

  document.getElementById("db-search").addEventListener("input", renderCatalog);

  document.getElementById("create-db-btn").addEventListener("click", function() {
    var name = document.getElementById("new-db-name").value.trim();
    if (!name) { toast("Enter a database name", "info"); return; }
    api.createDb(name).then(function() {
      toast('Database "' + name + '" created', "success");
      document.getElementById("new-db-name").value = "";
      refreshRail(name);
      location.hash = "#/db/" + enc(name) + "/docs";
    }).catch(function(e) { toast(e.message, "error"); });
  });

  document.getElementById("new-db-name").addEventListener("keydown", function(e) {
    if (e.key === "Enter") document.getElementById("create-db-btn").click();
  });

  document.getElementById("catalog-list").addEventListener("click", function(e) {
    var btn = e.target.closest("[data-del]");
    if (!btn) return;
    var db = btn.getAttribute("data-del");
    showConfirm("Delete database", 'Delete "' + db + '" and all its documents? This cannot be undone.', {
      okLabel: "Delete", danger: true
    }).then(function(ok) {
      if (!ok) return;
      api.deleteDb(db).then(function() {
        toast('Deleted "' + db + '"', "success");
        delete stats[db];
        load();
        refreshRail(null);
      }).catch(function(e) { toast(e.message, "error"); });
    });
  });

  load();
}

/* ═══════════════════════════════════════════════════
   Database shell
═══════════════════════════════════════════════════ */

function renderDatabase(r) {
  setTopbar(dbTopbar(r.db, r.tab));
  bindIndexToggle();
  bindDeleteDb(r.db);
  loadDbMeta(r.db);
  bounceStage();

  if (r.tab === "docs") docsView(r.db, r.docId || null);
  else if (r.tab === "design") designView(r.db, r.ddoc || null, r.view || null);
  else if (r.tab === "changes") changesView(r.db);
  else if (r.tab === "vacuum") vacuumView(r.db);
}

/* ═══════════════════════════════════════════════════
   Documents
═══════════════════════════════════════════════════ */

function docsView(db, initialId) {
  setIndexVisible(true);
  indexEl.classList.remove("open");

  var selectedId = initialId || "";
  var allRows = [];
  var previews = {};
  var page = 1;
  var pageSize = 50;
  var totalRows = 0;

  indexEl.innerHTML =
    '<div class="index-head">' +
      '<div class="index-title">Documents</div>' +
      '<div class="index-tools">' +
        '<button type="button" class="btn-icon" id="refresh-docs-btn" title="Refresh">↻</button>' +
        '<button type="button" class="btn-primary btn-small" id="new-doc-btn">New</button>' +
      '</div>' +
    '</div>' +
    '<input class="index-search" id="doc-search" placeholder="Filter by ID…" />' +
    '<div class="index-scroll" id="doc-list"></div>' +
    '<div class="index-foot" id="doc-pager"></div>';

  stageEl.innerHTML =
    '<div class="editor">' +
      '<div class="editor-bar">' +
        '<span class="editor-title" id="editor-title">New document</span>' +
        '<span id="json-status" class="json-valid is-hidden">valid JSON</span>' +
        '<button class="btn-ghost btn-small" id="fmt-btn">Format</button>' +
        '<button class="btn-danger btn-small" id="del-doc-btn" disabled>Delete</button>' +
        '<button class="btn-primary btn-small" id="save-doc-btn">Save <kbd>'+ saveShortcutLabel() +'</kbd></button>' +
      '</div>' +
      '<div class="editor-body">' +
        '<textarea class="canvas" id="doc-editor" spellcheck="false">{\n  "hello": "world"\n}</textarea>' +
        '<div id="doc-feedback" class="editor-feedback"></div>' +
      '</div>' +
    '</div>';

  var editor = document.getElementById("doc-editor");
  var status = document.getElementById("json-status");

  editor.addEventListener("input", function() {
    try { JSON.parse(editor.value); status.className="json-valid"; status.textContent="valid JSON"; }
    catch(e) { status.className="json-invalid"; status.textContent="invalid JSON"; }
    setHidden(status, false);
  });

  editor.addEventListener("keydown", function(e) {
    if ((e.ctrlKey || e.metaKey) && e.key === "s") {
      e.preventDefault();
      document.getElementById("save-doc-btn").click();
    }
  });

  document.getElementById("fmt-btn").addEventListener("click", function() {
    editor.value = prettyJSON(editor.value);
  });

  function totalPages() {
    return Math.max(1, Math.ceil(totalRows / pageSize) || 1);
  }

  function renderList() {
    var q = (document.getElementById("doc-search") || {}).value || "";
    var rows = q
      ? allRows.filter(function(r){ return String(r.id).toLowerCase().indexOf(q.toLowerCase()) >= 0; })
      : allRows;
    var list = document.getElementById("doc-list");
    if (!list) return;
    if (!rows.length) {
      list.innerHTML = emptyHTML("No documents.", true);
      return;
    }
    list.innerHTML = rows.map(function(row) {
      var sel = row.id === selectedId ? " active" : "";
      var del = row.deleted ? '<span class="badge deleted">del</span>' : "";
      var prev = previews[row.id] || "";
      return '<button type="button" class="index-row'+ sel +'" data-id="'+ esc(row.id) +'">' +
        '<span class="id">'+ esc(row.id) +' '+ del +'</span>' +
        '<span class="rev">'+ esc(row.rev || "") +'</span>' +
        (prev ? '<span class="meta" title="'+ esc(prev) +'">·</span>' : "") +
      '</button>';
    }).join("");
  }

  function renderPager() {
    var pager = document.getElementById("doc-pager");
    if (!pager) return;
    if (!totalRows) {
      pager.innerHTML = '<span>0 documents</span>';
      return;
    }
    var pages = totalPages();
    var from = (page - 1) * pageSize + 1;
    var to = Math.min(page * pageSize, totalRows);
    pager.innerHTML =
      '<button type="button" class="btn-ghost btn-small" id="doc-prev"' + (page <= 1 ? " disabled" : "") + ">Prev</button>" +
      '<span>' + from + "–" + to + " of " + totalRows + "</span>" +
      '<button type="button" class="btn-ghost btn-small" id="doc-next"' + (page >= pages ? " disabled" : "") + ">Next</button>";
    document.getElementById("doc-prev").addEventListener("click", function() {
      if (page <= 1) return;
      page--;
      loadList();
    });
    document.getElementById("doc-next").addEventListener("click", function() {
      if (page >= pages) return;
      page++;
      loadList();
    });
  }

  function loadList(thenOpen) {
    document.getElementById("doc-list").innerHTML = emptyHTML('<span class="spinner"></span>', true);
    api.allDocs(db, page, pageSize).then(function(data) {
      allRows = (data && data.rows) || [];
      totalRows = (data && data.total_rows) || 0;
      var pages = totalPages();
      if (page > pages) {
        page = pages;
        if (totalRows > 0 && allRows.length === 0) {
          loadList(thenOpen);
          return;
        }
      }
      renderList();
      renderPager();
      if (thenOpen) openDoc(thenOpen);
    }).catch(function(e) { toast(e.message, "error"); });
  }

  function setDocHash(id) {
    var want = id ? hrefDoc(db, id) : ("#/db/" + enc(db) + "/docs");
    if (location.hash !== want) history.replaceState(null, "", want);
  }

  function openDoc(id, skipHash) {
    selectedId = id;
    renderList();
    document.getElementById("editor-title").textContent = id;
    document.getElementById("del-doc-btn").disabled = false;
    document.getElementById("doc-feedback").textContent = "";
    setHidden(status, true);
    if (!skipHash) setDocHash(id);
    api.getDoc(db, id).then(function(doc) {
      editor.value = JSON.stringify(doc, null, 2);
      setHidden(status, false);
      status.className = "json-valid";
      status.textContent = "valid JSON";
      previews[id] = docPreview(doc);
      renderList();
    }).catch(function(e) { toast(e.message, "error"); });
  }

  function newDoc(skipHash) {
    selectedId = "";
    document.getElementById("editor-title").textContent = "New document";
    document.getElementById("del-doc-btn").disabled = true;
    document.getElementById("doc-feedback").textContent = "";
    editor.value = '{\n  "hello": "world"\n}';
    setHidden(status, true);
    renderList();
    if (!skipHash) setDocHash(null);
  }

  liveDocs = { open: openDoc, newDoc: newDoc };

  document.getElementById("doc-search").addEventListener("input", renderList);

  document.getElementById("doc-list").addEventListener("click", function(e) {
    var row = e.target.closest("[data-id]");
    if (!row) return;
    openDoc(row.getAttribute("data-id"));
    indexEl.classList.remove("open");
  });

  document.getElementById("new-doc-btn").addEventListener("click", newDoc);
  document.getElementById("refresh-docs-btn").addEventListener("click", function(){ loadList(); });

  document.getElementById("save-doc-btn").addEventListener("click", function() {
    var txt = editor.value;
    var parsed;
    try { parsed = JSON.parse(txt); } catch(e) { toast("Invalid JSON: " + e.message, "error"); return; }
    var id = parsed._id || selectedId;
    var p = id ? api.putDoc(db, id, JSON.stringify(parsed)) : api.postDoc(db, JSON.stringify(parsed));
    var btn = this;
    btn.disabled = true;
    p.then(function(result) {
      var newId = result._id || id;
      toast('Saved "' + newId + '" rev ' + result._rev, "success");
      loadDbMeta(db);
      loadList(newId);
    }).catch(function(e) {
      toast(e.message, "error");
    }).then(function(){ btn.disabled = false; });
  });

  document.getElementById("del-doc-btn").addEventListener("click", function() {
    var parsed;
    try { parsed = JSON.parse(editor.value); } catch(e) { toast("Invalid JSON", "error"); return; }
    if (!parsed._id || parsed._rev == null) {
      toast("Document needs _id and _rev to delete", "error"); return;
    }
    showConfirm("Delete document", 'Delete "' + parsed._id + '"? This cannot be undone.', {
      okLabel: "Delete", danger: true
    }).then(function(ok) {
      if (!ok) return;
      api.deleteDoc(db, parsed._id, Number(parsed._rev)).then(function() {
        toast('Deleted "' + parsed._id + '"', "success");
        delete previews[parsed._id];
        loadDbMeta(db);
        newDoc();
        loadList();
      }).catch(function(e) { toast(e.message, "error"); });
    });
  });

  if (initialId) loadList(initialId);
  else loadList();
}

/* ═══════════════════════════════════════════════════
   Design & Views
═══════════════════════════════════════════════════ */

function shortDesignId(id) {
  return String(id || "").replace(/^_design\//, "");
}

function emptyView() {
  return {
    setup: ["CREATE TABLE IF NOT EXISTS my_index (key, doc_id, PRIMARY KEY(doc_id)) WITHOUT ROWID"],
    run: [
      "DELETE FROM my_index WHERE doc_id in (SELECT doc_id FROM latest_changes WHERE deleted = 1)",
      "INSERT OR REPLACE INTO my_index (key, doc_id) SELECT json_extract(data, '$.key'), doc_id FROM latest_documents WHERE deleted = 0 AND json_extract(data, '$.key') is not null"
    ],
    select: {
      default: "SELECT JSON_OBJECT('rows', JSON_GROUP_ARRAY(JSON_OBJECT('key', key, 'id', doc_id))) FROM my_index"
    }
  };
}

function newDesignTemplate(shortId) {
  return {
    _id: "_design/" + shortId,
    views: {
      by_title: {
        setup: ["CREATE TABLE IF NOT EXISTS posts (title, doc_id, PRIMARY KEY(doc_id)) WITHOUT ROWID"],
        run: [
          "DELETE FROM posts WHERE doc_id in (SELECT doc_id FROM latest_changes WHERE deleted = 1)",
          "INSERT OR REPLACE INTO posts (title, doc_id) SELECT json_extract(data, '$.title'), doc_id FROM latest_documents WHERE deleted = 0 AND json_extract(data, '$.title') is not null"
        ],
        select: {
          default: "SELECT JSON_OBJECT('rows', JSON_GROUP_ARRAY(JSON_OBJECT('title', title, 'id', doc_id))) FROM posts"
        }
      }
    }
  };
}

function normalizeView(v) {
  v = v || {};
  return {
    setup: Array.isArray(v.setup) ? v.setup.slice() : [],
    run: Array.isArray(v.run) ? v.run.slice() : [],
    select: (v.select && typeof v.select === "object") ? Object.assign({}, v.select) : { default: "" }
  };
}

function designView(db, preferDdoc, preferView) {
  setIndexVisible(true);
  indexEl.classList.remove("open");

  var designIds = [];
  var doc = null;
  var shortId = "";
  var selectedView = preferView || "";
  var selectedSelect = "default";
  var mode = "form";
  var dirty = false;
  var isNew = false;

  indexEl.innerHTML =
    '<div class="index-head">' +
      '<div class="index-title">Design</div>' +
      '<div class="index-tools">' +
        '<button type="button" class="btn-icon" id="refresh-designs-btn" title="Refresh">↻</button>' +
        '<button type="button" class="btn-primary btn-small" id="new-design-btn">New</button>' +
      '</div>' +
    '</div>' +
    '<div class="index-scroll">' +
      '<div class="index-section">Design docs</div>' +
      '<div class="index-group" id="design-list"></div>' +
      '<div class="index-section">Views</div>' +
      '<div id="view-list"></div>' +
    '</div>' +
    '<div class="index-foot">' +
      '<button type="button" class="btn-ghost btn-small" id="add-view-btn" disabled>+ View</button>' +
    '</div>';

  stageEl.innerHTML =
    '<div class="design-stage">' +
      '<div class="design-bar">' +
        '<span class="editor-title" id="design-title">Select a design doc</span>' +
        '<span id="design-rev" class="badge is-hidden"></span>' +
        '<span id="design-json-status" class="json-valid is-hidden"></span>' +
        '<div class="mode-toggle" id="mode-toggle">' +
          '<button type="button" class="mode-btn active" data-mode="form">Form</button>' +
          '<button type="button" class="mode-btn" data-mode="json">JSON</button>' +
        '</div>' +
        '<button class="btn-ghost btn-small is-hidden" id="fmt-design-btn">Format</button>' +
        '<button class="btn-danger btn-small" id="del-design-btn" disabled>Delete</button>' +
        '<button class="btn-primary btn-small" id="save-design-btn" disabled>Save <kbd>'+ saveShortcutLabel() +'</kbd></button>' +
      '</div>' +
      '<div id="design-form" class="view-form"></div>' +
      '<textarea class="view-json-editor is-hidden" id="design-json" spellcheck="false"></textarea>' +
      '<div class="run-panel">' +
        '<div class="run-panel-head">' +
          '<h3>View atelier</h3>' +
          '<span id="view-lag-badge" class="badge lag-badge is-hidden" title="View catch-up vs database update_seq"></span>' +
        '</div>' +
        '<div class="run-toolbar">' +
          '<label class="field-label">View <select id="run-view-sel"></select></label>' +
          '<label class="field-label">Select <select id="run-select-sel"></select></label>' +
          '<label class="field-label check-label"><input type="checkbox" id="run-stale" /> Stale</label>' +
          '<button class="btn-primary btn-small" id="run-view-btn" disabled>Run</button>' +
        '</div>' +
        '<div class="run-toolbar" id="run-param-fields"></div>' +
        '<div class="run-toolbar atelier-toolbar">' +
          '<label class="field-label">Since <input id="dry-since" class="input-seq" value="0" /></label>' +
          '<label class="field-label">Window <input id="dry-limit" class="input-narrow" value="300" title="Docs in dry-run window" /></label>' +
          '<label class="field-label check-label"><input type="checkbox" id="dry-include-sql" /> Generated SQL</label>' +
          '<button class="btn-ghost btn-small" id="dry-run-btn" disabled>Dry-run</button>' +
        '</div>' +
        '<div id="sql-inline-error" class="sql-inline-error is-hidden"></div>' +
        '<div id="view-result" class="code-block">' +
          '<span class="muted">Pick a view. Dry-run works on unsaved SQL; Run needs a saved design doc.</span>' +
        '</div>' +
      '</div>' +
    '</div>';

  var designListEl = document.getElementById("design-list");
  var viewListEl = document.getElementById("view-list");
  var formEl = document.getElementById("design-form");
  var jsonEl = document.getElementById("design-json");
  var jsonStatus = document.getElementById("design-json-status");
  var runViewSel = document.getElementById("run-view-sel");
  var runSelectSel = document.getElementById("run-select-sel");
  var runParamFields = document.getElementById("run-param-fields");
  var lagBadge = document.getElementById("view-lag-badge");
  var sqlInlineError = document.getElementById("sql-inline-error");
  var paramValueCache = {};

  function viewNames() {
    return doc && doc.views ? Object.keys(doc.views) : [];
  }

  function clearSQLInlineError() {
    sqlInlineError.textContent = "";
    sqlInlineError.className = "sql-inline-error is-hidden";
    formEl.querySelectorAll(".sql-row.has-error").forEach(function(el) {
      el.classList.remove("has-error");
    });
  }

  function showSQLInlineError(reason, errors) {
    clearSQLInlineError();
    var info = null;
    if (errors && errors.length) {
      var e0 = errors[0];
      info = {
        phase: e0.phase,
        index: e0.index,
        message: e0.phase === "keyword" ? ("invalid keyword: " + e0.message) : e0.message
      };
    } else {
      info = parseSQLErrorReason(reason);
    }
    var text = reason || (info && info.message) || "SQL error";
    sqlInlineError.textContent = text;
    sqlInlineError.classList.remove("is-hidden");
    if (!info || !info.phase || info.phase === "keyword") return;
    if (mode !== "form") return;
    if (info.phase === "select") {
      var selArea = document.getElementById("select-sql");
      if (selArea) {
        var row = selArea.closest(".sql-row");
        if (row) row.classList.add("has-error");
      }
      return;
    }
    var areas = formEl.querySelectorAll('.sql-area[data-sql="'+ info.phase +'"]');
    var target = areas[info.index];
    if (target) {
      var parent = target.closest(".sql-row");
      if (parent) parent.classList.add("has-error");
    }
  }

  function collectRunParams() {
    var params = {};
    runParamFields.querySelectorAll("input[data-param]").forEach(function(inp) {
      var key = inp.getAttribute("data-param");
      var val = inp.value;
      paramValueCache[key] = val;
      if (val !== "") params[key] = val;
    });
    return params;
  }

  function refreshParamFields() {
    syncFormFromDom();
    var viewName = runViewSel.value || selectedView;
    var selName = runSelectSel.value || selectedSelect || "default";
    var v = doc && doc.views && doc.views[viewName];
    var sql = v && v.select ? (v.select[selName] || "") : "";
    var names = extractSelectParams(sql);
    if (!names.length) {
      runParamFields.innerHTML = '<span class="muted param-empty">No ${param} placeholders in this select.</span>';
      return;
    }
    runParamFields.innerHTML = names.map(function(name) {
      var cached = paramValueCache[name];
      var fallback = name === "limit" ? "50" : (name === "offset" ? "0" : "");
      var val = cached != null ? cached : fallback;
      return '<label class="field-label">'+ esc(name) +
        ' <input data-param="'+ esc(name) +'" class="input-param" value="'+ esc(val) +'" /></label>';
    }).join("");
  }

  function refreshLagBadge() {
    if (!doc || isNew || !shortId || !selectedView) {
      setHidden(lagBadge, true);
      return;
    }
    var viewName = runViewSel.value || selectedView;
    api.viewStatus(db, shortId, viewName).then(function(st) {
      setHidden(lagBadge, false);
      lagBadge.classList.remove("lag-ok", "lag-warn", "lag-missing");
      if (!st.built) {
        lagBadge.textContent = "not built";
        lagBadge.classList.add("lag-missing");
        lagBadge.title = "No persistent view file yet";
        return;
      }
      var lag = st.lag || 0;
      lagBadge.textContent = lag === 0 ? "caught up" : ("lag " + lag);
      lagBadge.classList.add(lag === 0 ? "lag-ok" : "lag-warn");
      lagBadge.title = "db " + st.db_update_seq + " · view " + st.view_update_seq +
        (st.open ? " · open" : "");
    }).catch(function() {
      setHidden(lagBadge, true);
    });
  }

  function draftViewBody() {
    var viewName = runViewSel.value || selectedView;
    var source = doc;
    if (mode === "json") {
      try { source = JSON.parse(jsonEl.value); }
      catch (e) { toast("Invalid JSON: " + e.message, "error"); return null; }
    } else {
      syncFormFromDom();
    }
    var v = source && source.views && source.views[viewName];
    if (!v) return null;
    v = normalizeView(v);
    return {
      setup: (v.setup || []).slice(),
      run: (v.run || []).slice(),
      select: Object.assign({}, v.select || {})
    };
  }

  function currentView() {
    if (!doc || !selectedView || !doc.views[selectedView]) return null;
    return doc.views[selectedView];
  }

  function ensureSelectKey() {
    var v = currentView();
    if (!v) return;
    var keys = Object.keys(v.select || {});
    if (!keys.length) {
      v.select = { default: "" };
      selectedSelect = "default";
    } else if (keys.indexOf(selectedSelect) < 0) {
      selectedSelect = keys[0];
    }
  }

  function syncHash() {
    if (!shortId || isNew) return;
    var want = hrefDesign(db, shortId, selectedView || null);
    if (location.hash !== want) history.replaceState(null, "", want);
  }

  function applyRoute(ddoc, view) {
    if (ddoc && ddoc !== shortId && !isNew) {
      preferView = view || null;
      selectDesign(ddoc);
      return;
    }
    if (view && view !== selectedView && doc && doc.views && doc.views[view]) {
      syncFormFromDom();
      selectedView = view;
      ensureSelectKey();
      renderAll();
    }
  }

  liveDesign = { applyRoute: applyRoute };

  function setMode(next) {
    if (next === mode) return;
    if (next === "json") {
      syncFormFromDom();
      jsonEl.value = JSON.stringify(doc, null, 2);
      updateJsonStatus();
    } else {
      try {
        doc = JSON.parse(jsonEl.value);
        if (!doc.views || typeof doc.views !== "object") doc.views = {};
        Object.keys(doc.views).forEach(function(k) {
          doc.views[k] = normalizeView(doc.views[k]);
        });
        if (doc._id) shortId = shortDesignId(doc._id);
        var names = viewNames();
        if (names.indexOf(selectedView) < 0) selectedView = names[0] || "";
        ensureSelectKey();
      } catch (e) {
        toast("Fix invalid JSON before switching to Form", "error");
        return;
      }
    }
    mode = next;
    document.querySelectorAll("#mode-toggle .mode-btn").forEach(function(btn) {
      btn.classList.toggle("active", btn.getAttribute("data-mode") === mode);
    });
    setHidden(formEl, mode !== "form");
    setHidden(jsonEl, mode !== "json");
    setHidden(document.getElementById("fmt-design-btn"), mode !== "json");
    setHidden(jsonStatus, mode !== "json");
    if (mode === "form") renderForm();
    renderBrowser();
    fillRunControls();
  }

  function updateJsonStatus() {
    try {
      JSON.parse(jsonEl.value);
      jsonStatus.className = "json-valid";
      jsonStatus.textContent = "valid JSON";
    } catch (e) {
      jsonStatus.className = "json-invalid";
      jsonStatus.textContent = "invalid JSON";
    }
  }

  function syncFormFromDom() {
    if (mode !== "form" || !doc || !selectedView) return;
    var v = currentView();
    if (!v) return;
    var nameInput = document.getElementById("view-name-input");
    if (nameInput) {
      var newName = nameInput.value.trim();
      if (newName && newName !== selectedView && !doc.views[newName]) {
        doc.views[newName] = v;
        delete doc.views[selectedView];
        selectedView = newName;
      } else if (nameInput.value !== selectedView) {
        nameInput.value = selectedView;
      }
    }
    var setupAreas = formEl.querySelectorAll("[data-sql='setup']");
    v.setup = Array.prototype.map.call(setupAreas, function(el){ return el.value; });
    var runAreas = formEl.querySelectorAll("[data-sql='run']");
    v.run = Array.prototype.map.call(runAreas, function(el){ return el.value; });
    var selArea = document.getElementById("select-sql");
    if (selArea) {
      if (!v.select) v.select = {};
      v.select[selectedSelect] = selArea.value;
    }
  }

  function getDocForSave() {
    if (mode === "json") return JSON.parse(jsonEl.value);
    syncFormFromDom();
    if (shortId) doc._id = "_design/" + shortId;
    return doc;
  }

  function renderBrowser() {
    if (!designIds.length && !isNew) {
      designListEl.innerHTML = emptyHTML("No design docs.", true);
    } else {
      var ids = designIds.slice();
      if (isNew && ids.indexOf(shortId) < 0) ids = [shortId].concat(ids);
      designListEl.innerHTML = ids.map(function(id) {
        var sel = id === shortId ? " active" : "";
        var mark = (isNew && id === shortId) || (dirty && id === shortId)
          ? ' <span class="dirty-dot" title="unsaved">●</span>' : "";
        return '<button type="button" class="index-row'+ sel +'" data-design="'+ esc(id) +'">' +
          '<span class="id">'+ esc(id) + mark +'</span>' +
          (id === "_views" ? '<span class="badge">builtin</span>' : "") +
        '</button>';
      }).join("");
    }

    var names = viewNames();
    document.getElementById("add-view-btn").disabled = !doc;
    if (!doc) {
      viewListEl.innerHTML = emptyHTML("—", true);
    } else if (!names.length) {
      viewListEl.innerHTML = emptyHTML("No views yet.", true);
    } else {
      viewListEl.innerHTML = names.map(function(name) {
        var sel = name === selectedView ? " active" : "";
        return '<button type="button" class="index-row'+ sel +'" data-view="'+ esc(name) +'">' +
          '<span class="id">'+ esc(name) +'</span>' +
        '</button>';
      }).join("");
    }
  }

  function sqlListHtml(kind, statements) {
    if (!statements.length) return emptyHTML("No statements.");
    return statements.map(function(sql, i) {
      return '<div class="sql-row">' +
        '<textarea class="sql-area" data-sql="'+ kind +'" data-idx="'+ i +'" spellcheck="false" rows="3">'+ esc(sql) +'</textarea>' +
        '<button type="button" class="btn-icon sql-remove" data-remove="'+ kind +'" data-idx="'+ i +'" title="Remove">×</button>' +
      '</div>';
    }).join("");
  }

  function renderForm() {
    if (!doc) {
      formEl.innerHTML = '<div class="empty">Select or create a design document.</div>';
      return;
    }
    ensureSelectKey();
    var v = currentView();
    if (!v) {
      formEl.innerHTML = '<div class="empty">No views yet. Click <strong>+ View</strong>.</div>';
      return;
    }
    var selectKeys = Object.keys(v.select || {});
    formEl.innerHTML =
      '<div class="view-form-header">' +
        '<label class="field-label grow">View name ' +
          '<input id="view-name-input" value="'+ esc(selectedView) +'" />' +
        '</label>' +
        '<button type="button" class="btn-ghost btn-small" id="remove-view-btn">Remove view</button>' +
      '</div>' +
      '<section class="view-section">' +
        '<div class="view-section-head">' +
          '<div><div class="view-section-title">Setup</div>' +
          '<p class="hint">DDL that creates index tables.</p></div>' +
          '<button type="button" class="btn-ghost btn-small" data-add="setup">+ Statement</button>' +
        '</div>' +
        '<div class="sql-list">'+ sqlListHtml("setup", v.setup) +'</div>' +
      '</section>' +
      '<section class="view-section">' +
        '<div class="view-section-head">' +
          '<div><div class="view-section-title">Run</div>' +
          '<p class="hint">Incremental updates via <code>latest_documents</code> / <code>latest_changes</code>.</p></div>' +
          '<button type="button" class="btn-ghost btn-small" data-add="run">+ Statement</button>' +
        '</div>' +
        '<div class="sql-list">'+ sqlListHtml("run", v.run) +'</div>' +
      '</section>' +
      '<section class="view-section">' +
        '<div class="view-section-head">' +
          '<div><div class="view-section-title">Selects</div>' +
          '<p class="hint">Named queries. Params like <code>${limit}</code>, <code>${startkey}</code>.</p></div>' +
          '<button type="button" class="btn-ghost btn-small" id="add-select-btn">+ Select</button>' +
        '</div>' +
        '<div class="select-chips" id="select-chips">' +
          selectKeys.map(function(k) {
            return '<button type="button" class="select-chip'+ (k === selectedSelect ? " active" : "") +'" data-select="'+ esc(k) +'">'+ esc(k) +'</button>';
          }).join("") +
        '</div>' +
        '<div class="sql-row">' +
          '<textarea class="sql-area" id="select-sql" spellcheck="false" rows="5">'+ esc(v.select[selectedSelect] || "") +'</textarea>' +
          (selectedSelect !== "default"
            ? '<button type="button" class="btn-icon" id="remove-select-btn" title="Remove select">×</button>'
            : "") +
        '</div>' +
      '</section>';
  }

  function updateToolbar() {
    var title = document.getElementById("design-title");
    var rev = document.getElementById("design-rev");
    var saveBtn = document.getElementById("save-design-btn");
    var delBtn = document.getElementById("del-design-btn");
    var runBtn = document.getElementById("run-view-btn");
    var dryBtn = document.getElementById("dry-run-btn");
    if (!doc) {
      title.textContent = "Select a design doc";
      setHidden(rev, true);
      saveBtn.disabled = true;
      delBtn.disabled = true;
      runBtn.disabled = true;
      dryBtn.disabled = true;
      setHidden(lagBadge, true);
      return;
    }
    title.textContent = shortId + (dirty ? " •" : "");
    if (doc._rev != null) {
      setHidden(rev, false);
      rev.textContent = "rev " + doc._rev;
    } else setHidden(rev, true);
    saveBtn.disabled = false;
    delBtn.disabled = isNew || shortId === "_views" || doc._rev == null;
    runBtn.disabled = !selectedView || isNew;
    dryBtn.disabled = !selectedView;
  }

  function fillRunControls() {
    var names = viewNames();
    var prevView = runViewSel.value || selectedView;
    runViewSel.innerHTML = names.map(function(n) {
      return '<option value="'+ esc(n) +'">'+ esc(n) +'</option>';
    }).join("");
    if (names.indexOf(prevView) >= 0) runViewSel.value = prevView;
    else if (selectedView) runViewSel.value = selectedView;

    var v = doc && doc.views && doc.views[runViewSel.value];
    var keys = v && v.select ? Object.keys(v.select) : ["default"];
    if (!keys.length) keys = ["default"];
    var prevSel = selectedSelect || runSelectSel.value;
    runSelectSel.innerHTML = keys.map(function(k) {
      return '<option value="'+ esc(k) +'">'+ esc(k) +'</option>';
    }).join("");
    if (keys.indexOf(prevSel) >= 0) runSelectSel.value = prevSel;
    else runSelectSel.value = keys[0];
    selectedSelect = runSelectSel.value;
    refreshParamFields();
    refreshLagBadge();
  }

  function renderAll() {
    renderBrowser();
    if (mode === "form") renderForm();
    else {
      jsonEl.value = JSON.stringify(doc, null, 2);
      updateJsonStatus();
    }
    fillRunControls();
    updateToolbar();
    syncHash();
  }

  function selectDesign(id, opts) {
    opts = opts || {};
    function go() {
      shortId = id;
      isNew = false;
      dirty = false;
      api.getDesign(db, id).then(function(data) {
        doc = data;
        if (!doc.views) doc.views = {};
        Object.keys(doc.views).forEach(function(k) {
          doc.views[k] = normalizeView(doc.views[k]);
        });
        shortId = shortDesignId(doc._id || id);
        var names = viewNames();
        if (preferView && names.indexOf(preferView) >= 0) {
          selectedView = preferView;
          preferView = null;
        } else {
          selectedView = names.indexOf(selectedView) >= 0 ? selectedView : (names[0] || "");
        }
        ensureSelectKey();
        mode = "form";
        document.querySelectorAll("#mode-toggle .mode-btn").forEach(function(btn) {
          btn.classList.toggle("active", btn.getAttribute("data-mode") === "form");
        });
        setHidden(formEl, false);
        setHidden(jsonEl, true);
        setHidden(document.getElementById("fmt-design-btn"), true);
        setHidden(jsonStatus, true);
        renderAll();
      }).catch(function(e) { toast(e.message, "error"); });
    }
    if (dirty && !opts.force) {
      showConfirm("Unsaved changes", "Discard changes to the current design doc?", {
        okLabel: "Discard", danger: true
      }).then(function(ok) { if (ok) go(); });
    } else go();
  }

  function loadDesignList(preferId, reloadCurrent) {
    api.listDesignDocs(db).then(function(data) {
      designIds = ((data && data.rows) || [])
        .map(function(r){ return shortDesignId(r.id); })
        .filter(Boolean);
      if (!designIds.length) designIds = ["_views"];
      var pick = preferId || preferDdoc || shortId || "_views";
      preferDdoc = null;
      if (designIds.indexOf(pick) < 0) pick = designIds[0];
      if (!doc || shortId !== pick || reloadCurrent) selectDesign(pick, { force: true });
      else renderBrowser();
    }).catch(function(e) {
      toast(e.message, "error");
      designIds = ["_views"];
      selectDesign("_views", { force: true });
    });
  }

  designListEl.addEventListener("click", function(e) {
    var btn = e.target.closest("[data-design]");
    if (!btn) return;
    var id = btn.getAttribute("data-design");
    if (id === shortId && !isNew) return;
    selectDesign(id);
    indexEl.classList.remove("open");
  });

  viewListEl.addEventListener("click", function(e) {
    var btn = e.target.closest("[data-view]");
    if (!btn || !doc) return;
    syncFormFromDom();
    selectedView = btn.getAttribute("data-view");
    ensureSelectKey();
    renderAll();
    indexEl.classList.remove("open");
  });

  document.getElementById("refresh-designs-btn").addEventListener("click", function() {
    if (dirty) {
      showConfirm("Unsaved changes", "Discard changes and reload design docs?", {
        okLabel: "Discard", danger: true
      }).then(function(ok) {
        if (!ok) return;
        dirty = false;
        loadDesignList(shortId, true);
      });
      return;
    }
    loadDesignList(shortId, true);
  });

  document.getElementById("new-design-btn").addEventListener("click", function() {
    showPrompt("New design document", "Enter a short id (saved as _design/<id>).", {
      placeholder: "posts", value: "posts", okLabel: "Create"
    }).then(function(id) {
      if (!id) return;
      id = shortDesignId(id).replace(/[^A-Za-z0-9_$-]/g, "_");
      if (!id) { toast("Invalid design id", "error"); return; }
      if (id === "_views") { toast("_views is reserved", "error"); return; }
      function create() {
        doc = newDesignTemplate(id);
        shortId = id;
        selectedView = "by_title";
        selectedSelect = "default";
        isNew = true;
        dirty = true;
        mode = "form";
        document.querySelectorAll("#mode-toggle .mode-btn").forEach(function(btn) {
          btn.classList.toggle("active", btn.getAttribute("data-mode") === "form");
        });
        setHidden(formEl, false);
        setHidden(jsonEl, true);
        setHidden(document.getElementById("fmt-design-btn"), true);
        setHidden(jsonStatus, true);
        renderAll();
      }
      if (dirty) {
        showConfirm("Unsaved changes", "Discard changes and create a new design doc?", {
          okLabel: "Discard", danger: true
        }).then(function(ok) { if (ok) create(); });
      } else create();
    });
  });

  document.getElementById("add-view-btn").addEventListener("click", function() {
    if (!doc) return;
    syncFormFromDom();
    showPrompt("Add view", "Name for the new view.", {
      placeholder: "by_field", value: "by_field", okLabel: "Add"
    }).then(function(name) {
      if (!name) return;
      name = name.trim().replace(/[^A-Za-z0-9_$-]/g, "_");
      if (!name) { toast("Invalid view name", "error"); return; }
      if (doc.views[name]) { toast("View already exists", "error"); return; }
      doc.views[name] = emptyView();
      selectedView = name;
      selectedSelect = "default";
      dirty = true;
      renderAll();
    });
  });

  formEl.addEventListener("click", function(e) {
    var add = e.target.closest("[data-add]");
    if (add) {
      syncFormFromDom();
      var kind = add.getAttribute("data-add");
      var v = currentView();
      if (!v) return;
      v[kind].push("");
      dirty = true;
      renderForm();
      fillRunControls();
      updateToolbar();
      return;
    }
    var remove = e.target.closest("[data-remove]");
    if (remove) {
      syncFormFromDom();
      var rk = remove.getAttribute("data-remove");
      var idx = parseInt(remove.getAttribute("data-idx"), 10);
      var cv = currentView();
      if (!cv) return;
      cv[rk].splice(idx, 1);
      dirty = true;
      renderForm();
      updateToolbar();
      return;
    }
    var chip = e.target.closest("[data-select]");
    if (chip) {
      syncFormFromDom();
      selectedSelect = chip.getAttribute("data-select");
      renderForm();
      fillRunControls();
      return;
    }
    if (e.target.id === "add-select-btn") {
      syncFormFromDom();
      showPrompt("Add select", "Name for the select query (e.g. with_docs).", {
        placeholder: "with_docs", value: "with_docs", okLabel: "Add"
      }).then(function(name) {
        if (!name) return;
        name = name.trim().replace(/[^A-Za-z0-9_$-]/g, "_");
        var vv = currentView();
        if (!vv) return;
        if (vv.select[name]) { toast("Select already exists", "error"); return; }
        vv.select[name] = "";
        selectedSelect = name;
        dirty = true;
        renderForm();
        fillRunControls();
        updateToolbar();
      });
      return;
    }
    if (e.target.id === "remove-select-btn") {
      syncFormFromDom();
      var v2 = currentView();
      if (!v2 || selectedSelect === "default") return;
      delete v2.select[selectedSelect];
      selectedSelect = Object.keys(v2.select)[0] || "default";
      if (!v2.select[selectedSelect]) v2.select.default = "";
      dirty = true;
      renderForm();
      fillRunControls();
      updateToolbar();
      return;
    }
    if (e.target.id === "remove-view-btn") {
      syncFormFromDom();
      showConfirm("Remove view", 'Remove view "' + selectedView + '" from this design doc?', {
        okLabel: "Remove", danger: true
      }).then(function(ok) {
        if (!ok) return;
        delete doc.views[selectedView];
        var names = viewNames();
        selectedView = names[0] || "";
        ensureSelectKey();
        dirty = true;
        renderAll();
      });
    }
  });

  formEl.addEventListener("input", function() {
    if (!doc) return;
    dirty = true;
    updateToolbar();
  });

  formEl.addEventListener("change", function(e) {
    if (e.target.id === "view-name-input") {
      syncFormFromDom();
      dirty = true;
      renderBrowser();
      fillRunControls();
      updateToolbar();
      syncHash();
    }
  });

  document.getElementById("mode-toggle").addEventListener("click", function(e) {
    var btn = e.target.closest("[data-mode]");
    if (!btn || !doc) return;
    setMode(btn.getAttribute("data-mode"));
  });

  document.getElementById("fmt-design-btn").addEventListener("click", function() {
    jsonEl.value = prettyJSON(jsonEl.value);
    updateJsonStatus();
  });

  jsonEl.addEventListener("input", function() {
    dirty = true;
    updateJsonStatus();
    updateToolbar();
  });

  function saveDesign() {
    var parsed;
    try { parsed = getDocForSave(); }
    catch (e) { toast("Invalid JSON: " + e.message, "error"); return; }
    if (!parsed.views || typeof parsed.views !== "object") {
      toast("Design doc needs a views object", "error"); return;
    }
    var id = shortDesignId(parsed._id || shortId);
    if (!id) { toast("Design doc needs an id", "error"); return; }
    parsed._id = "_design/" + id;
    var btn = document.getElementById("save-design-btn");
    btn.disabled = true;
    clearSQLInlineError();
    api.putDesign(db, id, JSON.stringify(parsed)).then(function(r) {
      toast("Design doc saved (rev " + r._rev + ")", "success");
      isNew = false;
      dirty = false;
      shortId = id;
      if (designIds.indexOf(id) < 0) designIds.push(id);
      selectDesign(id, { force: true });
    }).catch(function(e) {
      toast(e.message, "error");
      showSQLInlineError(e.message, e.data && e.data.errors);
    }).then(function(){ btn.disabled = false; updateToolbar(); });
  }

  document.getElementById("save-design-btn").addEventListener("click", saveDesign);

  document.getElementById("del-design-btn").addEventListener("click", function() {
    if (!doc || isNew) return;
    if (shortId === "_views") {
      toast("Cannot delete the builtin _views design doc", "error");
      return;
    }
    showConfirm("Delete design document", 'Delete "_design/' + shortId + '"? This cannot be undone.', {
      okLabel: "Delete", danger: true
    }).then(function(ok) {
      if (!ok) return;
      api.deleteDesign(db, shortId, doc._rev).then(function() {
        toast('Deleted "_design/' + shortId + '"', "success");
        doc = null;
        shortId = "";
        dirty = false;
        isNew = false;
        loadDesignList("_views");
      }).catch(function(e) { toast(e.message, "error"); });
    });
  });

  document.getElementById("run-view-btn").addEventListener("click", function() {
    if (!doc || isNew) {
      toast("Save the design doc before running", "error");
      return;
    }
    syncFormFromDom();
    clearSQLInlineError();
    var view = runViewSel.value || selectedView;
    var sel = runSelectSel.value || "default";
    var stale = document.getElementById("run-stale").checked;
    var res = document.getElementById("view-result");
    var btn = this;
    var params = collectRunParams();
    if (stale) params.stale = "true";
    res.innerHTML = '<span class="spinner"></span>';
    btn.disabled = true;
    api.selectView(db, shortId, view, sel, params).then(function(data) {
      res.innerHTML = jsonHighlight(JSON.stringify(data, null, 2));
      refreshLagBadge();
    }).catch(function(e) {
      toast(e.message, "error");
      showSQLInlineError(e.message, e.data && e.data.errors);
      res.innerHTML = '<span class="error-text">'+ esc(e.message) +'</span>';
    }).then(function(){ btn.disabled = false; updateToolbar(); });
  });

  document.getElementById("dry-run-btn").addEventListener("click", function() {
    if (!doc || !selectedView) {
      toast("Select a view to dry-run", "error");
      return;
    }
    var draft = draftViewBody();
    if (!draft) {
      toast("No view draft available", "error");
      return;
    }
    clearSQLInlineError();
    var view = runViewSel.value || selectedView;
    var sel = runSelectSel.value || "default";
    var since = parseInt(document.getElementById("dry-since").value, 10) || 0;
    var limit = parseInt(document.getElementById("dry-limit").value, 10) || 300;
    var includeSQL = document.getElementById("dry-include-sql").checked;
    var res = document.getElementById("view-result");
    var btn = this;
    var body = {
      setup: draft.setup,
      run: draft.run,
      select: draft.select,
      since: since,
      limit: limit,
      select_name: sel,
      params: collectRunParams(),
      include_sql: includeSQL
    };
    var ddoc = shortId || "draft";
    res.innerHTML = '<span class="spinner"></span>';
    btn.disabled = true;
    api.dryRunView(db, ddoc, view, body).then(function(data) {
      res.innerHTML = jsonHighlight(JSON.stringify(data, null, 2));
    }).catch(function(e) {
      toast(e.message, "error");
      var payload = e.data;
      showSQLInlineError(e.message, payload && payload.errors);
      if (payload && typeof payload === "object") {
        res.innerHTML = jsonHighlight(JSON.stringify(payload, null, 2));
      } else {
        res.innerHTML = '<span class="error-text">'+ esc(e.message) +'</span>';
      }
    }).then(function(){ btn.disabled = false; updateToolbar(); });
  });

  runViewSel.addEventListener("change", function() {
    selectedView = runViewSel.value;
    ensureSelectKey();
    if (mode === "form") renderForm();
    renderBrowser();
    fillRunControls();
    syncHash();
  });

  runSelectSel.addEventListener("change", function() {
    selectedSelect = runSelectSel.value || "default";
    refreshParamFields();
  });

  formEl.addEventListener("input", function(e) {
    if (e.target && e.target.id === "select-sql") refreshParamFields();
  });

  stageEl.addEventListener("keydown", function(e) {
    if ((e.ctrlKey || e.metaKey) && e.key === "s") {
      e.preventDefault();
      if (!document.getElementById("save-design-btn").disabled) saveDesign();
    }
  });

  loadDesignList(preferDdoc || "_views");
}

/* ═══════════════════════════════════════════════════
   Changes
═══════════════════════════════════════════════════ */

function changesView(db) {
  setIndexVisible(false);
  indexEl.innerHTML = "";
  indexEl.classList.remove("open");

  var MAX_ROWS = 500;
  var seen = 0;
  var empty = true;
  var sub = null;

  stageEl.innerHTML =
    '<div class="plain-stage">' +
      '<div class="toolbar">' +
        '<label class="inline-label">Since <input id="since-in" class="input-seq" value="0" /></label>' +
        '<label class="inline-label">Batch <input id="limit-in" class="input-limit" value="200" title="Catch-up batch size" /></label>' +
        '<button class="btn-primary btn-small" id="changes-toggle-btn">Stop</button>' +
        '<button class="btn-ghost btn-small" id="changes-clear-btn">Clear</button>' +
        '<span id="changes-live" class="live-badge live-connecting">connecting</span>' +
        '<span id="changes-count" class="muted"></span>' +
      '</div>' +
      '<div class="table-wrap" id="changes-scroll">' +
        '<table class="data"><thead><tr><th>Seq</th><th>ID</th><th>Rev</th><th></th></tr></thead>' +
        '<tbody id="changes-tbody">' +
          '<tr class="changes-empty"><td colspan="4">'+ emptyHTML("Listening for changes…") +'</td></tr>' +
        '</tbody></table>' +
      '</div>' +
    '</div>';

  var tbody = document.getElementById("changes-tbody");
  var countEl = document.getElementById("changes-count");
  var liveEl = document.getElementById("changes-live");
  var toggleBtn = document.getElementById("changes-toggle-btn");
  var sinceIn = document.getElementById("since-in");
  var limitIn = document.getElementById("limit-in");
  var scrollEl = document.getElementById("changes-scroll");

  function setLiveStatus(status) {
    liveEl.className = "live-badge live-" + status;
    liveEl.textContent = status;
  }

  function updateCount() {
    countEl.textContent = seen ? (seen + " events") : "";
  }

  function rowHTML(r) {
    var del = r.deleted ? '<span class="badge deleted badge-gap">deleted</span>' : "";
    var design = String(r.id).startsWith("_design/")
      ? '<span class="badge badge-gap">design</span>' : "";
    return '<tr>' +
      '<td><span class="badge">'+ esc(r.update_seq) +'</span></td>' +
      '<td><span class="doc-id">'+ esc(r.id) + design + del +'</span></td>' +
      '<td><span class="badge">'+ esc(r.rev) +'</span></td>' +
      '<td><button class="btn-icon btn-small" data-copy="'+ esc(r.id) +'" title="Copy ID">⧉</button></td>' +
    '</tr>';
  }

  function appendChange(r) {
    if (!r || r.update_seq == null) return;
    if (empty) {
      tbody.innerHTML = "";
      empty = false;
    }
    var nearBottom = scrollEl.scrollHeight - scrollEl.scrollTop - scrollEl.clientHeight < 80;
    tbody.insertAdjacentHTML("beforeend", rowHTML(r));
    seen++;
    while (tbody.children.length > MAX_ROWS) {
      tbody.removeChild(tbody.firstChild);
    }
    sinceIn.value = String(r.update_seq);
    updateCount();
    if (nearBottom) scrollEl.scrollTop = scrollEl.scrollHeight;
  }

  function stop() {
    if (sub) {
      sub.abort();
      sub = null;
    }
    if (liveChanges === apiHandle) liveChanges = null;
    toggleBtn.textContent = "Start";
    setLiveStatus("stopped");
  }

  function start() {
    stop();
    var since = parseInt(sinceIn.value, 10) || 0;
    var lim = parseInt(limitIn.value, 10) || 200;
    toggleBtn.textContent = "Stop";
    setLiveStatus("connecting");
    sub = followChanges({
      db: db,
      since: since,
      limit: lim,
      onStatus: setLiveStatus,
      onError: function(err) {
        if (err && err.name === "AbortError") return;
        // transient; badge shows reconnecting
      }
    }, appendChange);
    liveChanges = apiHandle;
  }

  function clearRows() {
    tbody.innerHTML = '<tr class="changes-empty"><td colspan="4">'+ emptyHTML("Listening for changes…") +'</td></tr>';
    empty = true;
    seen = 0;
    updateCount();
  }

  var apiHandle = {
    abort: function() {
      if (sub) {
        sub.abort();
        sub = null;
      }
    }
  };

  toggleBtn.addEventListener("click", function() {
    if (sub) stop();
    else start();
  });

  document.getElementById("changes-clear-btn").addEventListener("click", clearRows);

  stageEl.addEventListener("click", function(e) {
    var btn = e.target.closest("[data-copy]");
    if (btn) { copyText(btn.getAttribute("data-copy")); toast("Copied", "info", 1500); }
  });

  liveChanges = apiHandle;
  start();
}

/* ═══════════════════════════════════════════════════
   Vacuum
═══════════════════════════════════════════════════ */

function vacuumView(db) {
  setIndexVisible(false);
  indexEl.innerHTML = "";
  indexEl.classList.remove("open");

  stageEl.innerHTML =
    '<div class="plain-stage">' +
      '<h2 style="font-size:1.25rem;font-weight:600">Live vacuum</h2>' +
      '<p class="vacuum-copy">' +
        'Vacuum copies live documents into a fresh SQLite file (compacting deleted rows) ' +
        'then atomically swaps it in. The database stays readable during the operation.' +
      '</p>' +
      '<div class="inline-actions">' +
        '<button class="btn-primary" id="vacuum-btn">Run vacuum</button>' +
        '<span id="vacuum-msg" class="muted"></span>' +
      '</div>' +
    '</div>';

  document.getElementById("vacuum-btn").addEventListener("click", function() {
    var btn = this;
    showConfirm('Run vacuum on "' + db + '"?',
      "This compacts the database file by removing deleted documents. The database stays readable while it runs.",
      { okLabel: "Run vacuum" }
    ).then(function(ok) {
      if (!ok) return;
      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Running…';
      document.getElementById("vacuum-msg").textContent = "";
      api.vacuum(db).then(function() {
        toast("Vacuum complete", "success");
        document.getElementById("vacuum-msg").textContent = "Done — " + new Date().toLocaleTimeString();
        btn.innerHTML = "Run vacuum";
        btn.disabled = false;
        loadDbMeta(db);
      }).catch(function(e) {
        toast(e.message, "error");
        btn.innerHTML = "Run vacuum";
        btn.disabled = false;
      });
    });
  });
}

/* ═══════════════════════════════════════════════════
   Boot
═══════════════════════════════════════════════════ */

fillServerIdentity();
refreshRail(null);
route();

})();
