/**
 * Tiny continuous _changes helper for kdb3 (SSE feed=eventsource).
 *
 * Uses fetch + ReadableStream so Authorization: Bearer works (native EventSource cannot set headers).
 *
 * Usage:
 *   const sub = follow({ url: 'http://127.0.0.1:8001', db: 'testdb', since: 0, token: '…' }, (change) => {
 *     console.log(change); // { update_seq, id, rev, deleted? }
 *   });
 *   // later: sub.abort(); console.log(sub.since());
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.kdb3Follow = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  function follow(opts, onChange) {
    if (!opts || !opts.db) {
      throw new Error('follow: opts.db is required');
    }
    if (typeof onChange !== 'function') {
      throw new Error('follow: onChange callback is required');
    }

    var baseUrl = String(opts.url || '').replace(/\/+$/, '');
    var db = opts.db;
    var since = opts.since != null ? Number(opts.since) : 0;
    var token = opts.token || '';
    var limit = opts.limit || 1000;
    var onError = typeof opts.onError === 'function' ? opts.onError : null;
    var backoffMs = opts.backoffMs != null ? opts.backoffMs : 1000;
    var maxBackoffMs = opts.maxBackoffMs != null ? opts.maxBackoffMs : 15000;

    var aborted = false;
    var controller = null;
    var currentBackoff = backoffMs;

    function buildURL() {
      return (
        baseUrl +
        '/' +
        encodeURIComponent(db) +
        '/_changes?feed=eventsource&since=' +
        encodeURIComponent(String(since)) +
        '&limit=' +
        encodeURIComponent(String(limit))
      );
    }

    function headers() {
      var h = { Accept: 'text/event-stream' };
      if (token) {
        h.Authorization = 'Bearer ' + token;
      }
      return h;
    }

    function handleDataLine(line) {
      if (line.indexOf('data:') !== 0) {
        return;
      }
      var payload = line.slice(5).replace(/^\s+/, '');
      if (!payload) {
        return;
      }
      var change;
      try {
        change = JSON.parse(payload);
      } catch (e) {
        if (onError) onError(e);
        return;
      }
      if (change && typeof change.update_seq === 'number' && change.update_seq > since) {
        since = change.update_seq;
      }
      onChange(change);
    }

    function processBuffer(buf) {
      var parts = buf.split('\n');
      var rest = parts.pop();
      for (var i = 0; i < parts.length; i++) {
        var line = parts[i].replace(/\r$/, '');
        if (!line || line.charAt(0) === ':') {
          continue;
        }
        handleDataLine(line);
      }
      return rest || '';
    }

    function sleep(ms) {
      return new Promise(function (resolve) {
        setTimeout(resolve, ms);
      });
    }

    async function run() {
      while (!aborted) {
        controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
        try {
          var res = await fetch(buildURL(), {
            headers: headers(),
            signal: controller ? controller.signal : undefined,
          });
          if (!res.ok) {
            var body = '';
            try {
              body = await res.text();
            } catch (_) {}
            throw new Error('follow: HTTP ' + res.status + (body ? ' ' + body : ''));
          }
          if (!res.body || !res.body.getReader) {
            throw new Error('follow: ReadableStream not supported');
          }

          currentBackoff = backoffMs;
          var reader = res.body.getReader();
          var decoder = new TextDecoder('utf-8');
          var pending = '';

          while (!aborted) {
            var chunk = await reader.read();
            if (chunk.done) {
              break;
            }
            pending += decoder.decode(chunk.value, { stream: true });
            pending = processBuffer(pending);
          }
        } catch (err) {
          if (aborted) {
            return;
          }
          if (onError) {
            onError(err);
          }
        }

        if (aborted) {
          return;
        }
        await sleep(currentBackoff);
        currentBackoff = Math.min(currentBackoff * 2, maxBackoffMs);
      }
    }

    run();

    return {
      abort: function () {
        aborted = true;
        if (controller) {
          try {
            controller.abort();
          } catch (_) {}
        }
      },
      since: function () {
        return since;
      },
    };
  }

  return { follow: follow };
});
