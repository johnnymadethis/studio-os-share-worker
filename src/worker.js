// Studio OS — share worker.
// Premium platinum share page streamed from R2 (free egress). Random unguessable ids.
// Modes: listen+download or listen-only (/d blocked). Waveform player, glass controls.

import { LOGO_B64 } from "./logo.js"

export default {
  async fetch(request, env, ctx) {
    try { return await handle(request, env, ctx) }
    catch (e) {
      await logErr(env, `${request.method} ${new URL(request.url).pathname}: ${(e && e.message) || e}`)
      return new Response("Server error", { status: 500 })
    }
  },
}

// Records a worker-side error to a small ring buffer in KV so the app's Health → Problems can
// pull it (`/api/errors`). Reserved key "__errlog" (excluded from the share count).
async function logErr(env, msg) {
  try {
    const m = String(msg).slice(0, 300)
    const cur = JSON.parse((await env.SHARES.get("__errlog")) || "[]")
    const last = cur[cur.length - 1]
    // Don't burn a KV write (free tier ~1000/day) re-logging the SAME error within 5 min — a
    // recurring fault (bot hitting a bad path, transient R2/D1 blip) would otherwise hammer KV writes.
    if (last && last.m === m && Date.now() - last.t < 300000) return
    cur.push({ t: Date.now(), m })
    while (cur.length > 60) cur.shift()
    await env.SHARES.put("__errlog", JSON.stringify(cur))
  } catch (e) { /* never throw from the logger */ }
}

// Records a share OPEN (page load) or PLAY (audio stream) into D1 so the app's Health can show
// per-link activity + a "listening now" dot. Play is throttled to 1 per 30s per share (audio
// streams fire many range requests) to bound writes.
async function recordEvent(env, id, kind, title, artist, throttle = true) {
  try {
    await env.DB.prepare("CREATE TABLE IF NOT EXISTS share_events (share_id TEXT, title TEXT, artist TEXT, kind TEXT, created INTEGER)").run()
    if (kind === "play" && throttle) {
      const last = await env.DB.prepare("SELECT created FROM share_events WHERE share_id=?1 AND kind='play' ORDER BY created DESC LIMIT 1").bind(id).first()
      if (last && Date.now() - last.created < 30000) return
    }
    await env.DB.prepare("INSERT INTO share_events (share_id,title,artist,kind,created) VALUES (?1,?2,?3,?4,?5)")
      .bind(id, String(title || "").slice(0, 200), String(artist || "").slice(0, 200), kind, Date.now()).run()
  } catch (e) { /* activity logging is best-effort */ }
}

// A real human download = a fresh GET (no Range, or a Range that starts at byte 0). Resumable
// managers + Safari/preview fire many partial range requests for ONE download; counting each made
// "Downloaded" fire 2-3× per click. Link-unfurl bots (Slack/iMessage/WhatsApp) and scanners follow
// the download anchor with no human — filter those out by user-agent.
function isFullGet(request) {
  const r = request.headers.get("range")
  return !r || /^bytes=0-/.test(r)
}
function isBot(request) {
  const ua = (request.headers.get("user-agent") || "").toLowerCase()
  return !ua || /bot|crawl|spider|slack|discord|whatsapp|telegram|facebookexternalhit|embedly|preview|curl|wget|python-requests|headless|monitor|pingdom|uptime/.test(ua)
}
function countsAsDownload(request) { return isFullGet(request) && !isBot(request) }

async function handle(request, env, ctx) {
    const url = new URL(request.url)
    const p = url.pathname

    // Listen Together: WebSocket room per share id. The app connects as HOST (?role=host&t=<token>,
    // validated); the share page connects as guest (public — knowing the unguessable id IS the
    // invite). The Durable Object relays host play/pause/seek to guests + presence both ways.
    if (p.startsWith("/ws/")) {
      const id = p.slice(4)
      if (!id || id.includes("/")) return new Response("bad id", { status: 400 })
      if (request.headers.get("upgrade") !== "websocket") return new Response("expected websocket", { status: 426 })
      if (url.searchParams.get("role") === "host" && url.searchParams.get("t") !== env.SHARE_TOKEN)
        return new Response("unauthorized", { status: 401 })
      return env.LISTEN.get(env.LISTEN.idFromName(id)).fetch(request)
    }

    // Public-page footer text ("by <name>") — configurable per deployment so anyone running their own
    // worker (own Cloudflare account, own token) can put their own name on their share pages instead of
    // the default. Stored once in KV; falls back to "by johnnymadethis" if never set (Johnny's own
    // deployment is unaffected until he explicitly sets this from Settings).
    if (p === "/api/brand" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const b = await request.json().catch(() => null)
      const text = (typeof b?.text === "string" ? b.text : "").trim().slice(0, 60)
      await env.SHARES.put("cfg:brandFooter", text || "by johnnymadethis")
      return json({ ok: true })
    }

    if (p === "/api/share" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const b = await request.json().catch(() => null)
      if (!b) return json({ error: "bad body" }, 400)
      // Folder share: a one-level list of items (files + subfolders). No audio/artwork required.
      if (b.type === "folder") {
        const hasTree = Array.isArray(b.tree)
        if (!hasTree && !Array.isArray(b.items)) return json({ error: "tree or items required" }, 400)
        const fid = randomId()
        let artworkKey = ""
        if (b.artworkData) {
          try {
            const bytes = Uint8Array.from(atob(b.artworkData), (c) => c.charCodeAt(0))
            artworkKey = `share-art/${fid}.jpg`
            await env.BUCKET.put(artworkKey, bytes, { httpMetadata: { contentType: b.artworkContentType || "image/jpeg" } })
          } catch (e) { artworkKey = "" }
        }
        const rec = { type: "folder", title: b.title || "Folder", artworkKey }
        // Live folder: store the R2 prefix so the page can list current contents (new files show up).
        if (b.folderKey) {
          // Live folder prefix — must be a well-formed library path, never share machinery.
          const fk = safeKey(String(b.folderKey).replace(/\/+$/, ""))
          if (fk && !/^(share-|health|__)/.test(fk)) rec.folderKey = fk
        }
        if (hasTree) {
          // Recursive tree for navigation + flat files list for streaming (by index).
          let n = 0
          const clean = (arr) => (Array.isArray(arr) ? arr : []).slice(0, 500).map((it) => {
            if (++n > 4000) return null
            if (it.folder) return { name: String(it.name || "").slice(0, 200), folder: true, items: clean(it.items) }
            return {
              name: String(it.name || "").slice(0, 200), ext: String(it.ext || "").slice(0, 8),
              audio: !!it.audio, i: Number.isInteger(it.i) ? it.i : -1,
              peaks: Array.isArray(it.peaks) ? it.peaks.slice(0, 256).map((x) => Math.max(0, Math.min(1, Number(x) || 0))) : null,
            }
          }).filter(Boolean)
          rec.tree = clean(b.tree)
          rec.files = (Array.isArray(b.files) ? b.files : []).slice(0, 4000)
            .map((f) => ({ key: String(f.key || "").slice(0, 600), name: String(f.name || "").slice(0, 200) }))
        } else {
          rec.items = b.items.slice(0, 200).map((it) => ({
            name: String(it.name || "").slice(0, 200),
            ext: String(it.ext || "").slice(0, 8),
            key: it.folder ? "" : String(it.key || "").slice(0, 600),
            audio: !!it.audio, folder: !!it.folder,
            peaks: Array.isArray(it.peaks) ? it.peaks.slice(0, 256).map((x) => Math.max(0, Math.min(1, Number(x) || 0))) : null,
          }))
        }
        await env.SHARES.put(fid, JSON.stringify(rec))
        return json({ id: fid, url: `${url.origin}/s/${fid}` })
      }
      // Pre-save / smart link: a public page that shows "Coming MM/DD" before releaseDate and flips
      // to manual DSP buttons on/after it. Stored under an r:-prefixed id (separate from share ids,
      // excluded from the share count). No exp → never expires; the page just changes content by date.
      if (b.type === "presave") {
        const rid = "r:" + randomId()
        let artworkKey = ""
        if (b.artworkData) {
          try {
            const bytes = Uint8Array.from(atob(b.artworkData), (c) => c.charCodeAt(0))
            artworkKey = `share-art/r${randomId()}.jpg`
            await env.BUCKET.put(artworkKey, bytes, { httpMetadata: { contentType: b.artworkContentType || "image/jpeg" } })
          } catch (e) { artworkKey = "" }
        }
        const dsp = (Array.isArray(b.dspLinks) ? b.dspLinks : []).slice(0, 8)
          .map((x) => ({ name: String(x.name || "").slice(0, 40), url: String(x.url || "").slice(0, 600) }))
          .filter((x) => x.name && /^https:\/\//i.test(x.url))
        await env.SHARES.put(rid, JSON.stringify({
          type: "presave",
          releaseDate: (typeof b.releaseDate === "number" && b.releaseDate > 0) ? b.releaseDate : 0,
          title: b.title || "Untitled", artist: b.artist || "", artworkKey, dspLinks: dsp,
        }))
        return json({ id: rid, url: `${url.origin}/s/${rid}` })
      }
      if (!b.audioKey) return json({ error: "audioKey required" }, 400)
      const id = randomId()
      let artworkKey = b.artworkKey || ""
      // Optional inline artwork (e.g. the app's artist avatar) — store it in R2 under the share.
      if (b.artworkData) {
        try {
          const bytes = Uint8Array.from(atob(b.artworkData), (c) => c.charCodeAt(0))
          artworkKey = `share-art/${id}.jpg`
          await env.BUCKET.put(artworkKey, bytes, { httpMetadata: { contentType: b.artworkContentType || "image/jpeg" } })
        } catch (e) { artworkKey = b.artworkKey || "" }
      }
      const normPeaks = (x) => Array.isArray(x) ? x.slice(0, 512).map((v) => Math.max(0, Math.min(1, Number(v) || 0))) : null
      await env.SHARES.put(id, JSON.stringify({
        audioKey: b.audioKey, artworkKey,
        title: b.title || "Untitled", artist: b.artist || "",
        review: b.review === true,   // Song Review page: play + SoundCloud-style timestamped notes
        // A review link is an ANCHOR per song: v1 is the first audio; new versions are appended via
        // POST /api/share/<id>/version and selectable on the page (each with its own peaks + notes).
        versions: b.review === true
          ? [{ audioKey: b.audioKey, peaks: normPeaks(b.peaks), label: (typeof b.versionLabel === "string" && b.versionLabel) ? b.versionLabel.slice(0, 120) : "v1", addedAt: Date.now() }]
          : undefined,
        downloadName: b.downloadName || basename(b.audioKey),
        artworkName: b.artworkName || (artworkKey ? basename(artworkKey) : ""),
        allowDownload: b.allowDownload !== false,
        artworkDownloadable: b.artworkDownloadable === true,  // cover = yes; avatar = display-only
        lyrics: typeof b.lyrics === "string" ? b.lyrics.slice(0, 20000) : "",
        credits: typeof b.credits === "string" ? b.credits.slice(0, 20000) : "",
        peaks: normPeaks(b.peaks),
        exp: (typeof b.exp === "number" && b.exp > 0) ? b.exp : undefined,   // expiry (epoch ms); single songs only
      }))
      return json({ id, url: `${url.origin}/s/${id}` })
    }

    // Append a new VERSION to an existing review anchor link (Bearer). Keeps the same id/URL +
    // accumulated feedback; the page gains a v-selector. Each version has its own audioKey + peaks.
    if (p.startsWith("/api/share/") && p.endsWith("/version") && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const id = p.slice("/api/share/".length, -"/version".length)
      const meta = await meta_(env, id)
      if (!meta || !meta.review) return json({ error: "not a review" }, 404)
      const b = await request.json().catch(() => null)
      if (!b || !b.audioKey) return json({ error: "audioKey required" }, 400)
      const versions = Array.isArray(meta.versions) && meta.versions.length
        ? meta.versions
        : [{ audioKey: meta.audioKey, peaks: meta.peaks || null, label: "v1", addedAt: Date.now() }]
      // Dedup: same audio already a version → don't append a duplicate (re-share just reuses it).
      const dupe = versions.findIndex((v) => v && v.audioKey === b.audioKey)
      if (dupe >= 0) return json({ index: dupe, count: versions.length, url: `${url.origin}/s/${id}`, deduped: true })
      const idx = versions.length
      versions.push({
        audioKey: b.audioKey,
        peaks: Array.isArray(b.peaks) ? b.peaks.slice(0, 512).map((x) => Math.max(0, Math.min(1, Number(x) || 0))) : null,
        label: (typeof b.label === "string" && b.label) ? b.label.slice(0, 120) : "v" + (idx + 1),
        addedAt: Date.now(),
      })
      meta.versions = versions
      await env.SHARES.put(id, JSON.stringify(meta))
      return json({ index: idx, count: versions.length, url: `${url.origin}/s/${id}` })
    }

    // Remove a VERSION from a review anchor (Bearer) — the song/version was deleted in the app.
    // Tombstones the entry (null) so OTHER versions keep their index-based /av + /rn, and deletes
    // that version's notes thread. renderReviewOrganizer filters out tombstones.
    if (p.startsWith("/api/share/") && p.endsWith("/version/delete") && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const id = p.slice("/api/share/".length, -"/version/delete".length)
      const meta = await meta_(env, id)
      if (!meta || !meta.review) return json({ error: "not a review" }, 404)
      const b = await request.json().catch(() => null)
      if (!b || !b.audioKey) return json({ error: "audioKey required" }, 400)
      const versions = Array.isArray(meta.versions) ? meta.versions : []
      const idx = versions.findIndex((v) => v && v.audioKey === b.audioKey)
      if (idx < 0) return json({ ok: true, notFound: true })
      versions[idx] = null
      meta.versions = versions
      await env.SHARES.put(id, JSON.stringify(meta))
      await env.SHARES.delete("rn:" + id + (idx > 0 ? ":" + idx : ""))
      return json({ ok: true, live: versions.filter(Boolean).length })
    }

    // Fully delete a SUPERSEDED share by id (Bearer) — the app made a fresh link for the same song,
    // so the old one must die in the cloud too (constant-cleanup rule). Drops the KV record + every
    // per-version note thread (rn:<id>[:i]). R2 audio is the library mirror (shared key) → kept.
    if (p.startsWith("/api/share/") && request.method === "DELETE" &&
        !p.endsWith("/version") && !p.endsWith("/version/delete")) {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const id = p.slice("/api/share/".length)
      if (!id || id.includes("/")) return json({ error: "bad id" }, 400)
      const meta = await meta_(env, id)
      await env.SHARES.delete(id)
      await env.SHARES.delete("rn:" + id)
      if (meta && Array.isArray(meta.versions)) {
        for (let i = 1; i < meta.versions.length; i++) await env.SHARES.delete("rn:" + id + ":" + i)
      }
      return json({ ok: true })
    }

    // --- Studio OS: structured split/master sheet CONTENT (entries/percent/signed state). ---
    // One small JSON blob of every sheet, keyed by SplitSheet.key ("kind:artist - song"). Mac is the
    // sole writer (PUT, Bearer); iOS reads (GET). Reserved "__sheets" key → excluded from share counts
    // and untouched by move/delete/rename scans. GET cached 60s; PUT busts the cache. Metadata only.
    if (p === "/api/sheets") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const cache = caches.default
      const cacheKey = new Request(url.origin + "/__cache/sheets")
      if (request.method === "GET") {
        const hit = await cache.match(cacheKey)
        if (hit) return hit
        const body = (await env.SHARES.get("__sheets")) || "{}"
        const res = new Response(body, { headers: { "content-type": "application/json", "cache-control": "max-age=60" } })
        await cache.put(cacheKey, res.clone())
        return res
      }
      if (request.method === "PUT") {
        const raw = await request.text()
        if (raw.length > 20 * 1024 * 1024) return json({ error: "too large" }, 413)
        try { JSON.parse(raw) } catch (e) { return json({ error: "bad json" }, 400) }
        await env.SHARES.put("__sheets", raw)
        await cache.delete(cacheKey)
        return json({ ok: true })
      }
      return new Response("method not allowed", { status: 405 })
    }

    // --- Studio OS: Calendar release events mirror. Mac writes (PUT), iOS reads (GET). One JSON
    // array of events. Reserved "__calendar" key → excluded from share counts + mutation scans. ---
    if (p === "/api/calendar") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const cache = caches.default
      const cacheKey = new Request(url.origin + "/__cache/calendar")
      if (request.method === "GET") {
        const hit = await cache.match(cacheKey)
        if (hit) return hit
        const body = (await env.SHARES.get("__calendar")) || "[]"
        const res = new Response(body, { headers: { "content-type": "application/json", "cache-control": "max-age=60" } })
        await cache.put(cacheKey, res.clone())
        return res
      }
      if (request.method === "PUT") {
        const raw = await request.text()
        if (raw.length > 8 * 1024 * 1024) return json({ error: "too large" }, 413)
        try { JSON.parse(raw) } catch (e) { return json({ error: "bad json" }, 400) }
        await env.SHARES.put("__calendar", raw)
        await cache.delete(cacheKey)
        return json({ ok: true })
      }
      return new Response("method not allowed", { status: 405 })
    }

    // --- iOS companion: browse the R2 library (folders/files) + stream a file by key ---
    // Mirrors the same bucket the share links read. Bearer SHARE_TOKEN, same as /api/share.
    if (p === "/list" && request.method === "GET") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      let path = (url.searchParams.get("path") || "").replace(/^\/+|\/+$/g, "")
      if (path === "root") path = ""
      const prefix = path ? path + "/" : ""
      const folders = [], files = []
      let cursor, guard = 0
      do {
        const r = await env.BUCKET.list({ prefix, delimiter: "/", cursor })
        for (const pre of (r.delimitedPrefixes || [])) folders.push({ name: pre.slice(prefix.length).replace(/\/$/, ""), type: "folder" })
        for (const o of (r.objects || [])) {
          if (o.key === prefix) continue                 // skip the folder placeholder object
          const name = o.key.slice(prefix.length)
          if (name && !name.includes("/")) files.push({ name, type: "file", size: o.size })
        }
        cursor = r.truncated ? r.cursor : undefined
      } while (cursor && ++guard < 200)   // cap pages so a pathological folder can't exhaust the R2 list quota
      return json([...folders, ...files])
    }

    if (p === "/file" && request.method === "GET") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const key = safeKey(url.searchParams.get("path"))
      if (!key) return new Response("bad path", { status: 400 })
      return streamObject(env, key, request, false)
    }

    // --- Studio OS app: ensure a library object is in R2 before sharing. macOS shares files by
    // key, but a Google-Drive placeholder isn't mirrored to R2 until materialized, so the page
    // would 404. HEAD = exists check; PUT = upload the bytes the app read locally.
    if (p === "/object") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const key = safeKey(url.searchParams.get("path"))
      if (!key) return new Response("bad path", { status: 400 })
      if (request.method === "HEAD" || request.method === "GET") {
        const head = await env.BUCKET.head(key)
        return new Response(null, { status: head ? 200 : 404 })
      }
      if (request.method === "PUT") {
        const len = Number(request.headers.get("content-length") || 0)
        if (len > 300 * 1024 * 1024) return json({ error: "too large" }, 413)   // defense-in-depth (platform caps ~100MB anyway)
        await env.BUCKET.put(key, request.body, {
          httpMetadata: { contentType: request.headers.get("content-type") || "application/octet-stream" },
        })
        return json({ ok: true })
      }
      // Delete the R2 object (or, with ?folder=1, everything under the prefix). Object-only — does
      // NOT purge share records (that needs a KV list() scan → quota); the app deletes files often so
      // this stays cheap. A share to a deleted file just 404s its audio (effectively dead).
      if (request.method === "DELETE") {
        if (url.searchParams.get("folder") === "1") {
          const keys = await listAll(env, key.replace(/\/+$/, "") + "/")
          for (const k of keys) await env.BUCKET.delete(k)
          return json({ ok: true, deleted: keys.length })
        }
        await env.BUCKET.delete(key)
        return json({ ok: true })
      }
      return new Response("method not allowed", { status: 405 })
    }

    // --- iOS companion: MOVE a file. R2 has no native rename → copy the bytes to the new key + delete
    // the old. Then re-link any share that pointed at the old key so already-sent links keep working. ---
    if (p === "/move" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const b = await request.json().catch(() => null)
      if (!b) return json({ error: "bad request" }, 400)
      const from = String(b.from || "").replace(/^\/+|\/+$/g, "")
      const toFolder = String(b.to || "").replace(/^\/+|\/+$/g, "")
      if (!from) return json({ error: "from required" }, 400)
      // Auto-detect: a real object at `from` = a FILE move; otherwise treat as a FOLDER (drag-drop only
      // sends an id, not the type, so the worker decides).
      const fileObj = b.folder ? null : await env.BUCKET.get(from)
      if (!fileObj) {
        const newPrefix = (toFolder ? toFolder + "/" : "") + basename(from)
        if (newPrefix === from) return json({ ok: true })
        const map = await rekeyFolder(env, from, newPrefix)
        if (!map.length) return json({ error: "not found" }, 404)
        ctx.waitUntil(relinkMany(env, map))
        return json({ ok: true, moved: map.length })
      }
      const to = (toFolder ? toFolder + "/" : "") + basename(from)
      if (to === from) return json({ ok: true })
      if (await env.BUCKET.head(to)) return json({ error: "exists" }, 409)   // never overwrite a file at the target
      await env.BUCKET.put(to, fileObj.body, { httpMetadata: fileObj.httpMetadata })
      await env.BUCKET.delete(from)
      if (await env.BUCKET.head(from)) await env.BUCKET.delete(from)
      ctx.waitUntil(relinkShares(env, from, to))   // re-link shares AFTER responding → instant move
      return json({ ok: true, to })
    }

    if (p === "/delete" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const b = await request.json().catch(() => null)
      if (!b) return json({ error: "bad request" }, 400)
      const key = String(b.key || "").replace(/^\/+|\/+$/g, "")
      if (!key) return json({ error: "key required" }, 400)
      if (b.folder) {   // delete a whole folder = every object under the prefix
        const keys = await listAll(env, key + "/")
        for (const k of keys) await env.BUCKET.delete(k)
        ctx.waitUntil(purgeMany(env, keys))
        return json({ ok: true, deleted: keys.length })
      }
      await env.BUCKET.delete(key)
      ctx.waitUntil(purgeShares(env, key))   // purge shares AFTER responding → instant delete
      return json({ ok: true })
    }

    // Keep already-sent share links alive when a file is moved OR renamed in the app: move the R2
    // object from → to (if it's there and the target is free) and re-point every share that used the
    // old key. `from`/`to` are full relative keys, so this handles both a folder change and a rename.
    if (p === "/relink" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const b = await request.json().catch(() => null)
      if (!b) return json({ error: "bad request" }, 400)
      const from = String(b.from || "").replace(/^\/+|\/+$/g, "")
      const to = String(b.to || "").replace(/^\/+|\/+$/g, "")
      if (!from || !to || from === to) return json({ ok: true })
      const obj = await env.BUCKET.get(from)
      if (obj && !(await env.BUCKET.head(to))) {
        await env.BUCKET.put(to, obj.body, { httpMetadata: obj.httpMetadata })
        await env.BUCKET.delete(from)
      }
      const n = await relinkShares(env, from, to)   // remap share audioKeys/versions/files → new key
      return json({ ok: true, relinked: n })
    }

    if (p === "/rename" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const b = await request.json().catch(() => null)
      if (!b) return json({ error: "bad request" }, 400)
      const from = String(b.from || "").replace(/^\/+|\/+$/g, "")
      const name = String(b.name || "").replace(/\//g, "").trim()
      if (!from || !name) return json({ error: "from + name required" }, 400)
      const parent = from.includes("/") ? from.slice(0, from.lastIndexOf("/")) : ""
      if (b.folder) {   // rename a folder = re-key everything under it under the new name
        const newPrefix = (parent ? parent + "/" : "") + name
        if (newPrefix === from) return json({ ok: true })
        const map = await rekeyFolder(env, from, newPrefix)
        ctx.waitUntil(relinkMany(env, map))
        return json({ ok: true, renamed: map.length })
      }
      // file: keep the original extension if the new name omits it
      const dot = from.lastIndexOf("."), slash = from.lastIndexOf("/")
      const ext = dot > slash ? from.slice(dot) : ""
      let nm = name
      if (ext && !nm.toLowerCase().endsWith(ext.toLowerCase())) nm += ext
      const to = (parent ? parent + "/" : "") + nm
      if (to === from) return json({ ok: true })
      // Cheap HEAD checks up front (existence + collision), then respond INSTANTLY — the byte
      // copy+delete+relink runs in the background. R2 has no native rename, so a synchronous copy
      // proxied the whole file (a 30 MB master took seconds) before the app got its "renamed".
      if (!(await env.BUCKET.head(from))) return json({ error: "not found" }, 404)
      if (await env.BUCKET.head(to)) return json({ error: "exists" }, 409)
      ctx.waitUntil((async () => {
        const obj = await env.BUCKET.get(from)
        if (!obj) return
        await env.BUCKET.put(to, obj.body, { httpMetadata: obj.httpMetadata })
        await env.BUCKET.delete(from)
        await relinkShares(env, from, to)
      })())
      return json({ ok: true, to })
    }

    // Usage for the in-app Health screen: R2 storage bytes + object count + active share count.
    // CACHED for 5 min via the Cache API: the macOS/iOS apps poll this on a timer, and each
    // uncached call does an R2 list + a KV `list()` — the KV free tier allows only ~1000 list ops/DAY,
    // so unthrottled polling blew the quota (the "50% of KV limit" warning). The cache makes the
    // expensive R2/KV listing run at most once per 5 min no matter how often the apps ask.
    if (p === "/api/usage" && request.method === "GET") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      // KV snapshot recomputed at most every 30 min. The old `caches.default` guard is best-effort +
      // per-colo: the iOS poll (every 90s, roaming colos on cellular) bypassed it and ran a fresh
      // KV `list()` per miss → "KV list() limit exceeded for the day". Now: 1 KV get + (≤1/30min) 1 put.
      const TTL = 30 * 60 * 1000
      const snap = JSON.parse((await env.SHARES.get("__usage")) || "null")
      if (snap && (Date.now() - snap.t) < TTL) {
        return json({ bytes: snap.bytes, objects: snap.objects, shares: snap.shares })
      }
      let bytes = 0, objects = 0, cursor
      do {
        const r = await env.BUCKET.list({ limit: 1000, cursor })
        for (const o of r.objects) {
          // Count only real LIBRARY files (skip the app's own share-art / health-probe objects).
          if (o.key.startsWith("share-art/") || o.key.startsWith("health/")) continue
          bytes += o.size || 0
          objects++
        }
        cursor = r.truncated ? r.cursor : undefined
      } while (cursor)
      let shares = 0, kc
      do {
        const r = await env.SHARES.list({ limit: 1000, cursor: kc })
        shares += (r.keys || []).filter((k) => !k.name.startsWith("__") && !k.name.startsWith("r:") && !k.name.startsWith("rn:")).length   // skip reserved + presave + review-notes keys
        kc = r.list_complete ? undefined : r.cursor
      } while (kc)
      await env.SHARES.put("__usage", JSON.stringify({ t: Date.now(), bytes, objects, shares }))
      return json({ bytes, objects, shares })
    }

    // Recent worker-side errors, for the app's Health → Problems.
    if (p === "/api/errors" && request.method === "GET") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const list = JSON.parse((await env.SHARES.get("__errlog")) || "[]")
      return json({ errors: list })
    }

    // Per-share-link activity (opens/plays + last times) for the app's Health screen.
    if (p === "/api/activity" && request.method === "GET") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      try {
        await env.DB.prepare("CREATE TABLE IF NOT EXISTS share_events (share_id TEXT, title TEXT, artist TEXT, kind TEXT, created INTEGER)").run()
        const r = await env.DB.prepare(`
          SELECT share_id,
                 COALESCE((SELECT title FROM share_events s2 WHERE s2.share_id=share_events.share_id AND s2.kind='play' AND s2.title<>'' ORDER BY s2.created DESC LIMIT 1), MAX(title)) AS title, MAX(artist) AS artist,
                 SUM(CASE WHEN kind='open' THEN 1 ELSE 0 END) AS opens,
                 SUM(CASE WHEN kind='play' THEN 1 ELSE 0 END) AS plays,
                 SUM(CASE WHEN kind='download' THEN 1 ELSE 0 END) AS downloads,
                 MAX(CASE WHEN kind='open' THEN created END) AS lastOpen,
                 MAX(CASE WHEN kind='play' THEN created END) AS lastPlay,
                 MAX(CASE WHEN kind='download' THEN created END) AS lastDownload,
                 MAX(created) AS last
          FROM share_events GROUP BY share_id ORDER BY last DESC LIMIT 60`).all()
        return json({ activity: r.results || [] })
      } catch (e) { return json({ activity: [] }) }
    }

    if (p === "/logo") {
      const bytes = Uint8Array.from(atob(LOGO_B64), (c) => c.charCodeAt(0))
      return new Response(bytes, { headers: { "content-type": "image/png", "cache-control": "public, max-age=86400" } })
    }
    if (p.startsWith("/s/")) {
      const meta = await meta_(env, p.slice(3))
      if (!meta) return new Response("Not found", { status: 404 })
      if (isExpired(meta)) return expiredPage()
      // (Dedup of duplicate / tombstoned versions happens at RENDER time, null-safe, keeping each
      // survivor's original index for /av + /rn. No persist/reindex here — that broke note mapping
      // and crashed on tombstoned (null) versions.)
      // Analytics AFTER the response (waitUntil) — the page's TTFB no longer waits on D1.
      ctx.waitUntil(recordEvent(env, p.slice(3), "open", meta.title, meta.artist))
      const brand = (await env.SHARES.get("cfg:brandFooter")) || "by johnnymadethis"
      return html(renderPage(p.slice(3), meta, parseInt(url.searchParams.get("v") || "0", 10) || 0, url.origin, brand))
    }
    if (p.startsWith("/a/")) {
      // Audio stream ONLY — no play event here. `preload="metadata"` + range/scrub requests hit this
      // just from OPENING the page, which would wrongly count as a listen. Real plays come from the
      // /p/ beacon (fired on the actual <audio> "play" event + every 18s while playing).
      const meta = await meta_(env, p.slice(3))
      if (!meta?.audioKey) return new Response("Not found", { status: 404 })
      if (isExpired(meta)) return new Response("Expired", { status: 410 })
      return streamObject(env, meta.audioKey, request, false)
    }
    // Per-version audio stream for review pages: /av/<id>/<v> (v = version index). Falls back to the
    // anchor's audioKey for v0 / legacy links with no versions array.
    if (p.startsWith("/av/")) {
      const seg = p.slice(4).split("/")
      const meta = await meta_(env, seg[0])
      if (!meta) return new Response("Not found", { status: 404 })
      if (isExpired(meta)) return new Response("Expired", { status: 410 })
      const vi = Math.max(0, parseInt(seg[1] || "0", 10) || 0)
      const key = (Array.isArray(meta.versions) && meta.versions[vi] && meta.versions[vi].audioKey) || meta.audioKey
      if (!key) return new Response("Not found", { status: 404 })
      return streamObject(env, key, request, false)
    }
    // Play beacon: the share page POSTs this the moment the user presses play, then every ~18s WHILE
    // the audio is actually playing (stops on pause/close). This is the ONLY source of "play" events,
    // so a listen reflects real playback — opening the link never counts. No meta_ KV read here: the
    // title/artist already landed in D1 from the page-open event, and /api/activity reads them with
    // MAX(title) — fetching meta on every 18s heartbeat would burn KV reads for nothing.
    if (p.startsWith("/p/") && request.method === "POST") {
      // Optional ?t=<song> — the folder page sends the now-playing song name so /api/activity can show
      // WHICH song is playing (single-song pages omit it and fall back to the page-open title).
      await recordEvent(env, p.slice(3), "play", (url.searchParams.get("t") || "").slice(0, 200), "")
      return new Response("", { status: 204 })
    }
    // Song Review notes: SoundCloud-style timestamped comments stored per share under "rn:<id>".
    // GET lists them (public — same origin as the page); POST appends one (public, link = capability).
    if (p.startsWith("/rn/")) {
      // /rn/<id> = version 1 (legacy key "rn:<id>", keeps old feedback). /rn/<id>/<v> = version v
      // (key "rn:<id>:<v>") so each version has its own comment thread.
      const seg = p.slice(4).split("/")
      const rid = seg[0]
      const key = "rn:" + rid + (seg[1] ? ":" + seg[1] : "")
      if (request.method === "GET") {
        const raw = await env.SHARES.get(key)
        return new Response(raw || "[]", { headers: { "content-type": "application/json", "cache-control": "no-cache" } })
      }
      if (request.method === "POST") {
        const meta = await meta_(env, rid)
        if (!meta || !meta.review) return json({ error: "not a review" }, 404)
        const b = await request.json().catch(() => null)
        if (!b || typeof b.text !== "string") return json({ error: "bad body" }, 400)
        const note = {
          t: Math.max(0, Math.min(86400, Number(b.t) || 0)),
          text: b.text.slice(0, 500),
          by: (typeof b.by === "string" ? b.by : "").slice(0, 60),
          at: Date.now(),
        }
        if (!note.text.trim()) return json({ error: "empty" }, 400)
        // ponytail: read-modify-write on KV; a song review has few concurrent commenters so a lost
        // update is unlikely. Add a Durable Object only if reviews get busy.
        const cur = JSON.parse((await env.SHARES.get(key)) || "[]")
        // Light throttle so nobody floods the KV write quota with a comment loop (reuses the read above).
        if (cur.length && note.at - (cur[cur.length - 1].at || 0) < 2000) return json({ error: "too fast" }, 429)
        cur.push(note)
        if (cur.length > 500) cur.splice(0, cur.length - 500)
        await env.SHARES.put(key, JSON.stringify(cur))
        return json(cur)
      }
      return new Response("method not allowed", { status: 405 })
    }
    if (p.startsWith("/img/")) {
      const meta = await meta_(env, p.slice(5))
      if (!meta?.artworkKey) return new Response("", { status: 404 })
      if (isExpired(meta)) return new Response("Gone", { status: 410 })
      return streamObject(env, meta.artworkKey, request, false)
    }
    if (p.startsWith("/d/")) {
      const meta = await meta_(env, p.slice(3))
      if (!meta?.audioKey) return new Response("Not found", { status: 404 })
      if (isExpired(meta)) return new Response("Expired", { status: 410 })
      if (!meta.allowDownload) return new Response("Download disabled", { status: 403 })
      if (countsAsDownload(request)) ctx.waitUntil(recordEvent(env, p.slice(3), "download", meta.title, meta.artist))
      return streamObject(env, meta.audioKey, request, true, meta.downloadName)
    }
    if (p.startsWith("/dimg/")) {
      const meta = await meta_(env, p.slice(6))
      if (!meta?.artworkKey) return new Response("Not found", { status: 404 })
      if (!meta.allowDownload || !meta.artworkDownloadable) return new Response("Download disabled", { status: 403 })
      if (countsAsDownload(request)) ctx.waitUntil(recordEvent(env, p.slice(6), "download", (meta.title || "") + " (artwork)", meta.artist))
      return streamObject(env, meta.artworkKey, request, true, meta.artworkName || "artwork.jpg")
    }
    // Folder-share item: /fi/<id>/<index>  (?dl=1 → download). Streams item by its R2 key.
    if (p.startsWith("/fi/")) {
      const seg = p.slice(4).split("/")
      const meta = await meta_(env, seg[0])
      const idx = +seg[1]
      const it = meta?.files?.[idx] || meta?.items?.[idx]   // new flat files list, or legacy items
      if (!it?.key) return new Response("Not found", { status: 404 })
      if (isExpired(meta)) return new Response("Gone", { status: 410 })   // expiry applies to items too
      const isDownload = url.searchParams.get("dl") === "1"
      // Download counts (real full GET only); streaming does NOT (open/preload hits this). The folder
      // page's /p/ beacon records the real listen when the user actually plays a song.
      if (isDownload && countsAsDownload(request)) ctx.waitUntil(recordEvent(env, seg[0], "download", it.name, meta?.title))
      return streamObject(env, it.key, request, isDownload, it.name)
    }
    // Live folder tree — list the shared folder's CURRENT R2 contents (new files appear on refresh).
    if (p.startsWith("/st/")) {
      const sid = p.slice(4)
      const meta = await meta_(env, sid)
      if (!meta || meta.type !== "folder" || !meta.folderKey) return json({ error: "not live" }, 404)
      if (isExpired(meta)) return json({ error: "expired" }, 410)
      return new Response(JSON.stringify({ tree: await liveTree(env, meta.folderKey) }),
        { headers: { "content-type": "application/json", "cache-control": "no-cache" } })
    }
    // Stream/download a live-folder file BY KEY — validated to stay inside the share's folder prefix.
    if (p.startsWith("/fk/")) {
      const sid = p.slice(4)
      const meta = await meta_(env, sid)
      if (!meta || !meta.folderKey) return new Response("Not found", { status: 404 })
      if (isExpired(meta)) return new Response("Gone", { status: 410 })
      const base = meta.folderKey
      const k = (url.searchParams.get("k") || "").replace(/^\/+/, "")
      if (!k || k.indexOf("..") >= 0 || !(k === base || k.startsWith(base + "/"))) return new Response("Forbidden", { status: 403 })
      const isDownload = url.searchParams.get("dl") === "1"
      const name = k.split("/").pop()
      if (isDownload && countsAsDownload(request)) ctx.waitUntil(recordEvent(env, sid, "download", name, meta.title))
      return streamObject(env, k, request, isDownload, name)
    }
    // Download a whole folder share (or a subfolder via ?p=Sub/Path) as a streamed ZIP.
    if (p.startsWith("/zip/")) {
      const meta = await meta_(env, p.slice(5))
      if (!meta || meta.type !== "folder") return new Response("Not found", { status: 404 })
      if (isExpired(meta)) return new Response("Gone", { status: 410 })
      let files, zipName = meta.title || "folder"
      if (meta.folderKey) {
        // Live: zip the folder's CURRENT contents.
        let nodes = await liveTree(env, meta.folderKey)
        const sub = url.searchParams.get("p")
        if (sub) {
          const node = findFolder(nodes, sub.split("/").filter(Boolean))
          if (!node) return new Response("Not found", { status: 404 })
          nodes = node.items; zipName = node.name || zipName
        }
        files = collectLive(nodes, "")
      } else if (Array.isArray(meta.tree)) {
        let nodes = meta.tree
        const sub = url.searchParams.get("p")
        if (sub) {
          const node = findFolder(meta.tree, sub.split("/").filter(Boolean))
          if (!node) return new Response("Not found", { status: 404 })
          nodes = node.items; zipName = node.name || zipName
        }
        files = collectFiles(nodes, meta.files || [], "")
      } else {
        files = (meta.items || []).filter((it) => it.key && !it.folder).map((it) => ({ name: it.name, key: it.key }))
      }
      if (!files.length) return new Response("Empty folder", { status: 404 })
      if (files.length > 2000) return new Response("Folder too large to zip", { status: 413 })   // cap fan-out
      return new Response(zipStream(env, files), {
        headers: {
          "content-type": "application/zip",
          "content-disposition": `attachment; filename="${zipName.replace(/[\\"\r\n]/g, "")}.zip"`,
        },
      })
    }
    return new Response("Studio OS", { status: 200 })
}

async function meta_(env, id) { const r = await env.SHARES.get(id); return r ? JSON.parse(r) : null }

// A share with an `exp` (epoch ms) in the past is dead — the page shows "expired", media returns 410.
function isExpired(m) { return !!(m && m.exp && Date.now() > m.exp) }
function expiredPage() {
  return html(`<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Link expirat</title><style>body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:radial-gradient(90% 50% at 50% 10%,#1a1a1f,#070708);color:#e7e3da;font-family:-apple-system,system-ui,sans-serif;text-align:center}.b{opacity:.9;padding:24px}h1{font-size:21px;font-weight:600;margin:0 0 8px}p{color:#8a8a8a;font-size:13px;margin:0}</style></head><body><div class=b><h1>Link expirat</h1><p>Acest link de share nu mai este valabil.</p></div></body></html>`)
}

// --- Folder ZIP (STORE method, streamed straight from R2, no compression / no deps) ---
const ZIP_CRC = (() => { const t = new Uint32Array(256); for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1); t[n] = c >>> 0 } return t })()
function crc32(crc, b) { for (let i = 0; i < b.length; i++) crc = (crc >>> 8) ^ ZIP_CRC[(crc ^ b[i]) & 0xff]; return crc >>> 0 }
// Flatten the nav tree into {name (relative path), key} pairs.
// Recursively list R2 under `base` LIVE → the same node shape the folder page renders, but each file
// carries its full R2 `k`ey (streamed via /fk). Used by live folder shares so newly-added files show up.
async function liveTree(env, base) {
  base = String(base || "").replace(/^\/+|\/+$/g, "")
  const audioExt = ["mp3", "wav", "m4a", "aac", "aiff", "flac", "ogg", "opus"]
  let count = 0
  async function walk(prefix) {
    const pfx = prefix ? prefix + "/" : ""
    const folders = [], files = []
    let cursor
    do {
      const r = await env.BUCKET.list({ prefix: pfx, delimiter: "/", cursor })
      for (const pre of (r.delimitedPrefixes || [])) folders.push(pre.replace(/\/$/, ""))
      for (const o of (r.objects || [])) {
        if (o.key === pfx) continue
        const name = o.key.slice(pfx.length)
        if (name && !name.includes("/")) files.push({ key: o.key, name })
      }
      cursor = r.truncated ? r.cursor : undefined
    } while (cursor)
    folders.sort((a, b) => a.localeCompare(b)); files.sort((a, b) => a.name.localeCompare(b.name))
    const nodes = []
    for (const f of folders) { if (++count > 5000) break; nodes.push({ name: f.slice(pfx.length), folder: true, items: await walk(f) }) }
    for (const f of files) { if (++count > 5000) break; const ext = (f.name.split(".").pop() || "").toLowerCase(); nodes.push({ name: f.name, ext, audio: audioExt.includes(ext), k: f.key }) }
    return nodes
  }
  return walk(base)
}
// Flatten a live tree → [{name (path), key}] for zipping.
function collectLive(nodes, prefix) {
  const out = []
  for (const n of (nodes || [])) {
    if (n.folder) out.push(...collectLive(n.items, (prefix || "") + n.name + "/"))
    else if (n.k) out.push({ name: (prefix || "") + n.name, key: n.k })
  }
  return out
}
function collectFiles(nodes, files, prefix) {
  const out = []
  for (const n of (nodes || [])) {
    if (n.folder) out.push(...collectFiles(n.items, files, prefix + n.name + "/"))
    else if (n.i >= 0 && files[n.i] && files[n.i].key) out.push({ name: prefix + n.name, key: files[n.i].key })
  }
  return out
}
function findFolder(nodes, segs) {
  let cur = nodes, node = null
  for (const s of segs) {
    node = (cur || []).find((n) => n.folder && n.name === s)
    if (!node) return null
    cur = node.items
  }
  return node
}
function zipStream(env, files) {
  const enc = new TextEncoder()
  const central = []
  let offset = 0
  return new ReadableStream({
    async start(controller) {
      const push = (bytes) => { controller.enqueue(bytes); offset += bytes.length }
      for (const f of files) {
        let obj = null
        try { obj = await env.BUCKET.get(f.key) } catch (e) { obj = null }
        if (!obj || !obj.body) continue
        const nameBytes = enc.encode(f.name)
        const localOffset = offset
        const lh = new Uint8Array(30 + nameBytes.length); const dv = new DataView(lh.buffer)
        dv.setUint32(0, 0x04034b50, true); dv.setUint16(4, 20, true); dv.setUint16(6, 0x0808, true) // flags: data-descriptor + UTF-8
        dv.setUint16(8, 0, true); dv.setUint16(10, 0, true); dv.setUint16(12, 0, true)
        dv.setUint32(14, 0, true); dv.setUint32(18, 0, true); dv.setUint32(22, 0, true)
        dv.setUint16(26, nameBytes.length, true); dv.setUint16(28, 0, true)
        lh.set(nameBytes, 30); push(lh)
        let crc = 0xffffffff, size = 0
        const reader = obj.body.getReader()
        while (true) { const { done, value } = await reader.read(); if (done) break; crc = crc32(crc, value); size += value.length; push(value) }
        crc = (crc ^ 0xffffffff) >>> 0
        const dd = new Uint8Array(16); const ddv = new DataView(dd.buffer)
        ddv.setUint32(0, 0x08074b50, true); ddv.setUint32(4, crc, true); ddv.setUint32(8, size >>> 0, true); ddv.setUint32(12, size >>> 0, true)
        push(dd)
        central.push({ crc, size, nameBytes, localOffset })
      }
      const cdStart = offset
      for (const c of central) {
        const ch = new Uint8Array(46 + c.nameBytes.length); const cv = new DataView(ch.buffer)
        cv.setUint32(0, 0x02014b50, true); cv.setUint16(4, 20, true); cv.setUint16(6, 20, true)
        cv.setUint16(8, 0x0808, true); cv.setUint16(10, 0, true); cv.setUint16(12, 0, true); cv.setUint16(14, 0, true)
        cv.setUint32(16, c.crc, true); cv.setUint32(20, c.size >>> 0, true); cv.setUint32(24, c.size >>> 0, true)
        cv.setUint16(28, c.nameBytes.length, true); cv.setUint16(30, 0, true); cv.setUint16(32, 0, true)
        cv.setUint16(34, 0, true); cv.setUint16(36, 0, true); cv.setUint32(38, 0, true); cv.setUint32(42, c.localOffset >>> 0, true)
        ch.set(c.nameBytes, 46); push(ch)
      }
      const cdSize = offset - cdStart
      const eo = new Uint8Array(22); const ev = new DataView(eo.buffer)
      ev.setUint32(0, 0x06054b50, true); ev.setUint16(4, 0, true); ev.setUint16(6, 0, true)
      ev.setUint16(8, central.length, true); ev.setUint16(10, central.length, true)
      ev.setUint32(12, cdSize >>> 0, true); ev.setUint32(16, cdStart >>> 0, true); ev.setUint16(20, 0, true)
      push(eo); controller.close()
    },
  })
}
function randomId(n = 16) { const a = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; const b = crypto.getRandomValues(new Uint8Array(n)); return Array.from(b, (x) => a[x % a.length]).join("") }

// Sanitize an R2 key from a request param: strip leading slashes, reject empty or `..` segments.
// (R2 is a flat store so `..` doesn't traverse, but rejecting it keeps keys well-formed + defensive.)
function safeKey(raw) {
  const k = String(raw || "").replace(/^\/+/, "")
  if (!k || k.split("/").some((s) => s === ".." || s === ".")) return null
  return k
}
function basename(k) { return k.split("/").pop() || "audio" }
// Human version label DERIVED from the audio file name (so a version always shows the real song /
// variant name, never a stored "v1"). Strips the extension + a leading "Artist - " / "Artist_".
function versionLabel(audioKey, artist) {
  let s = basename(audioKey || "").replace(/\.[^.]+$/, "")
  if (artist) s = s.replace(new RegExp("^\\s*" + artist.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "\\s*[-_]*\\s*", "i"), "")
  s = s.replace(/_+/g, " ").replace(/\s+/g, " ").trim()
  return s || basename(audioKey || "audio")
}

// After a file moves (key `from` → `to`), re-point every share that referenced the old key so links
// already sent out keep playing. Scans the SHARES KV (paginated) — moves are rare, so the cost is fine.
async function relinkShares(env, from, to) {
  let n = 0, cursor
  do {
    const list = await env.SHARES.list({ cursor })
    cursor = list.list_complete ? undefined : list.cursor
    for (const k of list.keys) {
      if (k.name.startsWith("__")) continue
      const raw = await env.SHARES.get(k.name)
      if (!raw) continue
      let rec; try { rec = JSON.parse(raw) } catch (e) { continue }
      let changed = false
      if (rec.audioKey === from) { rec.audioKey = to; changed = true }
      if (Array.isArray(rec.versions)) for (const v of rec.versions) if (v && v.audioKey === from) { v.audioKey = to; changed = true }
      if (Array.isArray(rec.files)) for (const f of rec.files) if (f.key === from) { f.key = to; changed = true }
      if (Array.isArray(rec.items)) for (const it of rec.items) if (it.key === from) { it.key = to; changed = true }
      if (changed) { await env.SHARES.put(k.name, JSON.stringify(rec)); n++ }
    }
  } while (cursor)
  return n
}

// All object keys under a prefix (paginated).
async function listAll(env, prefix) {
  const keys = []; let cursor
  do {
    const r = await env.BUCKET.list({ prefix, cursor })
    for (const o of (r.objects || [])) keys.push(o.key)
    cursor = r.truncated ? r.cursor : undefined
  } while (cursor)
  return keys
}

// Move a whole folder: COPY every object under `fromPrefix/` to `toPrefix/…` first, then delete the
// originals — copy-first so a partial failure never loses data (originals stay until all copies land).
// Returns [[oldKey,newKey],…] for share relinking.
async function rekeyFolder(env, fromPrefix, toPrefix) {
  const keys = await listAll(env, fromPrefix + "/")
  const map = []
  for (const k of keys) {
    const obj = await env.BUCKET.get(k)
    if (!obj) continue
    const nk = toPrefix + k.slice(fromPrefix.length)
    await env.BUCKET.put(nk, obj.body, { httpMetadata: obj.httpMetadata })
    map.push([k, nk])
  }
  for (const [oldKey] of map) await env.BUCKET.delete(oldKey)
  return map
}

// Re-point share links after a folder re-key (one scan, many old→new replacements).
async function relinkMany(env, pairs) {
  if (!pairs.length) return 0
  const m = new Map(pairs)
  let n = 0, cursor
  do {
    const list = await env.SHARES.list({ cursor })
    cursor = list.list_complete ? undefined : list.cursor
    for (const k of list.keys) {
      if (k.name.startsWith("__")) continue
      const raw = await env.SHARES.get(k.name); if (!raw) continue
      let rec; try { rec = JSON.parse(raw) } catch (e) { continue }
      let changed = false
      if (rec.audioKey && m.has(rec.audioKey)) { rec.audioKey = m.get(rec.audioKey); changed = true }
      if (Array.isArray(rec.files)) for (const f of rec.files) if (m.has(f.key)) { f.key = m.get(f.key); changed = true }
      if (Array.isArray(rec.items)) for (const it of rec.items) if (m.has(it.key)) { it.key = m.get(it.key); changed = true }
      if (changed) { await env.SHARES.put(k.name, JSON.stringify(rec)); n++ }
    }
  } while (cursor)
  return n
}

// Drop share links after a folder DELETE (one scan; remove any reference to the deleted keys).
async function purgeMany(env, keys) {
  if (!keys.length) return 0
  const set = new Set(keys)
  let n = 0, cursor
  do {
    const list = await env.SHARES.list({ cursor })
    cursor = list.list_complete ? undefined : list.cursor
    for (const k of list.keys) {
      if (k.name.startsWith("__")) continue
      const raw = await env.SHARES.get(k.name); if (!raw) continue
      let rec; try { rec = JSON.parse(raw) } catch (e) { continue }
      if (rec.audioKey && set.has(rec.audioKey)) { await env.SHARES.delete(k.name); n++; continue }
      let changed = false
      if (Array.isArray(rec.files)) { const b = rec.files.length; rec.files = rec.files.filter((f) => !set.has(f.key)); if (rec.files.length !== b) changed = true }
      if (Array.isArray(rec.items)) { const b = rec.items.length; rec.items = rec.items.filter((it) => !set.has(it.key)); if (rec.items.length !== b) changed = true }
      if (changed) { await env.SHARES.put(k.name, JSON.stringify(rec)); n++ }
    }
  } while (cursor)
  return n
}

// A file was DELETED → drop it from share links: delete a single-song share for that file outright,
// and remove the file from any folder share's lists (so its page no longer offers a dead row).
async function purgeShares(env, key) {
  let n = 0, cursor
  do {
    const list = await env.SHARES.list({ cursor })
    cursor = list.list_complete ? undefined : list.cursor
    for (const k of list.keys) {
      if (k.name.startsWith("__")) continue
      const raw = await env.SHARES.get(k.name)
      if (!raw) continue
      let rec; try { rec = JSON.parse(raw) } catch (e) { continue }
      if (rec.audioKey === key) { await env.SHARES.delete(k.name); n++; continue }
      let changed = false
      if (Array.isArray(rec.files)) { const b = rec.files.length; rec.files = rec.files.filter((f) => f.key !== key); if (rec.files.length !== b) changed = true }
      if (Array.isArray(rec.items)) { const b = rec.items.length; rec.items = rec.items.filter((it) => it.key !== key); if (rec.items.length !== b) changed = true }
      if (changed) { await env.SHARES.put(k.name, JSON.stringify(rec)); n++ }
    }
  } while (cursor)
  return n
}
function json(o, s = 200) { return new Response(JSON.stringify(o), { status: s, headers: { "content-type": "application/json" } }) }
function html(s) { return new Response(s, { headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-cache" } }) }

function parseRange(h) { if (!h) return null; const m = /^bytes=(\d+)-(\d*)$/.exec(h); if (!m) return null; const o = +m[1]; return m[2] ? { offset: o, length: +m[2] - o + 1 } : { offset: o } }

async function streamObject(env, key, request, download, downloadName) {
  const range = parseRange(request.headers.get("range"))
  const object = await env.BUCKET.get(key, range ? { range } : undefined)
  if (!object || !object.body) return new Response("Not found", { status: 404 })
  const headers = new Headers(); object.writeHttpMetadata(headers)
  headers.set("accept-ranges", "bytes"); headers.set("cache-control", "public, max-age=3600")
  if (download) headers.set("content-disposition", `attachment; filename="${(downloadName || "audio").replace(/"/g, "")}"`)
  const total = object.size
  if (range) {
    const start = object.range?.offset ?? range.offset
    const len = object.range?.length ?? (total - start)
    headers.set("content-range", `bytes ${start}-${start + len - 1}/${total}`); headers.set("content-length", String(len))
    return new Response(object.body, { status: 206, headers })
  }
  headers.set("content-length", String(total))
  return new Response(object.body, { status: 200, headers })
}

function esc(s) { return String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])) }

function isAudioKey(k) {
  const ext = String(k || "").split(".").pop().toLowerCase()
  return ["mp3", "wav", "m4a", "aac", "aiff", "flac", "ogg", "opus"].includes(ext)
}

// Single-file share whose key isn't audio (PDF, image, zip, …): no player/waveform, just the
// file name + a Download button. Same dark theme as the song page.
function renderFilePage(id, m, brand) {
  const ext = String(m.downloadName || m.audioKey || "").split(".").pop().toUpperCase().slice(0, 5)
  return `<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#070707"><title>${esc(m.title)}</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}html{background:#070707}
  body{background:radial-gradient(95% 50% at 50% 12%,#26262c 0%,#070707 60%);color:#fff;
    font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif;min-height:100vh;
    display:flex;flex-direction:column;align-items:center;justify-content:center;padding:40px 20px}
  .badge{width:120px;height:120px;border-radius:24px;display:flex;align-items:center;justify-content:center;
    font-size:24px;font-weight:700;letter-spacing:1px;color:#e7e3da;
    background:#1a1a1e;border:1px solid rgba(231,227,218,.22);box-shadow:0 22px 60px rgba(0,0,0,.65)}
  h1{font-size:20px;font-weight:600;text-align:center;margin-top:24px;letter-spacing:-.2px;word-break:break-word;max-width:340px}
  .dl{display:flex;align-items:center;gap:8px;margin-top:26px;text-decoration:none;color:#e7e3da;font-size:13px;font-weight:500;
    padding:12px 22px;border-radius:14px;background:rgba(231,227,218,.10);border:1px solid rgba(231,227,218,.24);
    backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px)}
  .foot{color:rgba(231,227,218,.4);font-size:9px;margin-top:30px;letter-spacing:.5px}
</style></head><body>
  <div class="badge">${esc(ext) || "FILE"}</div>
  <h1>${esc(m.title)}</h1>
  ${m.allowDownload ? `<a class="dl" href="/d/${id}"><svg viewBox="0 0 24 24" width="15" height="15" fill="currentColor"><path d="M12 16l-5-5h3V4h4v7h3l-5 5zm-7 2h14v2H5z"/></svg>Download</a>` : ""}
  <div class="foot">${esc(brand)}</div>
</body></html>`
}

// Client-side waveform: when a page has no stored peaks (folder-share items, or a single share made
// without peaks like an external upload), decode the audio in the browser via Web Audio and build the
// REAL wave. Lazy (only for the playing track) + best-effort (any failure keeps the fallback wave), so
// it improves existing links WITHOUT touching their stored records.
const DECODE_WAVE_JS = `
function _norm(out){var mx=0,k;for(k=0;k<out.length;k++)if(out[k]>mx)mx=out[k];if(mx>0)for(k=0;k<out.length;k++)out[k]/=mx;return out.map(function(v){return v<0.05?0.05:v;});}
// Compressed (mp3/m4a/aac/…): decode via Web Audio (small files). One shared, lazily-created context
// (iOS caps the number of AudioContexts) — never closed so repeated song opens don't exhaust them.
var _ac=null;
function _getAC(){try{if(!_ac){var AC=window.AudioContext||window.webkitAudioContext;if(!AC)return null;_ac=new AC();}return _ac;}catch(e){return null;}}
function reducePeaks(audio,bins){var ch=audio.getChannelData(0),n=ch.length,per=Math.max(1,Math.floor(n/bins)),out=[];
 for(var i=0;i<n;i+=per){var p=0,e=Math.min(i+per,n);for(var j=i;j<e;j++){var a=ch[j]<0?-ch[j]:ch[j];if(a>p)p=a;}out.push(p);}
 return _norm(out);}
// WAV: read PCM straight from the bytes, STRIDED (a 30 MB master never gets fully decoded → no OOM,
// no AudioContext/gesture needed). Handles 8/16/24/32-bit int + 32-bit float.
function wavPeaks(buf,bins){try{var dv=new DataView(buf);if(dv.byteLength<44)return null;
 var pos=12,fmt=null,dataOff=-1,dataLen=0;
 while(pos+8<=dv.byteLength){var id=String.fromCharCode(dv.getUint8(pos),dv.getUint8(pos+1),dv.getUint8(pos+2),dv.getUint8(pos+3));var sz=dv.getUint32(pos+4,true);
  if(id==="fmt "){fmt={af:dv.getUint16(pos+8,true),ch:dv.getUint16(pos+10,true),bits:dv.getUint16(pos+22,true)};}
  else if(id==="data"){dataOff=pos+8;dataLen=Math.min(sz,dv.byteLength-dataOff);break;}
  pos+=8+sz+(sz&1);}
 if(!fmt||dataOff<0||!fmt.bits)return null;
 var bps=fmt.bits/8,blk=bps*fmt.ch,frames=Math.floor(dataLen/blk);if(frames<=0)return null;
 var per=Math.max(1,Math.floor(frames/bins)),step=Math.max(1,Math.floor(per/24)),out=[],b,f,o,v;
 for(b=0;b<bins;b++){var s=b*per,e=Math.min(s+per,frames),p=0;
  for(f=s;f<e;f+=step){o=dataOff+f*blk;
   if(fmt.af===3){v=Math.abs(dv.getFloat32(o,true));}
   else if(fmt.bits===16){v=Math.abs(dv.getInt16(o,true))/32768;}
   else if(fmt.bits===24){v=Math.abs((dv.getInt8(o+2)<<16)|(dv.getUint8(o+1)<<8)|dv.getUint8(o))/8388608;}
   else if(fmt.bits===32){v=Math.abs(dv.getInt32(o,true))/2147483648;}
   else if(fmt.bits===8){v=Math.abs(dv.getUint8(o)-128)/128;}else{v=0;}
   if(v>p)p=v;}
  out.push(p);}
 return _norm(out);}catch(e){return null;}}
function decodeWave(url,bins,cb){try{
 fetch(url).then(function(r){return r.arrayBuffer();}).then(function(buf){
  try{var u=new Uint8Array(buf,0,4);if(u[0]===82&&u[1]===73&&u[2]===70&&u[3]===70){var wp=wavPeaks(buf,bins);if(wp&&wp.length){cb(wp);return;}}}catch(e){}
  var ac=_getAC();if(!ac)return;
  ac.decodeAudioData(buf,function(audio){try{cb(reducePeaks(audio,bins));}catch(e){}},function(){});
 }).catch(function(){});}catch(e){}}
`

// Multi-version review: a stacked, collapsible organizer (mirrors the app's song-variant list).
// Each version is its own card — variant name + waveform + its own notes thread. One shared audio.
function renderReviewOrganizer(id, m, origin) {
  // Dedupe by audioKey (collapses accidental repeats) keeping each survivor's ORIGINAL index for the
  // index-based /av + /rn endpoints, then show NEWEST FIRST. Every block stays open: a stacked list
  // of waveform + play + its own notes — all persisted until the song is removed from the app.
  const seen = new Set(), uniq = []
  ;(m.versions || []).forEach((v, i) => { if (v && !seen.has(v.audioKey)) { seen.add(v.audioKey); uniq.push({ v, i }) } })
  const order = uniq.slice().reverse()
  const lbl = (v) => v.label || versionLabel(v.audioKey, m.artist)
  const cfg = JSON.stringify({
    id,
    versions: order.map(({ v, i }) => ({
      label: lbl(v),
      peaks: v.peaks || null,
      av: `/av/${id}/${i}`,
      rn: i > 0 ? `/rn/${id}/${i}` : `/rn/${id}`,
    })),
  })
  const isAvatar = m.artworkKey && !m.artworkDownloadable
  const art = m.artworkKey ? `<img class="art${isAvatar ? " hex" : ""}" src="/img/${id}" alt="">` : `<img class="art logo" src="/logo" alt="">`
  const cards = order.map(({ v }, di) => `
    <div class="ver open" id="ver${di}">
      <div class="verh">
        <span class="lbl">${esc(lbl(v))}</span>
        <span class="cnt" id="vc${di}"></span>
      </div>
      <div class="verb">
        <div class="vwave" id="vw${di}"></div>
        <div class="vrow"><button class="vplay" id="vp${di}" aria-label="Play"><svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor"><path d="M8 5v14l11-7z"/></svg></button><span class="vtl" id="vtl${di}">0:00</span></div>
        <div class="rvadd"><div class="rvrow"><input class="rvi" id="vt${di}" placeholder="Add a note at this point…" maxlength="500" autocomplete="off" autocorrect="off" spellcheck="false" enterkeyhint="send"><button class="rvb" id="vpost${di}">Post</button></div></div>
        <div class="rvlist" id="vl${di}"></div>
      </div>
    </div>`).join("")
  return `<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#070707"><title>${esc(m.title)} — ${esc(m.artist)}</title>
${m.artworkKey ? `<meta property="og:image" content="${origin}/img/${id}">` : ""}
<style>
  :root{--p:#e7e3da;--soft:#8d8a82}*{box-sizing:border-box;margin:0;padding:0}
  html,body{background:#070707;color:#fff;font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif}
  body{min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:34px 18px 50px;
    background:radial-gradient(95% 50% at 50% 10%,#23232a 0%,#070707 58%)}
  .art{width:120px;height:120px;border-radius:20px;object-fit:cover;background:#1a1a1e;border:1px solid rgba(231,227,218,.22);box-shadow:0 18px 50px rgba(0,0,0,.6)}
  .art.hex{border:none;border-radius:0;box-shadow:none;clip-path:polygon(25% 6.7%,75% 6.7%,100% 50%,75% 93.3%,25% 93.3%,0 50%);filter:drop-shadow(0 14px 32px rgba(0,0,0,.6))}
  h1{font-size:19px;font-weight:600;text-align:center;margin-top:16px}
  .artist{color:var(--p);font-size:12px;margin-top:3px;letter-spacing:.6px}
  .vh2{color:var(--soft);font-size:11px;letter-spacing:1.4px;text-transform:uppercase;margin:26px 0 10px}
  .org{width:96vw;max-width:920px;display:flex;flex-direction:column;gap:12px}
  .ver{border-radius:16px;background:rgba(231,227,218,.045);border:1px solid rgba(231,227,218,.14);overflow:hidden}
  .verh{display:flex;align-items:center;gap:9px;padding:13px 15px;cursor:pointer;user-select:none}
  .verh .lbl{font-weight:600;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .verh .cnt{color:var(--soft);font-size:11px}
  .verh .chev{margin-left:auto;color:var(--soft);transition:transform .2s}
  .ver.open .verh .chev{transform:rotate(90deg)}
  .verb{display:none;padding:0 15px 15px}.ver.open .verb{display:block}
  .vwave{position:relative;height:64px;width:100%;margin:6px 0 14px;cursor:pointer;overflow:hidden;border-radius:18px;
    background:linear-gradient(180deg,#141417,#0b0b0d);display:flex;align-items:center;gap:1px;padding:0 8px;box-sizing:border-box;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.05),inset 0 0 0 1px rgba(231,227,218,.06),0 8px 22px rgba(0,0,0,.4)}
  .vwb{flex:1 1 0;min-width:0;border-radius:2px;background:rgba(231,227,218,.30)}
  .vwb.on{background:var(--p);opacity:.9}
  .vph{position:absolute;top:6px;bottom:6px;width:2.5px;border-radius:2px;background:#fff;transform:translateX(-50%);z-index:2;box-shadow:0 0 8px rgba(255,255,255,.5)}
  .vmark{position:absolute;top:4px;bottom:4px;width:2px;background:#ff9f43;transform:translateX(-50%);z-index:3;cursor:pointer;box-shadow:0 0 6px rgba(255,159,67,.6)}
  .vmark::after{content:"";position:absolute;top:-4px;left:-3px;width:8px;height:8px;border-radius:50%;background:#ff9f43}
  .vrow{display:flex;align-items:center;gap:10px;margin-bottom:10px}
  .vplay{width:38px;height:38px;border-radius:50%;cursor:pointer;color:var(--p);background:rgba(231,227,218,.12);
    border:1px solid rgba(231,227,218,.3);display:flex;align-items:center;justify-content:center}
  .vplay:active{transform:scale(.93)}
  .vtl{color:var(--soft);font-size:11px;font-variant-numeric:tabular-nums}
  .rvadd{margin-bottom:10px}.rvrow{display:flex;gap:8px;align-items:center}
  .rvi{flex:1;min-width:0;padding:10px 12px;border-radius:10px;background:rgba(255,255,255,.05);border:1px solid rgba(231,227,218,.14);color:#fff;font-size:16px;outline:none;font-family:inherit}
  .rvi:focus{border-color:rgba(231,227,218,.4)}
  .rvb{padding:10px 14px;border-radius:10px;background:rgba(231,227,218,.14);border:1px solid rgba(231,227,218,.3);color:var(--p);font-size:13px;cursor:pointer}
  .rvb:active{transform:scale(.96)}.rvb:disabled{opacity:.5}
  .rvlist{display:flex;flex-direction:column;gap:1px;max-height:240px;overflow-y:auto}
  .rvit{display:flex;gap:10px;padding:8px 6px;border-radius:8px;cursor:pointer;align-items:baseline}
  .rvit:hover{background:rgba(255,255,255,.04)}
  .rvit .tt{color:var(--p);font-variant-numeric:tabular-nums;font-size:12px;font-weight:600;min-width:38px}
  .rvit .bd{font-size:13px;color:rgba(255,255,255,.88);word-wrap:break-word;overflow-wrap:anywhere}
  .rvempty{color:var(--soft);font-size:12px;padding:6px 4px}
  .foot{color:rgba(231,227,218,.4);font-size:9px;margin-top:30px}
</style></head><body>
  ${art}
  <h1>${esc(m.title)}</h1>${m.artist ? `<div class="artist">${esc(m.artist)}</div>` : ""}
  <div class="vh2">Versions &amp; notes</div>
  <div class="org">${cards}</div>
  <div class="foot">Studio OS</div>
  <script type="application/json" id="cfg">${cfg}</script>
  <script>
(function(){
  var C=JSON.parse(document.getElementById("cfg").textContent),V=C.versions;
  var au=new Audio();au.preload="metadata";var activeV=-1;
  function fmt(s){s=Math.floor(s||0);var m=Math.floor(s/60),x=s%60;return m+":"+(x<10?"0":"")+x;}
  V.forEach(function(v,i){
    var card=document.getElementById("ver"+i),wave=document.getElementById("vw"+i),
        playBtn=document.getElementById("vp"+i),list=document.getElementById("vl"+i),
        txt=document.getElementById("vt"+i),postBtn=document.getElementById("vpost"+i),
        tlab=document.getElementById("vtl"+i),cnt=document.getElementById("vc"+i);
    var notes=[];
    var peaks=(v.peaks&&v.peaks.length)?v.peaks:[];
    var nbar=Math.max(24,Math.min(320,Math.round((wave.clientWidth||300)/3)||80));
    for(var b=0;b<nbar;b++){var idx=peaks.length?Math.floor(b*peaks.length/nbar):0;
      var el=document.createElement("div");el.className="vwb";el.style.height=Math.max(6,(peaks.length?peaks[idx]:0.2)*66)+"%";wave.appendChild(el);}
    var ph=document.createElement("div");ph.className="vph";ph.style.display="none";wave.appendChild(ph);
    function dur(){return (activeV===i?au.duration:0)||0;}
    function drawMarks(){var ex=wave.querySelectorAll(".vmark");for(var k=0;k<ex.length;k++)ex[k].remove();
      var d=dur();if(d<=0)return;
      notes.forEach(function(n){var f=Math.max(0,Math.min(1,n.t/d));var mk=document.createElement("div");mk.className="vmark";
        mk.style.left="calc(8px + "+(f*100)+"% - "+(f*16)+"px)";mk.title=n.text;
        mk.addEventListener("click",function(e){e.stopPropagation();seekTo(n.t);});wave.appendChild(mk);});}
    function renderNotes(){list.innerHTML="";if(cnt)cnt.textContent=notes.length?(notes.length+" note"+(notes.length>1?"s":"")):"";
      var sorted=notes.slice().sort(function(a,b){return a.t-b.t;});
      if(!sorted.length){var em=document.createElement("div");em.className="rvempty";em.textContent="No notes yet.";list.appendChild(em);}
      sorted.forEach(function(n){var it=document.createElement("div");it.className="rvit";
        var tt=document.createElement("span");tt.className="tt";tt.textContent=fmt(n.t);
        var bd=document.createElement("div");bd.className="bd";bd.textContent=n.text;
        it.appendChild(tt);it.appendChild(bd);it.addEventListener("click",function(){seekTo(n.t);});list.appendChild(it);});
      drawMarks();}
    function loadNotes(){fetch(v.rn).then(function(r){return r.json();}).then(function(a){notes=Array.isArray(a)?a:[];renderNotes();}).catch(function(){});}
    function activate(){if(activeV!==i){activeV=i;au.src=v.av;}}
    function whenReady(fn){if(au.readyState>=1)fn();else au.addEventListener("loadedmetadata",function h(){au.removeEventListener("loadedmetadata",h);fn();});}
    function seekTo(t){activate();whenReady(function(){try{au.currentTime=t;au.play();}catch(e){}});}
    playBtn.addEventListener("click",function(){activate();if(au.paused){au.play();}else{au.pause();}});
    wave.addEventListener("click",function(e){activate();var r=wave.getBoundingClientRect();var f=(e.clientX-r.left)/r.width;
      whenReady(function(){au.currentTime=f*(au.duration||0);au.play();});});
    postBtn.addEventListener("click",function(){var t=(txt.value||"").trim();if(!t)return;
      var note={t:(activeV===i?au.currentTime:0)||0,text:t,by:""};postBtn.disabled=true;
      fetch(v.rn,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(note)})
        .then(function(r){return r.json();}).then(function(res){txt.value="";if(Array.isArray(res)){notes=res;renderNotes();}else loadNotes();})
        .catch(function(){loadNotes();}).then(function(){postBtn.disabled=false;});});
    txt.addEventListener("keydown",function(e){if(e.key==="Enter")postBtn.click();});
    au.addEventListener("timeupdate",function(){if(activeV!==i)return;var d=au.duration||0;if(d<=0)return;var f=au.currentTime/d;
      ph.style.display="block";ph.style.left="calc(8px + "+(f*100)+"% - "+(f*16)+"px)";
      var bb=wave.querySelectorAll(".vwb"),on=Math.floor(f*bb.length);
      for(var k=0;k<bb.length;k++){if(k<on)bb[k].classList.add("on");else bb[k].classList.remove("on");}
      if(tlab)tlab.textContent=fmt(au.currentTime)+" / "+fmt(d);drawMarks();});
    au.addEventListener("loadedmetadata",function(){if(activeV===i)drawMarks();});
    loadNotes();
  });
})();
  </script>
</body></html>`
}

function renderPage(id, m, v = 0, origin, brand) {
  if (m.type === "folder") return renderFolderPage(id, m, brand)
  if (m.type === "presave") return renderPresavePage(id, m, origin)
  if (!isAudioKey(m.audioKey)) return renderFilePage(id, m, brand)
  // Review with 2+ DISTINCT versions → the stacked list (newest first, each its own waveform + notes).
  // A single version → the clean single page below.
  if (m.review && Array.isArray(m.versions)) {
    const ak = new Set(m.versions.filter(Boolean).map((x) => x.audioKey))
    if (ak.size > 1) return renderReviewOrganizer(id, m, origin)
  }
  // Review anchor: single clean page (one version). Pick the requested version — audio,
  // peaks and notes are per-version. Dedupe by audioKey first (collapses accidental duplicates from
  // testing) while keeping each survivor's ORIGINAL index, so its index-based /av and /rn endpoints
  // still resolve. The <select> reloads the page with ?v=<i>.
  let versions = (Array.isArray(m.versions) && m.versions.length) ? m.versions : null
  if (versions) {
    const seen = new Set(), dd = []
    versions.forEach((vv, i) => { if (vv && !seen.has(vv.audioKey)) { seen.add(vv.audioKey); dd.push({ label: vv.label, peaks: vv.peaks, _i: i }) } })
    versions = dd.length ? dd : null
  }
  const vi = versions ? Math.max(0, Math.min(versions.length - 1, v | 0)) : 0
  const oi = versions ? versions[vi]._i : 0           // original index for /av + /rn endpoints
  const vPeaks = versions ? (versions[vi].peaks || m.peaks) : m.peaks
  const audioSrc = versions ? `/av/${id}/${oi}` : `/a/${id}`
  const rnEndpoint = oi > 0 ? `/rn/${id}/${oi}` : `/rn/${id}`
  const verSel = (m.review && versions && versions.length > 1)
    ? `<select class="rvsel" onchange="location.search='?v='+this.value">` +
      versions.map((vv, i) => `<option value="${i}"${i === vi ? " selected" : ""}>${esc(vv.label || ("v" + (i + 1)))}</option>`).join("") +
      `</select>` : ""
  const cfg = JSON.stringify({ id, peaks: vPeaks || null, rn: rnEndpoint })
  // The artwork is the artist AVATAR when it's display-only (not a downloadable cover) — show it
  // as a hexagon to match the app's Dashboard. Downloadable covers stay square/rounded.
  const isAvatar = m.artworkKey && !m.artworkDownloadable
  const art = m.artworkKey ? `<img class="art${isAvatar ? " hex" : ""}" src="/img/${id}" alt="">` : `<img class="art logo" src="/logo" alt="">`
  // Song Review: a SoundCloud-style notes panel under the player (markers on the wave + comment list).
  const reviewCSS = m.review ? `
  .rv{position:relative;width:100%;margin-top:18px;padding:18px 20px 20px;border-radius:18px;text-align:left;background:rgba(231,227,218,.045);border:1px solid rgba(231,227,218,.14);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px)}
  .rv h2{color:var(--p);font-size:11px;font-weight:600;letter-spacing:1.6px;text-transform:uppercase;margin-bottom:12px}
  .rvadd{display:flex;flex-direction:column;gap:8px;margin-bottom:14px}
  .rvi{padding:10px 12px;border-radius:10px;background:rgba(255,255,255,.05);border:1px solid rgba(231,227,218,.14);color:#fff;font-size:16px;outline:none;font-family:inherit}
  .rvi:focus{border-color:rgba(231,227,218,.4)}
  .rvrow{display:flex;gap:8px;align-items:center}
  .rvt{font-variant-numeric:tabular-nums;color:var(--p);font-size:12px;min-width:40px;text-align:center}
  .rvi{flex:1;min-width:0}
  .rvb{padding:10px 16px;border-radius:10px;background:rgba(231,227,218,.14);border:1px solid rgba(231,227,218,.3);color:var(--p);font-size:13px;cursor:pointer;white-space:nowrap}
  .rvb:active{transform:scale(.96)}.rvb:disabled{opacity:.5;cursor:default}
  .rvlist{display:flex;flex-direction:column;gap:1px;max-height:320px;overflow-y:auto}
  .rvit{display:flex;gap:10px;padding:9px 6px;border-radius:8px;cursor:pointer;align-items:baseline}
  .rvit:hover{background:rgba(255,255,255,.04)}
  .rvit .tt{color:var(--p);font-variant-numeric:tabular-nums;font-size:12px;font-weight:600;min-width:40px}
  .rvit .bd{font-size:13px;color:rgba(255,255,255,.88);word-wrap:break-word;overflow-wrap:anywhere}
  .rvit .nm{font-size:11px;color:var(--soft);margin-top:2px}
  .rvempty{color:var(--soft);font-size:12px;padding:6px 4px}
  .rvmark{position:absolute;top:6px;bottom:6px;width:2px;background:#ff9f43;z-index:3;cursor:pointer;transform:translateX(-50%);box-shadow:0 0 6px rgba(255,159,67,.6)}
  .rvmark::after{content:"";position:absolute;top:-4px;left:-3px;width:8px;height:8px;border-radius:50%;background:#ff9f43}
  .rvver{float:right;font-weight:400;letter-spacing:0}
  .rvsel{background:rgba(231,227,218,.10);color:var(--p);border:1px solid rgba(231,227,218,.25);border-radius:8px;padding:3px 8px;font-size:12px;font-family:inherit;cursor:pointer;outline:none}` : ""
  const reviewHTML = m.review ? `<section class="rv">
    <h2>Notes${verSel ? `<span class="rvver">${verSel}</span>` : ""}</h2>
    <div class="rvadd">
      <div class="rvrow"><span class="rvt" id="rvt">0:00</span><input id="rvtext" class="rvi" placeholder="Add a note at this point…" maxlength="500" autocomplete="off" autocorrect="off" autocapitalize="sentences" spellcheck="false" enterkeyhint="send"><button id="rvbtn" class="rvb">Post</button></div>
    </div>
    <div id="rvlist" class="rvlist"></div>
  </section>` : ""
  const reviewJS = m.review ? `<script>
(function(){
  var C=JSON.parse(document.getElementById("cfg").textContent),ID=C.id;
  var au=document.getElementById("au"),wave=document.getElementById("wave"),
      list=document.getElementById("rvlist"),txt=document.getElementById("rvtext"),
      btn=document.getElementById("rvbtn"),tlab=document.getElementById("rvt");
  function fmt(s){s=Math.floor(s||0);var m=Math.floor(s/60),x=s%60;return m+":"+(x<10?"0":"")+x;}
  var notes=[];
  function seek(t){try{au.currentTime=t;au.play();}catch(e){}}
  function render(){
    var ex=wave.querySelectorAll(".rvmark");for(var i=0;i<ex.length;i++)ex[i].remove();
    var d=au.duration||0;
    notes.forEach(function(n){if(d>0){var mk=document.createElement("div");mk.className="rvmark";var f=Math.max(0,Math.min(1,n.t/d));mk.style.left="calc(8px + "+(f*100)+"% - "+(f*16)+"px)";mk.title=n.text;mk.addEventListener("click",function(ev){ev.stopPropagation();seek(n.t);});wave.appendChild(mk);}});
    list.innerHTML="";
    var sorted=notes.slice().sort(function(a,b){return a.t-b.t;});
    if(!sorted.length){var em=document.createElement("div");em.className="rvempty";em.textContent="No notes yet — play and add one.";list.appendChild(em);return;}
    sorted.forEach(function(n){
      var it=document.createElement("div");it.className="rvit";
      var tt=document.createElement("span");tt.className="tt";tt.textContent=fmt(n.t);
      var bx=document.createElement("div");
      var bd=document.createElement("div");bd.className="bd";bd.textContent=n.text;
      var nmd=document.createElement("div");nmd.className="nm";nmd.textContent=n.by||"Guest";
      bx.appendChild(bd);bx.appendChild(nmd);it.appendChild(tt);it.appendChild(bx);
      it.addEventListener("click",function(){seek(n.t);});list.appendChild(it);
    });
  }
  function load(){fetch(C.rn).then(function(r){return r.json();}).then(function(a){notes=Array.isArray(a)?a:[];render();}).catch(function(){});}
  au.addEventListener("loadedmetadata",render);
  setInterval(function(){if(tlab)tlab.textContent=fmt(au.currentTime);},250);
  function post(){
    var t=(txt.value||"").trim();if(!t)return;
    var note={t:au.currentTime||0,text:t,by:""};
    btn.disabled=true;
    fetch(C.rn,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(note)})
      .then(function(r){return r.json();}).then(function(res){txt.value="";if(Array.isArray(res)){notes=res;render();}else load();})
      .catch(function(){load();}).then(function(){btn.disabled=false;});
  }
  if(btn){btn.addEventListener("click",post);txt.addEventListener("keydown",function(e){if(e.key==="Enter")post();});}
  load();
})();
</script>` : ""
  return `<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#070707">
<title>${esc(m.title)} — ${esc(m.artist)}</title>
<meta property="og:title" content="${esc(m.title)}"><meta property="og:description" content="${esc(m.artist)}">
${m.artworkKey ? `<meta property="og:image" content="${origin}/img/${id}">` : ""}
<style>
  :root{--p:#e7e3da;--ink:#0b0b0c;--soft:#8d8a82}
  *{box-sizing:border-box;margin:0;padding:0}
  html{background:#070707}
  body{background:#070707;color:#fff;
    font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif;min-height:100vh;
    display:flex;flex-direction:column;align-items:center;justify-content:center;padding:40px 20px}
  /* Backdrop: white radial light + an animated particle-wave canvas, behind everything. */
  .bgfx{position:fixed;inset:0;z-index:0;overflow:hidden;
    background:radial-gradient(95% 50% at 50% 12%,#26262c 0%,#070707 60%)}
  .bgfx canvas{position:absolute;inset:0;width:100%;height:100%;filter:blur(2px)}
  .wrap{position:relative;z-index:1;width:100%;max-width:360px;display:flex;flex-direction:column;align-items:center}
  .art{width:184px;height:184px;border-radius:24px;object-fit:cover;display:block;
    background:#1a1a1e;border:1px solid rgba(231,227,218,.22);box-shadow:0 22px 60px rgba(0,0,0,.65)}
  .art.logo{object-fit:cover;background:#171717}
  /* Artist avatar → regular flat-top hexagon (matches the app Dashboard). */
  .art.hex{border:none;border-radius:0;box-shadow:none;background:transparent;
    clip-path:polygon(25% 6.7%,75% 6.7%,100% 50%,75% 93.3%,25% 93.3%,0 50%);
    filter:drop-shadow(0 18px 40px rgba(0,0,0,.6))}
  h1{font-size:20px;font-weight:600;text-align:center;margin-top:22px;letter-spacing:-.2px}
  .artist{color:var(--p);font-size:12px;text-align:center;margin-top:4px;letter-spacing:.7px}
  /* Apple-style scrolling waveform: a fixed playhead with the wave sliding LEFT past it. */
  .wave{position:relative;height:64px;width:96vw;max-width:920px;margin-top:30px;cursor:pointer;overflow:hidden;touch-action:none;
    background:linear-gradient(180deg,#141417,#0b0b0d);border-radius:20px;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.05),inset 0 0 0 1px rgba(231,227,218,.06),0 10px 28px rgba(0,0,0,.45)}
  .wstrip{position:absolute;top:0;left:0;right:0;height:100%;display:flex;align-items:center;gap:1px;padding:0 8px;box-sizing:border-box}
  .wb{flex:1 1 0;min-width:0;border-radius:2px;background:rgba(231,227,218,.30)}
  .wb.on{background:var(--p);opacity:.85}
  .wph{position:absolute;top:8px;bottom:8px;left:0;width:2.5px;border-radius:2px;background:#fff;
    transform:translateX(-50%);z-index:2;box-shadow:0 0 10px rgba(255,255,255,.55)}
  .time{display:flex;justify-content:space-between;width:96vw;max-width:920px;margin-top:8px;color:var(--soft);font-size:11px;font-variant-numeric:tabular-nums}
  .play{width:62px;height:62px;border-radius:50%;margin-top:20px;cursor:pointer;color:var(--p);
    background:rgba(231,227,218,.10);border:1px solid rgba(231,227,218,.30);
    backdrop-filter:blur(16px) saturate(160%);-webkit-backdrop-filter:blur(16px) saturate(160%);
    box-shadow:0 10px 30px rgba(0,0,0,.45), inset 0 1px 0 rgba(255,255,255,.18), inset 0 -8px 16px rgba(0,0,0,.25);
    display:flex;align-items:center;justify-content:center;
    transition:transform .25s cubic-bezier(.2,.9,.25,1), background .25s, box-shadow .25s}
  .play:hover{background:rgba(231,227,218,.16)}
  .play:active{transform:scale(.93)}
  .play svg{width:22px;height:22px;fill:var(--p);transition:opacity .2s, transform .2s}
  .dl{display:flex;align-items:center;gap:8px;margin-top:22px;text-decoration:none;color:var(--p);font-size:13px;font-weight:500;
    padding:11px 20px;border-radius:14px;background:rgba(231,227,218,.08);border:1px solid rgba(231,227,218,.22);
    backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px)}
  .files{display:flex;flex-wrap:wrap;gap:10px;justify-content:center;width:100%;margin-top:22px}
  .files .dl{margin-top:0}
  .note{position:relative;width:100%;margin-top:18px;padding:18px 20px 20px;border-radius:18px;text-align:left;
    background:rgba(231,227,218,.045);border:1px solid rgba(231,227,218,.14);
    backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px)}
  .note h2{color:var(--p);font-size:11px;font-weight:600;letter-spacing:1.6px;text-transform:uppercase;margin-bottom:12px}
  .note .body{font-size:13px;line-height:1.7;white-space:pre-wrap;word-wrap:break-word;color:rgba(255,255,255,.85);
    max-height:340px;overflow-y:auto}
  .cp{position:absolute;top:14px;right:14px;width:30px;height:30px;border-radius:8px;cursor:pointer;
    display:flex;align-items:center;justify-content:center;color:var(--p);
    background:rgba(231,227,218,.06);border:1px solid rgba(231,227,218,.18)}
  .cp:hover{background:rgba(231,227,218,.12)}
  .cp.ok{color:#7ee0a0;border-color:rgba(126,224,160,.4)}
  .foot{color:rgba(231,227,218,.4);font-size:9px;margin-top:30px;letter-spacing:.5px}
  @media (max-width:600px){
    body{justify-content:flex-start;padding:20px 20px 26px}
    .wave,.time{width:98vw}
    .bgfx canvas{filter:blur(3px)}
    /* iOS-26-style liquid glass on the foreground (phone only), no animation. */
    .art{box-shadow:0 22px 60px rgba(0,0,0,.65),inset 0 1.5px 0 rgba(255,255,255,.22),inset 0 0 0 1px rgba(231,227,218,.16)}
    .dl,.note,.play{backdrop-filter:blur(24px) saturate(180%);-webkit-backdrop-filter:blur(24px) saturate(180%)}
    .dl{background:rgba(231,227,218,.12);box-shadow:0 8px 22px rgba(0,0,0,.4),inset 0 1px 0 rgba(255,255,255,.30),inset 0 -10px 18px rgba(0,0,0,.22)}
    .note{background:rgba(231,227,218,.07);box-shadow:inset 0 1px 0 rgba(255,255,255,.16)}
    .play{box-shadow:0 12px 32px rgba(0,0,0,.5),inset 0 1.5px 0 rgba(255,255,255,.35),inset 0 -10px 18px rgba(0,0,0,.28)}
  }
  ${reviewCSS}
</style></head><body>
<div class="bgfx"><canvas id="pw"></canvas></div>
<div class="wrap">
  ${art}
  <h1>${esc(m.title)}</h1>
  ${m.artist ? `<div class="artist">${esc(m.artist)}</div>` : ""}
  <div class="wave" id="wave"></div>
  <div class="time"><span id="cur">0:00</span><span id="dur">0:00</span></div>
  <button class="play" id="play" aria-label="Play"><svg id="ic" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg></button>
  ${m.allowDownload ? `<div class="files">
    <a class="dl" href="/d/${id}"><svg viewBox="0 0 24 24" width="15" height="15" fill="currentColor"><path d="M12 16l-5-5h3V4h4v7h3l-5 5zm-7 2h14v2H5z"/></svg>${m.artworkDownloadable ? "Master" : "Download"}</a>
    ${(m.artworkKey && m.artworkDownloadable) ? `<a class="dl" href="/dimg/${id}"><svg viewBox="0 0 24 24" width="15" height="15" fill="currentColor"><path d="M21 19V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2zM8.5 13.5l2.5 3.01L14.5 12l4.5 6H5l3.5-4.5z"/></svg>Artwork</a>` : ""}
  </div>` : ""}
  <audio id="au" src="${audioSrc}" preload="metadata"></audio>
  ${reviewHTML}
  ${m.lyrics ? `<section class="note"><button class="cp" aria-label="Copy lyrics"><svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg></button><h2>Lyrics</h2><div class="body">${esc(m.lyrics)}</div></section>` : ""}
  ${m.credits ? `<section class="note"><button class="cp" aria-label="Copy credits"><svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg></button><h2>Credits</h2><div class="body">${esc(m.credits)}</div></section>` : ""}
  <div class="foot">${esc(brand)}</div>
</div>
<script id="cfg" type="application/json">${cfg}</script>
<script>
(function(){
  var C=JSON.parse(document.getElementById("cfg").textContent);
  var au=document.getElementById("au"),play=document.getElementById("play"),ic=document.getElementById("ic"),
      wave=document.getElementById("wave"),cur=document.getElementById("cur"),dur=document.getElementById("dur");
  var PLAY="M8 5v14l11-7z",PAUSE="M7 5h4v14H7zM13 5h4v14h-4z";
  function fmt(s){s=Math.floor(s||0);var m=Math.floor(s/60),x=s%60;return m+":"+(x<10?"0":"")+x;}
  ${DECODE_WAVE_JS}
  var strip=null,ph=null,bars=[],lastN=-1,scrub=null;
  // Downsample to ~1 bar per 6px of the box: desktop fills with many fine bars (uses all peaks),
  // narrow/phone screens get fewer so bars never collapse to hair-thin. max-over-bucket keeps shape.
  function buildWave(raw){
    wave.innerHTML="";bars=[];lastN=-1;
    var peaks=(raw&&raw.length)?raw.slice():[];
    if(!peaks.length){for(var i=0;i<64;i++){peaks.push(0.25+0.6*Math.abs(Math.sin(i*0.5))*(0.6+0.4*Math.random()));}}
    (function(){var box=wave.clientWidth||900,t=Math.max(24,Math.min(800,Math.floor(box/3))),n=peaks.length;
     if(n>t){var ds=[],b=n/t;for(var q=0;q<t;q++){var s=Math.floor(q*b),e=Math.max(s+1,Math.floor((q+1)*b)),mx=0;for(var u=s;u<e;u++)if(peaks[u]>mx)mx=peaks[u];ds.push(mx);}peaks=ds;}
     else if(n>1&&n<t){var up=[];for(var j=0;j<t;j++){var p=j*(n-1)/(t-1),i0=Math.floor(p),i1=Math.min(n-1,i0+1),fr=p-i0;up.push(peaks[i0]*(1-fr)+peaks[i1]*fr);}peaks=up;}})();
    strip=document.createElement("div");strip.className="wstrip";wave.appendChild(strip);
    ph=document.createElement("div");ph.className="wph";wave.appendChild(ph);
    peaks.forEach(function(v){var d=document.createElement("div");d.className="wb";d.style.height=Math.max(6,v*66)+"%";strip.appendChild(d);bars.push(d);});
  }
  buildWave(C.peaks);
  // No stored peaks (e.g. an external upload share) → decode the audio in the browser for the REAL wave.
  if(!C.peaks||!C.peaks.length){decodeWave("/a/"+C.id,240,function(p){if(p&&p.length)buildWave(p);});}
  function paint(){
    var dur=au.duration||0,ct=(scrub!=null?scrub:au.currentTime)||0,f=dur?Math.max(0,Math.min(1,ct/dur)):0;
    ph.style.left="calc(8px + "+(f*100)+"% - "+(f*16)+"px)";     // match the wstrip 8px inset → playhead tracks the lit bars exactly
    var n=Math.round(f*bars.length);
    if(n!==lastN){for(var i=0;i<bars.length;i++){if(i<n)bars[i].classList.add("on");else bars[i].classList.remove("on");}lastN=n;}
  }
  (function tick(){paint();requestAnimationFrame(tick);})();   // smooth continuous scroll
  play.onclick=function(){ if(au.paused){au.play();} else {au.pause();} };
  au.addEventListener("play",function(){try{fetch("/p/"+C.id,{method:"POST",keepalive:true});}catch(e){}ic.style.transform="scale(.6)";ic.style.opacity="0";setTimeout(function(){ic.firstChild.setAttribute("d",PAUSE);ic.style.transform="";ic.style.opacity="";},120);play.setAttribute("aria-label","Pause");});
  au.addEventListener("pause",function(){ic.style.transform="scale(.6)";ic.style.opacity="0";setTimeout(function(){ic.firstChild.setAttribute("d",PLAY);ic.style.transform="";ic.style.opacity="";},120);play.setAttribute("aria-label","Play");});
  au.addEventListener("loadedmetadata",function(){dur.textContent=fmt(au.duration);});
  au.addEventListener("timeupdate",function(){cur.textContent=fmt(au.currentTime);paint();});
  // Heartbeat: ping only while actually playing → "listening now" reflects real play/pause/close.
  setInterval(function(){ if(!au.paused&&!au.ended){ try{fetch("/p/"+C.id,{method:"POST",keepalive:true});}catch(e){} } },18000);
  // Scrub by DRAG (wave slides under the fixed playhead) + precise TAP (seek to the bar you touch),
  // both relative to the playhead. Pauses while scrubbing, resumes after — like the iPhone.
  (function(){
    var dn=false,mv=0,sx=0,wp=false;
    function frac(x){var r=wave.getBoundingClientRect();return Math.max(0,Math.min(1,(x-r.left)/r.width));}
    function commit(){ if(scrub!=null){au.currentTime=Math.max(0,Math.min(au.duration,scrub));scrub=null;} if(wp){wp=false;au.play();} }
    wave.addEventListener("pointerdown",function(e){if(!au.duration)return;dn=true;sx=e.clientX;mv=0;wp=!au.paused;if(wp)au.pause();scrub=frac(e.clientX)*au.duration;try{wave.setPointerCapture(e.pointerId);}catch(_){}});
    wave.addEventListener("pointermove",function(e){if(!dn||!au.duration)return;e.preventDefault();mv=Math.max(mv,Math.abs(e.clientX-sx));scrub=frac(e.clientX)*au.duration;});
    function end(e){if(!dn)return;dn=false;if(au.duration)scrub=frac(e.clientX)*au.duration;commit();}
    wave.addEventListener("pointerup",end);wave.addEventListener("pointercancel",end);
  })();
  document.querySelectorAll(".cp").forEach(function(btn){btn.addEventListener("click",function(){
    var body=btn.parentNode.querySelector(".body"); if(!body)return;
    navigator.clipboard.writeText(body.innerText).then(function(){btn.classList.add("ok");setTimeout(function(){btn.classList.remove("ok");},1200);});
  });});
})();
</script>
<script>
(function(){
  var cv=document.getElementById("pw"); if(!cv) return;
  var ctx=cv.getContext("2d"), W=0,H=0, lastW=-1, DPR=Math.min(2,window.devicePixelRatio||1);
  var phone=window.matchMedia&&window.matchMedia("(max-width:600px)").matches;
  // Resize only on width change — mobile address-bar show/hide changes height and was
  // making the wave jump (the "glitch").
  function size(){ if(window.innerWidth===lastW) return; lastW=window.innerWidth;
    W=window.innerWidth; H=window.innerHeight; cv.width=W*DPR; cv.height=H*DPR; ctx.setTransform(DPR,0,0,DPR,0,0); }
  size(); window.addEventListener("resize",size);
  var reduce=window.matchMedia&&window.matchMedia("(prefers-reduced-motion:reduce)").matches;
  var COLS=phone?54:90, ROWS=phone?34:52, amp=phone?0.55:1, t=0, last=0;
  function draw(){
    ctx.clearRect(0,0,W,H);
    for(var r=0;r<ROWS;r++){
      var dz=r/(ROWS-1), baseY=H*(0.30+0.78*dz), spread=W*(0.7+0.7*dz), rad=0.5+1.8*dz;
      for(var c=0;c<COLS;c++){
        var u=c/(COLS-1)-0.5;
        var h=Math.sin(u*6+t+r*0.33)*0.55+Math.sin(u*12-t*0.7+r*0.18)*0.3;
        var x=W*0.5+u*spread, y=baseY-h*36*(0.4+dz);
        if(y<-4||y>H+4) continue;
        var a=(0.07+0.32*dz)*(0.5+0.5*(h*0.5+0.5))*amp;
        ctx.fillStyle="rgba(235,231,222,"+a.toFixed(3)+")";
        ctx.beginPath(); ctx.arc(x,y,rad,0,6.283); ctx.fill();
      }
    }
  }
  function loop(ts){ if(ts-last>33){ last=ts; t+=0.02; draw(); } requestAnimationFrame(loop); }
  if(reduce){ draw(); } else { requestAnimationFrame(loop); }
})();
</script>
${reviewJS}
<script>
(function(){
  var au=document.getElementById("au"),cfgEl=document.getElementById("cfg");
  if(!au||!cfgEl)return;
  var ID=JSON.parse(cfgEl.textContent).id;
  var pill=document.createElement("button");
  pill.style.cssText="display:none;align-items:center;gap:7px;margin:16px auto 0;padding:7px 16px;border-radius:99px;background:rgba(255,255,255,.08);border:1px solid rgba(231,227,218,.3);color:#e7e3da;font:12px -apple-system,system-ui,sans-serif;cursor:pointer;position:relative;z-index:1";
  var wave=document.getElementById("wave");
  if(wave&&wave.parentNode)wave.parentNode.insertBefore(pill,wave);else document.body.appendChild(pill);
  // Live chat panel (hidden until connected). Sits at the bottom of the card.
  var chat=document.createElement("section");
  chat.style.cssText="display:none;width:100%;max-width:560px;margin:14px auto 0;padding:14px 16px;border-radius:16px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14);backdrop-filter:blur(10px);text-align:left;position:relative;z-index:1";
  chat.innerHTML='<div style="color:#e7e3da;font-size:11px;letter-spacing:1.6px;margin-bottom:10px">LIVE CHAT</div>'+
    '<div id="cmsgs" style="display:flex;flex-direction:column;gap:7px;max-height:240px;overflow-y:auto;margin-bottom:10px"></div>'+
    '<div style="display:flex;gap:8px"><input id="cin" maxlength="500" placeholder="Message…" autocomplete="off" style="flex:1;min-width:0;padding:9px 12px;border-radius:10px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14);color:#fff;font-size:16px;outline:none"><button id="csend" style="padding:9px 16px;border-radius:10px;background:rgba(231,227,218,.14);border:1px solid rgba(231,227,218,.3);color:#e7e3da;font-size:13px;cursor:pointer">Send</button></div>';
  // Live NOTES panel: timestamped review markers anyone can drop while listening.
  var notesP=document.createElement("section");
  notesP.style.cssText="display:none;width:100%;max-width:560px;margin:14px auto 0;padding:14px 16px;border-radius:16px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14);backdrop-filter:blur(10px);text-align:left;position:relative;z-index:1";
  notesP.innerHTML='<div style="color:#e7e3da;font-size:11px;letter-spacing:1.6px;margin-bottom:10px">LIVE NOTES</div>'+
    '<div id="nlist" style="display:flex;flex-direction:column;gap:5px;max-height:220px;overflow-y:auto;margin-bottom:10px"></div>'+
    '<div style="display:flex;gap:8px"><input id="nin" maxlength="500" placeholder="Note at this point…" autocomplete="off" style="flex:1;min-width:0;padding:9px 12px;border-radius:10px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14);color:#fff;font-size:16px;outline:none"><button id="nadd" style="padding:9px 14px;border-radius:10px;background:rgba(103,232,249,.14);border:1px solid rgba(103,232,249,.35);color:#67e8f9;font-size:13px;cursor:pointer;white-space:nowrap">Add <span id="nat">0:00</span></button></div>';
  document.querySelector(".wrap").appendChild(notesP);
  var chat=document.createElement("section");
  chat.style.cssText="display:none;width:100%;max-width:560px;margin:14px auto 0;padding:14px 16px;border-radius:16px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14);backdrop-filter:blur(10px);text-align:left;position:relative;z-index:1";
  chat.innerHTML='<div style="color:#e7e3da;font-size:11px;letter-spacing:1.6px;margin-bottom:10px">LIVE CHAT</div>'+
    '<div id="cmsgs" style="display:flex;flex-direction:column;gap:7px;max-height:240px;overflow-y:auto;margin-bottom:10px"></div>'+
    '<div style="display:flex;gap:8px"><input id="cin" maxlength="500" placeholder="Message…" autocomplete="off" style="flex:1;min-width:0;padding:9px 12px;border-radius:10px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14);color:#fff;font-size:16px;outline:none"><button id="csend" style="padding:9px 16px;border-radius:10px;background:rgba(231,227,218,.14);border:1px solid rgba(231,227,218,.3);color:#e7e3da;font-size:13px;cursor:pointer">Send</button></div>';
  document.querySelector(".wrap").appendChild(chat);
  var cmsgs=chat.querySelector("#cmsgs"),cin=chat.querySelector("#cin"),csend=chat.querySelector("#csend");
  var nlist=notesP.querySelector("#nlist"),nin=notesP.querySelector("#nin"),nadd=notesP.querySelector("#nadd"),nat=notesP.querySelector("#nat");
  function esc(s){var d=document.createElement("div");d.textContent=s;return d.innerHTML;}
  function fmtT(s){s=Math.floor(s||0);var m=Math.floor(s/60),x=s%60;return m+":"+(x<10?"0":"")+x;}
  // A guest name, asked once and remembered, so the host knows who's talking.
  var NAME=(function(){try{return localStorage.getItem("sos_livename")||"";}catch(e){return"";}})();
  function myName(){ return NAME||"Guest"; }
  function nameHTML(m){ var c=m.host?"#67e8f9":"#f0b429"; return '<span style="color:'+c+';font-weight:600">'+esc(m.from)+'</span>'; }
  function addChat(m){
    var row=document.createElement("div");
    row.innerHTML='<div style="font-size:13px;color:rgba(255,255,255,.9)">'+nameHTML(m)+' <span style="color:rgba(255,255,255,.85)">'+esc(m.text)+'</span></div>';
    cmsgs.appendChild(row);cmsgs.scrollTop=cmsgs.scrollHeight;
  }
  function sendChat(){var t=(cin.value||"").trim();if(!t||!ws||ws.readyState!==1)return;ws.send(JSON.stringify({type:"chat",from:myName(),text:t}));cin.value="";}
  csend.addEventListener("click",sendChat);
  cin.addEventListener("keydown",function(e){if(e.key==="Enter")sendChat();});
  // Notes + wave markers -------------------------------------------------------
  var notes=[];
  function renderMarkers(){
    var ex=wave.querySelectorAll(".lvmark");for(var i=0;i<ex.length;i++)ex[i].remove();
    var d=au.duration||0; if(d<=0)return;
    notes.forEach(function(n){var f=Math.max(0,Math.min(1,n.t/d));
      var mk=document.createElement("div");mk.className="lvmark";
      mk.style.cssText="position:absolute;top:4px;bottom:4px;width:2px;background:#67e8f9;z-index:4;cursor:pointer;box-shadow:0 0 6px rgba(103,232,249,.7);left:calc(8px + "+(f*100)+"% - "+(f*16)+"px)";
      mk.title=n.text;mk.addEventListener("click",function(ev){ev.stopPropagation();jumpTo(n.t);});
      wave.appendChild(mk);});
  }
  function renderNotes(){
    nlist.innerHTML="";
    if(!notes.length){nlist.innerHTML='<div style="color:rgba(255,255,255,.5);font-size:12px">No notes yet — add one at the current spot.</div>';return;}
    notes.slice().sort(function(a,b){return a.t-b.t;}).forEach(function(n){
      var row=document.createElement("div");row.style.cssText="display:flex;align-items:baseline;gap:8px;font-size:13px";
      var play='<button data-t="'+n.t+'" class="lvplay" style="background:none;border:none;color:#67e8f9;cursor:pointer;font-size:12px;padding:0">▶ '+fmtT(n.t)+'</button>';
      row.innerHTML=play+' '+nameHTML(n)+' <span style="color:rgba(255,255,255,.85)">'+esc(n.text)+'</span>';
      nlist.appendChild(row);
    });
    nlist.querySelectorAll(".lvplay").forEach(function(b){b.addEventListener("click",function(){jumpTo(parseFloat(b.getAttribute("data-t"))||0);});});
  }
  function addNote(m){ notes.push({t:m.t||0,text:m.text||"",from:m.from||"Guest",host:!!m.host}); renderNotes(); renderMarkers(); }
  function sendNote(){var t=(nin.value||"").trim();if(!t||!ws||ws.readyState!==1)return;ws.send(JSON.stringify({type:"note",from:myName(),t:au.currentTime||0,text:t}));nin.value="";}
  nadd.addEventListener("click",sendNote);
  nin.addEventListener("keydown",function(e){if(e.key==="Enter")sendNote();});
  setInterval(function(){if(nat)nat.textContent=fmtT(au.currentTime);},300);
  au.addEventListener("loadedmetadata",renderMarkers);
  var ws=null,joined=false,host=false,pending=null,ended=false;
  var igPlay=0,igPause=0,igSeek=0;   // consume-one: swallow the play/pause/seek events our own apply() triggers
  function dot(c){return '<span style="width:8px;height:8px;border-radius:50%;background:'+c+';display:inline-block"></span>';}
  // Onboarding gate: force a name (no default) → a big pulsing LIVE circle to tap → then the player.
  var pulseCSS=document.createElement("style");
  pulseCSS.textContent="@keyframes lvpulse{0%{box-shadow:0 0 0 0 rgba(46,204,113,.55)}70%{box-shadow:0 0 0 30px rgba(46,204,113,0)}100%{box-shadow:0 0 0 0 rgba(46,204,113,0)}}.lvcircle{animation:lvpulse 1.6s infinite}.lvcircle:active{transform:scale(.94)}";
  document.head.appendChild(pulseCSS);
  var gate=document.createElement("div");
  gate.style.cssText="position:fixed;inset:0;z-index:60;display:none;flex-direction:column;align-items:center;justify-content:center;gap:24px;background:rgba(7,7,8,.94);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);padding:28px;text-align:center";
  gate.innerHTML='<div id="gstep1" style="display:flex;flex-direction:column;align-items:center;gap:16px;width:100%;max-width:340px">'
    +'<div style="color:#e7e3da;font-size:12px;letter-spacing:1.8px">LIVE REVIEW</div>'
    +'<div style="color:#fff;font-size:22px;font-weight:600">Who’s listening?</div>'
    +'<div style="color:#8d8a82;font-size:13px;line-height:1.5">Enter your name so the host knows who’s giving feedback.</div>'
    +'<input id="gname" maxlength="40" placeholder="Your name" autocomplete="name" autocorrect="off" style="width:100%;padding:14px 16px;border-radius:12px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.16);color:#fff;font-size:17px;outline:none;text-align:center">'
    +'<button id="gcont" style="width:100%;padding:14px;border-radius:12px;background:#e7e3da;border:none;color:#0b0b0c;font-size:15px;font-weight:600;cursor:pointer;opacity:.4">Continue</button>'
    +'</div>'
    +'<div id="gstep2" style="display:none;flex-direction:column;align-items:center;gap:22px">'
    +'<div class="lvcircle" style="width:172px;height:172px;border-radius:50%;background:radial-gradient(circle at 50% 40%,#2ecc71,#0d6b38);display:flex;align-items:center;justify-content:center;cursor:pointer">'
    +'<div style="color:#fff;font-size:24px;font-weight:700;letter-spacing:1.5px">LIVE</div></div>'
    +'<div style="color:#e7e3da;font-size:14px">Tap to join <span id="ghost"></span></div>'
    +'</div>';
  document.body.appendChild(gate);
  var gname=gate.querySelector("#gname"),gcont=gate.querySelector("#gcont"),gstep1=gate.querySelector("#gstep1"),gstep2=gate.querySelector("#gstep2"),gcircle=gate.querySelector(".lvcircle");
  var gateShown=false,step2At=0;
  gname.addEventListener("input",function(){var v=gname.value.trim();gcont.style.opacity=v?"1":".4";});
  gname.addEventListener("keydown",function(e){if(e.key==="Enter")toStep2();});
  gcont.addEventListener("click",toStep2);
  function toStep2(){var v=gname.value.trim();if(!v)return;NAME=v.slice(0,40);try{localStorage.setItem("sos_livename",NAME);}catch(e){}gstep1.style.display="none";gstep2.style.display="flex";step2At=Date.now();}
  // step2At guard: on a phone the SAME tap that hit Continue lands on the circle that appears in its
  // place (ghost-click passthrough) → it would auto-join, skipping the circle. Ignore taps for 500ms.
  gcircle.addEventListener("click",function(){ if(Date.now()-step2At<500)return; joined=true;ended=false;gate.style.display="none";if(pending){apply(pending);pending=null;}update(); });
  // ALWAYS start on the name screen (pre-filled with the remembered name, editable) — never skip it,
  // so the name is confirmed each session and a stale localStorage value isn't silently reused.
  function showGate(){gate.style.display="flex";gstep1.style.display="flex";gstep2.style.display="none";if(NAME){gname.value=NAME;gcont.style.opacity="1";}setTimeout(function(){try{gname.focus();gname.select();}catch(e){}},60);}
  function update(){
    // Show the gate ONCE when a live host is first seen, and KEEP it (don't collapse on a transient
    // host=false flicker) until the user actually joins via the circle.
    if(ended){ gate.style.display="none"; }
    else if(!joined && host && !gateShown){ showGate(); gateShown=true; }
    else if(joined){ gate.style.display="none"; }
    if(joined&&!ended){pill.style.display="inline-flex";pill.innerHTML=dot("#2ecc71")+" Live — you can play, pause and add notes";pill.style.cursor="default";}
    else if(ended&&(joined||gateShown)){pill.style.display="inline-flex";pill.innerHTML=dot("#8d8a82")+" Live ended";pill.style.cursor="default";}
    else pill.style.display="none";
    var on=(joined&&!ended);chat.style.display=on?"block":"none";notesP.style.display=on?"block":"none";
  }
  // Two-way transport. Seek straight to the reported pos (no cross-clock delta — skew made it jump)
  // and only HARD-correct big drift. Each mutation we make here bumps a consume-one counter so the
  // resulting play/pause/seeked event is swallowed (not echoed back). Counters are robust to async
  // timing (unlike a fixed timer) and self-heal via catch-decrement if a play() is autoplay-blocked.
  function setTime(pos){ if(Math.abs((au.currentTime||0)-pos)>0.05){ igSeek++; try{au.currentTime=pos;}catch(e){igSeek--;} } }
  function nudge(pos, thresh){ if(Math.abs((au.currentTime||0)-pos)>thresh){ setTime(pos); } }
  function doPlay(){ if(au.paused){ igPlay++; au.play().catch(function(){igPlay--;}); } }
  function doPause(){ if(!au.paused){ igPause++; au.pause(); } }
  function apply(m){
    var pos=typeof m.pos==="number"?m.pos:0;
    if(m.type==="play"){ nudge(pos,0.35); doPlay(); }
    else if(m.type==="pause"){ doPause(); nudge(pos,0.35); }
    else if(m.type==="seek"){ setTime(pos); }
    else if(m.type==="state"){
      if(m.playing===false){ doPause(); nudge(pos,0.5); }
      else { nudge(pos,1.2); doPlay(); }
    }
  }
  // A marker tap is a USER action → let its events broadcast so the host + others jump too.
  function jumpTo(t){ if(!joined)return; try{au.currentTime=t;}catch(e){} au.play().catch(function(){}); }
  function sendT(type){ if(!joined||ended||!ws||ws.readyState!==1)return; ws.send(JSON.stringify({type:type,pos:au.currentTime||0,playing:!au.paused,at:Date.now()})); }
  au.addEventListener("play",function(){ if(igPlay>0){igPlay--;return;} sendT("play"); });
  au.addEventListener("pause",function(){ if(igPause>0){igPause--;return;} sendT("pause"); });
  var seekTimer=null;
  au.addEventListener("seeked",function(){ if(igSeek>0){igSeek--;return;} clearTimeout(seekTimer); seekTimer=setTimeout(function(){sendT("seek");},120); });
  function connect(){
    try{ws=new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws/"+ID);}catch(e){return;}
    ws.onmessage=function(ev){
      var m;try{m=JSON.parse(ev.data);}catch(e){return;}
      if(m.type==="presence"){var was=host;host=m.host;if(host&&!was){try{au.preload="auto";au.load();}catch(e){}}if(!host&&was&&joined){ended=true;au.pause();}update();return;}
      if(m.type==="end"){if(joined){ended=true;au.pause();}host=false;update();return;}
      if(m.type==="chat"){addChat(m);return;}
      if(m.type==="note"){addNote(m);return;}
      if(!joined){pending=m;return;}
      apply(m);
    };
    ws.onclose=function(){host=false;update();setTimeout(connect,3000);};
  }
  connect();
})();
</script>
</body></html>`
}

function fileIcon(it) {
  if (it.folder) return `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"><path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/></svg>`
  if (it.audio) return `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><circle cx="6" cy="18" r="2.3"/><circle cx="17" cy="16" r="2.3"/><path d="M8.3 18V6l11-2v10"/></svg>`
  const e = (it.ext || "").toLowerCase()
  if (["jpg","jpeg","png","heic","tiff","webp","gif"].includes(e)) return `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"><rect x="3" y="4" width="18" height="16" rx="2"/><circle cx="8.5" cy="9.5" r="1.5"/><path d="M21 16l-5-5L5 20"/></svg>`
  if (["mp4","mov","m4v","avi","mkv"].includes(e)) return `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"><rect x="3" y="5" width="18" height="14" rx="2"/><path d="M10 9l5 3-5 3z" fill="currentColor" stroke="none"/></svg>`
  if (["zip","rar","7z"].includes(e)) return `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"><rect x="3" y="4" width="18" height="16" rx="2"/><path d="M12 4v16M9.5 8h5M9.5 12h5"/></svg>`
  return `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"/><path d="M14 3v5h5"/></svg>`
}

// Pre-save / smart link page: "Coming MM/DD" before the release date, manual DSP buttons on/after it.
// Pure server-side date flip (Date.now() vs m.releaseDate); the /s/ route is uncached so each load
// re-evaluates — the page flips itself the first request on release day, no purge needed.
function renderPresavePage(id, m, origin) {
  const live = m.releaseDate > 0 && Date.now() >= m.releaseDate
  const d = new Date(m.releaseDate || Date.now())
  const mmdd = (d.getMonth() + 1) + "/" + d.getDate()
  const art = m.artworkKey ? `<img class="art" src="/img/${id}" alt="">` : `<img class="art logo" src="/logo" alt="">`
  const body = live
    ? `<div class="dsps">` + (m.dspLinks || []).map((x) =>
        `<a class="dsp" href="${esc(x.url)}" target="_blank" rel="noopener">${esc(x.name)}</a>`).join("") + `</div>`
    : `<div class="soon">Coming ${esc(mmdd)}</div>`
  return `<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#070707">
<title>${esc(m.title)} — ${esc(m.artist)}</title>
<meta property="og:title" content="${esc(m.title)}"><meta property="og:description" content="${esc(m.artist)}">
${m.artworkKey ? `<meta property="og:image" content="${origin}/img/${id}">` : ""}
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  html,body{background:#070707;color:#fff;font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif}
  body{min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:40px 20px;
    background:radial-gradient(95% 50% at 50% 12%,#26262c 0%,#070707 60%)}
  .wrap{width:100%;max-width:360px;display:flex;flex-direction:column;align-items:center}
  .art{width:200px;height:200px;border-radius:24px;object-fit:cover;display:block;
    background:#1a1a1e;border:1px solid rgba(231,227,218,.22);box-shadow:0 22px 60px rgba(0,0,0,.65)}
  .art.logo{background:#171717}
  h1{font-size:21px;font-weight:600;text-align:center;margin-top:24px;letter-spacing:-.2px}
  .artist{color:#e7e3da;font-size:12px;text-align:center;margin-top:5px;letter-spacing:.7px;text-transform:uppercase}
  .soon{margin-top:30px;color:#e7e3da;font-size:15px;font-weight:600;letter-spacing:.4px;
    padding:11px 22px;border:1px solid rgba(231,227,218,.25);border-radius:999px}
  .dsps{margin-top:28px;width:100%;display:flex;flex-direction:column;gap:10px}
  .dsp{display:block;text-align:center;text-decoration:none;color:#0b0b0c;font-weight:600;font-size:15px;
    padding:13px 18px;border-radius:14px;background:linear-gradient(180deg,#efece4,#d8d3c7)}
  .dsp:active{transform:scale(.99)}
</style></head><body>
<div class="wrap">
  ${art}
  <h1>${esc(m.title)}</h1>
  <div class="artist">${esc(m.artist)}</div>
  ${body}
</div></body></html>`
}

function renderFolderPage(id, m, brand) {
  var PLAY = `<svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>`
  var DL = `<svg viewBox="0 0 24 24" width="15" height="15" fill="currentColor"><path d="M12 16l-5-5h3V4h4v7h3l-5 5zm-7 2h14v2H5z"/></svg>`
  const CHEV = `<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 6l6 6-6 6"/></svg>`
  const items = Array.isArray(m.items) ? m.items : []
  // Prefer the recursive tree; fall back to legacy flat items (old shares — subfolders inert).
  const tree = Array.isArray(m.tree) ? m.tree
    : items.map((it, i) => it.folder ? { name: it.name, folder: true, items: [] }
                                     : { name: it.name, ext: it.ext, audio: it.audio, peaks: it.peaks || null, i })
  const cfg = JSON.stringify({
    id, title: m.title || "Folder", tree, live: !!m.folderKey,
    icons: { folder: fileIcon({ folder: true }), audio: fileIcon({ audio: true }), file: fileIcon({}), play: PLAY, dl: DL, chev: CHEV },
  }).replace(/</g, "\\u003c")   // safe to embed inside <script>
  const artSrc = m.artworkKey ? `/img/${id}` : `/logo`
  return `<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#070707">
<title>${esc(m.title)}</title>
<style>
  :root{--p:#e7e3da;--soft:#8d8a82}
  *{box-sizing:border-box;margin:0;padding:0}
  html{background:#070707}
  body{background:#070707;color:#fff;font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif;min-height:100vh;
    display:flex;flex-direction:column;align-items:center;justify-content:flex-start;padding:46px 18px 30px}
  .bgfx{position:fixed;inset:0;z-index:0;overflow:hidden;background:radial-gradient(95% 50% at 50% 12%,#26262c 0%,#070707 60%)}
  .bgfx canvas{position:absolute;inset:0;width:100%;height:100%;filter:blur(2px)}
  .view{position:relative;z-index:1;width:100%;display:flex;flex-direction:column;align-items:center}
  .fwrap{width:100%;max-width:460px;display:flex;flex-direction:column;align-items:center}
  .fhead{display:flex;flex-direction:column;align-items:center;gap:4px;text-align:center;animation:frise .55s ease both}
  .ficon{width:84px;height:84px;border-radius:22px;display:flex;align-items:center;justify-content:center;color:var(--p);
    background:rgba(231,227,218,.10);border:1px solid rgba(231,227,218,.24);
    backdrop-filter:blur(18px) saturate(160%);-webkit-backdrop-filter:blur(18px) saturate(160%);
    box-shadow:0 14px 36px rgba(0,0,0,.5),inset 0 1.5px 0 rgba(255,255,255,.25)}
  .ficon svg{width:38px;height:38px}
  h1{font-size:20px;font-weight:600;margin-top:12px;letter-spacing:-.2px;text-align:center}
  .fcount{color:var(--soft);font-size:12px;letter-spacing:.5px}
  .flist{width:100%;max-width:460px;margin-top:26px;display:flex;flex-direction:column;gap:9px}
  .fitem{opacity:0;animation:frise .5s ease forwards}
  .frow{display:flex;align-items:center;gap:12px;width:100%;padding:11px 13px;border-radius:14px;text-align:left;font:inherit;color:inherit;
    background:rgba(231,227,218,.055);border:1px solid rgba(231,227,218,.12);
    backdrop-filter:blur(16px) saturate(170%);-webkit-backdrop-filter:blur(16px) saturate(170%);
    box-shadow:inset 0 1px 0 rgba(255,255,255,.10)}
  .frow.fol{opacity:.82}
  .frow.open{cursor:pointer;transition:background .2s ease,transform .15s ease}
  .frow.open:hover{background:rgba(231,227,218,.09)}
  .frow.open:active{transform:scale(.99)}
  .fr-ic{width:36px;height:36px;flex:0 0 36px;border-radius:10px;display:flex;align-items:center;justify-content:center;color:var(--p);background:rgba(231,227,218,.08)}
  .fr-nm{flex:1;min-width:0;color:#ece8df;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .fr-ext{font-size:9px;font-weight:600;color:var(--soft);letter-spacing:.5px}
  .fr-act{width:32px;height:32px;flex:0 0 32px;text-decoration:none;border-radius:9px;display:flex;align-items:center;justify-content:center;color:var(--p);
    background:rgba(231,227,218,.10);border:1px solid rgba(231,227,218,.2)}
  .ffoot{color:rgba(231,227,218,.4);font-size:9px;letter-spacing:.5px;margin-top:26px}
  .back{align-self:flex-start;display:flex;align-items:center;gap:7px;margin-bottom:16px;cursor:pointer;color:var(--p);font-size:13px;font-weight:500;max-width:80%;overflow:hidden;
    padding:9px 16px 9px 12px;border-radius:13px;background:rgba(231,227,218,.08);border:1px solid rgba(231,227,218,.2);
    backdrop-filter:blur(18px) saturate(170%);-webkit-backdrop-filter:blur(18px) saturate(170%)}
  .swrap{width:100%;max-width:360px;display:flex;flex-direction:column;align-items:center}
  .art{width:184px;height:184px;border-radius:24px;object-fit:cover;display:block;background:#1a1a1e;border:1px solid rgba(231,227,218,.22);box-shadow:0 22px 60px rgba(0,0,0,.65)}
  .art.logo{object-fit:cover;background:#171717}
  .art.hex{border:none;border-radius:0;box-shadow:none;background:transparent;clip-path:polygon(25% 6.7%,75% 6.7%,100% 50%,75% 93.3%,25% 93.3%,0 50%);filter:drop-shadow(0 18px 40px rgba(0,0,0,.6))}
  .fhex{width:96px;height:96px}
  .h1s{font-size:20px;font-weight:600;text-align:center;margin-top:22px;letter-spacing:-.2px;max-width:90vw;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .artist{color:var(--p);font-size:12px;text-align:center;margin-top:4px;letter-spacing:.7px}
  /* Apple-style scrolling waveform: a fixed playhead with the wave sliding LEFT past it. */
  .wave{position:relative;height:64px;width:96vw;max-width:920px;margin-top:30px;cursor:pointer;overflow:hidden;touch-action:none;
    background:linear-gradient(180deg,#141417,#0b0b0d);border-radius:20px;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.05),inset 0 0 0 1px rgba(231,227,218,.06),0 10px 28px rgba(0,0,0,.45)}
  .wstrip{position:absolute;top:0;left:0;right:0;height:100%;display:flex;align-items:center;gap:1px;padding:0 8px;box-sizing:border-box}
  .wb{flex:1 1 0;min-width:0;border-radius:2px;background:rgba(231,227,218,.30)}
  .wb.on{background:var(--p);opacity:.85}
  .wph{position:absolute;top:8px;bottom:8px;left:0;width:2.5px;border-radius:2px;background:#fff;
    transform:translateX(-50%);z-index:2;box-shadow:0 0 10px rgba(255,255,255,.55)}
  .time{display:flex;justify-content:space-between;width:96vw;max-width:920px;margin-top:8px;color:var(--soft);font-size:11px;font-variant-numeric:tabular-nums}
  .play{width:62px;height:62px;border-radius:50%;margin-top:20px;cursor:pointer;color:var(--p);
    background:rgba(231,227,218,.10);border:1px solid rgba(231,227,218,.30);
    backdrop-filter:blur(16px) saturate(160%);-webkit-backdrop-filter:blur(16px) saturate(160%);
    box-shadow:0 10px 30px rgba(0,0,0,.45), inset 0 1px 0 rgba(255,255,255,.18), inset 0 -8px 16px rgba(0,0,0,.25);
    display:flex;align-items:center;justify-content:center;transition:transform .25s cubic-bezier(.2,.9,.25,1), background .25s, box-shadow .25s}
  .play:hover{background:rgba(231,227,218,.16)}
  .play:active{transform:scale(.93)}
  .play svg{width:22px;height:22px;fill:var(--p);transition:opacity .2s, transform .2s}
  .dl{display:flex;align-items:center;gap:8px;margin-top:22px;text-decoration:none;color:var(--p);font-size:13px;font-weight:500;
    padding:11px 20px;border-radius:14px;background:rgba(231,227,218,.08);border:1px solid rgba(231,227,218,.22);
    backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px)}
  .anim-in{animation:slideIn .42s cubic-bezier(.3,.9,.3,1) both}
  .anim-back{animation:slideBack .42s cubic-bezier(.3,.9,.3,1) both}
  @keyframes frise{from{opacity:0;transform:translateY(14px)}to{opacity:1;transform:none}}
  @keyframes slideIn{from{opacity:0;transform:translateX(26px)}to{opacity:1;transform:none}}
  @keyframes slideBack{from{opacity:0;transform:translateX(-26px)}to{opacity:1;transform:none}}
  @media (max-width:600px){.wave,.time{width:98vw}
    .art{box-shadow:0 22px 60px rgba(0,0,0,.65),inset 0 1.5px 0 rgba(255,255,255,.22),inset 0 0 0 1px rgba(231,227,218,.16)}
    .play{backdrop-filter:blur(24px) saturate(180%);-webkit-backdrop-filter:blur(24px) saturate(180%);box-shadow:0 12px 32px rgba(0,0,0,.5),inset 0 1.5px 0 rgba(255,255,255,.35),inset 0 -10px 18px rgba(0,0,0,.28)}}
  @media (prefers-reduced-motion:reduce){.fitem,.fhead,.anim-in,.anim-back{animation:none;opacity:1}}
</style></head><body>
<div class="bgfx"><canvas id="pw"></canvas></div>
<div class="view" id="listView">
  <button class="back" id="fback" style="display:none"><svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 5l-7 7 7 7"/></svg><span class="fr-nm" id="fbacklbl">Back</span></button>
  <div class="fwrap"><div class="fhead">${m.artworkKey
    ? `<img class="art hex fhex" src="/img/${id}" alt="">`
    : `<div class="ficon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"><path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/></svg></div>`}<h1 id="ftitle">${esc(m.title)}</h1><div class="fcount" id="fcount"></div></div></div>
  <div class="flist" id="flist"></div>
  <div class="ffoot">${esc(brand)}</div>
</div>
<div class="view" id="songView" style="display:none">
  <button class="back" id="back"><svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 5l-7 7 7 7"/></svg><span class="fr-nm">${esc(m.title)}</span></button>
  <div class="swrap">
    <img class="art${m.artworkKey ? " hex" : " logo"}" id="svArt" src="${artSrc}" alt="">
    <div class="h1s" id="svTitle"></div>
    <div class="artist">${esc(m.title)}</div>
    <div class="wave" id="svWave"></div>
    <div class="time"><span id="svCur">0:00</span><span id="svDur">0:00</span></div>
    <button class="play" id="svPlay" aria-label="Play"><svg id="svIc" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg></button>
    <div class="ffoot">${esc(brand)}</div>
  </div>
</div>
<script>
(function(){
  var cv=document.getElementById("pw"); if(!cv) return;
  var ctx=cv.getContext("2d"), W=0,H=0, lastW=-1, DPR=Math.min(2,window.devicePixelRatio||1);
  var phone=window.matchMedia&&window.matchMedia("(max-width:600px)").matches;
  function size(){ if(window.innerWidth===lastW) return; lastW=window.innerWidth;
    W=window.innerWidth; H=window.innerHeight; cv.width=W*DPR; cv.height=H*DPR; ctx.setTransform(DPR,0,0,DPR,0,0); }
  size(); window.addEventListener("resize",size);
  var reduce=window.matchMedia&&window.matchMedia("(prefers-reduced-motion:reduce)").matches;
  var COLS=phone?54:90, ROWS=phone?34:52, amp=phone?0.55:1, t=0, last=0;
  function draw(){
    ctx.clearRect(0,0,W,H);
    for(var r=0;r<ROWS;r++){
      var dz=r/(ROWS-1), baseY=H*(0.30+0.78*dz), spread=W*(0.7+0.7*dz), rad=0.5+1.8*dz;
      for(var c=0;c<COLS;c++){
        var u=c/(COLS-1)-0.5;
        var h=Math.sin(u*6+t+r*0.33)*0.55+Math.sin(u*12-t*0.7+r*0.18)*0.3;
        var x=W*0.5+u*spread, y=baseY-h*36*(0.4+dz);
        if(y<-4||y>H+4) continue;
        var a=(0.07+0.32*dz)*(0.5+0.5*(h*0.5+0.5))*amp;
        ctx.fillStyle="rgba(235,231,222,"+a.toFixed(3)+")";
        ctx.beginPath(); ctx.arc(x,y,rad,0,6.283); ctx.fill();
      }
    }
  }
  function loop(ts){ if(ts-last>33){ last=ts; t+=0.02; draw(); } requestAnimationFrame(loop); }
  if(reduce){ draw(); } else { requestAnimationFrame(loop); }
})();
(function(){
  var C=${cfg};
  var listV=document.getElementById("listView"), songV=document.getElementById("songView"), sBack=document.getElementById("back");
  var flist=document.getElementById("flist"), ftitle=document.getElementById("ftitle"), fcount=document.getElementById("fcount"),
      fback=document.getElementById("fback"), fbacklbl=document.getElementById("fbacklbl");
  var au=new Audio(), bars=[], strip=null, ph=null, lastN=-1, scrub=null, waveTok=0;   // fill-width wave, progress playhead
  ${DECODE_WAVE_JS}
  var play=document.getElementById("svPlay"), ic=document.getElementById("svIc"), wave=document.getElementById("svWave"),
      cur=document.getElementById("svCur"), dur=document.getElementById("svDur"), title=document.getElementById("svTitle");
  var PLAYP="M8 5v14l11-7z", PAUSEP="M7 5h4v14H7zM13 5h4v14h-4z";
  var stack=[{name:C.title, items:C.tree||[]}];
  function fmt(s){s=Math.floor(s||0);var m=Math.floor(s/60),x=s%60;return m+":"+(x<10?"0":"")+x;}
  function el(t,c){var e=document.createElement(t); if(c)e.className=c; return e;}
  function ic_(svg){var d=el("div","fr-ic"); d.innerHTML=svg; return d;}
  function nm_(t){var d=el("div","fr-nm"); d.textContent=t; return d;}
  function node(){return stack[stack.length-1];}
  function nFiles(n){return (n.items||[]).filter(function(x){return !x.folder;}).length;}
  // Live items carry a full R2 key (.k → /fk); legacy snapshot items carry an index (.i → /fi).
  function fileURL(it,dl){var b=(it.k!=null)?("/fk/"+C.id+"?k="+encodeURIComponent(it.k)):("/fi/"+C.id+"/"+it.i);return b+(dl?((it.k!=null?"&":"?")+"dl=1"):"");}
  // --- folder navigation (drill in/out) ---
  function renderLevel(){
    var n=node(); flist.innerHTML=""; bars=[];
    ftitle.textContent=n.name; var c=nFiles(n); fcount.textContent=c+" file"+(c===1?"":"s");
    if(stack.length>1){fback.style.display="";fbacklbl.textContent=stack[stack.length-2].name;}else{fback.style.display="none";}
    (n.items||[]).forEach(function(it,i){
      var item=el("div","fitem"); item.style.animationDelay=(0.06+i*0.045)+"s";
      if(it.folder){
        var b=el("button","frow open"); b.appendChild(ic_(C.icons.folder)); b.appendChild(nm_(it.name));
        var a=el("span","fr-act"); a.innerHTML=C.icons.chev; b.appendChild(a);
        b.addEventListener("click",function(){ stack.push(it); animIn(); });
        item.appendChild(b);
      } else if(it.audio){
        var b=el("button","frow open"); b.appendChild(ic_(C.icons.audio)); b.appendChild(nm_(it.name));
        var x=el("span","fr-ext"); x.textContent=(it.ext||"").toUpperCase(); b.appendChild(x);
        var a=el("span","fr-act"); a.innerHTML=C.icons.play; b.appendChild(a);
        b.addEventListener("click",function(){ openSong(it); });
        item.appendChild(b);
      } else {
        var d=el("div","frow"); d.appendChild(ic_(C.icons.file)); d.appendChild(nm_(it.name));
        var x=el("span","fr-ext"); x.textContent=(it.ext||"").toUpperCase(); d.appendChild(x);
        var dl=el("a","fr-act"); dl.href=fileURL(it,true); dl.setAttribute("aria-label","Download"); dl.innerHTML=C.icons.dl; d.appendChild(dl);
        item.appendChild(d);
      }
      flist.appendChild(item);
    });
  }
  function animIn(){ listV.classList.remove("anim-in"); void listV.offsetWidth; listV.classList.add("anim-in"); renderLevel(); window.scrollTo(0,0); }
  fback.addEventListener("click",function(){ if(stack.length>1){ stack.pop(); listV.classList.remove("anim-back"); void listV.offsetWidth; listV.classList.add("anim-back"); renderLevel(); } });
  // --- song view ---
  function buildWave(peaks){ wave.innerHTML=""; bars=[]; lastN=-1;
    if(!peaks||!peaks.length){peaks=[];for(var k=0;k<64;k++){peaks.push(0.25+0.6*Math.abs(Math.sin(k*0.5))*(0.6+0.4*Math.random()));}}
    (function(){var box=wave.clientWidth||900,t=Math.max(24,Math.min(800,Math.floor(box/3))),n=peaks.length;
     if(n>t){var ds=[],b=n/t;for(var q=0;q<t;q++){var s=Math.floor(q*b),e=Math.max(s+1,Math.floor((q+1)*b)),mx=0;for(var u=s;u<e;u++)if(peaks[u]>mx)mx=peaks[u];ds.push(mx);}peaks=ds;}
     else if(n>1&&n<t){var up=[];for(var j=0;j<t;j++){var p=j*(n-1)/(t-1),i0=Math.floor(p),i1=Math.min(n-1,i0+1),fr=p-i0;up.push(peaks[i0]*(1-fr)+peaks[i1]*fr);}peaks=up;}})();
    strip=el("div","wstrip"); wave.appendChild(strip);
    ph=el("div","wph"); wave.appendChild(ph);
    peaks.forEach(function(v){var d=el("div","wb");d.style.height=Math.max(6,v*66)+"%";strip.appendChild(d);bars.push(d);}); }
  function paint(){ if(!strip||!ph)return;
    var dur=au.duration||0,ct=(scrub!=null?scrub:au.currentTime)||0,f=dur?Math.max(0,Math.min(1,ct/dur)):0;
    ph.style.left="calc(8px + "+(f*100)+"% - "+(f*16)+"px)";     // match the wstrip 8px inset
    var n=Math.round(f*bars.length);
    if(n!==lastN){for(var j=0;j<bars.length;j++){if(j<n)bars[j].classList.add("on");else bars[j].classList.remove("on");}lastN=n;}
  }
  (function tick(){paint();requestAnimationFrame(tick);})();   // smooth continuous scroll
  var nowName="";
  function openSong(it){
    title.textContent=String(it.name||"").replace(/\.[^.]+$/,"");
    nowName=title.textContent;
    var myTok=++waveTok;
    buildWave(it.peaks||[]);
    var src=fileURL(it,false);
    au.src=src;
    listV.style.display="none"; songV.style.display="flex";
    songV.classList.remove("anim-in"); void songV.offsetWidth; songV.classList.add("anim-in");
    window.scrollTo(0,0); au.play();
    // Folder items carry no stored peaks → decode the audio in-browser for the REAL wave (cached on
    // the item so re-open is instant; token guards against a fast song switch landing on the wrong wave).
    if(!it.peaks||!it.peaks.length){decodeWave(src,240,function(p){if(p&&p.length&&myTok===waveTok){it.peaks=p;buildWave(p);}});}
  }
  sBack.addEventListener("click",function(){ au.pause(); songV.style.display="none"; listV.style.display="flex"; renderLevel(); });
  play.onclick=function(){ if(au.paused)au.play(); else au.pause(); };
  au.addEventListener("play",function(){try{fetch("/p/"+C.id+(nowName?("?t="+encodeURIComponent(nowName)):""),{method:"POST",keepalive:true});}catch(e){}ic.style.opacity="0";setTimeout(function(){ic.firstChild.setAttribute("d",PAUSEP);ic.style.opacity="";},120);play.setAttribute("aria-label","Pause");});
  au.addEventListener("pause",function(){ic.style.opacity="0";setTimeout(function(){ic.firstChild.setAttribute("d",PLAYP);ic.style.opacity="";},120);play.setAttribute("aria-label","Play");});
  au.addEventListener("loadedmetadata",function(){dur.textContent=fmt(au.duration);});
  au.addEventListener("timeupdate",function(){cur.textContent=fmt(au.currentTime);paint();});
  setInterval(function(){ if(!au.paused&&!au.ended){ try{fetch("/p/"+C.id,{method:"POST",keepalive:true});}catch(e){} } },18000);
  // Scrub by DRAG (wave slides under the fixed playhead) + precise TAP (seek to the bar you touch),
  // both relative to the playhead. Pauses while scrubbing, resumes after — like the iPhone.
  (function(){
    var dn=false,mv=0,sx=0,wp=false;
    function frac(x){var r=wave.getBoundingClientRect();return Math.max(0,Math.min(1,(x-r.left)/r.width));}
    function commit(){ if(scrub!=null){au.currentTime=Math.max(0,Math.min(au.duration,scrub));scrub=null;} if(wp){wp=false;au.play();} }
    wave.addEventListener("pointerdown",function(e){if(!au.duration)return;dn=true;sx=e.clientX;mv=0;wp=!au.paused;if(wp)au.pause();scrub=frac(e.clientX)*au.duration;try{wave.setPointerCapture(e.pointerId);}catch(_){}});
    wave.addEventListener("pointermove",function(e){if(!dn||!au.duration)return;e.preventDefault();mv=Math.max(mv,Math.abs(e.clientX-sx));scrub=frac(e.clientX)*au.duration;});
    function end(e){if(!dn)return;dn=false;if(au.duration)scrub=frac(e.clientX)*au.duration;commit();}
    wave.addEventListener("pointerup",end);wave.addEventListener("pointercancel",end);
  })();
  renderLevel();
  // Live shares: replace the embedded snapshot with the folder's CURRENT R2 contents (new files show
  // up on refresh). First paint already happened from the snapshot, so this just reconciles.
  if(C.live){try{fetch("/st/"+C.id).then(function(r){return r.json();}).then(function(d){
    if(d&&d.tree){stack[0].items=d.tree; if(stack.length===1 && listV.style.display!=="none") renderLevel();}
  }).catch(function(){});}catch(e){}}
})();
</script>
</body></html>`
}

// ---------------------------------------------------------------------------
// Listen Together room: one Durable Object per share id. Host (the app) drives
// play/pause/seek; guests (share-page browsers) follow. Control-plane only —
// each client streams the audio itself from R2, so quality is full and the
// only sync cost is the control message latency (~100-300ms).
// Hibernation API keeps the room free while idle.
export class ListenRoom {
  constructor(state) { this.state = state }

  async fetch(request) {
    const url = new URL(request.url)
    const role = url.searchParams.get("role") === "host" ? "host" : "guest"
    const pair = new WebSocketPair()
    // serializeAttachment survives hibernation; tags let us count roles cheaply.
    this.state.acceptWebSocket(pair[1], [role])
    pair[1].serializeAttachment({ role })
    this.sendPresence()
    // A late joiner immediately gets the last known transport state + recent chat + note history.
    if (this.lastState) { try { pair[1].send(this.lastState) } catch (e) {} }
    for (const c of (this.chatLog || [])) { try { pair[1].send(c) } catch (e) {} }
    for (const n of (this.noteLog || [])) { try { pair[1].send(n) } catch (e) {} }
    return new Response(null, { status: 101, webSocket: pair[0] })
  }

  webSocketMessage(ws, message) {
    if (typeof message !== "string" || message.length > 4096) return
    const att = ws.deserializeAttachment() || {}
    let m; try { m = JSON.parse(message) } catch (e) { return }
    const from = String(m.from || (att.role === "host" ? "Host" : "Guest")).slice(0, 40)
    // Chat (bidirectional) — broadcast to everyone + short history for late joiners.
    if (m.type === "chat") {
      const text = String(m.text || "").slice(0, 500).trim()
      if (!text) return
      const out = JSON.stringify({ type: "chat", from, host: att.role === "host", text, at: Date.now() })
      this.chatLog = this.chatLog || []
      this.chatLog.push(out); while (this.chatLog.length > 40) this.chatLog.shift()
      for (const w of this.state.getWebSockets()) { try { w.send(out) } catch (e) {} }
      return
    }
    // A timestamped review note pinned to a song position — anyone can drop one. Broadcast + keep
    // history so late joiners + the host's app rebuild every marker.
    if (m.type === "note") {
      const text = String(m.text || "").slice(0, 500).trim()
      if (!text) return
      const t = Math.max(0, Number(m.t) || 0)
      const out = JSON.stringify({ type: "note", from, host: att.role === "host", t, text, at: Date.now() })
      this.noteLog = this.noteLog || []
      this.noteLog.push(out); while (this.noteLog.length > 200) this.noteLog.shift()
      for (const w of this.state.getWebSockets()) { try { w.send(out) } catch (e) {} }
      return
    }
    // Transport is now TWO-WAY: host OR any guest can play/pause/seek. Relay to everyone EXCEPT the
    // sender (the sender already did the action locally) — that exclusion is what stops echo loops.
    if (m.type === "play" || m.type === "pause" || m.type === "seek" || m.type === "state") {
      this.lastState = message
      for (const w of this.state.getWebSockets()) { if (w !== ws) { try { w.send(message) } catch (e) {} } }
    }
  }

  webSocketClose(ws) { this.dropped(ws) }
  webSocketError(ws) { this.dropped(ws) }

  dropped(ws) {
    const att = ws.deserializeAttachment() || {}
    if (att.role === "host") {
      this.lastState = null
      // Host left → tell guests the session ended (they pause + show "host left").
      for (const g of this.state.getWebSockets("guest")) {
        try { g.send('{"type":"end"}') } catch (e) {}
      }
    }
    this.sendPresence(ws)
  }

  // {type:"presence", host:<bool>, guests:<n>} to EVERYONE (app shows guest count,
  // page shows "connected to host"). `closing` is excluded from the counts.
  sendPresence(closing) {
    const all = this.state.getWebSockets().filter((w) => w !== closing)
    const host = all.some((w) => (w.deserializeAttachment() || {}).role === "host")
    const guests = all.length - (host ? 1 : 0)
    const msg = JSON.stringify({ type: "presence", host, guests })
    for (const w of all) { try { w.send(msg) } catch (e) {} }
  }
}
