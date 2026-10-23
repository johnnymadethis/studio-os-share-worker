// Studio OS — share worker.
// Premium platinum share page streamed from R2 (free egress). Random unguessable ids.
// Modes: listen+download or listen-only (/d blocked). Waveform player, glass controls.

import { LOGO_B64 } from "./logo.js"
// MIME parsing for the invoice mailbox. Hand-rolling multipart/base64/quoted-printable is exactly
// the kind of "almost works" parser that breaks on the one mail that matters.
import PostalMime from "postal-mime"

export default {
  async fetch(request, env, ctx) {
    try { return await handle(request, env, ctx) }
    catch (e) {
      await logErr(env, `${request.method} ${new URL(request.url).pathname}: ${(e && e.message) || e}`)
      return new Response("Server error", { status: 500 })
    }
  },

  /// Hourly tick: the trash/staging sweep, and once a day an invoice that's been sitting unpaid
  /// for a month gets a nudge.
  async scheduled(event, env, ctx) {
    ctx.waitUntil(sweepTrash(env))   // also drains stale __up/ chunked-upload staging
    // Once a day at 10:00 Romania — asked of the zone, so it stays 10:00 across the DST switch.
    if (roHour(event.scheduledTime) === 10) {
      try { await remindUnpaidRent(env) } catch (e) { await logErr(env, `cron rent: ${(e && e.message) || e}`) }
    }
  },

  /// Studio Rent's inbox. The landlord's invoices are forwarded here by a Gmail filter; every PDF
  /// lands in R2 and shows up as pending in the app, which reads the amounts itself (PDFKit) —
  /// no PDF parser on the edge.
  async email(message, env, ctx) {
    try { await takeInvoiceMail(message, env) }
    catch (e) { await logErr(env, `email: ${(e && e.message) || e}`) }
  },
}

async function takeInvoiceMail(message, env) {
  const parsed = await PostalMime.parse(await new Response(message.raw).arrayBuffer())
  const pdfs = (parsed.attachments || []).filter(a =>
    (a.mimeType || "").includes("pdf") || /\.pdf$/i.test(a.filename || ""))
  if (!pdfs.length) {
    // No PDF → not an invoice. Most likely Gmail's OWN "confirm this forwarding address" mail, which
    // has nowhere else to be read (this inbox has no UI) — stash it so it can be read back out via
    // the KV, instead of silently dropping something the owner is waiting on.
    await env.SHARES.put("__mailPending", JSON.stringify({
      from: parsed.from?.address || "", subject: parsed.subject || "",
      text: (parsed.text || "").slice(0, 4000), at: Date.now(),
    }))
    // Say so instead of going quiet: a forward that lost its attachments looks exactly like nothing
    // happening, and the only way to tell used to be asking the KV.
    await sendPush(env, "📭 Mail fără PDF",
                   `"${(parsed.subject || "fără subiect").slice(0, 60)}" a ajuns, dar n-avea niciun atașament.`,
                   undefined, "rent")
    return
  }

  const inbox = JSON.parse((await env.SHARES.get("__rentInbox")) || "[]")
  const stamp = new Date().toISOString().slice(0, 10)
  for (const [i, a] of pdfs.entries()) {
    const safe = (a.filename || `factura-${i + 1}.pdf`).replace(/[^\w.\-]+/g, "_")
    const key = `rent/${stamp}-${Date.now()}-${i}-${safe}`
    await env.BUCKET.put(key, a.content, { httpMetadata: { contentType: "application/pdf" } })
    inbox.push({ key, name: safe, from: parsed.from?.address || "", subject: parsed.subject || "", at: Date.now() })
  }
  while (inbox.length > 60) inbox.shift()          // the app consumes these; keep the tail short
  await env.SHARES.put("__rentInbox", JSON.stringify(inbox))
  await sendPush(env, "📄 Facturi noi", `${pdfs.length} PDF de la ${parsed.from?.address || "administrator"}`, undefined, "rent")
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







async function recordEvent(env, id, kind, title, artist, throttle = true) {
  try {
    await env.DB.prepare("CREATE TABLE IF NOT EXISTS share_events (share_id TEXT, title TEXT, artist TEXT, kind TEXT, created INTEGER)").run()
    const t = String(title || "").slice(0, 200)
    const who_ = String(artist || "").slice(0, 200)
    const now = Date.now()

    // One human action reaches this more than once: the share page beats every 18s while a song plays,
    // and a download arrives as the browser's speculative GET, the real GET, and any resume-from-byte-0
    // (isFullGet counts all three). So play and download each get two windows — a short one deciding
    // whether this is even a new EVENT, and a longer one deciding whether it deserves a NOTIFICATION.
    //
    // The event window lives in the INSERT's WHERE NOT EXISTS, not in an if() after a SELECT, because
    // those repeat requests overlap in flight: a read-then-write guard lets both sides read "nothing
    // recorded yet" and both push. Whoever's INSERT reports changes=1 owns the event; the loser stops.
    //
    // Download is keyed on the file name as well — a folder share hands out genuinely different files
    // and each of those is worth its own notification. Play is keyed on the share alone, since all the
    // heartbeats of one listening session belong together.
    let last = null, pushWindow = 0, inserted = true
    if (kind === "play" || kind === "download") {
      const isDl = kind === "download"
      pushWindow = isDl ? 600000 : 120000
      const cutoff = now - (throttle ? (isDl ? 60000 : 30000) : 0)
      last = await env.DB.prepare(
        `SELECT created FROM share_events WHERE share_id=?1 AND kind=?2${isDl ? " AND title=?3" : ""} ORDER BY created DESC LIMIT 1`
      ).bind(...(isDl ? [id, kind, t] : [id, kind])).first()
      const res = await env.DB.prepare(
        `INSERT INTO share_events (share_id,title,artist,kind,created) SELECT ?1,?2,?3,?4,?5 ` +
        `WHERE NOT EXISTS (SELECT 1 FROM share_events WHERE share_id=?1 AND kind=?4${isDl ? " AND title=?2" : ""} AND created > ?6)`
      ).bind(id, t, who_, kind, now, cutoff).run()
      inserted = !!res.meta?.changes
    } else {
      await env.DB.prepare("INSERT INTO share_events (share_id,title,artist,kind,created) VALUES (?1,?2,?3,?4,?5)")
        .bind(id, t, who_, kind, now).run()
    }
    if (!inserted) return

    // The notification window: only the RISING edge is news. A listen that's been running for a while
    // shouldn't re-announce itself every 30s, and re-downloading the same file soon after is the same
    // person retrying. A fresh listen (2 min quiet) or a genuine re-download (10 min) notifies again.
    if (last && now - last.created < pushWindow) return

    // Real push (fires even with the phone app closed). Song + artist + the action itself, in English.
    // Play/download beacons don't always carry the artist (or the title, for single-song links) — read
    // the share meta once to fill the gaps so the notification never degrades to "a track".
    if (kind === "play") {
      let song = title, who = artist
      if (!song || !who) { const m = await meta_(env, id); if (m) { song = song || m.title; who = who || m.artist } }
      await sendPush(env, "🎧 Now playing", who ? `${song} — ${who}` : (song || "Unknown track"), "listen")
    } else if (kind === "download") {
      let song = title, who = artist
      if (!song || !who) { const m = await meta_(env, id); if (m) { song = song || m.title; who = who || m.artist } }
      await sendPush(env, "⬇️ Downloaded", who ? `${song} — ${who}` : (song || "A file"), "download")
    }
  } catch (e) { /* activity logging is best-effort */ }
}

// --- APNs: sign a fresh auth JWT (ES256, cached ~50 min per isolate — Apple allows reuse up to 1h)
// and push to the one registered device. Best-effort everywhere: a failed push never breaks the
// share flow it's piggybacking on.
let _apnsJWT = null
function b64u(bytes) {
  const bin = typeof bytes === "string" ? bytes : String.fromCharCode(...new Uint8Array(bytes))
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "")
}
async function apnsJWT(env) {
  const now = Math.floor(Date.now() / 1000)
  if (_apnsJWT && _apnsJWT.exp > now + 60) return _apnsJWT.token
  const signingInput = `${b64u(JSON.stringify({ alg: "ES256", kid: env.APNS_KEY_ID }))}.${b64u(JSON.stringify({ iss: env.APNS_TEAM_ID, iat: now }))}`
  const der = Uint8Array.from(atob(env.APNS_KEY.replace(/-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\s/g, "")), c => c.charCodeAt(0))
  const key = await crypto.subtle.importKey("pkcs8", der.buffer, { name: "ECDSA", namedCurve: "P-256" }, false, ["sign"])
  const sig = await crypto.subtle.sign({ name: "ECDSA", hash: "SHA-256" }, key, new TextEncoder().encode(signingInput))
  const token = `${signingInput}.${b64u(sig)}`
  _apnsJWT = { token, exp: now + 3000 }
  return token
}
// Which push KINDS the phone actually wants (Settings toggles synced from the app). Default true for
// both so a phone that never synced (older app build) keeps today's always-on behavior.
async function pushKindOn(env, kind) {
  const raw = await env.SHARES.get("__pushkinds")
  if (!raw) return true
  try { const k = JSON.parse(raw); return k[kind] !== false } catch (e) { return true }
}





async function sendPush(env, title, body, kind, tap) {
  try {
    if (kind && !(await pushKindOn(env, kind))) return   // this notification type is off on the phone
    const deviceToken = await env.SHARES.get("__pushtoken")
    if (!deviceToken || !env.APNS_KEY) return
    const jwt = await apnsJWT(env)
    // Try-both-APNs-environments-and-remember: a token belongs to ONE environment, and
    // whether the owner's phone runs a dev build (sandbox) or a TestFlight one (production) changes
    // under his feet. Pinning to APNS_ENV alone meant the day the install type changed, every own
    // notification died silently. Remembered winner in __pushtokenenv under "" (the owner slot).
    const envs = JSON.parse((await env.SHARES.get("__ptokenenv")) || "{}")
    const preferred = envs[""] || env.APNS_ENV || "sandbox"
    const order = preferred === "production" ? ["production", "sandbox"] : ["sandbox", "production"]
    for (const which of order) {
      const host = which === "production" ? "api.push.apple.com" : "api.sandbox.push.apple.com"
      const res = await fetch(`https://${host}/3/device/${deviceToken}`, {
        method: "POST",
        headers: {
          "authorization": `bearer ${jwt}`,
          "apns-topic": env.APNS_BUNDLE_ID,
          "apns-push-type": "alert",
          "apns-priority": "10",
        },
        // `tap` tells the app WHICH screen to open when the banner is tapped ("rent").
        // `content-available` wakes the app so it can fetch + parse BEFORE the banner is tapped —
        // by the time the owner opens it, the month is already on screen.
        body: JSON.stringify({ aps: { alert: { title, body: body.slice(0, 200) }, sound: "default", "content-available": 1 }, tap }),
      })
      if (res.ok) {
        if (envs[""] !== which) { envs[""] = which; await env.SHARES.put("__ptokenenv", JSON.stringify(envs)) }
        return
      }
      const text = await res.text()
      if (res.status === 400 && text.includes("BadDeviceToken")) continue   // wrong door — try the other
      await logErr(env, `apns ${res.status}: ${text}`)
      return
    }
    await logErr(env, `apns: token rejected by both environments`)
  } catch (e) { await logErr(env, `apns throw: ${(e && e.message) || e}`) }
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

// Resolves the bearer on /list, /file and /api/whoami to either the owner (full access, the app's
// own SHARE_TOKEN) or a scoped guest link (name + the artist folders they're allowed to see).
async function authFor(request, env) {
  const bearer = (request.headers.get("authorization") || "").replace(/^Bearer /, "")
  if (!bearer) return null
  if (bearer === env.SHARE_TOKEN) return { owner: true }
  if (bearer.length > 128) return null   // over KV's key limit → would throw (500) instead of 401
  const rec = JSON.parse((await env.SHARES.get("guest:" + bearer)) || "null")
  if (!rec) return null
  return { owner: false, name: rec.name, artists: rec.artists, calendar: !!rec.calendar }
}

async function handle(request, env, ctx) {
    const url = new URL(request.url)
    const p = url.pathname

    // Team members (label edition): a member token acts as the owner token for exactly the routes
    // its role allows — one choke point here, so no route below needs to know about members.
    // x-studio-actor carries who did it, for the activity log.
    const bearer = (request.headers.get("authorization") || "").replace(/^Bearer /, "")
    if (bearer && bearer !== env.SHARE_TOKEN && bearer.length <= 128) {
      const m = JSON.parse((await env.SHARES.get("member:" + bearer)) || "null")
      if (m) {
        if (p === "/api/whoami") return json({ type: "member", name: m.name, role: m.role })
        if (!memberMay(m.role, request.method, p)) return json({ error: "forbidden" }, 403)
        const h = new Headers(request.headers)
        h.set("authorization", `Bearer ${env.SHARE_TOKEN}`)
        h.set("x-studio-actor", m.name)
        request = new Request(request, { headers: h })
      }
    }

    // Listen Together: WebSocket room per share id. The app connects as HOST (?role=host&t=<token>,
    // validated); the share page connects as guest (public — knowing the unguessable id IS the
    // invite). The Durable Object relays host play/pause/seek to guests + presence both ways.
    if (p.startsWith("/ws/")) {
      const id = p.slice(4)
      if (!id || id.includes("/")) return new Response("bad id", { status: 400 })
      if (request.headers.get("upgrade") !== "websocket") return new Response("expected websocket", { status: 426 })
      const wsRole = url.searchParams.get("role")
      if ((wsRole === "host" || wsRole === "screen") && url.searchParams.get("t") !== env.SHARE_TOKEN)
        return new Response("unauthorized", { status: 401 })
      return env.LISTEN.get(env.LISTEN.idFromName(id)).fetch(request)
    }

    // --- RemoSession: live 1-to-1 studio session (host = the app, guest = a desktop browser).
    // Control + audio ride the SAME ListenRoom WS as Listen Together. Binary frame protocol
    // (relayed verbatim by the DO, never stored): 4-byte header [kind u8: 1=program stereo
    // host→guest, 2=guest mic mono guest→host, 3=host talkback mono host→guest | channel count u8 |
    // seq u16 LE, wraps] + Int16 LE interleaved PCM @ 48 kHz, exactly 960 frames per packet (20 ms)
    // → payload 3840 bytes stereo / 1920 mono. ---
    if (p === "/api/rs" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const b = await request.json().catch(() => null)
      if (!b) return json({ error: "bad body" }, 400)
      const id = randomId()
      await env.SHARES.put("rs:" + id, JSON.stringify({
        artist: String(b.artist || "").slice(0, 120), title: String(b.title || "").slice(0, 200), at: Date.now(),
      }), { expirationTtl: 86400 })
      return json({ id, url: `${url.origin}/rs/${id}` })
    }
    if (p === "/api/rs-cloud" && request.method === "GET") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const sess = new Map()
      let cur
      do {
        const r = await env.BUCKET.list({ prefix: "rs-takes/", cursor: cur, limit: 1000, include: ["customMetadata"] })
        cur = r.truncated ? r.cursor : undefined
        for (const o of r.objects) {
          const id = o.key.split("/")[1]
          if (!id) continue
          let s = sess.get(id)
          if (!s) { s = { id, artist: "", title: "", at: 0, takes: 0, bytes: 0 }; sess.set(id, s) }
          s.takes++; s.bytes += o.size || 0
          const up = o.uploaded ? new Date(o.uploaded).getTime() : 0
          if (up > s.at) s.at = up
          const md = o.customMetadata || {}
          if (!s.title && !s.artist) { s.artist = md.artist || ""; s.title = md.title || "" }
        }
      } while (cur)
      return json({ sessions: [...sess.values()].sort((a, b) => b.at - a.at) })
    }
    if (p.startsWith("/api/rs/") && request.method === "DELETE") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const id = p.slice("/api/rs/".length)
      if (!id || id.includes("/")) return json({ error: "bad id" }, 400)
      await env.SHARES.delete("rs:" + id)
      // The app downloads each take the instant it lands, so by now they are on disk. Purge the
      // R2 copies — a closed session must leave nothing behind (same rule as an ephemeral share).
      let cur
      do {
        const r = await env.BUCKET.list({ prefix: `rs-takes/${id}/`, cursor: cur, limit: 1000 })
        cur = r.truncated ? r.cursor : undefined
        for (const o of r.objects) { try { await env.BUCKET.delete(o.key) } catch (e) {} }
      } while (cur)
      return json({ ok: true })
    }
    // Takes: PUT is PUBLIC (the unguessable session id is the capability, same model as the guest
    // WS); GET is host-only (Bearer) — the app pulls each take down before ending the session.
    if (p.startsWith("/rs/") && p.includes("/take/")) {
      const seg = p.slice(4).split("/")
      const rid = seg[0], n = parseInt(seg[2] || "", 10)
      if (seg[1] !== "take" || seg.length !== 3 || !rid || !(n >= 1 && n <= 40)) return json({ error: "bad path" }, 400)
      const recRaw = await env.SHARES.get("rs:" + rid)
      if (!recRaw) return json({ error: "not found" }, 404)
      const key = `rs-takes/${rid}/${n}.wav`
      if (request.method === "PUT") {
        if (Number(request.headers.get("content-length") || 0) > 300 * 1024 * 1024) return json({ error: "too big" }, 413)
        const rec = JSON.parse(recRaw)
        await env.BUCKET.put(key, request.body, { httpMetadata: { contentType: "audio/wav" },
          customMetadata: { artist: rec.artist || "", title: rec.title || "" } })
        return json({ key })
      }
      if (request.method === "GET") {
        if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
        const resp = await streamObject(env, key, request, true, `take-${n}.wav`)
        const h = new Headers(resp.headers); h.set("Cache-Control", "private, no-store")
        return new Response(resp.body, { status: resp.status, headers: h })
      }
    }
    if (p.startsWith("/rs/") && request.method === "GET") {
      // no-store: browsers (iOS especially) kept serving STALE session pages through reloads,
      // running old protocol code. A session page must never be cached.
      const rid = p.slice(4)
      if (!rid || rid.includes("/")) return new Response("Not found", { status: 404 })
      const rec = JSON.parse((await env.SHARES.get("rs:" + rid)) || "null")
      if (!rec) return new Response("Not found", { status: 404 })
      return new Response(renderRemoSessionPage(rid, rec), { headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store, max-age=0" } })
    }

    // Public-page footer text ("by <name>") — configurable per deployment so anyone running their own
    // worker (own Cloudflare account, own token) can put their own name on their share pages instead of
    // the default. Stored once in KV; falls back to a neutral default if never set.
    if (p === "/api/brand" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const b = await request.json().catch(() => null)
      const text = (typeof b?.text === "string" ? b.text : "").trim().slice(0, 60)
      await env.SHARES.put("cfg:brandFooter", text || "Shared with Studio OS")
      // Optional per-deployment logo (a label's own mark) served at /logo in place of the built-in
      // one. Base64 PNG, ~256px from the app; "" clears it back to the default.
      if (typeof b?.logo === "string") {
        if (!b.logo) await env.SHARES.delete("cfg:brandLogo")
        else if (b.logo.length <= 400000 && /^[A-Za-z0-9+/=]+$/.test(b.logo)) await env.SHARES.put("cfg:brandLogo", b.logo)
        else return json({ error: "bad logo" }, 400)
      }
      return json({ ok: true })
    }

    if (p === "/api/share" && request.method === "POST") {
      const who = await authFor(request, env)
      if (!who) return json({ error: "unauthorized" }, 401)
      const b = await request.json().catch(() => null)
      if (!b) return json({ error: "bad body" }, 400)
      // A guest link only ever makes song shares (Play/Download Link from their own artists) — not
      // folder packages or pre-save pages, and only for a song under an artist they're scoped to.
      if (!who.owner) {
        if (b.type === "folder" || b.type === "presave") return json({ error: "forbidden" }, 403)
        if (!b.audioKey || !who.artists.includes(String(b.audioKey).split("/")[0])) return json({ error: "forbidden" }, 403)
      }
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
        let ogKey = ""
        if (b.ogData) {
          try {
            const bytes = Uint8Array.from(atob(b.ogData), (c) => c.charCodeAt(0))
            ogKey = `share-art/${fid}-og.jpg`
            await env.BUCKET.put(ogKey, bytes, { httpMetadata: { contentType: "image/jpeg" } })
          } catch (e) { ogKey = "" }
        }
        const rec = { type: "folder", title: b.title || "Folder", artworkKey, ogKey: ogKey || undefined }
        // Live folder: store the R2 prefix so the page can list current contents (new files show up).
        if (b.folderKey) {
          // Live folder prefix — must be a well-formed library path, never share machinery.
          const fk = safeKey(String(b.folderKey).replace(/\/+$/, ""))
          if (fk && !/^(share-|health|trash\/|rent\/|__)/.test(fk)) rec.folderKey = fk
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
      // Remote split-sheet signing: the same page the LAN QR flow serves, but reachable from
      // anywhere — for a contributor who didn't sign during the session. Signatures land in
      // "sg:<id>"; the Mac polls, ingests them, then deletes the whole thing (link dies).
      if (b.type === "signsheet") {
        if (!Array.isArray(b.entries) || !b.entries.length) return json({ error: "entries required" }, 400)
        const sid = randomId()
        await env.SHARES.put(sid, JSON.stringify({
          type: "signsheet",
          sheetKey: String(b.sheetKey || "").slice(0, 300),
          kind: b.kind === "master" ? "master" : "split",
          artist: String(b.artist || "").slice(0, 200),
          song: String(b.song || "").slice(0, 200),
          entries: b.entries.slice(0, 24).map((e) => ({
            name: String(e.name || "").slice(0, 120),
            role: String(e.role || "").slice(0, 60),
            pct: Math.max(0, Math.min(100, Number(e.pct) || 0)),
          })),
          // Names already signed IN THE APP (QR/LAN or an earlier session) — rendered as done, with
          // no Sign button, so nobody re-signs a row that's already settled.
          localSigned: (Array.isArray(b.localSigned) ? b.localSigned : []).slice(0, 24).map((n) => String(n).slice(0, 120)),
          createdAt: Date.now(),
        }))
        return json({ id: sid, url: `${url.origin}/s/${sid}` })
      }
      if (!b.audioKey) return json({ error: "audioKey required" }, 400)
      const id = randomId()
      // Only the owner may reference an existing R2 object as artwork — a guest could otherwise aim it at
      // any library file (outside its scope) and stream it via /img/<id>.
      let artworkKey = who.owner ? (b.artworkKey || "") : ""
      // Optional inline artwork (e.g. the app's artist avatar) — store it in R2 under the share.
      if (b.artworkData) {
        try {
          const bytes = Uint8Array.from(atob(b.artworkData), (c) => c.charCodeAt(0))
          artworkKey = `share-art/${id}.jpg`
          await env.BUCKET.put(artworkKey, bytes, { httpMetadata: { contentType: b.artworkContentType || "image/jpeg" } })
        } catch (e) { artworkKey = who.owner ? (b.artworkKey || "") : "" }
      }
      // Optional tiny og:image companion — makes messengers render the compact link card instead of
      // a big banner, WITHOUT shrinking the page's own artwork. Additive: old records have no ogKey
      // and keep serving /img/ exactly as before, so already-sent links are untouched.
      let ogKey = ""
      if (b.ogData) {
        try {
          const bytes = Uint8Array.from(atob(b.ogData), (c) => c.charCodeAt(0))
          ogKey = `share-art/${id}-og.jpg`
          await env.BUCKET.put(ogKey, bytes, { httpMetadata: { contentType: "image/jpeg" } })
        } catch (e) { ogKey = "" }
      }
      const normPeaks = (x) => Array.isArray(x) ? x.slice(0, 512).map((v) => Math.max(0, Math.min(1, Number(v) || 0))) : null
      await env.SHARES.put(id, JSON.stringify({
        audioKey: b.audioKey, artworkKey, ogKey: ogKey || undefined,
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
      await env.SHARES.delete("sg:" + id)   // collected signatures die with the sign link
      if (meta && Array.isArray(meta.versions)) {
        for (let i = 1; i < meta.versions.length; i++) await env.SHARES.delete("rn:" + id + ":" + i)
      }
      if (meta?.artworkKey?.startsWith("share-art/")) await env.BUCKET.delete(meta.artworkKey)   // don't strand the uploaded artwork in R2
      if (meta?.ogKey) await env.BUCKET.delete(meta.ogKey)
      return json({ ok: true })
    }









    // --- Studio Rent: the forwarded-invoice inbox + the parsed months. Owner only, all of it. ---
    if (p.startsWith("/api/rent")) {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)

      // What the mail handler dropped in and the app hasn't read yet.
      if (p === "/api/rent/inbox" && request.method === "GET") {
        return json({ items: JSON.parse((await env.SHARES.get("__rentInbox")) || "[]") })
      }
      // The PDF itself — the app parses the amounts locally and keeps its own copy.
      if (p === "/api/rent/file" && request.method === "GET") {
        const key = url.searchParams.get("key") || ""
        if (!key.startsWith("rent/")) return json({ error: "bad key" }, 400)
        const obj = await env.BUCKET.get(key)
        if (!obj) return json({ error: "not found" }, 404)
        return new Response(obj.body, { headers: { "content-type": "application/pdf" } })
      }
      // Consumed: drop it from the pending list (the R2 copy stays as the archive).
      if (p === "/api/rent/seen" && request.method === "POST") {
        const b = await request.json().catch(() => ({}))
        const done = new Set(Array.isArray(b.keys) ? b.keys : [])
        const left = JSON.parse((await env.SHARES.get("__rentInbox")) || "[]").filter(x => !done.has(x.key))
        await env.SHARES.put("__rentInbox", JSON.stringify(left))
        return json({ ok: true, left: left.length })
      }
      // The table itself, so it survives a reinstall and matches on every device.
      if (p === "/api/rent" && request.method === "GET") {
        return new Response((await env.SHARES.get("__rent")) || "[]", { headers: { "content-type": "application/json" } })
      }
      if (p === "/api/rent" && request.method === "PUT") {
        const raw = await request.text()
        if (raw.length > 512 * 1024) return json({ error: "too large" }, 413)
        try { JSON.parse(raw) } catch (e) { return json({ error: "bad json" }, 400) }
        await env.SHARES.put("__rent", raw)
        return json({ ok: true })
      }
      return json({ error: "not found" }, 404)
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

    // --- Mac <-> Mac state mirror (artist photos/framing, hive order, hidden, collections, a few
    // settings). One flat {field: string} map in KV under a reserved "__state" key (excluded from share
    // counts), versioned by `rev`: a PUT must carry the rev it read (`prev`) or it is refused with 409, so a
    // stale Mac can never silently overwrite a newer one. Photo bytes live in KV too, content-addressed
    // ("__sp:<hash>") — NOT in R2: the bucket is bisynced into every Mac's library folder, so any extra
    // prefix there would show up as a fake artist folder. Owner token only. ---
    if (p === "/api/state" || p.startsWith("/api/state/photo/")) {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const noStore = { "content-type": "application/json", "cache-control": "no-store" }
      if (p === "/api/state") {
        const cur = JSON.parse((await env.SHARES.get("__state")) || '{"rev":0,"fields":{}}')
        if (request.method === "GET") return new Response(JSON.stringify(cur), { headers: noStore })
        if (request.method === "PUT") {
          const raw = await request.text()
          if (raw.length > 2 * 1024 * 1024) return json({ error: "too large" }, 413)
          let b
          try { b = JSON.parse(raw) } catch (e) { return json({ error: "bad json" }, 400) }
          const f = b && b.fields
          if (!f || typeof f !== "object" || Array.isArray(f) || Object.values(f).some((v) => typeof v !== "string")) return json({ error: "bad fields" }, 400)
          if ((b.prev | 0) !== (cur.rev | 0)) return json({ error: "conflict", rev: cur.rev }, 409)
          const rev = (cur.rev | 0) + 1
          await env.SHARES.put("__state", JSON.stringify({ rev, by: String(b.by || "").slice(0, 80), at: Date.now(), fields: f }))
          return json({ ok: true, rev })
        }
        return new Response("method not allowed", { status: 405 })
      }
      const hash = p.slice("/api/state/photo/".length)
      if (!/^[0-9a-f]{12,64}$/.test(hash)) return new Response("bad hash", { status: 400 })
      if (request.method === "PUT") {
        const buf = await request.arrayBuffer()
        if (!buf.byteLength || buf.byteLength > 4 * 1024 * 1024) return json({ error: "bad size" }, 413)
        await env.SHARES.put("__sp:" + hash, buf)
        return json({ ok: true })
      }
      if (request.method === "GET") {
        const buf = await env.SHARES.get("__sp:" + hash, "arrayBuffer")
        if (!buf) return new Response("not found", { status: 404 })
        return new Response(buf, { headers: { "content-type": "application/octet-stream", "cache-control": "private, max-age=31536000, immutable" } })
      }
      return new Response("method not allowed", { status: 405 })
    }

    // --- Studio OS: Calendar release events mirror. Mac writes (PUT), iOS reads (GET). One JSON
    // array of events. Reserved "__calendar" key → excluded from share counts + mutation scans. ---
    // The SESSION calendar mirror — the one calendar the owner picks in the iPhone app, published for
    // guests to read. Deliberately a separate key from "__calendar" (the Mac's release calendar), or
    // the two would overwrite each other on every push.
    if (p === "/api/sessions") {
      const who = await authFor(request, env)
      if (!who) return json({ error: "unauthorized" }, 401)
      // A guest may READ it only when their link has calendar turned on (Mac → Persoane).
      if (!who.owner && (request.method !== "GET" || !who.calendar)) return json({ error: "forbidden" }, 403)
      const cache = caches.default
      const cacheKey = new Request(url.origin + "/__cache/sessions")
      if (request.method === "GET") {
        const hit = await cache.match(cacheKey)
        if (hit) return hit
        const body = (await env.SHARES.get("__sessions")) || "[]"
        const res = new Response(body, { headers: { "content-type": "application/json", "cache-control": "max-age=60" } })
        await cache.put(cacheKey, res.clone())
        return res
      }
      if (request.method === "PUT") {
        const raw = await request.text()
        if (raw.length > 8 * 1024 * 1024) return json({ error: "too large" }, 413)
        try { JSON.parse(raw) } catch (e) { return json({ error: "bad json" }, 400) }
        await env.SHARES.put("__sessions", raw)
        await cache.delete(cacheKey)
        return json({ ok: true })
      }
      return new Response("method not allowed", { status: 405 })
    }

    // --- APNs: iPhone registers its device token so the Worker can push REAL notifications (the
    // events below already happen server-side) even when the app is fully closed. Single-device —
    // this is the owner's own phone, not a multi-user product. Reserved "__pushtoken" key. ---
    if (p === "/api/push-token" && request.method === "POST") {
      // Owner-only: the single "__pushtoken" all share/listen/download alerts go to.
      const who = await authFor(request, env)
      if (!who) return json({ error: "unauthorized" }, 401)
      const { token } = await request.json().catch(() => ({}))
      if (!token || typeof token !== "string") return json({ error: "bad token" }, 400)
      if (who.owner) await env.SHARES.put("__pushtoken", token)
      return json({ ok: true })
    }
    // The phone's notification Settings toggles, synced here so a real APNs push (sent server-side,
    // fires even with the app closed) respects "don't notify me when someone listens" the same way
    // the app's own local checks do. Body: { listen: bool, download: bool }.
    if (p === "/api/push-kinds" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const b = await request.json().catch(() => ({}))
      await env.SHARES.put("__pushkinds", JSON.stringify({ listen: b.listen !== false, download: b.download !== false }))
      return json({ ok: true })
    }
    if (p === "/api/push-test" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      await sendPush(env, "Test — Push funcționează", "Notificare reală de la Cloudflare, chiar cu aplicația închisă.")
      return json({ ok: true })
    }
    // --- iOS companion: browse the R2 library (folders/files) + stream a file by key ---
    // Mirrors the same bucket the share links read. Bearer SHARE_TOKEN, same as /api/share.
    if (p === "/list" && request.method === "GET") {
      const who = await authFor(request, env)
      if (!who) return json({ error: "unauthorized" }, 401)
      let path = (url.searchParams.get("path") || "").replace(/^\/+|\/+$/g, "")
      if (path === "root") path = ""
      if (!who.owner) {
        const top = path.split("/")[0]
        if (top && !who.artists.includes(top)) return json({ error: "forbidden" }, 403)
      }
      const prefix = path ? path + "/" : ""
      const folders = [], files = []
      let cursor, guard = 0
      do {
        const r = await env.BUCKET.list({ prefix, delimiter: "/", cursor })
        for (const pre of (r.delimitedPrefixes || [])) folders.push({ name: pre.slice(prefix.length).replace(/\/$/, ""), type: "folder" })
        for (const o of (r.objects || [])) {
          if (o.key === prefix) continue                 // skip the folder placeholder object
          const name = o.key.slice(prefix.length)
          if (name && !name.includes("/")) files.push({ name, type: "file", size: o.size, at: new Date(o.uploaded).getTime() })
        }
        cursor = r.truncated ? r.cursor : undefined
      } while (cursor && ++guard < 200)   // cap pages so a pathological folder can't exhaust the R2 list quota
      // Guest at the bucket root: only their assigned artist folders, nothing else in the library.
      const scoped = (!who.owner && !path) ? folders.filter(f => who.artists.includes(f.name)) : [...folders, ...files]
      return json(scoped)
    }

    if (p === "/file" && request.method === "GET") {
      const who = await authFor(request, env)
      if (!who) return json({ error: "unauthorized" }, 401)
      const key = safeKey(url.searchParams.get("path"))
      if (!key) return new Response("bad path", { status: 400 })
      if (!who.owner && !who.artists.includes(key.split("/")[0])) return json({ error: "forbidden" }, 403)
      return streamObject(env, key, request, false)
    }

    // Who does this bearer belong to? Lets the app show owner vs. a scoped guest UI.
    if (p === "/api/whoami" && request.method === "GET") {
      const who = await authFor(request, env)
      if (!who) return json({ error: "unauthorized" }, 401)
      return json(who.owner ? { type: "owner" } : { type: "guest", name: who.name, artists: who.artists, calendar: who.calendar })
    }

    // Team members (label edition): one token per person, with a role (admin / member / viewer).
    // Managed by the owner token or an admin member (both arrive here as the owner token).
    if (p === "/api/members" || p.startsWith("/api/members/")) {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const token = p.slice("/api/members/".length)
      if (p === "/api/members" && request.method === "GET") {
        const out = []
        let cursor
        do {
          const r = await env.SHARES.list({ prefix: "member:", cursor })
          for (const k of r.keys) {
            const rec = JSON.parse((await env.SHARES.get(k.name)) || "null")
            if (rec) out.push({ token: k.name.slice("member:".length), name: rec.name, role: rec.role, createdAt: rec.createdAt })
          }
          cursor = r.list_complete ? undefined : r.cursor
        } while (cursor)
        return json(out)
      }
      if (p === "/api/members" && request.method === "POST") {
        const b = await request.json().catch(() => null)
        const name = String(b?.name || "").trim().slice(0, 60)
        if (!name || !MEMBER_ROLES.includes(b?.role)) return json({ error: "name and role (admin/member/viewer) required" }, 400)
        const t = crypto.randomUUID().replace(/-/g, "") + crypto.randomUUID().replace(/-/g, "")
        await env.SHARES.put("member:" + t, JSON.stringify({ name, role: b.role, createdAt: Date.now() }))
        return json({ token: t, name, role: b.role })
      }
      if (token && request.method === "PATCH") {
        const rec = JSON.parse((await env.SHARES.get("member:" + token)) || "null")
        if (!rec) return json({ error: "not found" }, 404)
        const b = await request.json().catch(() => null)
        if (b?.role !== undefined) {
          if (!MEMBER_ROLES.includes(b.role)) return json({ error: "bad role" }, 400)
          rec.role = b.role
        }
        if (typeof b?.name === "string" && b.name.trim()) rec.name = b.name.trim().slice(0, 60)
        await env.SHARES.put("member:" + token, JSON.stringify(rec))
        return json({ token, name: rec.name, role: rec.role })
      }
      if (token && request.method === "DELETE") {
        await env.SHARES.delete("member:" + token)
        return json({ ok: true })
      }
      return json({ error: "not found" }, 404)
    }

    // Guest-link management (owner only): create/list/revoke scoped iPhone links for other people.
    if (p === "/api/guests" && request.method === "GET") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const out = []
      let cursor
      do {
        const r = await env.SHARES.list({ prefix: "guest:", cursor })
        for (const k of r.keys) {
          const rec = JSON.parse((await env.SHARES.get(k.name)) || "null")
          if (rec) out.push({ token: k.name.slice("guest:".length), name: rec.name, artists: rec.artists, calendar: !!rec.calendar, createdAt: rec.createdAt })
        }
        cursor = r.list_complete ? undefined : r.cursor
      } while (cursor)
      return json(out)
    }

    if (p === "/api/guests" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const b = await request.json()
      const name = String(b.name || "").trim()
      const artists = Array.isArray(b.artists) ? b.artists.filter(a => typeof a === "string" && a) : []
      if (!name || !artists.length) return json({ error: "name and artists required" }, 400)
      const token = crypto.randomUUID().replace(/-/g, "")
      const calendar = !!b.calendar
      await env.SHARES.put("guest:" + token, JSON.stringify({ name, artists, calendar, createdAt: Date.now() }))
      return json({ token, name, artists, calendar })
    }

    if (p.startsWith("/api/guests/") && request.method === "DELETE") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const token = p.slice("/api/guests/".length)
      await env.SHARES.delete("guest:" + token)
      return json({ ok: true })
    }

    // Change who a link can already see — keeps the same token, so the person doesn't re-enter
    // anything: their app just sees a different set of artists next time it loads.
    if (p.startsWith("/api/guests/") && request.method === "PATCH") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const token = p.slice("/api/guests/".length)
      const rec = JSON.parse((await env.SHARES.get("guest:" + token)) || "null")
      if (!rec) return json({ error: "not found" }, 404)
      const b = await request.json()
      const artists = Array.isArray(b.artists) ? b.artists.filter(a => typeof a === "string" && a) : rec.artists
      if (!artists.length) return json({ error: "artists required" }, 400)
      rec.artists = artists
      if (typeof b.calendar === "boolean") rec.calendar = b.calendar
      await env.SHARES.put("guest:" + token, JSON.stringify(rec))
      return json({ token, name: rec.name, artists: rec.artists, calendar: !!rec.calendar })
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
        // Reserved prefixes are written only by the worker itself — a stray client PUT must not be
        // able to overwrite a trash/ backup (the copy-before-delete safety net) or share artwork.
        // `health/` is NOT here on purpose: that's the app's own read/write probe (verifyR2), and
        // /api/usage already excludes it from storage accounting — the client is meant to write it.
        if (/^(trash\/|share-art\/|rent\/)/.test(key)) return json({ error: "reserved prefix" }, 403)   // share-files/ stays: the iPhone's external share PUTs there
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
        const via = request.headers.get("x-studio-client") || "necunoscut"
        if (url.searchParams.get("folder") === "1") {
          const keys = await listAll(env, key.replace(/\/+$/, "") + "/")
          await trashKeys(env, keys, { op: "folder", path: key, via })
          for (const k of keys) await env.BUCKET.delete(k)
          ctx.waitUntil(sweepTrash(env))
          return json({ ok: true, deleted: keys.length })
        }
        await trashKeys(env, [key], { op: "file", path: key, via })
        await env.BUCKET.delete(key)
        ctx.waitUntil(sweepTrash(env))
        return json({ ok: true })
      }
      return new Response("method not allowed", { status: 405 })
    }

    // --- Chunked upload -------------------------------------------------------------------------
    // One big PUT is not always deliverable: a VPN/proxy network extension on the Mac (Tailscale's,
    // in the case this was written for) fails any URLSession body over ~2MB with EMSGSIZE, and the
    // platform caps a request body around 100MB regardless. So the app stages small chunks and the
    // Worker glues them together server-side, where neither limit applies.
    //
    // PUT  /object/chunk?path=<final key>&u=<upload id>&i=<index>              → stage one chunk
    // POST /object/assemble?path=<final key>&u=<upload id>&n=<count>&ct=<mime> → build the object
    if (p === "/object/chunk" || p === "/object/assemble") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const key = safeKey(url.searchParams.get("path"))
      const uid = (url.searchParams.get("u") || "").replace(/[^A-Za-z0-9-]/g, "")
      if (!key || !uid) return json({ error: "bad path or upload id" }, 400)
      if (/^(trash\/|share-art\/|rent\/)/.test(key)) return json({ error: "reserved prefix" }, 403)
      const chunkKey = (i) => `__up/${uid}/${String(i).padStart(5, "0")}`

      if (p === "/object/chunk" && request.method === "PUT") {
        const i = Number(url.searchParams.get("i"))
        if (!Number.isInteger(i) || i < 0 || i > 20000) return json({ error: "bad index" }, 400)
        await env.BUCKET.put(chunkKey(i), request.body)
        return json({ ok: true })
      }

      if (p === "/object/assemble" && request.method === "POST") {
        const n = Number(url.searchParams.get("n"))
        if (!Number.isInteger(n) || n < 1 || n > 20000) return json({ error: "bad count" }, 400)
        const ct = url.searchParams.get("ct") || "application/octet-stream"
        const mpu = await env.BUCKET.createMultipartUpload(key, { httpMetadata: { contentType: ct } })
        try {
          // R2 requires every part except the last to be >= 5 MiB, while the app's chunks are much
          // smaller (they have to be, to get past the proxy) — so coalesce chunks until a part is
          // big enough. Holds ~6MB at a time, well inside the Worker's memory budget.
          const MIN_PART = 5 * 1024 * 1024
          const parts = []
          let pending = [], pendingLen = 0, partNo = 1
          const flush = async () => {
            const merged = new Uint8Array(pendingLen)
            let off = 0
            for (const b of pending) { merged.set(b, off); off += b.length }
            parts.push(await mpu.uploadPart(partNo++, merged))
            pending = []; pendingLen = 0
          }
          for (let i = 0; i < n; i++) {
            const obj = await env.BUCKET.get(chunkKey(i))
            if (!obj) { await mpu.abort(); return json({ error: `missing chunk ${i}` }, 400) }
            const bytes = new Uint8Array(await obj.arrayBuffer())
            pending.push(bytes); pendingLen += bytes.length
            if (pendingLen >= MIN_PART) await flush()
          }
          if (pendingLen > 0) await flush()
          if (!parts.length) { await mpu.abort(); return json({ error: "no data" }, 400) }
          await mpu.complete(parts)
        } catch (e) {
          try { await mpu.abort() } catch (_) { /* nothing left to clean up */ }
          await logErr(env, `assemble ${key}: ${(e && e.message) || e}`)
          return json({ error: "assemble failed" }, 500)
        }
        // Staging objects are dead weight the moment the real object exists.
        ctx.waitUntil((async () => { for (let i = 0; i < n; i++) await env.BUCKET.delete(chunkKey(i)) })())
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

    // --- Recycle bin: what's in it, and putting something back ---
    // Empty the recycle bin NOW, in bounded batches — the app calls repeatedly until remaining=0.
    if (p === "/api/trash/clear" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      let deleted = 0, cursor, remaining = 0
      const r = await env.BUCKET.list({ prefix: "trash/", cursor, limit: 500 })
      for (const o of (r.objects || [])) {
        if (deleted >= 400) { remaining++; continue }
        await env.BUCKET.delete(o.key)
        deleted++
      }
      if (r.truncated) remaining += 500   // at least another page — the app keeps calling
      await journal(env, { at: Date.now(), why: { op: "clear", via: request.headers.get("x-studio-client") || "necunoscut" }, count: deleted, saved: 0, keys: [] })
      return json({ deleted, remaining })
    }
    if (p === "/api/trash" && request.method === "GET") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const items = []
      let cursor
      do {
        const r = await env.BUCKET.list({ prefix: "trash/", cursor, limit: 1000 })
        for (const o of (r.objects || [])) {
          const i = o.key.indexOf("/", 6)
          const stamp = parseInt(o.key.slice(6, i), 10)
          items.push({ key: o.key, original: o.key.slice(i + 1), at: stamp, size: o.size })
        }
        cursor = r.truncated ? r.cursor : undefined
      } while (cursor)
      items.sort((a, b) => b.at - a.at)
      return json({ days: TRASH_DAYS, items })
    }

    // Put it back where it came from. `prefix` restores a whole binned folder in one call.
    if (p === "/api/trash/restore" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const b = await request.json().catch(() => ({}))
      const wanted = String(b.key || b.prefix || "").replace(/^\/+/, "")
      if (!wanted.startsWith("trash/")) return json({ error: "bad key" }, 400)
      const keys = b.prefix ? await listAll(env, wanted.replace(/\/+$/, "") + "/") : [wanted]
      let back = 0
      for (const k of keys) {
        const i = k.indexOf("/", 6)
        const original = k.slice(i + 1)
        const obj = await env.BUCKET.get(k)
        if (!obj) continue
        await env.BUCKET.put(original, obj.body, { httpMetadata: obj.httpMetadata })
        await env.BUCKET.delete(k)
        back++
      }
      await journal(env, { at: Date.now(), why: { op: "restore", path: wanted, via: request.headers.get("x-studio-client") || "necunoscut" }, count: keys.length, saved: back, keys: keys.slice(0, 25) })
      return json({ ok: true, restored: back })
    }

    // The delete journal: what went, when, from which client.
    if (p === "/api/dellog" && request.method === "GET") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      return new Response((await env.SHARES.get("__dellog")) || "[]", { headers: { "content-type": "application/json" } })
    }

    if (p === "/delete" && request.method === "POST") {
      if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
      const b = await request.json().catch(() => null)
      if (!b) return json({ error: "bad request" }, 400)
      const key = String(b.key || "").replace(/^\/+|\/+$/g, "")
      if (!key) return json({ error: "key required" }, 400)
      const via = request.headers.get("x-studio-client") || "necunoscut"
      if (b.folder) {   // delete a whole folder = every object under the prefix
        const keys = await listAll(env, key + "/")
        await trashKeys(env, keys, { op: "folder", path: key, via })
        for (const k of keys) await env.BUCKET.delete(k)
        ctx.waitUntil(purgeMany(env, keys))
        ctx.waitUntil(sweepTrash(env))
        return json({ ok: true, deleted: keys.length })
      }
      await trashKeys(env, [key], { op: "file", path: key, via })
      await env.BUCKET.delete(key)
      ctx.waitUntil(purgeShares(env, key))   // purge shares AFTER responding → instant delete
      ctx.waitUntil(sweepTrash(env))
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
          // Count only real LIBRARY files (skip the app's own share-art / health-probe objects,
          // and __up/ staging chunks, which exist only between a chunk upload and its assemble).
          if (o.key.startsWith("share-art/") || o.key.startsWith("health/") || o.key.startsWith("__up/")) continue
          bytes += o.size || 0
          objects++
        }
        cursor = r.truncated ? r.cursor : undefined
      } while (cursor)
      let shares = 0, kc
      do {
        const r = await env.SHARES.list({ limit: 1000, cursor: kc })
        shares += (r.keys || []).filter((k) => !k.name.startsWith("__") && !k.name.startsWith("r:") && !k.name.startsWith("rn:") && !k.name.startsWith("rs:") && !k.name.startsWith("cfg:") && !k.name.startsWith("guest:") && !k.name.startsWith("member:")).length   // skip reserved + presave + review-notes + live-session keys
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
      const custom = await env.SHARES.get("cfg:brandLogo")
      const bytes = Uint8Array.from(atob(custom || LOGO_B64), (c) => c.charCodeAt(0))
      // A custom logo can change from Settings, so browsers re-check it sooner than the built-in one.
      return new Response(bytes, { headers: { "content-type": "image/png", "cache-control": `public, max-age=${custom ? 600 : 86400}` } })
    }
    if (p.startsWith("/s/")) {
      const meta = await meta_(env, p.slice(3))
      if (!meta) return new Response("Not found", { status: 404 })
      if (isExpired(meta)) return expiredPage()
      // (Dedup of duplicate / tombstoned versions happens at RENDER time, null-safe, keeping each
      // survivor's original index for /av + /rn. No persist/reindex here — that broke note mapping
      // and crashed on tombstoned (null) versions.)
      // Sign sheets need their signatures to know which rows are already done — fetched here rather
      // than in renderPage (which is sync). No play/open analytics for these: not a listening page.
      if (meta.type === "signsheet") {
        const sigs = JSON.parse((await env.SHARES.get("sg:" + p.slice(3))) || "{}")
        return html(renderSignSheetPage(p.slice(3), meta, sigs))
      }
      // Analytics AFTER the response (waitUntil) — the page's TTFB no longer waits on D1.
      ctx.waitUntil(recordEvent(env, p.slice(3), "open", meta.title, meta.artist))
      const brand = (await env.SHARES.get("cfg:brandFooter")) || "Shared with Studio OS"
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
      // Real shares only — otherwise any random id POSTed here writes D1 rows and pushes "Now playing".
      // (One KV read per 18s heartbeat; cheap next to the D1 work it gates.)
      if (!(await meta_(env, p.slice(3)))) return new Response(null, { status: 204 })
      // Optional ?t=<song> — the folder page sends the now-playing song name so /api/activity can show
      // WHICH song is playing (single-song pages omit it and fall back to the page-open title).
      await recordEvent(env, p.slice(3), "play", (url.searchParams.get("t") || "").slice(0, 200), "")
      return new Response(null, { status: 204 })
    }
    // Remote split-sheet signatures, stored per share under "sg:<id>" as { name: {at, png} }.
    // POST is PUBLIC (the link itself is the capability, same model as review notes); GET requires
    // the Bearer token because it returns the actual signature images for the Mac to ingest.
    if (p.startsWith("/sg/")) {
      const sid = p.slice(4)
      if (!sid || sid.includes("/")) return json({ error: "bad id" }, 400)
      const meta = await meta_(env, sid)
      if (!meta || meta.type !== "signsheet") return json({ error: "not a sign sheet" }, 404)
      const sigKey = "sg:" + sid

      if (request.method === "GET") {
        if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
        const sigs = JSON.parse((await env.SHARES.get(sigKey)) || "{}")
        return json({ song: meta.song, artist: meta.artist, kind: meta.kind, sheetKey: meta.sheetKey,
                      entries: meta.entries, signatures: sigs, localSigned: meta.localSigned || [] })
      }

      // The app reporting who has since signed IN THE APP, so the public page stops offering a Sign
      // button for them. Bearer-gated (only the owner's Mac), and written only when the set actually
      // changed — the app compares against the GET above first, so this is not a per-poll KV write.
      if (request.method === "PUT") {
        if ((request.headers.get("authorization") || "") !== `Bearer ${env.SHARE_TOKEN}`) return json({ error: "unauthorized" }, 401)
        const b = await request.json().catch(() => null)
        if (!Array.isArray(b?.localSigned)) return json({ error: "bad body" }, 400)
        meta.localSigned = b.localSigned.slice(0, 24).map((n) => String(n).slice(0, 120))
        await env.SHARES.put(sid, JSON.stringify(meta))
        return json({ ok: true })
      }

      if (request.method === "POST") {
        const b = await request.json().catch(() => null)
        const name = String(b?.name || "").trim()
        const png = String(b?.signature || "")
        if (!name || !png.startsWith("data:image/png;base64,")) return json({ error: "bad body" }, 400)
        if (png.length > 400000) return json({ error: "signature too large" }, 413)
        // Only a name that's actually on this sheet, and only once — a signed row can't be overwritten.
        if (!meta.entries.some((e) => sameName(e.name, name))) return json({ error: "unknown name" }, 404)
        const local = meta.localSigned || []
        if (local.some((k) => sameName(k, name))) return json({ error: "already signed" }, 409)   // signed in the app
        const sigs = JSON.parse((await env.SHARES.get(sigKey)) || "{}")
        if (Object.keys(sigs).some((k) => sameName(k, name))) return json({ error: "already signed" }, 409)
        sigs[name] = { at: Date.now(), png }
        await env.SHARES.put(sigKey, JSON.stringify(sigs))
        const done = (n) => Object.keys(sigs).some((k) => sameName(k, n)) || local.some((k) => sameName(k, n))
        const left = meta.entries.filter((e) => !done(e.name)).length
        await sendPush(env, "✍️ Split sheet signed",
                       left === 0 ? `${name} signed — ${meta.song} is complete` : `${name} signed ${meta.song} · ${left} left`,
                       "sign")
        return json({ ok: true, remaining: left })
      }
      return new Response("method not allowed", { status: 405 })
    }
    // Song Review notes: SoundCloud-style timestamped comments stored per share under "rn:<id>".
    // GET lists them (public — same origin as the page); POST appends one (public, link = capability).
    if (p.startsWith("/rn/")) {
      // /rn/<id> = version 1 (legacy key "rn:<id>", keeps old feedback). /rn/<id>/<v> = version v
      // (key "rn:<id>:<v>") so each version has its own comment thread.
      const seg = p.slice(4).split("/")
      const rid = seg[0]
      if (seg[1] !== undefined && !/^\d{1,3}$/.test(seg[1])) return json({ error: "bad version" }, 400)
      const key = "rn:" + rid + (seg[1] ? ":" + seg[1] : "")
      if (request.method === "GET") {
        const raw = await env.SHARES.get(key)
        return new Response(raw || "[]", { headers: { "content-type": "application/json", "cache-control": "no-cache" } })
      }
      if (request.method === "POST") {
        const meta = await meta_(env, rid)
        if (!meta || !meta.review) return json({ error: "not a review" }, 404)
        if (isExpired(meta)) return json({ error: "expired" }, 410)
        if (+(request.headers.get("content-length") || 0) > 8192) return json({ error: "too large" }, 413)
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
    if (p.startsWith("/ogi/")) {
      const meta = await meta_(env, p.slice(5))
      if (!meta?.ogKey) return new Response("", { status: 404 })
      if (isExpired(meta)) return new Response("Gone", { status: 410 })
      return streamObject(env, meta.ogKey, request, false)
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
      if (isExpired(meta)) return new Response("Expired", { status: 410 })
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
          "content-disposition": contentDisposition(zipName + ".zip"),
        },
      })
    }
    return new Response("Studio OS", { status: 200 })
}

// Share ids are randomId() or "r:"+randomId() — anything else (e.g. "__state", "guest:…") is not a share
// and must never be readable through the public routes.
async function meta_(env, id) {
  if (!/^(r:)?[A-Za-z0-9]{1,64}$/.test(id)) return null
  const r = await env.SHARES.get(id); return r ? JSON.parse(r) : null
}

// A share with an `exp` (epoch ms) in the past is dead — the page shows "expired", media returns 410.
function isExpired(m) { return !!(m && m.exp && Date.now() > m.exp) }
function expiredPage() {
  return html(`<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Link expired</title><style>body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:radial-gradient(90% 50% at 50% 10%,#1a1a1f,#070708);color:#e7e3da;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif;text-align:center}.b{opacity:.9;padding:24px}h1{font-size:21px;font-weight:600;margin:0 0 8px}p{color:#8a8a8a;font-size:13px;margin:0}</style></head><body><div class=b><h1>Link expired</h1><p>This share link is no longer available.</p></div></body></html>`)
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
const MEMBER_ROLES = ["admin", "member", "viewer"]
// What each team role may call (owner-token routes only; public pages need no token). Admin = all.
// Viewers read; members also upload, share and move/rename — deleting files and managing the team
// stay with admins.
const MEMBER_READ = [["GET", "/list"], ["GET", "/file"], ["GET", "/object"], ["HEAD", "/object"],
  ["GET", "/api/state"], ["GET", "/api/state/photo/*"], ["GET", "/api/sheets"], ["GET", "/api/usage"],
  ["GET", "/api/activity"]]
const MEMBER_EDIT = [["PUT", "/object"], ["PUT", "/object/chunk"], ["POST", "/object/assemble"],
  ["POST", "/api/share"], ["*", "/api/share/*"], ["POST", "/relink"], ["POST", "/move"], ["POST", "/rename"],
  ["*", "/api/sessions"], ["*", "/api/rs"], ["*", "/api/rs/*"]]
function memberMay(role, method, p) {
  if (role === "admin") return true
  const rules = role === "member" ? MEMBER_READ.concat(MEMBER_EDIT) : role === "viewer" ? MEMBER_READ : []
  return rules.some(([m, r]) => (m === "*" || m === method) && (r.endsWith("/*") ? p.startsWith(r.slice(0, -1)) : p === r))
}

function randomId(n = 16) { const a = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; const b = crypto.getRandomValues(new Uint8Array(n)); return Array.from(b, (x) => a[x % a.length]).join("") }

// Contributor-name match for signing: case- and diacritic-insensitive, whitespace-collapsed — the
// same folding SignServer.swift does on the LAN side, so "Ștefan  Popa" matches "stefan popa".
function sameName(a, b) {
  const f = (s) => String(s || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .toLowerCase().replace(/\s+/g, " ").trim()
  return f(a) === f(b)
}

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

// --- Recycle bin + delete journal -------------------------------------------------------------
// A folder deleted by accident used to be gone for good (no versioning on the bucket, no backup on
// the Mac). Deleting now COPIES to `trash/<epoch>/<original key>` first and only then removes the
// original, so anything can come back for 7 days. Copy-before-delete, never the other way round.
const TRASH_DAYS = 7

async function trashKeys(env, keys, why) {
  const stamp = Date.now()
  let saved = 0
  for (const k of keys) {
    if (k.startsWith("trash/")) continue                    // never bin the bin
    if (k.startsWith("__up/")) continue                     // staging chunks are scratch, not library
    try {
      const obj = await env.BUCKET.get(k)
      if (!obj) continue
      await env.BUCKET.put(`trash/${stamp}/${k}`, obj.body, { httpMetadata: obj.httpMetadata })
      saved++
    } catch (e) { await logErr(env, `trash put ${k}: ${(e && e.message) || e}`) }
  }
  await journal(env, { at: stamp, why, count: keys.length, saved, keys: keys.slice(0, 25) })
  return stamp
}

/// Who deleted what, and when. 200 entries kept — enough to answer "where did that song go".
async function journal(env, entry) {
  try {
    const log = JSON.parse((await env.SHARES.get("__dellog")) || "[]")
    log.push(entry)
    while (log.length > 200) log.shift()
    await env.SHARES.put("__dellog", JSON.stringify(log))
  } catch (e) { /* a missing journal entry must never block a delete */ }
}

/// Anything binned longer ago than TRASH_DAYS goes for real. Runs opportunistically after a delete,
/// so there's no cron to forget about.
async function sweepTrash(env) {
  try {
    const cutoff = Date.now() - TRASH_DAYS * 86400000
    let cursor, killed = 0
    do {
      const r = await env.BUCKET.list({ prefix: "trash/", cursor, limit: 1000 })
      for (const o of (r.objects || [])) {
        const stamp = parseInt(o.key.split("/")[1] || "0", 10)
        if (stamp && stamp < cutoff) { await env.BUCKET.delete(o.key); killed++ }
      }
      cursor = r.truncated ? r.cursor : undefined
    } while (cursor)
    // Stale chunked-upload staging: a successful assemble deletes its chunks immediately, so
    // anything under __up/ older than a day is an abandoned upload — clear it (bounded per pass;
    // this runs after every delete and hourly from cron, so leftovers drain quickly).
    const upCutoff = Date.now() - 86400000
    let upKilled = 0
    let upCursor
    do {
      const r = await env.BUCKET.list({ prefix: "__up/", cursor: upCursor, limit: 500 })
      for (const o of (r.objects || [])) {
        if (upKilled >= 300) break
        if (new Date(o.uploaded).getTime() < upCutoff) { await env.BUCKET.delete(o.key); upKilled++ }
      }
      upCursor = upKilled >= 300 ? undefined : (r.truncated ? r.cursor : undefined)
    } while (upCursor)
    return killed + upKilled
  } catch (e) { await logErr(env, `sweepTrash: ${(e && e.message) || e}`); return 0 }
}


// --- Cron jobs -------------------------------------------------------------------------------




const RO_TZ = "Europe/Bucharest"



/// Local hour in Romania right now (0–23).
function roHour(at) {
  return +new Intl.DateTimeFormat("en-US", { timeZone: RO_TZ, hour12: false, hour: "2-digit" })
    .format(new Date(at)) % 24
}


/// An invoice still unpaid a month after it arrived. Repeats every 3 days after that — a nudge, not
/// a nag. Only the owner gets these; the landlord's bills are nobody else's business.
async function remindUnpaidRent(env) {
  const months = JSON.parse((await env.SHARES.get("__rent")) || "[]")
  if (!Array.isArray(months) || !months.length) return
  const now = Date.now()
  const MONTH = 30 * 86400000
  let touched = false

  for (const m of months) {
    const rentDue = m.rentTotal && !(m.rentPaid ?? m.paid)
    const utilDue = m.utilTotal && !(m.utilPaid ?? m.paid)
    if (!rentDue && !utilDue) continue
    // A month from the MAIL, and only when that date is known. Rows imported before this existed
    // stay silent rather than being nagged about on a guessed date.
    const since = m.receivedAt
    if (!since || now - since < MONTH) continue
    if (m.remindedAt && now - m.remindedAt < 3 * 86400000) continue

    const days = Math.floor((now - since) / 86400000)
    const what = [rentDue ? "chiria" : null, utilDue ? "utilitățile" : null].filter(Boolean).join(" și ")
    await sendPush(env, "💸 Factură neplătită",
                   `${what} pe ${m.key} — ${days} zile de la primire`, undefined, "rent")
    m.remindedAt = now
    touched = true
  }
  if (touched) await env.SHARES.put("__rent", JSON.stringify(months))
}

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
      // Review versions too — same loop relinkShares has. Missing it meant a plain folder rename
      // 404'd every extra version on an already-sent Song Review link.
      if (Array.isArray(rec.versions)) for (const v of rec.versions) if (v && m.has(v.audioKey)) { v.audioKey = m.get(v.audioKey); changed = true }
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
      // Tombstone (don't splice) deleted review versions — index alignment with rn:<id>:<i> note
      // threads matters, and this is exactly what /api/share/<id>/version/delete does.
      if (Array.isArray(rec.versions)) for (let i = 0; i < rec.versions.length; i++) if (rec.versions[i] && set.has(rec.versions[i].audioKey)) { rec.versions[i] = null; changed = true }
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
      // Same version tombstoning as purgeMany — a deleted file must not leave a review version
      // pointing at a gone R2 object.
      if (Array.isArray(rec.versions)) for (let i = 0; i < rec.versions.length; i++) if (rec.versions[i] && rec.versions[i].audioKey === key) { rec.versions[i] = null; changed = true }
      if (Array.isArray(rec.files)) { const b = rec.files.length; rec.files = rec.files.filter((f) => f.key !== key); if (rec.files.length !== b) changed = true }
      if (Array.isArray(rec.items)) { const b = rec.items.length; rec.items = rec.items.filter((it) => it.key !== key); if (rec.items.length !== b) changed = true }
      if (changed) { await env.SHARES.put(k.name, JSON.stringify(rec)); n++ }
    }
  } while (cursor)
  return n
}
function json(o, s = 200) { return new Response(JSON.stringify(o), { status: s, headers: { "content-type": "application/json" } }) }
function html(s) { return new Response(s, { headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-cache" } }) }

function parseRange(h) { if (!h) return null; const m = /^bytes=(\d+)-(\d*)$/.exec(h); if (!m) return null; const o = +m[1]; if (m[2] && +m[2] < o) return null; return m[2] ? { offset: o, length: +m[2] - o + 1 } : { offset: o } }

// Header values must be Latin-1 — "Țară – demo.wav" made Headers.set throw (download = 500).
// ASCII fallback in filename=, the real name in RFC 5987 filename*=.
function contentDisposition(name) {
  const n = String(name), a = n.replace(/[^\x20-\x7e]|["\\]/g, "_")
  return `attachment; filename="${a}"; filename*=UTF-8''${encodeURIComponent(n)}`
}

async function streamObject(env, key, request, download, downloadName) {
  const range = parseRange(request.headers.get("range"))
  let object
  try { object = await env.BUCKET.get(key, range ? { range } : undefined) } catch (e) { return new Response(null, { status: 416 }) }
  if (!object || !object.body) return new Response("Not found", { status: 404 })
  const headers = new Headers(); object.writeHttpMetadata(headers)
  headers.set("x-content-type-options", "nosniff")
  headers.set("accept-ranges", "bytes"); headers.set("cache-control", "public, max-age=3600")
  if (download) headers.set("content-disposition", contentDisposition(downloadName || "audio"))
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
  }).replace(/</g, "\\u003c")
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
${m.artworkKey ? `<meta property="og:image" content="${origin}/${m.ogKey ? "ogi" : "img"}/${id}">` : ""}
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
  // Same listen beacon the single-song + folder pages send — without it a multi-version review
  // link showed 0 plays in Health and never fired "Now playing", even mid-session.
  au.addEventListener("play",function(){try{fetch("/p/"+C.id,{method:"POST",keepalive:true});}catch(e){}});
  setInterval(function(){ if(!au.paused&&!au.ended){ try{fetch("/p/"+C.id,{method:"POST",keepalive:true});}catch(e){} } },18000);
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
${m.artworkKey ? `<meta property="og:image" content="${origin}/${m.ogKey ? "ogi" : "img"}/${id}">` : ""}
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
  .cp.ok{color:#5cc78c;border-color:rgba(92,199,140,.4)}
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
// Remote split-sheet signing page — the cloud twin of the app's LAN QR page (SignPage.swift), for a
// contributor who wasn't at the session. Same look, same per-row Sign button + canvas pad; the only
// differences are it POSTs to /sg/<id> (no local server) and it says "All Good" once everyone signs.
// NOTE: no backticks or ${ inside the embedded <script> — it lives in this template literal.
function renderSignSheetPage(id, m, sigs) {
  // Signed on the web here, OR already signed inside the app (QR/LAN) — either way the row is done.
  const signedNames = Object.keys(sigs || {})
  const localSigned = m.localSigned || []
  const isSigned = (n) => signedNames.some((k) => sameName(k, n)) || localSigned.some((k) => sameName(k, n))
  const rows = (m.entries || []).map((e) => ({ ...e, signed: isSigned(e.name) }))
  const signedCount = rows.filter((r) => r.signed).length
  const allDone = rows.length > 0 && signedCount === rows.length
  const pctDone = rows.length ? Math.round((signedCount / rows.length) * 100) : 0
  const docName = m.kind === "master" ? "MASTER POINTS" : "SPLIT SHEET"
  const initials = (n) => String(n || "").trim().split(/\s+/).slice(0, 2).map((w) => w[0] || "").join("").toUpperCase() || "—"
  const when = (n) => {
    const hit = signedNames.find((k) => sameName(k, n))
    const at = hit && sigs[hit] && sigs[hit].at
    return at ? new Date(at).toLocaleDateString("en-GB", { day: "numeric", month: "short" }) : ""
  }

  const body = rows.map((r) => {
    const status = r.signed
      ? '<div class=okpill>✓ Signed' + (when(r.name) ? "<i>" + esc(when(r.name)) + "</i>" : "") + "</div>"
      : '<button class=rowsign data-name="' + esc(r.name) + '">Sign</button>'
    return '<div class=row><div class=ava>' + esc(initials(r.name)) + "</div>" +
      '<div class=who><div class=nm>' + esc(r.name || "—") + "</div>" +
      '<div class=role>' + esc(r.role || "") + "</div></div>" +
      '<div class=pct>' + Math.round(r.pct) + "%</div>" + status + "</div>"
  }).join("")

  const footer = allDone
    ? '<div class=alldone>✓ All Good — everyone has signed</div>'
    : '<div class=hint>Tap <b>Sign</b> next to your name, draw your signature, then <b>Done</b>.</div>'

  return `<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>${esc(m.song || "")} — ${docName}</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
:root{--p:#e7e3da;--soft:#8d8a82}
html{background:#070707}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,system-ui,sans-serif;color:#fff;
  padding:34px 16px;display:flex;justify-content:center;min-height:100vh;background:#070707}
.bgfx{position:fixed;inset:0;z-index:0;overflow:hidden;background:radial-gradient(95% 50% at 50% 12%,#26262c 0%,#070707 60%)}
.bgfx canvas{position:absolute;inset:0;width:100%;height:100%;filter:blur(2px)}
.stage{position:relative;z-index:1;width:100%;max-width:440px}
.brand{text-align:center;font-size:9px;font-weight:600;letter-spacing:3px;color:rgba(231,227,218,.5);margin-bottom:16px}
.card{position:relative;overflow:hidden;background:rgba(231,227,218,.05);
  border:1px solid rgba(231,227,218,.14);border-radius:24px;padding:22px;
  backdrop-filter:blur(30px) saturate(180%);-webkit-backdrop-filter:blur(30px) saturate(180%);
  box-shadow:0 24px 60px rgba(0,0,0,.55),inset 0 1px 0 rgba(255,255,255,.14)}
.head{display:flex;align-items:center;gap:13px;margin-bottom:18px}
.seal{width:44px;height:44px;border-radius:13px;flex:none;background:#000;
  border:1px solid rgba(231,227,218,.22);display:flex;align-items:center;justify-content:center;
  box-shadow:0 6px 16px rgba(0,0,0,.5),inset 0 1px 0 rgba(255,255,255,.16)}
.seal img{display:block;border-radius:8px}
.titles{min-width:0;flex:1}
.song{font-size:20px;font-weight:600;letter-spacing:-.2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.artist{font-size:12px;color:var(--p);margin-top:2px;letter-spacing:.5px}
.doc{flex:none;font-size:9px;font-weight:600;letter-spacing:1.2px;color:var(--p);
  border:1px solid rgba(231,227,218,.28);border-radius:999px;padding:5px 10px;background:rgba(231,227,218,.08)}
.progress{display:flex;align-items:center;gap:10px;margin-bottom:16px}
.ptrack{flex:1;height:4px;border-radius:99px;background:rgba(231,227,218,.10);overflow:hidden}
.pfill{height:100%;border-radius:99px;background:var(--p);transition:width .4s ease}
.plabel{font-size:11px;font-weight:500;color:var(--soft);white-space:nowrap;font-variant-numeric:tabular-nums}
.list{border:1px solid rgba(231,227,218,.10);border-radius:18px;overflow:hidden}
.row{display:flex;align-items:center;gap:11px;padding:13px 14px;
  border-top:1px solid rgba(231,227,218,.07);background:rgba(231,227,218,.02)}
.row:first-child{border-top:0}
.ava{width:36px;height:36px;border-radius:50%;flex:none;display:flex;align-items:center;justify-content:center;
  font-size:12px;font-weight:600;color:var(--p);background:rgba(231,227,218,.10);border:1px solid rgba(231,227,218,.22)}
.who{flex:1;min-width:0}
.nm{font-size:15px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.role{font-size:11.5px;color:var(--soft);margin-top:1px}
.pct{font-size:14px;font-weight:600;font-variant-numeric:tabular-nums;color:var(--p);flex:none}
.rowsign{flex:none;color:var(--p);background:rgba(231,227,218,.12);border:1px solid rgba(231,227,218,.30);
  border-radius:999px;padding:9px 18px;font-size:13px;font-weight:600;cursor:pointer;
  backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);box-shadow:inset 0 1px 0 rgba(255,255,255,.2)}
.rowsign:active{transform:scale(.96)}
.okpill{flex:none;display:flex;flex-direction:column;align-items:flex-end;gap:1px;color:#5cc78c;font-size:12.5px;font-weight:500}
.okpill i{font-style:normal;font-size:10px;color:var(--soft);font-weight:400}
.hint{margin-top:14px;font-size:12px;color:var(--soft);line-height:1.5;text-align:center}
.alldone{margin-top:16px;color:#5cc78c;font-size:14px;font-weight:600;text-align:center}
.foot{text-align:center;font-size:10px;color:rgba(231,227,218,.4);margin-top:18px;letter-spacing:.5px}
#pad.hidden{display:none!important}
#pad{position:fixed;inset:0;z-index:5;background:rgba(7,7,7,.7);-webkit-backdrop-filter:blur(16px);backdrop-filter:blur(16px);
  display:flex;align-items:center;justify-content:center;padding:18px;animation:fade .18s ease}
@keyframes fade{from{opacity:0}to{opacity:1}}
.padbox{background:rgba(231,227,218,.06);border:1px solid rgba(231,227,218,.16);border-radius:24px;padding:20px;width:100%;max-width:380px;
  backdrop-filter:blur(30px) saturate(180%);-webkit-backdrop-filter:blur(30px) saturate(180%);
  box-shadow:0 30px 70px rgba(0,0,0,.6),inset 0 1px 0 rgba(255,255,255,.16);animation:pop .22s cubic-bezier(.2,1.2,.4,1)}
@keyframes pop{from{transform:scale(.92);opacity:0}to{transform:scale(1);opacity:1}}
.padhead{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:12px}
.padlabel{font-size:9px;font-weight:600;letter-spacing:1.5px;color:var(--soft)}
.padname{font-size:15px;font-weight:600;color:#fff}
#cv{width:100%;height:210px;background:#fff;border-radius:14px;touch-action:none;display:block;cursor:crosshair;box-shadow:inset 0 0 0 1px rgba(0,0,0,.12)}
.padhint{text-align:center;font-size:10.5px;color:var(--soft);margin-top:8px}
.padbtns{display:flex;gap:8px;margin-top:14px}
.padbtns button{flex:1;padding:12px;border-radius:999px;font-size:14px;font-weight:600;border:1px solid rgba(231,227,218,.22);cursor:pointer}
.padbtns button:active{transform:scale(.97)}
.ghost{background:rgba(231,227,218,.08);color:#fff}
.primary{background:rgba(231,227,218,.16);color:var(--p);box-shadow:inset 0 1px 0 rgba(255,255,255,.22)}
</style></head><body>
<div class=bgfx><canvas id=pw></canvas></div>
<div class=stage>
  <div class=brand>STUDIO OS</div>
  <div class=card>
    <div class=head>
      <div class=seal><img src="/logo" alt="" width="26" height="26"></div>
      <div class=titles>
        <div class=song>${esc(m.song || "")}</div>
        <div class=artist>${esc(m.artist || "")}</div>
      </div>
      <div class=doc>${docName}</div>
    </div>
    <div class=progress>
      <div class=ptrack><div class=pfill style="width:${pctDone}%"></div></div>
      <div class=plabel>${signedCount}/${rows.length} signed</div>
    </div>
    <div class=list>${body}</div>
    ${footer}
  </div>
  <div class=foot>Secure signing · this link expires once everyone has signed</div>
</div>
<div id=pad class=hidden><div class=padbox>
  <div class=padhead>
    <div class=padlabel>SIGNATURE</div>
    <div class=padname id=padname></div>
  </div>
  <canvas id=cv></canvas>
  <div class=padhint>Sign above with your finger</div>
  <div class=padbtns>
    <button id=clear class=ghost>Clear</button>
    <button id=cancel class=ghost>Cancel</button>
    <button id=done class=primary>Done</button>
  </div>
</div></div>
<script>
(function(){var cv=document.getElementById('pw');if(!cv)return;var ctx=cv.getContext('2d'),W=0,H=0,lastW=-1,DPR=Math.min(2,window.devicePixelRatio||1);
var phone=window.matchMedia&&window.matchMedia('(max-width:600px)').matches;
function size(){if(window.innerWidth===lastW)return;lastW=window.innerWidth;W=window.innerWidth;H=window.innerHeight;cv.width=W*DPR;cv.height=H*DPR;ctx.setTransform(DPR,0,0,DPR,0,0);}
size();window.addEventListener('resize',size);
var reduce=window.matchMedia&&window.matchMedia('(prefers-reduced-motion:reduce)').matches;
var COLS=phone?54:90,ROWS=phone?34:52,amp=phone?0.55:1,t=0,last=0;
function draw(){ctx.clearRect(0,0,W,H);for(var r=0;r<ROWS;r++){var dz=r/(ROWS-1),baseY=H*(0.30+0.78*dz),spread=W*(0.7+0.7*dz),rad=0.5+1.8*dz;for(var c=0;c<COLS;c++){var u=c/(COLS-1)-0.5;var h=Math.sin(u*6+t+r*0.33)*0.55+Math.sin(u*12-t*0.7+r*0.18)*0.3;var x=W*0.5+u*spread,y=baseY-h*36*(0.4+dz);if(y<-4||y>H+4)continue;var a=(0.07+0.32*dz)*(0.5+0.5*(h*0.5+0.5))*amp;ctx.fillStyle='rgba(235,231,222,'+a.toFixed(3)+')';ctx.beginPath();ctx.arc(x,y,rad,0,6.283);ctx.fill();}}}
function loop(ts){if(ts-last>33){last=ts;t+=0.02;draw();}requestAnimationFrame(loop);}
if(reduce){draw();}else{requestAnimationFrame(loop);}})();
const SID='${id}';
const pad=document.getElementById('pad'),cv=document.getElementById('cv'),x=cv.getContext('2d');
let draw=false,cur='',dirty=false;
function fit(){
  const r=cv.getBoundingClientRect();const dpr=window.devicePixelRatio||1;
  cv.width=Math.round(r.width*dpr);cv.height=Math.round(r.height*dpr);
  x.setTransform(dpr,0,0,dpr,0,0);
  x.fillStyle='#ffffff';x.fillRect(0,0,cv.width,cv.height);
  x.strokeStyle='#111111';x.fillStyle='#111111';x.lineWidth=2.8;x.lineCap='round';x.lineJoin='round';
}
function P(e){const r=cv.getBoundingClientRect();const t=e.touches?e.touches[0]:e;return [t.clientX-r.left,t.clientY-r.top];}
function dot(a,b){x.beginPath();x.arc(a,b,1.4,0,7);x.fill();}
cv.addEventListener('pointerdown',e=>{draw=true;dirty=true;const[a,b]=P(e);dot(a,b);x.beginPath();x.moveTo(a,b);e.preventDefault();});
cv.addEventListener('pointermove',e=>{if(!draw)return;const[a,b]=P(e);x.lineTo(a,b);x.stroke();e.preventDefault();});
window.addEventListener('pointerup',()=>draw=false);
window.addEventListener('pageshow',e=>{if(e.persisted)location.reload();});
document.querySelectorAll('.rowsign').forEach(btn=>{
  btn.addEventListener('click',()=>{
    cur=btn.dataset.name||'';
    if(!cur)return;
    document.getElementById('padname').textContent=cur;
    pad.classList.remove('hidden');fit();dirty=false;
  });
});
function wipe(){x.save();x.setTransform(1,0,0,1,0,0);
  x.fillStyle='#ffffff';x.fillRect(0,0,cv.width,cv.height);x.restore();
  x.fillStyle='#111111';x.strokeStyle='#111111';dirty=false;}
document.getElementById('clear').onclick=wipe;
document.getElementById('cancel').onclick=()=>pad.classList.add('hidden');
document.getElementById('done').onclick=async()=>{
  if(!cur){alert('Tap Sign next to your name first.');pad.classList.add('hidden');return;}
  if(!dirty){alert('Please draw your signature.');return;}
  const sig=cv.toDataURL('image/png');
  try{
    const r=await fetch('/sg/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({name:cur,signature:sig})});
    const j=await r.json();
    if(j.ok){location.reload();}else{alert('Could not save: '+(j.error||'not found'));}
  }catch(e){alert('Network error: '+e.message);}
};
</script>
</body></html>`
}

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
${m.artworkKey ? `<meta property="og:image" content="${origin}/${m.ogKey ? "ogi" : "img"}/${id}">` : ""}
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
    <a class="dl" id="svDL" download aria-label="Download"><svg viewBox="0 0 24 24" width="15" height="15" fill="currentColor"><path d="M12 16l-5-5h3V4h4v7h3l-5 5zm-7 2h14v2H5z"/></svg>Download</a>
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
        // Audio: tap the row to play, PLUS a download button (audio was play-only before).
        var d=el("div","frow open"); d.appendChild(ic_(C.icons.audio)); d.appendChild(nm_(it.name));
        var x=el("span","fr-ext"); x.textContent=(it.ext||"").toUpperCase(); d.appendChild(x);
        var p=el("span","fr-act"); p.innerHTML=C.icons.play; d.appendChild(p);
        var dl=el("a","fr-act"); dl.href=fileURL(it,true); dl.setAttribute("aria-label","Download"); dl.innerHTML=C.icons.dl;
        dl.addEventListener("click",function(e){ e.stopPropagation(); });   // download, don't open the song
        d.appendChild(dl);
        d.addEventListener("click",function(){ openSong(it); });
        item.appendChild(d);
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
    var dlA=document.getElementById("svDL"); if(dlA) dlA.href=fileURL(it,true);
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
// RemoSession guest page: the artist's side of a live 1-to-1 studio session.
// Desktop-only. Program audio + host talkback arrive as binary PCM frames over
// the ListenRoom WebSocket and play through an AudioWorklet jitter buffer; the
// guest mic streams back the same way (protocol documented at the /rs routes).
// Recording is HOST-driven: {type:"rec", on:true, take} starts a local raw
// capture, {on:false} stops it — the take is WAV-encoded (24-bit mono 48k) in
// the browser, PUT to /rs/<id>/take/<n>, then announced with {type:"take"}.
function renderRemoSessionPage(id, m) {
  const cfg = JSON.stringify({ id, title: m.title || "Session", artist: m.artist || "" }).replace(/</g, "\\u003c")
  return `<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#171717"><title>${esc(m.title || "Session")} — RemoSession</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  html{background:#0D0F14}
  body{min-height:100vh;color:#EFEDE7;font:15px/1.5 ui-rounded,"SF Pro Rounded",-apple-system,system-ui,sans-serif;
    background:#0D0F14;background-attachment:fixed}
  /* Aurora: three wide colour fields drifting behind the glass. Heavy blur + low alpha keep them a
     backdrop the eye never tracks. Motion off under prefers-reduced-motion. */
  .aur{position:fixed;border-radius:999px;pointer-events:none;filter:blur(70px);z-index:0}
  .aur.a{width:900px;height:900px;left:-220px;top:-320px;background:radial-gradient(circle,rgba(96,140,220,.42),transparent 68%);animation:dA 26s ease-in-out infinite alternate}
  .aur.b{width:820px;height:820px;right:-200px;top:120px;background:radial-gradient(circle,rgba(200,120,180,.30),transparent 68%);animation:dB 32s ease-in-out infinite alternate}
  .aur.c{width:760px;height:760px;left:22%;bottom:-360px;background:radial-gradient(circle,rgba(90,200,180,.28),transparent 68%);animation:dC 38s ease-in-out infinite alternate}
  @keyframes dA{to{transform:translate(90px,70px) scale(1.10)}}
  @keyframes dB{to{transform:translate(-80px,60px) scale(1.08)}}
  @keyframes dC{to{transform:translate(70px,-70px) scale(1.12)}}
  @media(prefers-reduced-motion:reduce){.aur{animation:none}}
  #join,#live,.top,.ctrl,.drawer,#devpop,#statepill,#endcard,#recframe,#small{position:relative;z-index:1}
  .soft{color:rgba(255,255,255,.7)}
  .hex{width:34px;height:39px;flex:none;display:block}
  .brand{display:flex;align-items:center;gap:10px;font-weight:700;font-size:15px}
  .pill{display:flex;align-items:center;gap:9px;padding:10px 18px;border-radius:999px;background:rgba(255,255,255,.09);
    border:1px solid rgba(255,255,255,.16);backdrop-filter:blur(24px) saturate(160%);-webkit-backdrop-filter:blur(24px) saturate(160%);font-size:13px}
  .dot{width:8px;height:8px;border-radius:999px;flex:none}
  .card{background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.16);border-radius:22px;padding:28px;max-width:420px;text-align:center;
    backdrop-filter:blur(24px);-webkit-backdrop-filter:blur(24px)}
  .lab{font-size:11px;color:rgba(255,255,255,.7);margin:16px 0 6px;letter-spacing:.8px;text-transform:uppercase;font-weight:600}
  input,select{width:100%;padding:11px 14px;border-radius:10px;background:rgba(255,255,255,.04);
    border:1px solid rgba(255,255,255,.10);color:#E7E4DB;font:inherit;font-size:14px;outline:none;appearance:none}
  input:focus,select:focus{border-color:rgba(255,255,255,.3)}
  .btn{width:100%;padding:14px;border:0;border-radius:999px;background:#E7E4DB;color:#171717;font:inherit;font-weight:700;font-size:15px;cursor:pointer}
  .btn:disabled{opacity:.4;cursor:default}
  .mini{padding:7px 14px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.08);color:#E7E4DB;font:inherit;font-size:12px;font-weight:600;cursor:pointer}
  .warn{margin-top:20px;padding:13px 16px;border-radius:16px;font-size:13px;color:#F5A85C;background:rgba(245,168,92,.08);border:1px solid rgba(245,168,92,.3)}
  .errcard{margin-top:14px;padding:13px 16px;border-radius:16px;font-size:13px;color:#EE736B;background:rgba(238,115,107,.08);border:1px solid rgba(238,115,107,.35)}
  .spin{width:12px;height:12px;flex:none;border:2px solid rgba(255,255,255,.2);border-top-color:#E7E4DB;border-radius:999px;animation:sp 1s linear infinite}
  @keyframes sp{to{transform:rotate(360deg)}}
  #small{display:none}
  /* Phones join too — the desktop layout adapts instead of gating. */
  /* -- join -- */
  #join{min-height:100vh;display:flex;align-items:center;justify-content:center;padding:40px;position:relative}
  #join .brand{position:fixed;top:22px;left:26px}
  .jcard{width:520px;max-width:92vw;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.16);border-radius:28px;padding:34px 36px;
    backdrop-filter:blur(28px) saturate(160%);-webkit-backdrop-filter:blur(28px) saturate(160%);box-shadow:0 24px 60px rgba(0,0,0,.45)}
  h1{font-size:24px;font-weight:700;letter-spacing:-.2px}
  .meter{display:flex;gap:3px;align-items:flex-end;height:28px;margin-top:6px}
  .meter i{width:6px;height:4px;border-radius:6px;background:#5CC78C;opacity:.25}
  /* -- live -- */
  #live{min-height:100vh;display:none}
  .top{position:fixed;top:20px;left:26px;right:26px;display:flex;justify-content:space-between;align-items:center;z-index:20}
  .stage{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;pointer-events:none;z-index:22}
  #stagecard{display:flex;flex-direction:column;align-items:center;gap:14px;padding:22px 26px;border-radius:24px;
    border:1px solid transparent;transform-origin:top left;
    transition:background .65s ease,border-color .65s ease}
  /* Screen share on: the screen takes the stage; the RS crest + name become a compact host
     identity card pinned to the corner by FLEX, not viewport math — the calc(50vw…) transform
     landed ON TOP of the screen tile on tablet-sized screens. JS FLIP animates the move. */
  body.sharing #stagecard{position:absolute;right:20px;bottom:92px;pointer-events:auto;cursor:grab;
    gap:8px;padding:12px 18px;transform-origin:bottom right;touch-action:none;
    background:rgba(13,15,20,.66);border-color:rgba(255,255,255,.16);
    backdrop-filter:blur(24px);-webkit-backdrop-filter:blur(24px)}
  body.sharing #stagecard:active{cursor:grabbing}
  #pipsz{display:none;position:absolute;right:2px;bottom:2px;width:18px;height:18px;cursor:nwse-resize;
    color:rgba(231,228,219,.5);touch-action:none}
  body.sharing #pipsz{display:block}
  body.sharing #stagecap{display:none}
  body.sharing .bighex{width:58px;height:67px;filter:drop-shadow(0 8px 18px rgba(0,0,0,.5))}
  body.sharing .hostname{font-size:15px;letter-spacing:0}
  body.sharing .bigmeter{width:120px;height:14px;gap:2px}
  .bighex{width:150px;height:173px;flex:none;display:block;filter:drop-shadow(0 24px 60px rgba(0,0,0,.55))}
  #hexpair{display:none;gap:12px}
  .hhalf{position:relative;overflow:hidden;background:#0c0d10;width:min(30vw,320px);aspect-ratio:16/9;
    border-radius:16px;box-shadow:0 12px 32px rgba(0,0,0,.45)}
  .hseam{display:none}
  .hhalf video,.hhalf img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;
    transform:scaleX(-1);user-select:none;-webkit-user-select:none}
  .hph{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;opacity:.5}
  .phex{width:64px;height:74px}
  .who{position:absolute;bottom:8px;width:100%;text-align:center;font-weight:700;font-size:14px;
    letter-spacing:.3px;text-shadow:0 2px 10px rgba(0,0,0,.85);pointer-events:none}
  body.sharing .hhalf{width:172px;border-radius:10px}
  body.sharing .bigmeter{display:none}
  body.sharing #hexpair{gap:8px}
  body.sharing .who{font-size:11px;bottom:5px}
  body.cam #camvid{display:block!important}
  body.cam.jpeg #camvid{display:none!important}
  body.cam.jpeg #camimg{display:block!important}
  body.cam .hhalf.L .hph{display:none}
  body.gcam #gcamvid{display:block!important}
  body.gcam .hhalf.R .hph{display:none}
  body.cam #hexpair,body.gcam #hexpair{display:flex}
  body.cam .bighex,body.gcam .bighex{display:none}
  body.cam #hostname,body.gcam #hostname{display:none}
  .hostname{font-size:34px;font-weight:700;letter-spacing:-.6px}
  .bigmeter{width:min(560px,55vw);height:34px;display:flex;gap:4px;align-items:center}
  .bigmeter i{flex:1;height:4px;border-radius:6px;background:#fff;opacity:.30}
  .ctrl{position:fixed;bottom:26px;left:50%;transform:translateX(-50%);gap:6px;padding:8px 10px;z-index:45;
    background:rgba(13,15,20,.55);border-color:rgba(255,255,255,.16);box-shadow:0 16px 44px rgba(0,0,0,.5)}
  .cbtn{width:42px;height:42px;flex:none;border-radius:999px;border:1px solid rgba(255,255,255,.16);background:rgba(255,255,255,.10);color:#EFEDE7;cursor:pointer;display:flex;align-items:center;justify-content:center}
  .cbtn:hover{background:rgba(255,255,255,.18)}
  .cbtn.off{background:#EE736B;color:#171717}
  #recpill{display:none;align-items:center;gap:8px;color:#EE736B;font-size:13px;font-weight:600;padding:0 12px;white-space:nowrap;font-variant-numeric:tabular-nums}
  #recframe{display:none;position:fixed;inset:0;border:3px solid #EE736B;pointer-events:none;z-index:60}
  #statepill{display:none;position:fixed;bottom:96px;left:50%;transform:translateX(-50%);z-index:30}
  #devpop{display:none;position:fixed;bottom:88px;left:50%;transform:translateX(-50%);width:300px;z-index:35;
    background:rgba(13,15,20,.80);border:1px solid rgba(255,255,255,.16);border-radius:16px;padding:6px 18px 18px;
    backdrop-filter:blur(28px);-webkit-backdrop-filter:blur(28px)}
  /* -- drawer -- */
  .drawer{position:fixed;top:0;right:0;bottom:0;width:392px;z-index:40;display:flex;flex-direction:column;
    background:rgba(13,15,20,.72);border-left:1px solid rgba(255,255,255,.16);
    backdrop-filter:blur(30px) saturate(160%);-webkit-backdrop-filter:blur(30px) saturate(160%);
    transform:translateX(100%);transition:transform .28s cubic-bezier(.2,.8,.2,1)}
  .drawer.open{transform:none}
  .tabs{display:flex;gap:6px;padding:18px 18px 10px}
  .tab{flex:1;padding:8px;border-radius:999px;border:1px solid transparent;color:rgba(255,255,255,.7);font-size:13px;font-weight:600;text-align:center}
  .tab.on{background:rgba(255,255,255,.08);color:#E7E4DB;border-color:rgba(255,255,255,.10)}
  .pane{flex:1;overflow-y:auto;padding:8px 18px;display:none;flex-direction:column;gap:10px}
  .pane.on{display:flex}
  .take{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.10);border-radius:16px;padding:12px 14px;font-size:13px}
  .take .st{display:flex;align-items:center;gap:8px;margin-top:4px;font-size:12px}
  #endcard{display:none;position:fixed;inset:0;z-index:70;background:rgba(0,0,0,.6);align-items:center;justify-content:center}
</style></head><body>
<div class="aur a"></div><div class="aur b"></div><div class="aur c"></div>
<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs>
<linearGradient id="rsB" x1=".2" y1="0" x2=".8" y2="1"><stop offset="0" stop-color="#2F2E2C"/><stop offset=".55" stop-color="#43474A"/><stop offset="1" stop-color="#3F4141"/></linearGradient>
<radialGradient id="rsS" cx=".24" cy=".3" r=".48"><stop offset="0" stop-color="#D8BD93"/><stop offset=".55" stop-color="#C8AE88" stop-opacity=".75"/><stop offset="1" stop-color="#C8AE88" stop-opacity="0"/></radialGradient>
<radialGradient id="rsL" cx=".95" cy=".44" r=".46"><stop offset="0" stop-color="#8497AE"/><stop offset=".6" stop-color="#7F93AB" stop-opacity=".7"/><stop offset="1" stop-color="#7F93AB" stop-opacity="0"/></radialGradient>
<radialGradient id="rsG" cx=".48" cy=".68" r=".33"><stop offset="0" stop-color="#D0D0C8"/><stop offset=".6" stop-color="#C8C8C0" stop-opacity=".8"/><stop offset="1" stop-color="#C8C8C0" stop-opacity="0"/></radialGradient>
<clipPath id="rsC"><path d="M50 0 100 28.87 100 86.6 50 115.47 0 86.6 0 28.87Z"/></clipPath>
</defs><symbol id="rsmark" viewBox="0 0 100 115.47"><g clip-path="url(#rsC)">
<rect width="100" height="115.47" fill="url(#rsB)"/><rect width="100" height="115.47" fill="url(#rsS)"/>
<rect width="100" height="115.47" fill="url(#rsL)"/><rect width="100" height="115.47" fill="url(#rsG)"/></g>
<text x="50" y="70" text-anchor="middle" font-family="ui-rounded,'SF Pro Rounded',system-ui,sans-serif" font-size="32" font-weight="800" fill="#171717" letter-spacing="-1">RS</text></symbol></svg>
<script type="application/json" id="cfg">${cfg}</script>
<script type="audio/worklet" id="wk">
class RSPlay extends AudioWorkletProcessor{
  // FLAT TYPED RINGS, zero per-sample objects: the old queue-of-arrays version starved the
  // render deadline on older tablets (A10-era) - constant digital crackle. Everything here is
  // preallocated; process() only does indexed reads.
  constructor(){super();var s=this;
    this.r1=new Float32Array(262144);this.m1=262143;this.w1=0;this.rd1=0;   // program, stereo interleaved (~2.7 s)
    this.r3=new Float32Array(65536);this.m3=65535;this.w3=0;this.rd3=0;     // talkback, mono
    this.on1=false;this.warm1=false;this.on3=false;this.warm3=false;this.frz=false;
    this.tgt=7200;this.wmin=1e9;this.wc=0;this.c=0;this.cap=28800;
    this.pk=[];   // [absFrameStart, seq] per program packet - tiny, 50/s
    s.floor=7200;
    var h=function(e){var p=e.data;
      if(p&&p.port){p.port.onmessage=h;return;}
      if(p&&p.floor){s.floor=p.floor;s.tgt=Math.max(s.tgt,p.floor);return;}
      if(p&&p.freeze!==undefined){s.frz=!!p.freeze;return;}
      if(p.k===1){
        var d=p.d,n=d.length;
        if(s.w1+n-s.rd1>s.r1.length){s.rd1=s.w1+n-s.r1.length;}   // overrun: overwrite oldest
        var off=s.w1&s.m1,first=Math.min(n,s.r1.length-off);
        s.r1.set(d.subarray(0,first),off);
        if(n>first)s.r1.set(d.subarray(first),0);
        s.pk.push(Math.floor(s.w1/2),p.s);if(s.pk.length>400)s.pk.splice(0,2);
        s.w1+=n;
        // LATENCY-BOUNDED: over target+100 ms drop one packet-worth; way over (+300 ms) snap to target.
        var n1=(s.w1-s.rd1)/2;
        if(!s.frz){   // never re-time the ear mid-take: the stamp was taken once
          if(n1>s.tgt+4800)s.rd1+=1920;
          if(n1>s.tgt+14400)s.rd1=s.w1-s.tgt*2;
        }
      }else if(p.k===3){
        var d3=p.d,n3=d3.length;
        if(s.w3+n3-s.rd3>s.r3.length){s.rd3=s.w3+n3-s.r3.length;}
        var off3=s.w3&s.m3,f3=Math.min(n3,s.r3.length-off3);
        s.r3.set(d3.subarray(0,f3),off3);
        if(n3>f3)s.r3.set(d3.subarray(f3),0);
        s.w3+=n3;
        if((s.w3-s.rd3)>28800)s.rd3=s.w3-21600;
      }};
    this.port.onmessage=h;}
  process(inp,out){
    var L=out[0][0],R=out[0][1]||L,N=L.length,i;
    var n1=(this.w1-this.rd1)/2,n3=this.w3-this.rd3;
    this.wmin=Math.min(this.wmin,n1);
    if(++this.wc>=3750){
      if(!this.frz&&this.wmin>this.tgt*0.6)this.tgt=Math.max(this.tgt-1200,this.floor);
      this.wc=0;this.wmin=1e9;
    }
    if(!this.on1&&n1>=(this.warm1?Math.max(2400,this.tgt/3):this.tgt)){this.on1=true;this.warm1=true;}
    if(!this.on3&&n3>=(this.warm3?4800:14400)){this.on3=true;this.warm3=true;}
    var r1=this.r1,m1=this.m1,r3=this.r3,m3=this.m3,rd1=this.rd1,rd3=this.rd3;
    var use1=this.on1,use3=this.on3;
    for(i=0;i<N;i++){
      var l=0,r=0;
      if(use1){
        if(rd1<this.w1){l=r1[rd1&m1];r=r1[(rd1+1)&m1];rd1+=2;}
        else{use1=false;this.on1=false;this.ur=(this.ur||0)+1;this.tgt=Math.min(this.tgt+2400,28800);}
      }
      if(use3){
        if(rd3<this.w3){var tk=r3[rd3&m3]*0.9;l=l*0.7+tk;r=r*0.7+tk;rd3++;}
        else{use3=false;this.on3=false;this.u3=(this.u3||0)+1;}
      }
      L[i]=l;if(R!==L)R[i]=r;
    }
    this.rd1=rd1;this.rd3=rd3;
    while(this.pk.length>2&&this.pk[2]<=Math.floor(rd1/2))this.pk.splice(0,2);
    if(++this.c>=94){this.c=0;
      var ps=this.pk.length?this.pk[1]:-1;
      var pf=this.pk.length?Math.round((Math.floor(rd1/2)-this.pk[0])/48):0;
      this.port.postMessage({buf:Math.round(((this.w1-this.rd1)/2)/48),ps:ps,pf:pf,ct:currentTime,ur:this.ur||0,u3:this.u3||0});
    }
    return true;
  }
}
registerProcessor("rs-play",RSPlay);
class RSCap extends AudioWorkletProcessor{
  constructor(){super();var s=this;this.acc=new Float32Array(960);this.ai=0;this.p=0;this.prev=0;this.r=sampleRate/48000;this.net=null;
    this.port.onmessage=function(e){if(e.data&&e.data.port)s.net=e.data.port;};}
  push(v){this.acc[this.ai++]=v;
    if(this.ai===960){var o=this.acc;this.acc=new Float32Array(960);this.ai=0;
      // Straight to the network Worker (jank-proof), plus a copy to the page (meter + local rec).
      if(this.net){var c=new Float32Array(o);this.net.postMessage(o,[o.buffer]);this.port.postMessage({d:c,ct:currentTime},[c.buffer]);}
      else this.port.postMessage({d:o,ct:currentTime},[o.buffer]);}}
  process(inp){var ch=inp[0]&&inp[0][0];if(!ch)return true;var i;
    if(sampleRate===48000){for(i=0;i<ch.length;i++)this.push(ch[i]);}
    else{for(i=0;i<ch.length;i++){var cur=ch[i];   // linear resample to 48k
      while(this.p<1){this.push(this.prev+(cur-this.prev)*this.p);this.p+=this.r;}
      this.p-=1;this.prev=cur;}}
    return true;}
}
registerProcessor("rs-cap",RSCap);
class RSCal extends AudioWorkletProcessor{
  // Latency self-calibration: the page schedules a beep at t0 on THIS context's clock; the first
  // mic sample that crosses the threshold gives the true output->acoustic->input loop time —
  // exactly the hardware latency Safari never reports through any API.
  constructor(){super();var s=this;this.t0=-1;this.base=0;this.done=false;
    this.port.onmessage=function(e){s.t0=e.data.t0;s.base=0;s.done=false;};}
  process(inp){var ch=inp[0]&&inp[0][0];if(!ch)return true;
    if(this.t0<0||this.done)return true;
    var sum=0,i;for(i=0;i<ch.length;i++)sum+=ch[i]*ch[i];
    var rms=Math.sqrt(sum/ch.length);
    if(currentTime<this.t0-0.02){this.base=this.base*0.9+rms*0.1;return true;}
    if(rms>Math.max(0.01,this.base*8)){
      var th=Math.max(0.005,this.base*4),k=0;
      while(k<ch.length&&Math.abs(ch[k])<th)k++;
      this.done=true;
      this.port.postMessage({loop:(currentTime+k/sampleRate-this.t0)*1000});
    }
    return true;}
}
registerProcessor("rs-cal",RSCal);
</script>
<script type="text/plain" id="nwk">
// Network Worker: owns the WebSocket so audio never touches the page's main thread.
// Incoming binary goes STRAIGHT to the play worklet's port; mic blocks come STRAIGHT from
// the capture worklet's port. The page only ever sees JSON (chat/notes/state).
var ws=null,url="",play=null,cap=null,muted=false,seq=0,ended=false;
onmessage=function(e){var m=e.data;
  if(m.t==="init"){url=m.url;play=m.play;cap=m.cap;cap.onmessage=function(ev){onMic(ev.data);};connect();}
  else if(m.t==="send"){try{if(ws&&ws.readyState===1)ws.send(m.s);}catch(x){}}
  else if(m.t==="mute"){muted=m.v;}
  else if(m.t==="end"){ended=true;try{if(ws)ws.close();}catch(x){}}
};
function connect(){
  if(ended)return;
  ws=new WebSocket(url);
  ws.binaryType="arraybuffer";
  ws.onopen=function(){postMessage({t:"state",s:""});};
  ws.onmessage=function(ev){
    if(typeof ev.data==="string"){postMessage({t:"json",s:ev.data});return;}
    var dv=new DataView(ev.data),kind=dv.getUint8(0);
    if(kind===4){postMessage({t:"frame",d:ev.data},[ev.data]);return;}
    if(kind!==1&&kind!==3)return;
    var n=(ev.data.byteLength-4)/2,mono1=kind===1&&dv.getUint8(1)===1,f=new Float32Array(mono1?n*2:n);
    if(mono1){for(var i=0;i<n;i++){var v=dv.getInt16(4+i*2,true)/32768;f[i*2]=v;f[i*2+1]=v;}}
    else for(var i=0;i<n;i++)f[i]=dv.getInt16(4+i*2,true)/32768;
    play.postMessage({k:kind,d:f,s:dv.getUint16(2,true)},[f.buffer]);
  };
  ws.onclose=function(){if(ended)return;postMessage({t:"state",s:"re"});setTimeout(connect,3000);};
}
function onMic(f){
  if(muted||!ws||ws.readyState!==1||ws.bufferedAmount>49152)return;
  var buf=new ArrayBuffer(4+1920),dv=new DataView(buf);
  dv.setUint8(0,2);dv.setUint8(1,1);dv.setUint16(2,seq&0xFFFF,true);seq++;
  for(var i=0;i<960;i++){var v=Math.max(-1,Math.min(1,f[i]));dv.setInt16(4+i*2,v<0?v*32768:v*32767,true);}
  try{ws.send(buf);}catch(x){}
}
</script>

<div id="small"><div class="card">
  <svg class="hex" style="margin:0 auto 16px"><use href="#rsmark"/></svg>
  <b>RemoSession works on a desktop browser</b>
  <p class="soft" style="margin-top:8px;font-size:13px">Open this link on your computer.</p>
</div></div>

<div id="join">
  <div class="brand"><svg class="hex"><use href="#rsmark"/></svg><span>RemoSession</span></div>
  <div class="jcard">
    <h1>${esc(m.title || "Session")}</h1>
    <p class="soft" style="margin-top:4px">The host is waiting for you${m.artist ? " · " + esc(m.artist) : ""}</p>
    <div class="lab">Step 1 \u2014 Microphone</div>
    <select id="mic"><option value="">Default microphone</option></select>
    <p class="soft" style="margin-top:6px;font-size:12px">Your browser asks to allow the microphone when you press Join.</p>
    <div id="outrow" style="display:none"><div class="lab">Output</div><select id="out"></select></div>
    <div style="display:none"><div class="meter" id="testmeter"></div><button id="micbtn"></button></div>
    <div id="blocked" class="errcard" style="display:none"><b>Microphone blocked</b> — click the padlock in the address bar.
      <button id="askagain" class="mini" style="margin-left:8px">Ask again</button></div>
    <div class="lab">Step 2 \u2014 Your name</div><input id="name" placeholder="Your name" maxlength="40" autocomplete="name">
    <div class="warn">Wear headphones during the session \u2014 without them the mix bleeds into your mic and everyone hears an echo.<br>
    When you join you'll hear a short <b>sync beep</b>: keep the headphones OFF your ears for that moment, then put them on.</div>
  <p class="soft" style="margin-top:10px;font-size:12px">No mic? You can still join \u2014 listening only.</p>
    <button id="joinbtn" class="btn" style="margin-top:24px" disabled>Join session</button>
  </div>
</div>

<div id="live">
  <div class="top">
    <div style="display:flex;gap:14px;align-items:center">
      <div class="brand"><svg class="hex"><use href="#rsmark"/></svg><span>RemoSession</span></div>
      <div class="pill"><span class="dot" style="background:#EE736B"></span><b>LIVE</b>
        <span>${esc(m.title || "Session")}</span>
        <span id="elapsed" class="soft" style="font-variant-numeric:tabular-nums">00:00</span></div>
    </div>
    <div class="pill"><span class="dot" style="background:#5CC78C"></span>Lossless 24-bit
      <span id="lat" class="soft" style="font-variant-numeric:tabular-nums">—</span></div>
  </div>
  <div class="stage">
    <div id="stagecard">
      <svg class="bighex"><use href="#rsmark"/></svg>
      <div id="hexpair">
        <div class="hhalf L">
          <img id="camimg" alt="" draggable="false" style="display:none">
          <video id="camvid" autoplay playsinline muted style="display:none"></video>
          <div class="hph"><svg class="phex"><use href="#rsmark"/></svg></div>
          <div class="who" id="hostwho">Host</div>
        </div>
        <div class="hseam"></div>
        <div class="hhalf R">
          <video id="gcamvid" autoplay playsinline muted style="display:none"></video>
          <div class="hph"><svg class="phex"><use href="#rsmark"/></svg></div>
          <div class="who" id="guestwho">You</div>
        </div>
      </div>
      <div class="hostname" id="hostname">Host</div>
      <div class="bigmeter" id="bigmeter"></div>
      <div class="cap soft" id="stagecap" style="font-size:13px">Live from the studio</div>
      <svg id="pipsz" viewBox="0 0 18 18"><path d="M16 6v10H6M16 11v5h-5" stroke="currentColor" stroke-width="1.5" fill="none"/></svg>
    </div>
  </div>
  <div id="screenwrap" class="vtile" style="display:none">
    <div class="vbar">
      <span class="vdot"></span><span class="vtitle">Studio One \u2014 live</span>
      <span class="vrpill">\u25cf REC</span>
      <span style="flex:1"></span>
      <button class="vbtn" data-va="full" title="Full screen"><svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M9 3H3v6M15 3h6v6M9 21H3v-6M15 21h6v-6"/></svg></button>
      <button class="vbtn" data-va="hide" title="Hide"><svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M5 5l14 14M19 5L5 19"/></svg></button>
    </div>
    <img id="screenimg" alt="" draggable="false">
    <video id="screenvid" autoplay playsinline muted style="display:none"></video>
  </div>
  <div id="statepill" class="pill"></div>
  <div id="micwarn" class="pill" style="display:none;position:fixed;bottom:150px;left:50%;transform:translateX(-50%);z-index:30;color:#F5A85C;max-width:92vw">Your headset mic runs at phone quality \u2014 pick the built-in mic, keep the headset for listening.</div>
  <div class="ctrl pill">
    <button id="mute" class="cbtn" title="Mute microphone"><svg viewBox="0 0 24 24" width="17" height="17" fill="currentColor"><path d="M12 14a3 3 0 0 0 3-3V5a3 3 0 0 0-6 0v6a3 3 0 0 0 3 3zm5-3a5 5 0 0 1-10 0H5a7 7 0 0 0 6 6.92V21h2v-3.08A7 7 0 0 0 19 11h-2z"/></svg></button>
    <button id="dev" class="cbtn" title="Audio devices"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6 15l6-6 6 6"/></svg></button>
    <button id="screenbtn" class="cbtn" title="Studio screen" style="display:none"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="13" rx="2"/><path d="M8 21h8m-4-4v4"/></svg></button>
    <button id="gcambtn" class="cbtn" title="Your camera"><svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="6" width="13" height="12" rx="2"/><path d="M15 10l7-4v12l-7-4"/></svg></button>
    <button id="drawerbtn" class="cbtn" title="Takes"><svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor"><path d="M4 4h16v12H7l-3 3z"/></svg></button>
    <span id="recpill">● RECORDING 00:00 — your take is being captured locally</span>
  </div>
  <div id="devpop">
    <div class="lab">Microphone</div><select id="mic2"></select>
    <div class="lab">Output</div><select id="out2"></select>
  </div>
  <div class="drawer" id="drawer">
    <div class="tabs"><div class="tab on">Takes</div></div>
    <div class="pane on" id="p-takes"></div>
  </div>
  <div id="recframe"></div>
  <div id="hovertip" style="display:none;position:fixed;z-index:80;pointer-events:none;
    font-size:11px;letter-spacing:.3px;color:#E7E4DB;background:rgba(10,10,12,.88);
    border:1px solid rgba(231,228,219,.16);border-radius:8px;padding:4px 9px"></div>
  <style>
  @media(max-width:699px){
    .drawer{width:100vw}
    .ctrl{max-width:94vw;z-index:45}   /* stays ABOVE the full-width drawer — its button is the only way back */
    .jcard{width:94vw;padding:24px 20px}
    .top{left:14px;right:14px;flex-direction:column;gap:8px;align-items:flex-start}
    .top .pill{max-width:94vw}
    .top .pill span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:38vw}
    .bigmeter{width:78vw}
    #devpop{width:88vw}
    #statepill{bottom:110px;max-width:90vw}
  }
  .vtile{position:fixed;top:88px;left:50%;transform:translateX(-50%);z-index:16;width:fit-content;max-width:97vw;
    border-radius:16px;overflow:hidden;background:rgba(8,8,8,.96);
    border:1px solid rgba(231,228,219,.18)}
  .vtile>img,.vtile>video{display:block;width:auto;height:auto;max-width:97vw;max-height:calc(100vh - 236px);
    background:#000;user-select:none;-webkit-user-select:none}
  .vbar{display:flex;align-items:center;gap:8px;padding:7px 12px;cursor:grab;touch-action:none;
    background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.015));
    border-bottom:1px solid rgba(231,228,219,.08)}
  .vbar:active{cursor:grabbing}
  .vdot{width:7px;height:7px;border-radius:50%;background:#5CC78C;box-shadow:0 0 8px rgba(92,199,140,.7)}
  .vtitle{font-size:11.5px;letter-spacing:.4px;color:rgba(231,228,219,.8);text-transform:uppercase}
  .vbtn{display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:7px;
    border:1px solid rgba(231,228,219,.12);background:transparent;color:rgba(231,228,219,.75);cursor:pointer}
  .vbtn:hover{background:rgba(231,228,219,.1)}
  .vtile:fullscreen{max-width:100vw;width:100vw;border-radius:0;border:none;background:#000}
  .vtile:fullscreen>img,.vtile:fullscreen>video{width:100vw;max-width:100vw;max-height:calc(100vh - 40px);height:calc(100vh - 40px);object-fit:contain}
  .vtile.vfake{top:0!important;left:0!important;transform:none!important;width:100vw;max-width:100vw;height:100vh;
    border-radius:0;border:none;z-index:60;background:#000}
  .vtile.vfake>img,.vtile.vfake>video{width:100vw;max-width:100vw;height:calc(100vh - 40px);max-height:none;object-fit:contain}
  .vrpill{display:none;align-items:center;gap:5px;font-size:11px;font-weight:700;letter-spacing:.6px;
    color:#fff;background:#C93B34;border-radius:999px;padding:3px 10px;margin-left:6px}
  .vtile.vrec .vrpill{display:inline-flex;animation:vpulse 1.1s ease-in-out infinite}
  @keyframes vpulse{0%,100%{opacity:1}50%{opacity:.45}}
  @media(max-width:699px){.vtile img{max-height:44vh}}
  </style>
  <div id="endcard"><div class="card">
    <b style="font-size:18px">Session ended</b>
    <p id="endsum" class="soft" style="margin-top:10px;font-size:13px"></p>
  </div></div>
</div>

<script>
(function(){
"use strict";
var C=JSON.parse(document.getElementById("cfg").textContent);
function $(s){return document.querySelector(s);}
function esc(s){var d=document.createElement("div");d.textContent=s;return d.innerHTML;}
function fmt(s){s=Math.max(0,Math.floor(s));return (s/60<10?"0":"")+Math.floor(s/60)+":"+(s%60<10?"0":"")+(s%60);}
var ME="Guest",ws=null,ended=false,joined=false,hostOn=false,seq=0,muted=false,wake=null;
var ctx=null,playNode=null,capNode=null,micSrc=null,micStream=null,analyser=null;
var inLat=0;   // this mic's own capture latency (ms), when the browser reports it
var calMs=0;   // measured beep-loop hardware latency (ms); 0 = calibration didn't run/failed
var micLvl=0;  // smoothed RMS of the OWN mic — goes to the host so its log sees this side

// Play a short 4 kHz blip and time its round trip through the real speakers + mic on the
// context clock (rs-cal worklet). Uses a TEMPORARY echo-cancellation-free stream — AEC on the
// main mic would swallow the beep as "echo". Never blocks joining; failure just leaves calMs=0.
async function calibrate(){
  var st=null;
  try{
    st=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:false,noiseSuppression:false,autoGainControl:false}});
    var node=new AudioWorkletNode(ctx,"rs-cal");
    var src2=ctx.createMediaStreamSource(st);src2.connect(node);
    var mute=ctx.createGain();mute.gain.value=0;node.connect(mute);mute.connect(ctx.destination);
    var res=await new Promise(function(resolve){
      var to=setTimeout(function(){resolve(0);},900);
      node.port.onmessage=function(e){clearTimeout(to);resolve(e.data.loop||0);};
      var osc=ctx.createOscillator();osc.frequency.value=4000;
      var g=ctx.createGain();g.gain.value=0;
      osc.connect(g);g.connect(ctx.destination);
      var t0=ctx.currentTime+0.25;
      node.port.postMessage({t0:t0});
      g.gain.setValueAtTime(0,t0-0.01);g.gain.setValueAtTime(0.8,t0);g.gain.setValueAtTime(0,t0+0.08);
      osc.start(t0-0.01);osc.stop(t0+0.1);
    });
    src2.disconnect();node.disconnect();mute.disconnect();st.getTracks().forEach(function(t){t.stop();});
    if(res>3&&res<250)calMs=Math.round(res);
  }catch(e){try{if(st)st.getTracks().forEach(function(t){t.stop();});}catch(x){}}
}
var joinedAt=0;
var recOn=false,recLight=false,recChunks=[],recFrames=0,recTake=0,recRecvAt=0,recFirstAt=0;
var psNow=-1,psAt=0,psCt=0,loopO=0,recP0=-1;
var takes=[];   // {n,status,blob,dur,startAt,key,url}
var bufMs=0;

// ---- join screen: mic permission, device pick, level test ----
var micSel=$("#mic"),outSel=$("#out"),mic2=$("#mic2"),out2=$("#out2");
$("#name").value=(function(){try{return localStorage.getItem("rs_name")||""}catch(e){return ""}})();
(function(){var mt=$("#testmeter"),i;for(i=0;i<26;i++)mt.appendChild(document.createElement("i"));
  var bm=$("#bigmeter");for(i=0;i<48;i++)bm.appendChild(document.createElement("i"));})();
var testCtx=null,testAn=null,testSrc=null,testStream=null,testRAF=0,testBuf=null;
function askMic(devId){
  var c={echoCancellation:true,noiseSuppression:true};
  if(devId)c.deviceId={exact:devId};
  navigator.mediaDevices.getUserMedia({audio:c}).then(function(s){
    $("#blocked").style.display="none";$("#micbtn").textContent="Allowed";
    fillDevices();startTest(s);
  }).catch(function(){$("#blocked").style.display="block";});
}
function fillDevices(){navigator.mediaDevices.enumerateDevices().then(function(ds){
  if(!ds.some(function(d){return d.kind==="audioinput";}))return;   // pre-permission empty list: keep "Default microphone"
  [micSel,mic2].forEach(function(el){el.innerHTML="";});
  [outSel,out2].forEach(function(el){el.innerHTML="";});
  var mi=0,oi=0;
  ds.forEach(function(d){
    if(d.kind==="audioinput"){mi++;[micSel,mic2].forEach(function(el){
      var o=document.createElement("option");o.value=d.deviceId;o.textContent=d.label||("Microphone "+mi);el.appendChild(o);});}
    if(d.kind==="audiooutput"){oi++;[outSel,out2].forEach(function(el){
      var o=document.createElement("option");o.value=d.deviceId;o.textContent=d.label||("Output "+oi);el.appendChild(o);});}
  });
  var canOut=oi>0&&"setSinkId" in AudioContext.prototype;
  $("#outrow").style.display=canOut?"":"none";
});}
function startTest(s){
  if(testStream)testStream.getTracks().forEach(function(t){t.stop();});
  testStream=s;
  if(!testCtx)testCtx=new (window.AudioContext||window.webkitAudioContext)();
  if(testSrc)testSrc.disconnect();
  testSrc=testCtx.createMediaStreamSource(s);
  if(!testAn){testAn=testCtx.createAnalyser();testAn.fftSize=512;testBuf=new Uint8Array(512);}
  testSrc.connect(testAn);
  if(!testRAF)testLoop();
}
function testLoop(){
  testRAF=requestAnimationFrame(testLoop);
  if(!testAn)return;
  testAn.getByteTimeDomainData(testBuf);
  var pk=0,i;for(i=0;i<testBuf.length;i++){var v=Math.abs(testBuf[i]-128)/128;if(v>pk)pk=v;}
  var bars=$("#testmeter").children;
  for(i=0;i<bars.length;i++){var on=i/bars.length<pk*1.6;
    bars[i].style.opacity=on?"1":".25";bars[i].style.height=on?(6+i*0.7)+"px":"4px";}
}
function stopTest(){
  if(testRAF){cancelAnimationFrame(testRAF);testRAF=0;}
  if(testStream)testStream.getTracks().forEach(function(t){t.stop();});
  if(testCtx){try{testCtx.close();}catch(e){}testCtx=null;testAn=null;}
}
fillDevices();   // best effort pre-permission (labels may be generic until Join's allow)
navigator.mediaDevices.ondevicechange=function(){fillDevices();};
$("#name").addEventListener("input",function(){$("#joinbtn").disabled=!this.value.trim();});
try{if((localStorage.getItem("rs_name")||"").trim())$("#joinbtn").disabled=false;}catch(e){}
$("#askagain").onclick=function(){askMic();};

$("#joinbtn").onclick=function(){
  ME=($("#name").value||"").trim()||"Guest";
  var gw2=$("#guestwho");if(gw2)gw2.textContent=ME;
  try{localStorage.setItem("rs_name",ME);}catch(e){}
  start().catch(function(e){
    if(e&&e.message==="samplerate")return;
    if(ctx){try{ctx.close();}catch(x){}ctx=null;}
    var b=$("#blocked");b.innerHTML="<b>This browser can't run the session</b> \u2014 "+esc(String(e&&e.message||e));b.style.display="block";
  });
};

// ---- audio engine ----
async function start(){
  // iPhone: with the mic open, iOS routes playback to the EARPIECE (muffled phone-call sound)
  // unless the session type is declared. Safari's AudioSession API exists for exactly this.
  try{if(navigator.audioSession)navigator.audioSession.type="play-and-record";}catch(e){}
  ctx=new AudioContext({sampleRate:48000});
  try{await ctx.resume();}catch(e){}
  if(Math.round(ctx.sampleRate)!==48000){
    alert("This device's audio engine won't run at 48 kHz (got "+ctx.sampleRate+" Hz) - set the output device to 48 kHz and reload.");
    throw new Error("samplerate");
  }
  // Safari suspends the context behind our back (focus loss, device switch). Fight back.
  ctx.onstatechange=function(){if(ctx.state==="suspended"){ctx.resume().catch(function(){});}};
  var src=URL.createObjectURL(new Blob([document.getElementById("wk").textContent],{type:"application/javascript"}));
  await ctx.audioWorklet.addModule(src);
  playNode=new AudioWorkletNode(ctx,"rs-play",{numberOfInputs:0,outputChannelCount:[2]});
  if(navigator.maxTouchPoints>0&&window.innerWidth<900)playNode.port.postMessage({floor:12000});
  analyser=ctx.createAnalyser();analyser.fftSize=1024;
  playNode.connect(analyser);analyser.connect(ctx.destination);
  playNode.port.onmessage=function(e){bufMs=e.data.buf;
    if(e.data.ps>=0){psNow=e.data.ps+(e.data.pf||0)/20;psCt=e.data.ct||0;psAt=performance.now();}
    // 1/s: tell the host which program packet is at my ears RIGHT NOW, plus my own audio-path
    // latency. The message rides the same socket as the mic audio, so its arrival time IS the
    // uplink — the host derives the full round trip from it, no clock sync involved.
    if(e.data.ps>=0&&(NW||ws)){
      // Hardware part of the loop, best source first: measured beep loop; else the API-reported
      // output latency; else baseLatency + 22 ms (typical Mac CoreAudio out+in that Safari hides).
      // A hidden tab keeps buffering but can't be sung to — its ear-clock lies by whole
      // seconds and would steer the studio's take alignment. Never report latency from it.
      if(document.hidden)return;
      var o;
      if(calMs>0)o=calMs;
      else{var ol=(ctx.outputLatency||0)*1000;
        o=ol>0?Math.round(ol)+inLat:Math.round((ctx.baseLatency||0)*1000)+inLat+22;}
      loopO=o;
      // Self-diagnosis rides along: context state, mic track state, own mic level — the host
      // logs it, so a dead Safari capture is visible from the studio without guessing.
      var tk=micStream&&micStream.getAudioTracks()[0];
      var g={type:"lat",p:e.data.ps,o:o,st:ctx.state,
             trk:tk?(tk.readyState+(tk.muted?"/muted":"")):"none",
             mic:Math.round(micLvl*1000)/1000,cal:calMs,nw:NW?1:0,nm:ME,sr:Math.round(ctx.sampleRate),u:e.data.ur||0,u3:e.data.u3||0,r:recOn?1:0,f:upFail,b:Math.round(bufMs||0),bt:batPct,bp:batPlug?1:0};
      netSend(JSON.stringify(g));
    }};
  capNode=new AudioWorkletNode(ctx,"rs-cap");
  capNode.port.onmessage=function(e){onMicBlock(e.data.d||e.data,e.data.ct||0);};
  var sink=ctx.createGain();sink.gain.value=0;capNode.connect(sink);sink.connect(ctx.destination);   // keeps the capture node pulled
  // ORDER IS SAFARI-CRITICAL: Safari keeps ONE live mic capture per page — any stream opened
  // later kills the earlier one. So: close the join-card test stream, run the calibration
  // (temporary EC-free stream, closed inside), and open the REAL mic stream LAST.
  stopTest();
  if(outSel.value&&!out2.value)out2.value=outSel.value;
  if(out2.value&&ctx.setSinkId){try{await ctx.setSinkId(out2.value);}catch(e){}}
  await calibrate();   // after setSinkId — the beep must leave through what the artist hears
  try{await setMic(micSel.value);}
  catch(e){muted=true;$("#mute").classList.add("off");if(NW)try{NW.postMessage({t:"mute",v:true});}catch(x){}var mw=$("#micwarn");if(mw){mw.textContent="No microphone \u2014 listening only.";mw.style.display="flex";}}
  $("#join").style.display="none";$("#live").style.display="block";
  joined=true;joinedAt=Date.now();
  // Phones: keep the screen awake for the whole session; re-grab it when the tab returns.
  function grabWake(){try{if(navigator.wakeLock)navigator.wakeLock.request("screen").then(function(w){wake=w;}).catch(function(){});}catch(e){}}
  grabWake();
  document.addEventListener("visibilitychange",function(){
    if(document.visibilityState==="visible"){
      grabWake();if(ctx&&ctx.state==="suspended")ctx.resume().catch(function(){});
      var tk=micStream&&micStream.getAudioTracks()[0];
      if(!muted&&(!tk||tk.readyState!=="live"||tk.muted))setMic(micSel&&micSel.value).catch(function(){});
    }
  });
  try{bch.postMessage({t:"takeover",id:TABID});}catch(e){}
  connect();connectView();meterLoop();setInterval(clock,500);
}
async function setMic(devId){
  // RAW capture on purpose: EC/NS/AGC hand playback+mic to the OS voice pipeline (muffled
  // program, BT drops to phone-quality HFP). The page mandates headphones instead.
  var c={echoCancellation:false,noiseSuppression:false,autoGainControl:false};
  if(devId)c.deviceId={exact:devId};
  var s;
  try{s=await navigator.mediaDevices.getUserMedia({audio:c});}
  catch(e){
    // The picked device can vanish (headset unplugged, BT off). exact:{gone} fails FOREVER,
    // so the auto-reacquire (onended + all-zero watchdog) could never heal — fall back to
    // the default mic instead of streaming silence for the rest of the session.
    if(!devId)throw e;
    delete c.deviceId;
    s=await navigator.mediaDevices.getUserMedia({audio:c});
  }
  if(micStream)micStream.getTracks().forEach(function(t){t.stop();});
  micStream=s;
  try{var st=s.getAudioTracks()[0].getSettings();inLat=st&&st.latency?Math.round(st.latency*1000):0;
    var mw=$("#micwarn");
    if(st&&st.sampleRate&&st.sampleRate<=24000){if(mw)mw.style.display="flex";}
    else if(mw)mw.style.display="none";
  }catch(e){inLat=0;}
  // Safari kills capture tracks silently (device switch, another stream, sleep). A dead track
  // means dead vocals — re-acquire automatically instead of streaming silence forever.
  try{s.getAudioTracks()[0].onended=function(){if(joined)setMic(micSel.value).catch(function(){});};}catch(e){}
  if(micSrc)micSrc.disconnect();
  micSrc=ctx.createMediaStreamSource(s);micSrc.connect(capNode);
}
// Every 960-frame mono block from the capture worklet: record raw (pre-mute) while the host has
// the red light on, and stream to the host unless muted.
var micDeadSince=0,micReacqAt=0;
function onMicBlock(f,ct){
  var s2=0,i;for(i=0;i<f.length;i++)s2+=f[i]*f[i];
  micLvl=micLvl*0.8+Math.sqrt(s2/f.length)*0.2;   // own-mic level, reported to the host 1/s
  // iOS kills capture SILENTLY: track stays "live", samples go ALL-ZERO. Detector = literal
  // digital zero on every sample (a real mic always carries noise floor; an RMS threshold
  // here once re-cycled healthy quiet mics into a wedged state). 6 s dead => re-acquire,
  // at most once per 12 s.
  var allZero=true;
  for(i=0;i<f.length;i++){if(f[i]!==0){allZero=false;break;}}
  var now=performance.now();
  if(allZero&&!muted&&!recOn){
    if(!micDeadSince)micDeadSince=now;
    else if(now-micDeadSince>6000&&now-micReacqAt>12000){
      micReacqAt=now;micDeadSince=0;
      setMic(micSel&&micSel.value).catch(function(){});
    }
  }else micDeadSince=0;
  if(recOn){
    if(!recFirstAt){
      recFirstAt=performance.now();
      // Both stamps ride the AudioContext clock (same render thread) — 50 packets/s of
      // program per context second. Main-thread dispatch delay cancels out entirely.
      recP0=psNow<0?-1:(ct&&psCt?psNow+(ct-psCt)*50:psNow+(recFirstAt-psAt)*0.05);
    }
    recChunks.push(f);recFrames+=f.length;
  }
  if(!muted&&!NW)sendPCM(f);   // fallback only — with the worker up, the cap worklet feeds it directly
}
function sendPCM(f){
  if(!ws||ws.readyState!==1)return;
  var buf=new ArrayBuffer(4+1920),dv=new DataView(buf);
  dv.setUint8(0,2);dv.setUint8(1,1);dv.setUint16(2,seq&0xFFFF,true);seq++;
  for(var i=0;i<960;i++){var s=Math.max(-1,Math.min(1,f[i]));dv.setInt16(4+i*2,s<0?s*32768:s*32767,true);}
  try{ws.send(buf);}catch(e){}
}
function onFrame(buf){
  var dv=new DataView(buf),kind=dv.getUint8(0);
  if(kind===4){showFrame(buf);return;}
  if(kind!==1&&kind!==3)return;
  var n=(buf.byteLength-4)/2,mono1=kind===1&&dv.getUint8(1)===1,f=new Float32Array(mono1?n*2:n);
  if(mono1){for(var i=0;i<n;i++){var v=dv.getInt16(4+i*2,true)/32768;f[i*2]=v;f[i*2+1]=v;}}
  else for(var i=0;i<n;i++)f[i]=dv.getInt16(4+i*2,true)/32768;
  if(playNode)playNode.port.postMessage({k:kind,d:f,s:dv.getUint16(2,true)},[f.buffer]);
}

// ---- network worker (audio never touches this thread), legacy fallback if it can't boot ----
var NW=null,nwAlive=false;
function netSend(s){
  if(NW)NW.postMessage({t:"send",s:s});
  else if(ws&&ws.readyState===1){try{ws.send(s);}catch(e){}}
}
function connect(){
  if(ended)return;
  try{
    var src=URL.createObjectURL(new Blob([document.getElementById("nwk").textContent],{type:"application/javascript"}));
    NW=new Worker(src);
    var chP=new MessageChannel(),chC=new MessageChannel();
    playNode.port.postMessage({port:chP.port1},[chP.port1]);
    capNode.port.postMessage({port:chC.port1},[chC.port1]);
    NW.postMessage({t:"init",
      url:(location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws/"+C.id+"?dev="+DEV,
      play:chP.port2,cap:chC.port2},[chP.port2,chC.port2]);
    if(muted)NW.postMessage({t:"mute",v:true});
    NW.onmessage=function(e){var m=e.data;nwAlive=true;
      if(m.t==="json")onJSON(m.s);
      else if(m.t==="frame")showFrame(m.d);
      else if(m.t==="state")setState(m.s==="re"?"re":(hostOn?null:"wait"));
    };
    NW.onerror=function(){fallbackLegacy();};
    // A worker that boots but never speaks (port-transfer quirks, worker-WS restrictions)
    // gets 4 s, then the proven main-thread path takes over.
    setTimeout(function(){if(!nwAlive)fallbackLegacy();},8000);
  }catch(e){fallbackLegacy();}
}
function fallbackLegacy(){
  if(ended||ws)return;
  if(NW){try{NW.terminate();}catch(e){}NW=null;}
  ws=new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws/"+C.id+"?dev="+DEV);
  ws.binaryType="arraybuffer";
  ws.onopen=function(){setState(hostOn?null:"wait");};
  ws.onmessage=function(ev){if(typeof ev.data==="string")onJSON(ev.data);else onFrame(ev.data);};
  ws.onclose=function(){if(ended)return;setState("re");ws=null;setTimeout(fallbackLegacy,3000);};
}
function onJSON(s){
  var m;try{m=JSON.parse(s);}catch(e){return;}
  if(m.type==="presence"){hostOn=!!m.host;setState(hostOn?null:"wait");if(m.host)rtcHello();return;}
  if(m.type==="rec"){if(m.on)recStart(Number(m.take)||1,!!m.light);else recStop();return;}
  if(m.type==="rtc"){onRtc(m);return;}
  if(m.type==="hn"){var h=$("#hostname");if(h&&m.name)h.textContent=m.name;var hw=$("#hostwho");if(hw&&m.name)hw.textContent=m.name;return;}
  if(m.type==="cam"){
    if(!m.on){document.body.classList.remove("cam");var ci=$("#camimg");if(ci){ci.style.display="none";ci.removeAttribute("src");}}
    else{var cv=$("#camvid");if(cv&&cv.videoWidth)document.body.classList.add("cam");}
    return;
  }
  if(m.type==="scr"){
    if(m.on){setTimeout(showScreenTile,300);setTimeout(showScreenTile,1200);setTimeout(showScreenTile,2600);return;}
    if(!m.on){
      var sw=$("#screenwrap");
      if(sw){sw.classList.remove("vfake");pipPlace();sw.style.display="none";}
      if(document.fullscreenElement){try{document.exitFullscreen();}catch(e){}}
      setSharing(false);scrSeen=false;
      var sb=$("#screenbtn");if(sb){sb.style.display="none";sb.classList.remove("off");}
      scrOn=true;
    }
    return;
  }
  if(m.type==="end"){endSession();return;}
  if(m.type==="busy"){busySession();return;}
}
function busySession(){
  ended=true;
  if(NW){try{NW.postMessage({t:"end"});}catch(e){}}
  try{if(ws)ws.close();}catch(e){}
  var d=document.createElement("div");
  d.style.cssText="position:fixed;inset:0;z-index:999;display:flex;align-items:center;justify-content:center;background:rgba(9,10,13,.97);padding:30px;text-align:center";
  d.innerHTML='<div style="max-width:460px;font-size:15px;line-height:1.65"><b style="font-size:19px">Only one guest per session</b><br><br>This session is already open on another device or tab. Close it there first, then reload this page.</div>';
  document.body.appendChild(d);
}
function setState(s){
  var el=$("#statepill");
  if(!s){el.style.display="none";return;}
  el.style.display="flex";
  el.innerHTML=s==="wait"
    ?'<span class="dot" style="background:#F5A85C"></span>Waiting for the host…'
    :'<span class="spin"></span>Reconnecting — your local recording keeps going';
}

// ---- takes drawer ----
$("#drawerbtn").onclick=function(){$("#drawer").classList.toggle("open");};

// ---- host-driven recording ----
function recStart(n,light){
  if(recOn&&recTake===n)return;   // host re-asserts state on (re)join — never wipe the running take
  // light once meant "capture nothing" — now the local recording is the TIMECODE take: the
  // first mic sample is stamped with the packet at the artist's ears (recP0), and the studio
  // places the uploaded file deterministically. Network latency never enters the equation.
  recLight=!!light;recOn=true;recTake=n;recChunks=[];recFrames=0;recFirstAt=0;recP0=-1;recRecvAt=performance.now();
  try{if(playNode)playNode.port.postMessage({freeze:1});}catch(e){}
  var vw=$("#screenwrap");if(vw)vw.classList.add("vrec");
  $("#recframe").style.display="block";$("#recpill").style.display="flex";
  var nar=window.innerWidth<700;
  $("#recpill").textContent=nar?"● REC":(recLight?"● RECORDING — the studio is recording you":"● RECORDING 00:00 — your take is being captured locally");
}
function recStop(){
  $("#recframe").style.display="none";$("#recpill").style.display="none";
  var vw=$("#screenwrap");if(vw)vw.classList.remove("vrec");
  recLight=false;
  try{if(playNode)playNode.port.postMessage({freeze:0});}catch(e){}
  if(!recOn)return;
  recOn=false;
  if(!recFrames){recChunks=[];return;}
  var t={n:recTake,status:"uploading",dur:recFrames/48000,
    startAt:recFirstAt?Math.max(0,recFirstAt-recRecvAt):0,key:"",url:"",p0:recP0,o:loopO};
  t.blob=wav24(recChunks,recFrames);
  recChunks=[];
  takes.push(t);renderTakes();upload(t);
}
var upFail="";
function upload(t,retry){
  t.status="uploading";renderTakes();
  fetch("/rs/"+C.id+"/take/"+t.n,{method:"PUT",body:t.blob})
    .then(function(r){if(!r.ok){upFail="http "+r.status;throw 0;}return r.json();})
    .then(function(d){
      upFail="";t.status="done";t.key=d.key;renderTakes();
      netSend(JSON.stringify({type:"take",from:ME,key:d.key,take:t.n,dur:t.dur,startAt:t.startAt,p0:t.p0,o:t.o}));
    })
    .catch(function(e){
      if(!retry){setTimeout(function(){upload(t,1);},2000);return;}
      if(!upFail)upFail=String(e&&e.message||e||"err");t.status="failed";renderTakes();$("#drawer").classList.add("open");});
}
function renderTakes(){
  var p=$("#p-takes");p.innerHTML="";
  takes.forEach(function(t,i){
    var d=document.createElement("div");d.className="take";
    var st=t.status==="uploading"?'<span class="spin"></span>Uploading…'
      :t.status==="done"?'<span class="dot" style="background:#5CC78C"></span><span style="color:#5CC78C">Uploaded</span> <a class="mini" style="text-decoration:none" download="take-'+t.n+'.wav" href="'+takeURL(t)+'">Download</a>'
      :'<span class="dot" style="background:#EE736B"></span><span style="color:#EE736B">Take didn&#39;t upload</span>'
        +' <button class="mini" data-r="'+i+'">Try again</button> <a class="mini" style="text-decoration:none" download="take-'+t.n+'.wav" href="'+takeURL(t)+'">Download</a>';
    d.innerHTML="<b>Take "+t.n+"</b> · "+fmt(t.dur)+'<div class="st">'+st+"</div>";
    p.appendChild(d);
  });
  p.querySelectorAll("[data-r]").forEach(function(b){b.onclick=function(){upload(takes[+b.dataset.r]);};});
}
function takeURL(t){if(!t.url)t.url=URL.createObjectURL(t.blob);return t.url;}
// Minimal WAV writer: 24-bit LE PCM, mono, 48 kHz.
function wav24(chunks,frames){
  var buf=new ArrayBuffer(44+frames*3),dv=new DataView(buf);
  function str(o,s){for(var i=0;i<s.length;i++)dv.setUint8(o+i,s.charCodeAt(i));}
  str(0,"RIFF");dv.setUint32(4,36+frames*3,true);str(8,"WAVE");
  str(12,"fmt ");dv.setUint32(16,16,true);dv.setUint16(20,1,true);dv.setUint16(22,1,true);
  dv.setUint32(24,48000,true);dv.setUint32(28,48000*3,true);dv.setUint16(32,3,true);dv.setUint16(34,24,true);
  str(36,"data");dv.setUint32(40,frames*3,true);
  var o=44;
  chunks.forEach(function(b){
    for(var i=0;i<b.length;i++){var s=Math.max(-1,Math.min(1,b[i]));var v=Math.round(s*8388607);if(v<0)v+=16777216;
      dv.setUint8(o++,v&255);dv.setUint8(o++,(v>>8)&255);dv.setUint8(o++,(v>>16)&255);}
  });
  return new Blob([buf],{type:"audio/wav"});
}

// ---- controls ----
$("#mute").onclick=function(){muted=!muted;if(NW)NW.postMessage({t:"mute",v:muted});this.classList.toggle("off",muted);this.title=muted?"Unmute microphone":"Mute microphone";};
$("#dev").onclick=function(){var pp=$("#devpop");pp.style.display=pp.style.display==="none"||!pp.style.display?"block":"none";};
mic2.onchange=function(){micSel.value=mic2.value;setMic(mic2.value).catch(function(){});};
out2.onchange=function(){if(ctx&&ctx.setSinkId)ctx.setSinkId(out2.value).catch(function(){});};

// ---- clock + meter ----
function elapsedSec(){return Math.floor((Date.now()-joinedAt)/1000);}
function clock(){
  $("#elapsed").textContent=fmt(elapsedSec());
  if(bufMs)$("#lat").textContent=Math.round(bufMs)+" ms";   // hold the last value - a 0 during an underrun made it blink
  var narrow=window.innerWidth<700;
  if(recOn)$("#recpill").textContent=narrow?"● REC "+fmt((performance.now()-recRecvAt)/1000):"● RECORDING "+fmt((performance.now()-recRecvAt)/1000)+" — your take is being captured locally";
  else if(recLight)$("#recpill").textContent=narrow?"● REC "+fmt((performance.now()-recRecvAt)/1000):"● RECORDING "+fmt((performance.now()-recRecvAt)/1000)+" — the studio is recording you";
}
var mW=[],mLvl=0,mBuf=new Uint8Array(1024);
function meterLoop(){
  requestAnimationFrame(meterLoop);
  if(!analyser)return;
  analyser.getByteTimeDomainData(mBuf);
  var pk=0,i;for(i=0;i<mBuf.length;i++){var v=Math.abs(mBuf[i]-128)/128;if(v>pk)pk=v;}
  mLvl=pk>mLvl?pk:mLvl*0.92;
  var bars=$("#bigmeter").children;
  if(!mW.length)for(i=0;i<bars.length;i++)mW.push(0.4+Math.random()*0.6);
  for(i=0;i<bars.length;i++)bars[i].style.height=(4+mLvl*28*mW[i])+"px";
}

// ---- end of session ----
var scrOn=true,scrSeen=false,scrURL=null,scrBusy=false,scrNext=null,wsv=null;
var batPct=-1,batPlug=true,batWarned=0;
try{if(navigator.getBattery)navigator.getBattery().then(function(b){
  function u(){batPct=Math.round(b.level*100);batPlug=b.charging;
    if(!batPlug&&batPct<=20&&Date.now()-batWarned>120000){batWarned=Date.now();
      var d=document.createElement("div");
      d.style.cssText="position:fixed;top:14px;left:50%;transform:translateX(-50%);z-index:998;background:#7a2e22;color:#fff;padding:10px 18px;border-radius:12px;font-size:13.5px;font-weight:600;box-shadow:0 8px 24px rgba(0,0,0,.5)";
      d.textContent="Battery "+batPct+"% — plug in your device, low power hurts the session";
      document.body.appendChild(d);setTimeout(function(){d.remove();},8000);}}
  u();b.onlevelchange=u;b.onchargingchange=u;});}catch(e){}
var gcamStream=null,gcamTx=null;
// Ship the self-view camera to the host over the pre-negotiated recvonly transceiver.
// replaceTrack needs NO renegotiation; the bitrate cap keeps the guest's uplink safe
// for the mic packets (voice is sacred, video yields).
function gcamSend(on){
  if(gcamTx){
    try{
      gcamTx.sender.replaceTrack(on&&gcamStream?gcamStream.getVideoTracks()[0]:null);
      if(on){
        var p=gcamTx.sender.getParameters();
        p.encodings=p.encodings&&p.encodings.length?p.encodings:[{}];
        p.encodings[0].maxBitrate=900000;
        p.encodings[0].scaleResolutionDownBy=2;
        gcamTx.sender.setParameters(p).catch(function(){});
      }
    }catch(e){}
  }
  netSend(JSON.stringify({type:"gc",on:on?1:0}));
}
function gcamToggle(){
  if(gcamStream){
    gcamSend(false);
    gcamStream.getTracks().forEach(function(tr){tr.stop();});
    gcamStream=null;
    var v=$("#gcamvid");if(v){try{v.srcObject=null;}catch(e){}}
    document.body.classList.remove("gcam");
    var b=$("#gcambtn");if(b)b.classList.add("off");
    return;
  }
  navigator.mediaDevices.getUserMedia({video:{facingMode:"user",width:{ideal:1280},height:{ideal:720}}})
    .then(function(s){
      gcamStream=s;
      var v=$("#gcamvid");v.srcObject=s;v.play&&v.play().catch(function(){});
      document.body.classList.add("gcam");
      var b=$("#gcambtn");if(b)b.classList.remove("off");
      s.getVideoTracks()[0].onended=function(){if(gcamStream){gcamToggle();}};
      gcamSend(true);
    })
    .catch(function(){});
}
var rpc=null,rtcUp=false,helloAt=0;
var TABID=Math.random().toString(36).slice(2);
var DEV="";try{DEV=localStorage.getItem("rs_dev")||"";if(!DEV){DEV=Math.random().toString(36).slice(2);localStorage.setItem("rs_dev",DEV);}}catch(e){DEV=TABID;}
try{
  var bch=new BroadcastChannel("rs_"+C.id);
  bch.onmessage=function(ev){
    if(ev.data&&ev.data.t==="takeover"&&ev.data.id!==TABID&&joined){
      try{recStop();}catch(e){}
      ended=true;
      if(NW){try{NW.postMessage({t:"end"});}catch(e){}}
      try{if(ws)ws.close();}catch(e){}
      try{if(wsv)wsv.close();}catch(e){}
      try{if(rpc)rpc.close();}catch(e){}
      if(micStream)micStream.getTracks().forEach(function(tr){tr.stop();});
      if(ctx){try{ctx.close();}catch(e){}}
      var d=document.createElement("div");
      d.style.cssText="position:fixed;inset:0;z-index:90;display:flex;align-items:center;justify-content:center;background:rgba(5,6,8,.94);color:#E7E4DB;font-size:15px";
      d.textContent="Session moved to a newer tab \u2014 you can close this one.";
      document.body.appendChild(d);
    }
  };
}catch(e){}
function rtcHello(){
  var now=Date.now();
  if(rtcUp||now-helloAt<5000)return;
  helloAt=now;netSend(JSON.stringify({type:"rtc",sub:"hello"}));
}
function rtcStart(offer){
  try{if(rpc)rpc.close();}catch(e){}
  rtcUp=false;
  rpc=new RTCPeerConnection({iceServers:[{urls:["stun:stun.l.google.com:19302","stun:stun1.l.google.com:19302"]}]});
  rpc.ontrack=function(ev){
    var sid=(ev.streams[0]&&ev.streams[0].id)||"";
    if(sid==="screen"){
      var v=$("#screenvid");v.srcObject=ev.streams[0];
      v.onresize=function(){showScreenTile();};
    }else if(sid==="cam"){
      var cv=$("#camvid");cv.srcObject=ev.streams[0];
      cv.onresize=function(){if(cv.videoWidth){document.body.classList.add("cam");document.body.classList.remove("jpeg");}};
    }
  };
  rpc.onicecandidate=function(ev){
    if(ev.candidate)netSend(JSON.stringify({type:"rtc",sub:"ice",cand:ev.candidate.candidate,
      mid:ev.candidate.sdpMid||"",idx:ev.candidate.sdpMLineIndex||0}));
  };
  rpc.oniceconnectionstatechange=function(){
    var st=rpc.iceConnectionState;
    if(st==="connected"||st==="completed"){rtcUp=true;document.body.classList.add("rtc");}
    else if(st==="failed"||st==="disconnected"||st==="closed"){
      rtcUp=false;document.body.classList.remove("rtc");
      if(st==="failed")setTimeout(rtcHello,1500);
      var sv=$("#screenvid");if(sv){sv.style.display="none";try{sv.srcObject=null;}catch(e){}}
      var im=$("#screenimg");if(im)im.style.display="";
      var cv=$("#camvid");if(cv){cv.style.display="none";try{cv.srcObject=null;}catch(e){}}
    }
  };
  rpc.setRemoteDescription({type:"offer",sdp:offer}).then(function(){
    // m-line order is fixed by the host: vscreen, vcam, then the recvonly slot for OUR camera.
    var vts=rpc.getTransceivers();
    gcamTx=vts[2]||null;
    if(gcamTx){
      try{gcamTx.direction="sendonly";}catch(e){}
      if(gcamStream)setTimeout(function(){gcamSend(true);},400);
    }
    return rpc.createAnswer();
  }).then(function(ans){
    return rpc.setLocalDescription(ans).then(function(){
      netSend(JSON.stringify({type:"rtc",sub:"answer",sdp:ans.sdp}));
    });
  }).catch(function(e){
    netSend(JSON.stringify({type:"rtc",sub:"err",msg:String(e&&e.message||e).slice(0,180)}));
  });
}
function onRtc(m){
  if(m.sub==="offer")rtcStart(m.sdp);
  else if(m.sub==="ice"&&rpc){
    try{rpc.addIceCandidate({candidate:m.cand,sdpMid:m.mid||null,sdpMLineIndex:m.idx||0});}catch(e){}
  }
}
function setSharing(on){
  on=!!on;
  if(document.body.classList.contains("sharing")===on)return;
  var el=$("#stagecard");
  if(!el){document.body.classList.toggle("sharing",on);return;}
  var r1=el.getBoundingClientRect();
  document.body.classList.toggle("sharing",on);
  var r2=el.getBoundingClientRect();
  var s=r2.width?r1.width/r2.width:1;
  el.style.transition="none";
  el.style.transformOrigin="top left";
  el.style.transform="translate("+(r1.left-r2.left)+"px,"+(r1.top-r2.top)+"px) scale("+s+")";
  requestAnimationFrame(function(){
    el.style.transition="transform .65s cubic-bezier(.22,.9,.24,1),background .65s ease,border-color .65s ease";
    el.style.transformOrigin="";
    el.style.transform=on?"scale("+pipS+")":"";
  });
}
// ---- host PiP: drag anywhere, corner resize, persists; rides INTO fullscreen (the
// fullscreen element shows only its own subtree, so the card re-parents into the tile).
var pipS=1;
try{var pv=JSON.parse(localStorage.getItem("rs_pip")||"0");if(pv&&pv.s)pipS=Math.max(.6,Math.min(1.4,pv.s));
  if(pv&&pv.r!=null){}}catch(e){}
function pipSave(){var el=$("#stagecard");try{localStorage.setItem("rs_pip",JSON.stringify(
  {s:pipS,r:parseFloat(el.style.right)||20,b:parseFloat(el.style.bottom)||92}));}catch(e){}}
function pipRestore(){var el=$("#stagecard");try{var v=JSON.parse(localStorage.getItem("rs_pip")||"0");
  if(v&&v.r!=null){el.style.right=v.r+"px";el.style.bottom=v.b+"px";}}catch(e){}}
function pipHome(){return document.fullscreenElement===$("#screenwrap")||$("#screenwrap").classList.contains("vfake")
  ?$("#screenwrap"):document.querySelector(".stage");}
function pipPlace(){var home=pipHome(),el=$("#stagecard");if(el.parentNode!==home)home.appendChild(el);}
(function(){
  var el=$("#stagecard"),sz=$("#pipsz");if(!el||!sz)return;
  var mode=0,sx=0,sy=0,r0=0,b0=0,s0=1;
  el.addEventListener("pointerdown",function(e){
    if(!document.body.classList.contains("sharing"))return;
    mode=e.target===sz||sz.contains(e.target)?2:1;
    el.setPointerCapture(e.pointerId);
    sx=e.clientX;sy=e.clientY;
    r0=parseFloat(el.style.right)||20;b0=parseFloat(el.style.bottom)||92;s0=pipS;
    e.preventDefault();
  });
  el.addEventListener("pointermove",function(e){
    if(!mode)return;
    var dx=e.clientX-sx,dy=e.clientY-sy;
    if(mode===1){
      var host=pipHome().getBoundingClientRect();
      el.style.right=Math.max(4,Math.min(host.width-80,r0-dx))+"px";
      el.style.bottom=Math.max(4,Math.min(host.height-60,b0-dy))+"px";
    }else{
      pipS=Math.max(.6,Math.min(1.4,s0+(-dx-dy)/240));
      el.style.transform="scale("+pipS+")";
    }
  });
  el.addEventListener("pointerup",function(){if(mode){mode=0;pipSave();}});
  document.addEventListener("fullscreenchange",pipPlace);
  pipRestore();
})();
function connectView(){
  if(ended)return;
  try{
    var u=(location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws/"+C.id+"?role=view";
    wsv=new WebSocket(u);wsv.binaryType="arraybuffer";
    wsv.onmessage=function(ev){
      if(typeof ev.data==="string")return;
      var k=new Uint8Array(ev.data,0,1)[0];
      if(k===4)showFrame(ev.data);
      else if(k===5)showCam(ev.data);
    };
    wsv.onclose=function(){if(!ended)setTimeout(connectView,3000);};
  }catch(e){}
}
// Generic draggable tile: grab the bar, drop anywhere; position remembered per tile.
function makeDraggable(tile,key){
  var bar=tile.querySelector(".vbar");if(!bar)return;
  // ponytail: no cross-session position restore — a spot saved on another viewport/orientation
  // parked the tile half off-screen (top clamp allowed y up to vh-80 with a 600px-tall tile).
  // Drag is per-session; the centered dock is always right on every device.
  var sx=0,sy=0,ox=0,oy=0,drag=false;
  bar.addEventListener("pointerdown",function(e){
    if(e.target.closest(".vbtn")||document.fullscreenElement)return;
    drag=true;bar.setPointerCapture(e.pointerId);
    var r=tile.getBoundingClientRect();
    tile.style.transform="none";tile.style.left=r.left+"px";tile.style.top=r.top+"px";
    sx=e.clientX;sy=e.clientY;ox=r.left;oy=r.top;e.preventDefault();
  });
  bar.addEventListener("pointermove",function(e){
    if(!drag)return;
    var x=Math.max(4,Math.min(window.innerWidth-80,ox+e.clientX-sx));
    var y=Math.max(4,Math.min(window.innerHeight-60,oy+e.clientY-sy));
    tile.style.left=x+"px";tile.style.top=y+"px";
  });
  bar.addEventListener("pointerup",function(e){if(!drag)return;drag=false;});
}
document.addEventListener("DOMContentLoaded",function(){
  if(window.matchMedia("(hover:hover)").matches){
    var tip=$("#hovertip");
    function bindTip(el,text){
      if(!el)return;
      el.addEventListener("pointermove",function(e){
        if(e.buttons||(el.id==="stagecard"&&!document.body.classList.contains("sharing"))){tip.style.display="none";return;}
        var host=document.fullscreenElement||document.body;
        if(tip.parentElement!==host)host.appendChild(tip);
        tip.textContent=text;tip.style.display="block";
        tip.style.left=Math.min(innerWidth-160,e.clientX+14)+"px";
        tip.style.top=Math.min(innerHeight-34,e.clientY+18)+"px";
      });
      el.addEventListener("pointerleave",function(){tip.style.display="none";});
      el.addEventListener("pointerdown",function(){tip.style.display="none";});
    }
    bindTip(document.querySelector("#screenwrap .vbar"),"move");
    bindTip($("#stagecard"),"move \u00b7 resize from corner");
  }
  var w=$("#screenwrap");
  if(w){
    makeDraggable(w,"screen2");
    w.querySelector('[data-va="full"]').onclick=function(){
      if(document.fullscreenElement){document.exitFullscreen();return;}
      if(w.classList.contains("vfake")){w.classList.remove("vfake");pipPlace();return;}
      if(w.requestFullscreen)w.requestFullscreen().catch(function(){w.classList.add("vfake");pipPlace();});
      else{w.classList.add("vfake");pipPlace();}
    };
    w.querySelector('[data-va="hide"]').onclick=function(){scrOn=false;w.classList.remove("vfake");pipPlace();w.style.display="none";setSharing(false);var b=$("#screenbtn");if(b)b.classList.add("off");};
    var im=$("#screenimg");if(im)im.ondblclick=function(){w.querySelector('[data-va="full"]').click();};
  }
  var gb=$("#gcambtn");if(gb){gb.classList.add("off");gb.onclick=gcamToggle;}
  var gw=$("#guestwho");if(gw){try{var nm=(localStorage.getItem("rs_name")||"").trim();if(nm)gw.textContent=nm;}catch(e){}}
  var b=$("#screenbtn");if(b)b.onclick=function(){scrOn=!scrOn;
    if(!scrOn){$("#screenwrap").classList.remove("vfake");pipPlace();}
    $("#screenwrap").style.display=scrOn?"block":"none";setSharing(scrOn);this.classList.toggle("off",!scrOn);};
});
function paintFrame(u){
  var im=$("#screenimg");
  im.onload=function(){
    if(scrURL)URL.revokeObjectURL(scrURL);scrURL=u;scrBusy=false;
    if(scrNext){var nx=scrNext;scrNext=null;scrBusy=true;paintFrame(nx);}
  };
  im.onerror=function(){URL.revokeObjectURL(u);scrBusy=false;};
  im.src=u;
}
var camBusy=false,camNext=null,camURL=null;
function paintCam(u){
  var im=$("#camimg");
  im.onload=function(){
    if(camURL)URL.revokeObjectURL(camURL);camURL=u;camBusy=false;
    if(camNext){var nx=camNext;camNext=null;camBusy=true;paintCam(nx);}
  };
  im.onerror=function(){URL.revokeObjectURL(u);camBusy=false;};
  im.src=u;
}
function showCam(buf){
  if(!document.body.classList.contains("cam"))document.body.classList.add("cam");
  document.body.classList.add("jpeg");
  var u=URL.createObjectURL(new Blob([new Uint8Array(buf,4)],{type:"image/jpeg"}));
  if(camBusy){if(camNext)URL.revokeObjectURL(camNext);camNext=u;return;}
  camBusy=true;paintCam(u);
}
function showScreenTile(){
  var v=$("#screenvid");
  if(!v||!v.videoWidth)return;
  v.style.display="block";
  var im=$("#screenimg");if(im)im.style.display="none";
  var w=$("#screenwrap");if(w&&scrOn)w.style.display="block";
  setSharing(true);
  if(!scrSeen){scrSeen=true;var b=$("#screenbtn");if(b)b.style.display="inline-flex";}
}
function showFrame(buf){
  var w=$("#screenwrap");
  if(!w)return;
  if(!scrSeen){scrSeen=true;var b=$("#screenbtn");if(b)b.style.display="inline-flex";}
  if(!scrOn)return;
  if(w.style.display==="none"||!w.style.display){w.style.display="block";setSharing(true);}
  var u=URL.createObjectURL(new Blob([new Uint8Array(buf,4)],{type:"image/jpeg"}));
  // Double-buffer: never swap src mid-decode; if a frame is still decoding, keep only the newest.
  if(scrBusy){if(scrNext)URL.revokeObjectURL(scrNext);scrNext=u;return;}
  scrBusy=true;paintFrame(u);
}
function endSession(){
  if(ended)return;
  try{recStop();}catch(e){}
  ended=true;
  try{if(wsv)wsv.close();}catch(e){}
  if(NW){try{NW.postMessage({t:"end"});}catch(e){}}
  try{if(ws)ws.close();}catch(e){}
  if(micStream)micStream.getTracks().forEach(function(t){t.stop();});
  if(ctx){try{ctx.close();}catch(e){}}
  var done=takes.filter(function(t){return t.status==="done";}).length;
  var lost=takes.filter(function(t){return t.status!=="done";});
  $("#endsum").textContent=done+" take"+(done===1?"":"s")+" uploaded";
  if(lost.length){
    var card=$("#endcard").firstElementChild||$("#endcard");
    lost.forEach(function(t){
      var a=document.createElement("a");
      a.className="mini";a.style.cssText="display:block;margin-top:10px;text-decoration:none";
      a.download="take-"+t.n+".wav";a.href=takeURL(t);
      a.textContent="Take "+t.n+" didn't upload \u2014 download it";
      card.appendChild(a);
    });
  }
  $("#endcard").style.display="flex";
  $("#statepill").style.display="none";$("#recframe").style.display="none";
}
})();
</script>
</body></html>`
}

// ---------------------------------------------------------------------------
// RemoSession room: one Durable Object per session id. It relays two things and stores neither:
// binary PCM/video frames (host <-> guest, host -> viewers) and short JSON control messages
// (presence, transport, rec, take, latency). Chat and notes were removed with the feature.
// Hibernation API keeps the room free while idle; a room carrying audio simply stays awake.
export class ListenRoom {
  constructor(state) { this.state = state }

  async fetch(request) {
    const url = new URL(request.url)
    const r0 = url.searchParams.get("role")
    const role = r0 === "host" ? "host" : r0 === "screen" ? "screen" : r0 === "view" ? "view" : "guest"
    if (role === "guest") {
      // ONE guest per session. A live guest on another device poisons latency reports and
      // doubles the DAW-leg mic feed, so the second device is refused with a clear screen.
      // The SAME device (persistent rs_dev id) always wins over its own older socket.
      const dev = url.searchParams.get("dev") || ""
      const now = Date.now(), seen = (this.seen ??= new Map())
      for (const w of this.state.getWebSockets("guest")) {
        const s = seen.get(w) ?? (seen.set(w, now), now)      // unseen = fresh, like sendPresence
        if (now - s > 20000) continue                          // stale socket — ignore
        const wdev = (w.deserializeAttachment() || {}).dev || ""
        if (dev && wdev === dev) { try { w.close(1011, "replaced") } catch (e) {} continue }
        const pair = new WebSocketPair()
        this.state.acceptWebSocket(pair[1], ["busy"])
        try { pair[1].send('{"type":"busy"}'); pair[1].close(4001, "busy") } catch (e) {}
        return new Response(null, { status: 101, webSocket: pair[0] })
      }
      const pair = new WebSocketPair()
      this.state.acceptWebSocket(pair[1], [role])
      pair[1].serializeAttachment({ role, dev })
      seen.set(pair[1], now)
      this.sendPresence()
      if (this.lastState) { try { pair[1].send(this.lastState) } catch (e) {} }
      return new Response(null, { status: 101, webSocket: pair[0] })
    }
    const pair = new WebSocketPair()
    // serializeAttachment survives hibernation; tags let us count roles cheaply.
    this.state.acceptWebSocket(pair[1], [role])
    pair[1].serializeAttachment({ role })
    this.sendPresence()
    // A late joiner immediately gets the last known transport state.
    if (this.lastState) { try { pair[1].send(this.lastState) } catch (e) {} }
    return new Response(null, { status: 101, webSocket: pair[0] })
  }

  webSocketMessage(ws, message) {
    (this.seen ??= new Map()).set(ws, Date.now())
    // RemoSession audio: binary PCM frames relay verbatim host→guests / guest→host. Never parsed,
    // never stored; a backed-up socket is SKIPPED, not queued — late audio is worse than lost audio.
    if (typeof message !== "string") {
      const role = (ws.deserializeAttachment() || {}).role
      if (role === "screen") {
        // Video ONLY to dedicated view sockets — mixing it into the audio socket made the
        // 49152 audio gate skip VOICE packets behind every queued frame (the regression).
        // The gate MUST sit well above ONE frame. At 65536 it was SMALLER than a single JPEG
        // (80-200 KB), so every frame arrived while the previous one was still flushing and was
        // skipped — the share looked like it kept falling over. 512 KB is ~3 frames of slack:
        // enough to ride a hiccup, still bounded so a stalled viewer can't build a backlog.
        for (const w of this.state.getWebSockets("view")) {
          // NOTE: bufferedAmount does NOT exist on Workers' WebSocket — this gate is inert;
          // real pacing lives in the app (governor + per-frame busy gate).
          if ((w.bufferedAmount ?? 0) > 524288) continue
          try { w.send(message) } catch (e) {}
        }
        return
      }
      if (role === "view") return
      for (const w of this.state.getWebSockets(role === "host" ? "guest" : "host")) {
        // Skip a guest whose socket holds more than ~250 ms of audio — matching the page's
        // own jitter ceiling. The old 256 KB (~1.3 s) let a whole burst form before skipping.
        if ((w.bufferedAmount ?? 0) > 49152) continue
        try { w.send(message) } catch (e) {}
      }
      return
    }
    if (message.length > 32768) return   // rtc offers/answers (SDP) run 5-10 KB
    let m; try { m = JSON.parse(message) } catch (e) { return }
    // Transport is TWO-WAY: host OR any guest can play/pause/seek. Relay to everyone EXCEPT the
    // sender (the sender already did the action locally) — that exclusion is what stops echo loops.
    // "rec"/"take" (RemoSession record control + take announcements) ride the same relay but are
    // momentary — they must never become the lastState replayed to a late joiner.
    if (m.type === "play" || m.type === "pause" || m.type === "seek" || m.type === "state" || m.type === "rec" || m.type === "take" || m.type === "lat" || m.type === "scr" || m.type === "cam" || m.type === "rtc" || m.type === "hn" || m.type === "gc") {
      if (m.type !== "rec" && m.type !== "take" && m.type !== "lat" && m.type !== "rtc") this.lastState = message
      for (const w of this.state.getWebSockets()) { if (w !== ws) { try { w.send(message) } catch (e) {} } }
    }
  }

  webSocketClose(ws) { this.dropped(ws) }
  webSocketError(ws) { this.dropped(ws) }

  dropped(ws) {
    const att = ws.deserializeAttachment() || {}
    if (att.role === "host") {
      this.lastState = null
      // Grace window: the host app auto-reconnects in ~3 s after a socket blip. Only if no
      // host is back after 8 s do guests get the terminal "end" (which kills their session).
      setTimeout(() => {
        if (this.state.getWebSockets("host").length > 0) return
        for (const g of this.state.getWebSockets("guest")) {
          try { g.send('{"type":"end"}') } catch (e) {}
        }
      }, 8000)
    }
    this.sendPresence(ws)
  }

  // {type:"presence", host:<bool>, guests:<n>} to EVERYONE (app shows guest count,
  // page shows "connected to host"). `closing` is excluded from the counts.
  sendPresence(closing) {
    const all = this.state.getWebSockets().filter((w) => w !== closing)
    const host = all.some((w) => (w.deserializeAttachment() || {}).role === "host")
    const now = Date.now(), seen = (this.seen ??= new Map())
    const guests = all.filter((w) => {
      if ((w.deserializeAttachment() || {}).role !== "guest") return false
      const s = seen.get(w) ?? (seen.set(w, now), now)   // fresh joins/hibernation wakes count
      if (now - s > 60000) { try { w.close(1011, "stale") } catch (e) {} return false }
      return now - s < 20000
    }).length
    const msg = JSON.stringify({ type: "presence", host, guests })
    for (const w of all) { try { w.send(msg) } catch (e) {} }
  }
}
