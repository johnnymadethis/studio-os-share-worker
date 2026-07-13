# Studio OS — Share Worker

The Cloudflare Worker that powers Studio OS share links: the premium play/download page, Song Review
(timestamped web feedback), Listen Together (live WebSocket sync), pre-save links and folder shares.
Files stream from **your own** R2 bucket; nothing routes through anyone else's account.

## One-click setup

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/OWNER/REPO)

Clicking the button:

1. Asks you to sign in to **your** Cloudflare account and authorize the deploy.
2. Provisions a fresh **KV namespace**, **R2 bucket**, **D1 database** and **Durable Object** in your account.
3. Asks for `SHARE_TOKEN` — pick any random secret (e.g. run `openssl rand -hex 32`). Keep it; you'll paste it into the app.
4. Deploys the Worker to `https://studio-share.<your-account>.workers.dev`.

### Before you deploy — one manual step Cloudflare requires

Enable **R2** on your Cloudflare account once (dashboard → R2 → enable; Cloudflare asks for a card even
for the free tier). The song files live in R2, so the share links don't work without it.

### After it deploys

In **Studio OS → Settings → Cloud Share**, paste:

- **Worker URL** → the `https://studio-share.<your-account>.workers.dev` URL from the deploy result.
- **Share Token** → the exact `SHARE_TOKEN` value you chose during the deploy.

That's it — share links, Song Review and Listen Together now run entirely on your own account.

---

## Manual deploy (alternative)

```bash
npm i -g wrangler
wrangler kv namespace create SHARES        # copy the id into wrangler.jsonc
wrangler r2 bucket create studio-os
wrangler d1 create studio-feedback         # copy the database_id into wrangler.jsonc
wrangler secret put SHARE_TOKEN            # paste your random secret
wrangler deploy
```
