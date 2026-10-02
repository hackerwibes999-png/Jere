# XENORA Hosting Bot — VPS Edition

XENORA is a Telegram bot hosting manager plus a website hosting service designed for a personal Linux VPS.

## What is included
- Telegram bot hosting with source upload, token validation, deployment, logs, restart, stop, source replacement and admin management.
- Website hosting for static HTML/CSS/JS and supported dynamic runtimes available on the VPS.
- SQLite persistence for users, subscriptions, hosted bots and websites.
- Automatic restoration of hosted services after a VPS reboot.
- Public website proxy at `/site/<website-id>/`.
- Health endpoint at `/healthz`.
- Admin commands requested by the owner, including `/broadcast`, `/brodcast`, `/adduser`, `/banuser`, `/kickuser`, `/addadmin`, `/editsub`, `/hostedbots`, and `/mnghostebots`.
- Cleaner main menu; hosting plans are shown inside the matching Bot Hosting / Website Hosting flows.

## VPS install
1. Copy this folder to the VPS, for example `/opt/xenora`.
2. Run: `sudo bash setup_vps.sh`
3. Edit `/opt/xenora/.env`. Set `BOT_TOKEN` and preferably `XENORA_PUBLIC_BASE_URL`.
4. Restart: `sudo systemctl restart xenora`
5. Check: `sudo systemctl status xenora`
6. Check the web server locally: `curl http://127.0.0.1:8080/healthz`

## Public website URL
Every hosted website gets a shareable XENORA-branded URL. The priority is:

1. `XENORA_PUBLIC_BASE_URL` — recommended when you own a domain, e.g. `https://hosting.xenora.com`.
2. `XENORA_HOSTING_DOMAIN` — another branded domain can be supplied without the scheme.
3. Automatic fallback using `nip.io`, e.g. `http://xenora-203-0-113-10.nip.io:8080/site/ABC123/`. This requires no domain purchase and the hostname contains `xenora`.

For HTTPS, use a real domain plus Nginx/Caddy and set `XENORA_PUBLIC_BASE_URL` to the final HTTPS URL.

## Nginx
`nginx-xenora.conf.example` contains a reverse-proxy example. If Nginx terminates HTTPS, set `XENORA_PUBLIC_BASE_URL` to the final HTTPS domain.

## Persistent storage
Keep these directories persistent:
- `data/`
- `hosted_bots/`
- `hosted_websites/`
- `pending/`
- `logs/`

## Dynamic website requirements
XENORA starts dynamic websites locally and proxies them through the public web server. The uploaded application must honor the `PORT` environment variable for Python/Node/Go/Rust/etc. PHP uses the built-in PHP server automatically. The VPS must have the runtime/toolchain required by the uploaded project.

## Security
Only host source code you trust. Uploaded programs execute as processes on the VPS user account running XENORA. For multi-user/untrusted hosting, use OS/container isolation (for example Docker with per-project restrictions) before exposing the service publicly. XENORA's static scanner is a heuristic review aid, not a security sandbox.

## Important
No application can honestly guarantee execution on every hosting provider or arbitrary programming project. A personal VPS is the intended environment here because it gives XENORA persistent storage, long-running processes, networking, and control over installed runtimes.
