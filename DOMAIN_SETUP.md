# XENORA Website URL Setup

Every hosted website is given a URL by XENORA.

## No domain purchase required

Leave `XENORA_PUBLIC_BASE_URL` and `XENORA_HOSTING_DOMAIN` empty.

XENORA discovers the VPS public IPv4 and creates a Xenora-branded `nip.io` URL such as:

`http://xenora-203-0-113-10.nip.io:8080/site/ABC123/`

Make sure the VPS firewall/security group allows the XENORA web port (default `8080`).

## Recommended production setup

Use a domain you control and a reverse proxy (Nginx or Caddy). Set:

`XENORA_PUBLIC_BASE_URL=https://hosting.your-domain.example`

or:

`XENORA_HOSTING_DOMAIN=hosting.your-domain.example`

Point that domain to the VPS and proxy the domain to `127.0.0.1:8080`.

If you want the domain itself to contain Xenora, use a domain/subdomain containing `xenora`, for example `hosting.xenora.example`.

## Important

A source code package cannot register a real internet domain. A real custom domain must be registered and its DNS must point to your VPS. The automatic `nip.io` fallback is provided so the service still has a shareable Xenora-branded hostname without buying a domain.
