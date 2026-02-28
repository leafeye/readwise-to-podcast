# CLAUDE.md — Readwise to Podcast

## Project

Python pipeline: Readwise artikelen → NotebookLM podcasts → Cloudflare R2 → Pocket Casts.
Draait op VPS via systemd timer (elke 15 min, `--limit 1`).

## Deploy

1. Lokaal wijzigen en testen
2. `git commit && git push`
3. `ssh vps "cd ~/projects/readwise-to-podcast && git pull"`

**Nooit direct bewerken op de VPS.**

## Stack

- Python + uv, NotebookLM via `notebooklm-py` (Playwright)
- Readwise Reader API v3 (`withHtmlContent=true` voor article content)
- Cloudflare R2 (opslag + RSS feed)
- systemd timer: `readwise-podcast.service` + `.timer`
- Logs: `journalctl -u readwise-podcast`

## Structuur

| Bestand | Functie |
|---------|---------|
| `main.py` | Orchestratie, CLI flags, pipeline loop |
| `readwise.py` | Readwise API client |
| `podcast.py` | NotebookLM integratie (create, generate, download) |
| `r2_feed.py` | R2 upload + RSS feed generatie |
| `state.py` | State management (processed articles, pending notebooks) |

## Bekende aandachtspunten

- NotebookLM faalt soms bij korte artikelen (<2000 woorden)
- `add_text()` is betrouwbaarder dan `add_url()` (geen scraping/paywall issues)
- Archive.is URLs: originele URL extraheren als fallback
- Auth verloopt: periodiek `notebooklm login` op VPS
- **mp3_url in episodes.json is hardcoded bij creatie.** Als `R2_PUBLIC_URL` in .env fout staat, krijgen episodes foute URLs die niet vanzelf herstellen. Fix: episodes.json fixen + feed opnieuw genereren. Structurele fix (TODO): sla alleen relatieve paden op, bouw volledige URL bij feed-generatie.
