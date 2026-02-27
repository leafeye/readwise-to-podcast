"""Readwise-to-Podcast pipeline: fetch articles → generate audio → publish RSS."""

import argparse
import asyncio
import fcntl
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from notebooklm import AuthError, NotebookLMClient, RateLimitError

from podcast import (
    NOTEBOOK_MAX_AGE,
    cleanup_notebook,
    convert_to_mp3,
    start_podcast,
    try_download_podcast,
)
from r2_feed import generate_and_upload_feed, get_r2_client, upload_file
from readwise import fetch_new_articles
from state import (
    Episode,
    PendingNotebook,
    is_processed,
    load_episodes,
    load_state,
    mark_processed,
    save_episodes,
    save_state,
)

MAX_EPISODES_PER_RUN = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

LOCK_FILE = Path("state.lock")
REQUIRED_ENV = [
    "READWISE_TOKEN",
    "R2_ACCOUNT_ID",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
    "R2_BUCKET_NAME",
    "R2_PUBLIC_URL",
]


def validate_env() -> None:
    """Check required environment variables and ffmpeg availability."""
    missing = [v for v in REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    if not shutil.which("ffmpeg"):
        raise SystemExit("ffmpeg not found. Install it: brew install ffmpeg")


def acquire_lock() -> int:
    """Acquire file lock to prevent concurrent runs. Returns fd."""
    fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        raise SystemExit("Another instance is already running.")
    return fd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Readwise-to-Podcast pipeline")
    parser.add_argument(
        "--limit", type=int, default=MAX_EPISODES_PER_RUN,
        help=f"Max episodes to generate per run (default: {MAX_EPISODES_PER_RUN})",
    )
    parser.add_argument(
        "--init", action="store_true",
        help="Initialize state to 'now' without processing backlog",
    )
    parser.add_argument(
        "--recent", type=int, metavar="N",
        help="Ignore state and process the N most recent Readwise articles",
    )
    return parser.parse_args()


async def main():
    load_dotenv()
    args = parse_args()
    validate_env()
    lock_fd = acquire_lock()

    try:
        if args.init:
            state = load_state()
            state.last_run = datetime.now(timezone.utc).isoformat()
            save_state(state)
            logger.info(f"State initialized. last_run set to {state.last_run}")
            logger.info("Future runs will only process articles saved after this point.")
            return
        await _run_pipeline(
            limit=args.recent or args.limit,
            ignore_state=bool(args.recent),
        )
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


def _age_seconds(iso_timestamp: str) -> float:
    """Seconds since the given ISO timestamp."""
    started = datetime.fromisoformat(iso_timestamp)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - started).total_seconds()


async def _process_pending(
    nblm: NotebookLMClient,
    state,
    episodes: list[Episode],
    r2,
    bucket: str,
    public_url: str,
    tmp_dir: Path,
) -> int:
    """Check pending notebooks for completed audio. Returns number completed."""
    if not state.pending_notebooks:
        return 0

    logger.info(f"Checking {len(state.pending_notebooks)} pending notebook(s)...")
    completed = 0
    still_pending = []

    for pending in state.pending_notebooks:
        age = _age_seconds(pending.started_at)

        # Too old — clean up and give up
        if age > NOTEBOOK_MAX_AGE:
            logger.warning(
                f"Notebook {pending.notebook_id} older than "
                f"{NOTEBOOK_MAX_AGE // 60} min, cleaning up: {pending.title}"
            )
            await cleanup_notebook(nblm, pending.notebook_id)
            continue

        # Try to download (quick poll, no long wait)
        try:
            mp4_path = await try_download_podcast(
                nblm, pending.notebook_id, pending.task_id, tmp_dir, wait=False
            )
        except Exception as e:
            logger.error(f"Error checking pending {pending.title}: {e}")
            await cleanup_notebook(nblm, pending.notebook_id)
            continue

        if mp4_path is None:
            # Still generating — keep in pending
            logger.info(
                f"Still generating ({int(age // 60)} min): {pending.title}"
            )
            still_pending.append(pending)
            continue

        # Audio ready — process it
        try:
            mp3_path = convert_to_mp3(mp4_path)
            if mp3_path.stat().st_size < 100_000:
                logger.warning(f"Audio too short/empty for: {pending.title}")
            else:
                r2_key = f"episodes/{pending.article_id}.mp3"
                upload_file(r2, bucket, mp3_path, r2_key, "audio/mpeg")

                episodes.append(
                    Episode(
                        article_id=pending.article_id,
                        title=pending.title,
                        author=pending.author,
                        mp3_url=f"{public_url}/{r2_key}",
                        description=pending.summary,
                        source_url=pending.source_url,
                        pub_date=datetime.now(timezone.utc).isoformat(),
                        file_size=mp3_path.stat().st_size,
                    )
                )
                save_episodes(episodes)
                mark_processed(state, pending.article_id)
                completed += 1
                logger.info(f"✓ (pending) {pending.title}")
        finally:
            await cleanup_notebook(nblm, pending.notebook_id)
            for f in tmp_dir.iterdir():
                f.unlink(missing_ok=True)

    state.pending_notebooks = still_pending
    save_state(state)
    return completed


async def _run_pipeline(limit: int, ignore_state: bool = False):
    state = load_state()
    episodes = load_episodes()
    token = os.environ["READWISE_TOKEN"]

    # First run without state: seed last_run to now, skip backlog
    if state.last_run is None and not ignore_state:
        state.last_run = datetime.now(timezone.utc).isoformat()
        save_state(state)
        logger.info("First run — state initialized. Saving current timestamp.")
        logger.info("Run again to process newly saved articles from this point.")
        return

    r2 = get_r2_client()
    bucket = os.environ["R2_BUCKET_NAME"]
    public_url = os.environ["R2_PUBLIC_URL"].rstrip("/")
    tmp_dir = Path("tmp")
    tmp_dir.mkdir(exist_ok=True)

    async with await NotebookLMClient.from_storage() as nblm:
        # 1. Check pending notebooks from previous runs
        pending_completed = await _process_pending(
            nblm, state, episodes, r2, bucket, public_url, tmp_dir
        )

        # 2. Fetch new articles
        updated_after = None if ignore_state else state.last_run
        articles = await fetch_new_articles(token, updated_after)
        articles = [a for a in articles if a.source_url]

        if ignore_state:
            articles.sort(key=lambda a: a.updated_at, reverse=True)
            articles = articles[:limit]
            logger.info(f"--recent mode: selected {len(articles)} most recent articles")
        else:
            logger.info(f"Found {len(articles)} new articles with source URLs")

        if not articles:
            if episodes and pending_completed:
                generate_and_upload_feed(r2, bucket, public_url, episodes)
            if not state.pending_notebooks:
                state.last_run = datetime.now(timezone.utc).isoformat()
            save_state(state)
            return

        # 3. Process new articles
        processed_count = 0
        broke_early = False

        for i, article in enumerate(articles):
            if is_processed(state, article.id):
                continue

            if processed_count >= limit:
                logger.info(f"Limit reached ({limit}). Remaining articles next run.")
                broke_early = True
                break

            try:
                logger.info(
                    f"[{processed_count+1}/{limit}] Starting: {article.title}"
                )
                notebook_id, task_id = await start_podcast(
                    nblm, article.title, article.source_url,
                    content=article.content,
                )

                # Save as pending immediately (crash-safe)
                pending = PendingNotebook(
                    article_id=article.id,
                    notebook_id=notebook_id,
                    task_id=task_id,
                    title=article.title,
                    author=article.author,
                    summary=article.summary,
                    source_url=article.source_url,
                    started_at=datetime.now(timezone.utc).isoformat(),
                )
                state.pending_notebooks.append(pending)
                save_state(state)

                # Wait and try to download
                mp4_path = await try_download_podcast(
                    nblm, notebook_id, task_id, tmp_dir, wait=True
                )

                if mp4_path is None:
                    # Still generating — stays in pending for next run
                    logger.info(f"⏳ {article.title} — still generating, will retry")
                    continue

                # Success — convert, upload, register
                mp3_path = convert_to_mp3(mp4_path)
                if mp3_path.stat().st_size < 100_000:
                    logger.warning(f"Audio too short/empty for: {article.title}")
                    await cleanup_notebook(nblm, notebook_id)
                    state.pending_notebooks = [
                        p for p in state.pending_notebooks
                        if p.notebook_id != notebook_id
                    ]
                    save_state(state)
                    continue

                r2_key = f"episodes/{article.id}.mp3"
                upload_file(r2, bucket, mp3_path, r2_key, "audio/mpeg")

                episodes.append(
                    Episode(
                        article_id=article.id,
                        title=article.title,
                        author=article.author,
                        mp3_url=f"{public_url}/{r2_key}",
                        description=article.summary,
                        source_url=article.source_url,
                        pub_date=datetime.now(timezone.utc).isoformat(),
                        file_size=mp3_path.stat().st_size,
                    )
                )

                # Done — remove from pending, save
                await cleanup_notebook(nblm, notebook_id)
                state.pending_notebooks = [
                    p for p in state.pending_notebooks
                    if p.notebook_id != notebook_id
                ]
                save_episodes(episodes)
                mark_processed(state, article.id)
                save_state(state)
                processed_count += 1
                logger.info(f"✓ {article.title}")

            except AuthError:
                logger.error("NotebookLM sessie verlopen. Run: notebooklm login")
                broke_early = True
                break
            except RateLimitError:
                remaining = len(articles) - i
                logger.warning(
                    f"NotebookLM quota bereikt. {remaining} artikelen "
                    "wachten op volgende run."
                )
                broke_early = True
                break
            except Exception as e:
                logger.error(f"✗ {article.title}: {e}")
                continue
            finally:
                for f in tmp_dir.iterdir():
                    f.unlink(missing_ok=True)

    # 4. Update feed
    if episodes:
        generate_and_upload_feed(r2, bucket, public_url, episodes)

    # Only advance last_run if all done and no pending
    if not broke_early and not state.pending_notebooks:
        state.last_run = datetime.now(timezone.utc).isoformat()
        save_state(state)

    total = processed_count + pending_completed
    logger.info(f"Done. Processed {total} episodes ({pending_completed} pending + {processed_count} new).")


if __name__ == "__main__":
    asyncio.run(main())
