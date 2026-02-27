"""Readwise-to-Podcast pipeline: fetch articles → generate audio → publish RSS."""

import asyncio
import fcntl
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from notebooklm import AuthError, NotebookLMClient, RateLimitError

from podcast import convert_to_mp3, generate_podcast
from r2_feed import generate_and_upload_feed, get_r2_client, upload_file
from readwise import fetch_new_articles
from state import Episode, is_processed, load_episodes, load_state
from state import mark_processed, save_episodes, save_state

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


async def main():
    load_dotenv()
    validate_env()
    lock_fd = acquire_lock()

    try:
        await _run_pipeline()
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


async def _run_pipeline():
    state = load_state()
    episodes = load_episodes()
    token = os.environ["READWISE_TOKEN"]

    # 1. Fetch new articles
    articles = await fetch_new_articles(token, state.last_run)
    articles = [a for a in articles if a.source_url]
    logger.info(f"Found {len(articles)} new articles with source URLs")

    if not articles:
        state.last_run = datetime.now(timezone.utc).isoformat()
        save_state(state)
        return

    # 2. Process articles
    r2 = get_r2_client()
    bucket = os.environ["R2_BUCKET_NAME"]
    public_url = os.environ["R2_PUBLIC_URL"].rstrip("/")
    tmp_dir = Path("tmp")
    tmp_dir.mkdir(exist_ok=True)

    processed_count = 0

    async with await NotebookLMClient.from_storage() as nblm:
        for i, article in enumerate(articles):
            if is_processed(state, article.id):
                continue

            try:
                # Generate podcast
                logger.info(f"[{i+1}/{len(articles)}] Generating: {article.title}")
                mp4_path = await generate_podcast(
                    nblm, article.title, article.source_url, tmp_dir
                )

                # Convert and validate
                mp3_path = convert_to_mp3(mp4_path)
                if mp3_path.stat().st_size < 100_000:
                    logger.warning(f"Audio too short/empty for: {article.title}")
                    continue

                # Upload to R2
                r2_key = f"episodes/{article.id}.mp3"
                upload_file(r2, bucket, mp3_path, r2_key, "audio/mpeg")

                # Register episode
                mp3_url = f"{public_url}/{r2_key}"
                episodes.append(
                    Episode(
                        article_id=article.id,
                        title=article.title,
                        author=article.author,
                        mp3_url=mp3_url,
                        description=article.summary,
                        source_url=article.source_url,
                        pub_date=datetime.now(timezone.utc).isoformat(),
                        file_size=mp3_path.stat().st_size,
                    )
                )

                # Save after each episode: episodes FIRST, then state
                save_episodes(episodes)
                mark_processed(state, article.id)
                save_state(state)
                processed_count += 1
                logger.info(f"✓ {article.title}")

            except AuthError:
                logger.error(
                    "NotebookLM sessie verlopen. Run: notebooklm login"
                )
                break
            except RateLimitError:
                remaining = len(articles) - i
                logger.warning(
                    f"NotebookLM quota bereikt. {remaining} artikelen "
                    "wachten op volgende run."
                )
                break
            except Exception as e:
                logger.error(f"✗ {article.title}: {e}")
                continue
            finally:
                # Cleanup tmp files for this article
                for f in tmp_dir.iterdir():
                    f.unlink(missing_ok=True)

    # 3. Update feed (always, even if some articles failed)
    if episodes:
        generate_and_upload_feed(r2, bucket, public_url, episodes)

    state.last_run = datetime.now(timezone.utc).isoformat()
    save_state(state)
    logger.info(f"Done. Processed {processed_count}/{len(articles)} articles.")


if __name__ == "__main__":
    asyncio.run(main())
