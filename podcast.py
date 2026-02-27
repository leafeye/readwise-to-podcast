"""NotebookLM integration for podcast generation."""

import asyncio
import logging
import re
import subprocess
from pathlib import Path

from notebooklm import NotebookLMClient, RPCTimeoutError

logger = logging.getLogger(__name__)

INITIAL_WAIT = 10 * 60   # 10 min before first poll
POLL_INTERVAL = 60        # then every 60s
POLL_TIMEOUT = 20 * 60    # 20 min polling after initial wait (30 min total)
NOTEBOOK_MAX_AGE = 60 * 60  # clean up after 1 hour


def _strip_html(html: str) -> str:
    """Strip HTML tags to plain text."""
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text).strip()


async def start_podcast(
    client: NotebookLMClient,
    title: str,
    source_url: str,
    content: str | None = None,
    language: str = "nl",
) -> tuple[str, str]:
    """Create notebook, add source, start audio generation.

    Prefers Readwise content (reliable). Falls back to URL if no content.
    Returns (notebook_id, task_id). Does NOT wait for completion.
    Cleans up notebook on failure.
    """
    notebook = await client.notebooks.create(f"Podcast: {title}")
    try:
        if content:
            plain_text = _strip_html(content)
            await client.sources.add_text(
                notebook.id, title, plain_text, wait=True
            )
            logger.info("Source added via text (Readwise content)")
        else:
            await client.sources.add_url(notebook.id, source_url, wait=True)
            logger.info("Source added via URL (no Readwise content available)")
        status = await client.artifacts.generate_audio(notebook.id, language=language)
    except Exception:
        await cleanup_notebook(client, notebook.id)
        raise
    logger.info(f"Generation started: notebook={notebook.id} task={status.task_id}")
    return notebook.id, status.task_id


async def try_download_podcast(
    client: NotebookLMClient,
    notebook_id: str,
    task_id: str,
    output_dir: Path,
    wait: bool = True,
) -> Path | None:
    """Check if audio is ready and download it.

    If wait=True: sleep 10 min, then poll every 60s up to 20 min.
    If wait=False: single poll (for retrying pending notebooks).
    Returns path to .mp4 or None if not ready yet.
    """
    if wait:
        logger.info(f"Waiting {INITIAL_WAIT // 60} min before first check...")
        await asyncio.sleep(INITIAL_WAIT)

    try:
        result = await client.artifacts.wait_for_completion(
            notebook_id,
            task_id,
            initial_interval=POLL_INTERVAL,
            max_interval=POLL_INTERVAL,
            timeout=POLL_TIMEOUT if wait else POLL_INTERVAL * 2,
        )
    except (TimeoutError, RPCTimeoutError):
        logger.info(f"Notebook {notebook_id} still generating, will retry later")
        return None

    if result.is_failed:
        raise RuntimeError(
            f"Audio generation failed: {result.error or 'unknown error'}"
        )
    if not result.is_complete:
        return None

    output_path = output_dir / f"{notebook_id}.mp4"
    await client.artifacts.download_audio(
        notebook_id, output_path=str(output_path)
    )
    return output_path


async def cleanup_notebook(client: NotebookLMClient, notebook_id: str) -> None:
    """Delete a notebook. Logs but doesn't raise on failure."""
    try:
        await client.notebooks.delete(notebook_id)
        logger.info(f"Cleaned up notebook {notebook_id}")
    except Exception as e:
        logger.warning(f"Failed to clean up notebook {notebook_id}: {e}")


def convert_to_mp3(mp4_path: Path) -> Path:
    """Convert mp4 to mp3 via ffmpeg. Raises on failure."""
    mp3_path = mp4_path.with_suffix(".mp3")
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(mp4_path), "-q:a", "2", str(mp3_path)],
        check=True,
        capture_output=True,
    )
    return mp3_path
