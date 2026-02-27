"""NotebookLM integration for podcast generation."""

import logging
import subprocess
from pathlib import Path

from notebooklm import NotebookLMClient

logger = logging.getLogger(__name__)


async def generate_podcast(
    client: NotebookLMClient,
    title: str,
    source_url: str,
    output_dir: Path,
    language: str = "nl",
) -> Path:
    """Create notebook, add URL source, generate audio, download, cleanup.

    Returns path to downloaded .mp4 file.
    """
    notebook = await client.notebooks.create(f"Podcast: {title}")
    try:
        await client.sources.add_url(notebook.id, source_url, wait=True)

        status = await client.artifacts.generate_audio(
            notebook.id, language=language
        )
        await client.artifacts.wait_for_completion(
            notebook.id, status.task_id, timeout=600.0
        )

        output_path = output_dir / f"{notebook.id}.mp4"
        await client.artifacts.download_audio(
            notebook.id, output_path=str(output_path)
        )
        return output_path
    finally:
        await client.notebooks.delete(notebook.id)


def convert_to_mp3(mp4_path: Path) -> Path:
    """Convert mp4 to mp3 via ffmpeg. Raises on failure."""
    mp3_path = mp4_path.with_suffix(".mp3")
    subprocess.run(
        ["ffmpeg", "-i", str(mp4_path), "-q:a", "2", str(mp3_path)],
        check=True,
        capture_output=True,
    )
    return mp3_path
