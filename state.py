"""State and episode management with atomic JSON writes."""

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

STATE_FILE = Path("state.json")
EPISODES_FILE = Path("episodes.json")


@dataclass
class PendingNotebook:
    """A notebook where audio generation was started but not yet downloaded."""
    article_id: str
    notebook_id: str
    task_id: str
    title: str
    author: str
    summary: str
    source_url: str
    started_at: str


@dataclass
class State:
    last_run: str | None = None
    processed_articles: dict[str, str] = field(default_factory=dict)
    pending_notebooks: list[PendingNotebook] = field(default_factory=list)


@dataclass
class Episode:
    article_id: str
    title: str
    author: str
    r2_key: str
    description: str
    source_url: str
    pub_date: str
    file_size: int


def _atomic_write(path: Path, data) -> None:
    """Write JSON atomically: write to .tmp, then os.replace."""
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, path)


def load_state() -> State:
    if not STATE_FILE.exists():
        return State()
    data = json.loads(STATE_FILE.read_text())
    articles = data.get("processed_articles", [])
    pending = [PendingNotebook(**p) for p in data.get("pending_notebooks", [])]

    # Migrate list[str] → dict[str, str] (article_id → timestamp)
    if isinstance(articles, list):
        now = datetime.now(timezone.utc).isoformat()
        articles = {aid: now for aid in articles}

    return State(
        last_run=data.get("last_run"),
        processed_articles=articles,
        pending_notebooks=pending,
    )


def save_state(state: State) -> None:
    _atomic_write(STATE_FILE, asdict(state))


def is_processed(state: State, article_id: str) -> bool:
    return article_id in state.processed_articles


def mark_processed(state: State, article_id: str) -> None:
    if article_id not in state.processed_articles:
        state.processed_articles[article_id] = datetime.now(timezone.utc).isoformat()


def cleanup_processed_articles(state: State, max_age_days: int) -> int:
    """Remove processed_articles entries older than max_age_days. Returns count removed."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    to_remove = [
        aid for aid, ts in state.processed_articles.items()
        if datetime.fromisoformat(ts) < cutoff
    ]
    for aid in to_remove:
        del state.processed_articles[aid]
    if to_remove:
        logger.info(f"Cleaned up {len(to_remove)} old processed article(s) from state")
    return len(to_remove)


def _migrate_mp3_url(item: dict) -> dict:
    """Migrate old mp3_url (full URL) to r2_key (relative path)."""
    if "mp3_url" in item and "r2_key" not in item:
        url = item.pop("mp3_url")
        # Extract relative path: "https://pub-xxx.r2.dev/episodes/abc.mp3" → "episodes/abc.mp3"
        item["r2_key"] = url.split("/", 3)[-1] if "/" in url else url
    return item


def load_episodes() -> list[Episode]:
    if not EPISODES_FILE.exists():
        return []
    data = json.loads(EPISODES_FILE.read_text())
    # Dedup on article_id (keep last occurrence)
    seen: dict[str, Episode] = {}
    for item in data:
        ep = Episode(**_migrate_mp3_url(item))
        seen[ep.article_id] = ep
    return list(seen.values())


def save_episodes(episodes: list[Episode]) -> None:
    _atomic_write(EPISODES_FILE, [asdict(e) for e in episodes])
