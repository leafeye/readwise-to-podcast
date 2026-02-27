"""State and episode management with atomic JSON writes."""

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

STATE_FILE = Path("state.json")
EPISODES_FILE = Path("episodes.json")


@dataclass
class State:
    last_run: str | None = None
    processed_articles: list[str] = field(default_factory=list)


@dataclass
class Episode:
    article_id: str
    title: str
    author: str
    mp3_url: str
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
    return State(
        last_run=data.get("last_run"),
        processed_articles=data.get("processed_articles", []),
    )


def save_state(state: State) -> None:
    _atomic_write(STATE_FILE, asdict(state))


def is_processed(state: State, article_id: str) -> bool:
    return article_id in state.processed_articles


def mark_processed(state: State, article_id: str) -> None:
    if article_id not in state.processed_articles:
        state.processed_articles.append(article_id)


def load_episodes() -> list[Episode]:
    if not EPISODES_FILE.exists():
        return []
    data = json.loads(EPISODES_FILE.read_text())
    # Dedup on article_id (keep last occurrence)
    seen: dict[str, Episode] = {}
    for item in data:
        ep = Episode(**item)
        seen[ep.article_id] = ep
    return list(seen.values())


def save_episodes(episodes: list[Episode]) -> None:
    _atomic_write(EPISODES_FILE, [asdict(e) for e in episodes])
