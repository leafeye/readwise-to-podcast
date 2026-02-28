"""Readwise Reader API v3 client for fetching articles."""

import asyncio
import logging
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

API_BASE = "https://readwise.io/api/v3/list/"


@dataclass
class Article:
    id: str
    title: str
    author: str
    source_url: str | None
    summary: str
    content: str | None
    updated_at: str


async def fetch_new_articles(
    token: str, updated_after: str | None = None
) -> list[Article]:
    """Fetch new articles from Readwise Reader API.

    Handles pagination via nextPageCursor and rate limiting (429 + Retry-After).
    """
    articles: list[Article] = []
    params: dict = {"category": "article", "withHtmlContent": "true"}
    if updated_after:
        params["updatedAfter"] = updated_after

    async with httpx.AsyncClient(timeout=30.0) as client:
        headers = {"Authorization": f"Token {token}"}
        next_cursor: str | None = None

        while True:
            if next_cursor:
                params["pageCursor"] = next_cursor

            response = await _request_with_retry(client, headers, params)
            data = response.json()

            for item in data.get("results", []):
                articles.append(
                    Article(
                        id=item["id"],
                        title=item.get("title") or "Untitled",
                        author=item.get("author") or "Unknown",
                        source_url=item.get("source_url"),
                        summary=item.get("summary") or "",
                        content=item.get("html_content"),
                        updated_at=item.get("updated_at", ""),
                    )
                )

            next_cursor = data.get("nextPageCursor")
            if not next_cursor:
                break

    return articles


async def _request_with_retry(
    client: httpx.AsyncClient,
    headers: dict,
    params: dict,
    max_retries: int = 3,
) -> httpx.Response:
    """Make API request with retry on 429 (rate limit)."""
    for attempt in range(max_retries):
        response = await client.get(API_BASE, headers=headers, params=params)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            logger.warning(f"Rate limited, waiting {retry_after}s...")
            await asyncio.sleep(retry_after)
            continue

        response.raise_for_status()
        return response

    raise httpx.HTTPStatusError(
        "Rate limit exceeded after retries",
        request=response.request,
        response=response,
    )
