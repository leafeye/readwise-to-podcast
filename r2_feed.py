"""Cloudflare R2 upload and RSS feed generation."""

import html
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from feedgen.feed import FeedGenerator

from state import Episode

logger = logging.getLogger(__name__)

R2_FEED_KEY = "feed.xml"
R2_ARTWORK_KEY = "artwork.jpg"


def get_r2_client():
    """Create an S3-compatible client for Cloudflare R2."""
    account_id = os.environ["R2_ACCOUNT_ID"]
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def upload_file(
    client, bucket: str, local_path: Path, r2_key: str, content_type: str
) -> None:
    """Upload a local file to R2."""
    logger.info(f"Uploading {local_path.name} to {r2_key}...")
    client.upload_file(
        str(local_path),
        bucket,
        r2_key,
        ExtraArgs={"ContentType": content_type},
    )


def _artwork_exists(client, bucket: str) -> bool:
    """Check if podcast artwork exists on R2."""
    try:
        client.head_object(Bucket=bucket, Key=R2_ARTWORK_KEY)
        return True
    except ClientError:
        return False


def _build_show_notes(ep: Episode) -> tuple[str, str]:
    """Build HTML and plain-text show notes from episode metadata.

    Returns (html_description, plain_text_summary).
    """
    summary = html.escape(ep.description or f"Podcast van: {ep.title}")

    # HTML version (for <description>)
    html_parts = [f"<p>{summary}</p>"]
    if ep.author and ep.author != "Unknown":
        html_parts.append(f"<p>Auteur: {html.escape(ep.author)}</p>")
    if ep.source_url:
        html_parts.append(
            f'<p><a href="{html.escape(ep.source_url)}">Lees het originele artikel</a></p>'
        )
    html_desc = "\n".join(html_parts)

    # Plain-text version (for <itunes:summary>)
    text_parts = [ep.description or f"Podcast van: {ep.title}"]
    if ep.author and ep.author != "Unknown":
        text_parts.append(f"Auteur: {ep.author}")
    if ep.source_url:
        text_parts.append(f"Bron: {ep.source_url}")
    plain_text = "\n".join(text_parts)

    return html_desc, plain_text


def generate_and_upload_feed(
    client, bucket: str, public_url: str, episodes: list[Episode]
) -> None:
    """Generate RSS feed from episodes and upload to R2."""
    fg = FeedGenerator()
    fg.load_extension("podcast")

    feed_url = f"{public_url}/{R2_FEED_KEY}"
    fg.id(feed_url)
    fg.title("Readwise Podcast")
    fg.description("AI-gegenereerde Nederlandse podcast van Readwise-artikelen.")
    fg.link(href=feed_url, rel="self")
    fg.language("nl")

    fg.podcast.itunes_author("Levi")
    fg.podcast.itunes_summary(
        "AI-gegenereerde Nederlandse podcast van Readwise-artikelen."
    )
    fg.podcast.itunes_category("Technology")
    fg.podcast.itunes_explicit("no")

    # Artwork (optional — only added if the file exists on R2)
    if _artwork_exists(client, bucket):
        artwork_url = f"{public_url}/{R2_ARTWORK_KEY}"
        fg.podcast.itunes_image(artwork_url)
        fg.image(url=artwork_url, title="Readwise Podcast", link=feed_url)
        logger.info("Artwork found, adding to feed")
    else:
        logger.info("No artwork on R2, skipping artwork tags")

    # Add episodes in reverse chronological order
    for ep in sorted(episodes, key=lambda e: e.pub_date, reverse=True):
        mp3_url = f"{public_url}/{ep.r2_key}"
        fe = fg.add_entry()
        fe.id(mp3_url)
        fe.link(href=ep.source_url or mp3_url)
        fe.title(ep.title)

        html_desc, plain_text = _build_show_notes(ep)
        fe.description(html_desc)
        fe.podcast.itunes_summary(plain_text)

        fe.published(datetime.fromisoformat(ep.pub_date).replace(tzinfo=timezone.utc))
        fe.enclosure(mp3_url, str(ep.file_size), "audio/mpeg")
        if ep.author:
            fe.podcast.itunes_author(ep.author)

    feed_xml = fg.rss_str(pretty=True)

    logger.info("Uploading feed.xml...")
    client.put_object(
        Bucket=bucket,
        Key=R2_FEED_KEY,
        Body=feed_xml,
        ContentType="application/rss+xml",
    )
    logger.info(f"Feed updated with {len(episodes)} episode(s)")
