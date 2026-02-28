"""Cloudflare R2 upload and RSS feed generation."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from feedgen.feed import FeedGenerator

from state import Episode

logger = logging.getLogger(__name__)

R2_FEED_KEY = "feed.xml"


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

    # Add episodes in reverse chronological order
    for ep in sorted(episodes, key=lambda e: e.pub_date, reverse=True):
        mp3_url = f"{public_url}/{ep.r2_key}"
        fe = fg.add_entry()
        fe.id(mp3_url)
        fe.link(href=ep.source_url or mp3_url)
        fe.title(ep.title)
        fe.description(ep.description or f"Podcast van: {ep.title}")
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
