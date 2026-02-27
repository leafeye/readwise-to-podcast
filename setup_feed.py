"""Upload test episode and generate RSS feed for Cloudflare R2."""

import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from feedgen.feed import FeedGenerator

EPISODE_FILE = Path("test-podcast.mp3")
R2_EPISODES_PREFIX = "episodes/"
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


def upload_episode(client, bucket: str) -> int:
    """Upload test-podcast.mp3 to R2 and return file size in bytes."""
    key = f"{R2_EPISODES_PREFIX}{EPISODE_FILE.name}"
    file_size = EPISODE_FILE.stat().st_size

    print(f"Uploading {EPISODE_FILE} ({file_size / 1_000_000:.1f} MB) to {key}...")
    client.upload_file(
        str(EPISODE_FILE),
        bucket,
        key,
        ExtraArgs={"ContentType": "audio/mpeg"},
    )
    print("Upload complete.")
    return file_size


def generate_feed(public_url: str, episode_size: int) -> bytes:
    """Generate a podcast RSS feed with one test episode."""
    fg = FeedGenerator()
    fg.load_extension("podcast")

    # Feed metadata
    fg.id(f"{public_url}/{R2_FEED_KEY}")
    fg.title("Readwise Podcast")
    fg.description("AI-gegenereerde Nederlandse podcast van Readwise-artikelen.")
    fg.link(href=f"{public_url}/{R2_FEED_KEY}", rel="self")
    fg.language("nl")

    # iTunes / podcast metadata
    fg.podcast.itunes_author("Levi")
    fg.podcast.itunes_summary(
        "AI-gegenereerde Nederlandse podcast van Readwise-artikelen."
    )
    fg.podcast.itunes_category("Technology")
    fg.podcast.itunes_explicit("no")

    # Test episode
    episode_url = f"{public_url}/{R2_EPISODES_PREFIX}{EPISODE_FILE.name}"
    fe = fg.add_entry()
    fe.id(episode_url)
    fe.title("Test Episode — Readwise Podcast PoC")
    fe.description("Proof of concept: eerste automatisch gegenereerde aflevering.")
    fe.published(datetime(2026, 2, 27, tzinfo=timezone.utc))
    fe.enclosure(episode_url, str(episode_size), "audio/mpeg")
    fe.load_extension("podcast")
    fe.podcast.itunes_duration("24:00")

    return fg.rss_str(pretty=True)


def upload_feed(client, bucket: str, feed_xml: bytes):
    """Upload feed.xml to R2."""
    print(f"Uploading {R2_FEED_KEY}...")
    client.put_object(
        Bucket=bucket,
        Key=R2_FEED_KEY,
        Body=feed_xml,
        ContentType="application/rss+xml",
    )
    print("Feed uploaded.")


def main():
    # Validate environment
    for var in [
        "R2_ACCOUNT_ID",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET_NAME",
        "R2_PUBLIC_URL",
    ]:
        if not os.environ.get(var):
            raise SystemExit(f"Missing environment variable: {var}")

    bucket = os.environ["R2_BUCKET_NAME"]
    public_url = os.environ["R2_PUBLIC_URL"].rstrip("/")

    if not EPISODE_FILE.exists():
        raise SystemExit(f"{EPISODE_FILE} not found — run from project root.")

    client = get_r2_client()

    # 1. Upload episode
    episode_size = upload_episode(client, bucket)

    # 2. Generate and upload feed
    feed_xml = generate_feed(public_url, episode_size)
    upload_feed(client, bucket, feed_xml)

    # 3. Print verification URLs
    print(f"\n--- Verification ---")
    print(f"Episode: {public_url}/{R2_EPISODES_PREFIX}{EPISODE_FILE.name}")
    print(f"Feed:    {public_url}/{R2_FEED_KEY}")
    print(f"\nValidate feed: https://castfeedvalidator.com/?url={public_url}/{R2_FEED_KEY}")


if __name__ == "__main__":
    main()
