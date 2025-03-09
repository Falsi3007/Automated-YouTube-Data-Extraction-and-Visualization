from googleapiclient.discovery import build
import pandas as pd

api_key = 'AIzaSyCCDAGy22AI0NOZ-qUdmQeAefnrIV63lQ0'
channel_ids = ['UUDhrZyeG5YqRpRCk5xxiErg']  # Replace with VALID channel IDs

youtube = build("youtube", "v3", developerKey=api_key)

def get_videos_by_channel(channel_id):
    """Fetches video IDs from a given channel."""
    if not channel_id.startswith("UC"):  # Check for valid channel ID
        print(f"Skipping invalid channel ID: {channel_id}")
        return []

    videos, next_page = [], None
    try:
        while True:
            res = youtube.search().list(
                part="id", channelId=channel_id, maxResults=10, type="video"
            ).execute()
            videos.extend([item['id']['videoId'] for item in res.get('items', [])])
            next_page = res.get('nextPageToken')
            if not next_page: break
    except Exception as e:
        print(f"Error fetching videos for channel {channel_id}: {e}")
    return videos

# Test with one known working channel ID
test_channel = 'UCX6OQ3DkcsbYNE6H8uQQuVA'  # Replace with a valid channel ID
video_ids = get_videos_by_channel(test_channel)
print("Fetched video IDs:", video_ids)
