import snowflake.connector
import requests
import pandas as pd

connection = snowflake.connector.connect(
    user='PROJECT',
    password='Project@1234567',
    account='FZKREEM-OJB05768',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN'
)

cs = connection.cursor()
try:
    cs.execute("CREATE DATABASE IF NOT EXISTS API_Project")
    cs.execute("USE DATABASE API_Project")

    cs.execute("CREATE SCHEMA IF NOT EXISTS YT_DATA")
    cs.execute("USE SCHEMA YT_DATA")

    cs.execute("""
        CREATE TABLE IF NOT EXISTS youtube_video_data (
            videoId STRING,
            publishedAt STRING,
            channelTitle STRING,
            channelId STRING,
            videoTitle STRING,
            views STRING,
            likes STRING
        )
    """)

    # YouTube API code
    api_key = 'AIzaSyC41Mb1yBOBA6qX8ahORhWNhMkInIGjCgE'  
    channel_ids = [
         'UCaBNj5bfIpRGuEx3k3ekNoA',
                        'UCnfZSN7A09wNwYiUoincXZg'
    ]

    def get_channel_videos(api_key, channel_ids):
        all_data = []
        for channel_id in channel_ids:
            # Get uploads playlist ID for the channel
            channel_url = "https://www.googleapis.com/youtube/v3/channels"
            channel_params = {
                'key': api_key,
                'part': 'contentDetails',
                'id': channel_id
            }
            response = requests.get(channel_url, params=channel_params).json()

            # Get the uploads playlist ID
            uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

            # Get videos from the uploads playlist
            next_page_token = None
            while True:
                playlist_url = "https://www.googleapis.com/youtube/v3/playlistItems"
                playlist_params = {
                    'key': api_key,
                    'part': 'snippet',
                    'playlistId': uploads_playlist_id,
                }

                if next_page_token:
                    playlist_params['pageToken'] = next_page_token

                playlist_response = requests.get(playlist_url, params=playlist_params).json()

                # Extract video IDs
                for item in playlist_response.get('items', []):
                    video_id = item['snippet']['resourceId']['videoId']

                    # Get detailed information for each video
                    video_url = "https://www.googleapis.com/youtube/v3/videos"
                    video_params = {
                        'key': api_key,
                        'part': 'snippet,contentDetails,statistics',
                        'id': video_id
                    }

                    video_response = requests.get(video_url, params=video_params).json()

                    # Loop through items 
                    for video_item in video_response.get('items', []):
                        data = {
                            'videoId': video_item['id'],
                            'publishedAt': video_item['snippet']['publishedAt'],
                            'channelTitle': video_item['snippet']['channelTitle'],
                            'channelId': video_item['snippet']['channelId'],
                            'videoTitle': video_item['snippet']['title'],
                            'views': video_item['statistics'].get('viewCount', '0'),
                            'likes': video_item['statistics'].get('likeCount', '0'),
                        }
                        all_data.append(data)

                # Check if there are more pages
                next_page_token = playlist_response.get('nextPageToken')
                if not next_page_token:
                    break

        # Convert to DataFrame and return
        pd.set_option('display.max_columns', None)
        df = pd.DataFrame(all_data)
        return df

    # Fetch video data and insert into Snowflake
    df = get_channel_videos(api_key, channel_ids)
    for index, row in df.iterrows():
        cs.execute("""
            INSERT INTO youtube_video_data (videoId, publishedAt, channelTitle, channelId, videoTitle, views, likes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (row['videoId'], row['publishedAt'], row['channelTitle'], row['channelId'], row['videoTitle'], row['views'], row['likes']))

    # Verify the inserted data
    cs.execute("SELECT * FROM youtube_video_data")
    for row in cs.fetchall():
        print(row)
finally:
    cs.close()
connection.close()
