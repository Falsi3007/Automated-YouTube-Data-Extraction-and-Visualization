import snowflake.connector
import requests
import pandas as pd

# Establish the connection
connection = snowflake.connector.connect(
        user='PROJECT',
        password='Project@123456',
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

        # Create a new table for YouTube video data
        cs.execute("""
            CREATE OR REPLACE TABLE youtube_video_data (
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
        api_key = 'AIzaSyC41Mb1yBOBA6qX8ahORhWNhMkInIGjCgE'  # Hardcoded API key
        channel_id = 'UCDhrZyeG5YqRpRCk5xxiErg'

        def get_channel_videos(api_key, channel_id):
            # First, get all video IDs from the channel
            video_ids = []
           
            # Get uploads playlist ID for the channel
            channel_url = f"https://www.googleapis.com/youtube/v3/channels"
            channel_params = {
                'key': api_key,
                'part': 'contentDetails',
                'id': channel_id
            }
           
            response = requests.get(channel_url, params=channel_params).json()
           
            if 'items' not in response or not response['items']:
                print(f"No channel found with ID: {channel_id}")
                return pd.DataFrame()
           
            # Get the uploads playlist ID
            uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
           
            # Get videos from the uploads playlist
            next_page_token = None
            while True:
                playlist_url = f"https://www.googleapis.com/youtube/v3/playlistItems"
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
                    video_ids.append(item['snippet']['resourceId']['videoId'])
               
                # Check if there are more pages
                next_page_token = playlist_response.get('nextPageToken')
                if not next_page_token:
                    break
           
            # Now get detailed information for each video individually
            all_data = []
           
            # Process videos one by one
            for video_id in video_ids:
                video_url = f"https://www.googleapis.com/youtube/v3/videos"
                video_params = {
                    'key': api_key,
                    'part': 'snippet,contentDetails,statistics',
                    'id': video_id
                }
               
                response = requests.get(video_url, params=video_params).json()
               
                # Loop through items (should be just one item)
                for item in response.get('items', []):
                    data = {
                        'videoId': item['id'],
                        'publishedAt': item['snippet']['publishedAt'],
                        'channelTitle': item['snippet']['channelTitle'],
                        'channelId': item['snippet']['channelId'],
                        'videoTitle': item['snippet']['title'],
                        'views': item['statistics'].get('viewCount', '0'),
                        'likes': item['statistics'].get('likeCount', '0'),
                    }
                    all_data.append(data)
           
            # Convert to DataFrame and return
            pd.set_option('display.max_columns', None)
            df = pd.DataFrame(all_data)
            return df

        # Fetch video data and insert into Snowflake
        df = get_channel_videos(api_key, channel_id)
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
