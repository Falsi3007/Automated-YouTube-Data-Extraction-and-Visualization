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

        # Create a new table for YouTube playlist data
        cs.execute("""
            CREATE TABLE IF NOT EXISTS youtube_playlist_data (
                ChannelName STRING,
                PlaylistId STRING,
                PlaylistTitle STRING,
                TotalVideos INT
            )
        """)

        api_key = 'AIzaSyC41Mb1yBOBA6qX8ahORhWNhMkInIGjCgE'  
        channel_ids = [
        # 'UCaBNj5bfIpRGuEx3k3ekNoA'
                        'UCnfZSN7A09wNwYiUoincXZg']

        def get_channel_playlists(api_key, channel_ids):
            all_data = []
            base_url = 'https://www.googleapis.com/youtube/v3/playlists'
            
            for channel_id in channel_ids:
                next_page_token = None
                
                while True:
                    params = {
                        'part': 'snippet,contentDetails',
                        'channelId': channel_id,
                        'key': api_key,
                        'pageToken': next_page_token
                    }
                    response = requests.get(base_url, params=params).json()
                    
                    for item in response.get('items', []):
                        data = {
                            'ChannelName': item['snippet']['channelTitle'],
                            'PlaylistId': item['id'],
                            'PlaylistTitle': item['snippet']['title'],
                            'TotalVideos': item['contentDetails']['itemCount']
                        }
                        all_data.append(data)
                    
                    next_page_token = response.get('nextPageToken')
                    if not next_page_token:
                        break
            
            df = pd.DataFrame(all_data)
            return df

        # Fetch playlist and insert into Snowflake
        df = get_channel_playlists(api_key, channel_ids)
        for index, row in df.iterrows():
            cs.execute("""
                INSERT INTO youtube_playlist_data (ChannelName, PlaylistId, PlaylistTitle, TotalVideos)
                VALUES (%s, %s, %s, %s)
            """, (row['ChannelName'], row['PlaylistId'], row['PlaylistTitle'], row['TotalVideos']))

        cs.execute("SELECT * FROM youtube_playlist_data")
        for row in cs.fetchall():
            print(row)
finally:
        cs.close()
connection.close()
