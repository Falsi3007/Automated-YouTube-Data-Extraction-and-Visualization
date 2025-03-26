import snowflake.connector
import requests
import pandas as pd

from dotenv import dotenv_values
my_secrets = dotenv_values(".env")

# Establish the connection
connection = snowflake.connector.connect(
    user=my_secrets['USER'],
    password=my_secrets['PASSWORD'],
    account=my_secrets['ACCOUNT'],
    warehouse=my_secrets['WAREHOUSE'],
    role=my_secrets['ROLE']
)

cs = connection.cursor()
try:
    cs.execute("CREATE DATABASE IF NOT EXISTS API_Project")
    cs.execute("USE DATABASE API_Project")

    cs.execute("CREATE SCHEMA IF NOT EXISTS YT_DATA")
    cs.execute("USE SCHEMA YT_DATA")

    # Create a new table for YouTube channel data
    cs.execute("""
        CREATE TABLE IF NOT EXISTS youtube_channel_data (
            channelId STRING PRIMARY KEY,
            channelName STRING,
            subscribers STRING,
            views STRING,
            totalVideos STRING,
            publishedAt STRING
        )
    """)

    api_key = 'AIzaSyDF3YtxwYsc3X69585lLc3wvjNKTkSlT_I'
    channel_ids = [
        'UCDhrZyeG5YqRpRCk5xxiErg'
    ]

    def get_channel_stats(api_key, channel_ids):
        all_data = []
        base_url = 'https://www.googleapis.com/youtube/v3/channels'

        for channel_id in channel_ids:
            params = {
                'part': 'snippet,statistics',
                'id': channel_id,
                'key': api_key
            }
            response = requests.get(base_url, params=params).json()

            for item in response.get('items', []):
                data = {
                    'channelId': item['id'],
                    'channelName': item['snippet']['title'],
                    'subscribers': item['statistics']['subscriberCount'],
                    'views': item['statistics']['viewCount'],
                    'totalVideos': item['statistics']['videoCount'],
                    'publishedAt': item['snippet']['publishedAt']
                }
                all_data.append(data)

        df = pd.DataFrame(all_data)
        return df

    # Fetch channel data and insert into Snowflake
    df = get_channel_stats(api_key, channel_ids)
    for index, row in df.iterrows():
        # Check if the channelId already exists
        cs.execute("SELECT COUNT(*) FROM youtube_channel_data WHERE channelId = %s", (row['channelId'],))
        count = cs.fetchone()[0]
        
        if count == 0:
            cs.execute("""
                INSERT INTO youtube_channel_data (channelId, channelName, subscribers, views, totalVideos, publishedAt)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (row['channelId'], row['channelName'], row['subscribers'], row['views'], row['totalVideos'], row['publishedAt']))
        else:
            cs.execute("""
                UPDATE youtube_channel_data
                SET channelName = %s, subscribers = %s, views = %s, totalVideos = %s, publishedAt = %s
                WHERE channelId = %s
            """, (row['channelName'], row['subscribers'], row['views'], row['totalVideos'], row['publishedAt'], row['channelId']))

    cs.execute("SELECT * FROM youtube_channel_data")
    for row in cs.fetchall():
        print(row)
finally:
    cs.close()
connection.close()
