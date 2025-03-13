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

        # Create a new table for YouTube channel data
        cs.execute("""
            CREATE TABLE IF NOT EXISTS youtube_channel_data (
                channelName STRING,
                subscribers STRING,
                views STRING,
                totalVideos STRING,
                publishedAt STRING
            )
        """)

        api_key = 'AIzaSyDF3YtxwYsc3X69585lLc3wvjNKTkSlT_I'  
        channel_ids = [
            'UCsvqVGtbbyHaMoevxPAq9Fg', 'UCnfZSN7A09wNwYiUoincXZg', 'UCBJycsmduvYEL83R_U4JriQ'
            'UCBwmMxybNva6P_5VmxjzwqA', 'UCaBNj5bfIpRGuEx3k3ekNoA', 'UCkWbqlDAyJh2n8DN5X6NZyg',
            'UCh9nVJoWXmFb7sLApWGcLPQ', 'UCkAGrHCLFmlK3H2kd6isipg', 'UCjXd5MAsvEnCbzKlXdCYYKw',
            'UCmqfX0S3x0I3uwLkPdpX03w', 'UCWOA1ZGywLbqmigxE4Qlvuw', 'UCckHqySbfy5FcPP6MD_S-Yg',
            'UCq-Fj5jknLsUf-MWSy4_brA', 'UCJ5v_MCY6GNUBTO8-D3XoAg', 'UCiGm_E4ZwYSHV3bcW1pnSeQ',
            'UCyoXW-Dse7fURq30EWl_CUA', 'UCX6OQ3DkcsbYNE6H8uQQuVA', 'UCOQNJjhXwvAScuELTT_i7cQ',
            'UCIxLxlan8q9WA7sjuq6LdTQ', 'UCIsEhwBMPkRHsEgqYAPQHsA', 'UCEGC6iQjjJNCLkvLdi12BIg',
            'UCppHT7SZKKvar4Oc9J4oljQ', 'UC56gTxNs4f9xZ7Pa2i5xNzg', 'UCIwFjwMjI0y7PDBVEO9-bkQ',
            'UCqECaJ8Gagnn7YCbPEzWH6g', 'UCDhrZyeG5YqRpRCk5xxiErg','UCjm_qVkCPjOVDz9BWjNqO9A',
            'UCrmsp2voP5agAXWHvEPvxsg'
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
                        'channelName': item['snippet']['title'],
                        'subscribers': item['statistics']['subscriberCount'],
                        'views': item['statistics']['viewCount'],
                        'totalVideos': item['statistics']['videoCount'],
                        'publishedAt': item['snippet']['publishedAt']
                    }
                    all_data.append(data)
            
            df = pd.DataFrame(all_data)
            return df

        # Fetch channel and insert into Snowflake
        df = get_channel_stats(api_key, channel_ids)
        for index, row in df.iterrows():
            cs.execute("""
                INSERT INTO youtube_channel_data (channelName, subscribers, views, totalVideos, publishedAt)
                VALUES (%s, %s, %s, %s, %s)
            """, (row['channelName'], row['subscribers'], row['views'], row['totalVideos'], row['publishedAt']))

        cs.execute("SELECT * FROM youtube_channel_data")
        for row in cs.fetchall():
            print(row)
finally:
        cs.close()
connection.close()
