import requests
import pandas as pd

api_key = 'AIzaSyDF3YtxwYsc3X69585lLc3wvjNKTkSlT_I'
channel_ids = [
    'UCsvqVGtbbyHaMoevxPAq9Fg', 'UCnfZSN7A09wNwYiUoincXZg', 'UCBJycsmduvYEL83R_U4JriQ',
    'UCBwmMxybNva6P_5VmxjzwqA', 'UCaBNj5bfIpRGuEx3k3ekNoA', 'UCkWbqlDAyJh2n8DN5X6NZyg',
    'UCh9nVJoWXmFb7sLApWGcLPQ', 'UCkAGrHCLFmlK3H2kd6isipg', 'UCjXd5MAsvEnCbzKlXdCYYKw',
    'UCmqfX0S3x0I3uwLkPdpX03w', 'UCWOA1ZGywLbqmigxE4Qlvuw', 'UCckHqySbfy5FcPP6MD_S-Yg',
    'UCq-Fj5jknLsUf-MWSy4_brA', 'UCJ5v_MCY6GNUBTO8-D3XoAg', 'UCiGm_E4ZwYSHV3bcW1pnSeQ',
    'UCyoXW-Dse7fURq30EWl_CUA', 'UCX6OQ3DkcsbYNE6H8uQQuVA', 'UCOQNJjhXwvAScuELTT_i7cQ',
    'UCIxLxlan8q9WA7sjuq6LdTQ', 'UCIsEhwBMPkRHsEgqYAPQHsA', 'UCEGC6iQjjJNCLkvLdi12BIg',
    'UCppHT7SZKKvar4Oc9J4oljQ', 'UC56gTxNs4f9xZ7Pa2i5xNzg', 'UCIwFjwMjI0y7PDBVEO9-bkQ',
    'UCqECaJ8Gagnn7YCbPEzWH6g', 'UCDhrZyeG5YqRpRCk5xxiErg'
]

def get_channel_stats(api_key, channel_ids):
    all_data = []
    base_url = 'https://www.googleapis.com/youtube/v3/channels'
    
    for channel_id in channel_ids:
        params = {
            'part': 'snippet,contentDetails,statistics',
            'id': channel_id,
            'key': api_key
        }
        response = requests.get(base_url, params=params).json()
        
        for item in response.get('items', []):
            data = {
                'publishedAt': item['snippet']['publishedAt'],
                'channelName': item['snippet']['title'],
                'subscribers': item['statistics']['subscriberCount'],
                'views': item['statistics']['viewCount'],
                'totalVideos': item['statistics']['videoCount'],
                # 'channelId': item['contentDetails']['relatedPlaylists']['uploads'],
            }
            all_data.append(data)
    
    # Convert to DataFrame and print
    pd.set_option('display.max_columns', None)
    df = pd.DataFrame(all_data)
    print(df)

# Call the function to get output
get_channel_stats(api_key, channel_ids)
