import requests
import pandas as pd

api_key = 'AIzaSyC41Mb1yBOBA6qX8ahORhWNhMkInIGjCgE'
channel_ids = ['UCsvqVGtbbyHaMoevxPAq9Fg', 'UCnfZSN7A09wNwYiUoincXZg']

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
                    'ChannelId': channel_id,
                    'PlaylistId': item['id'],
                    'PlaylistTitle': item['snippet']['title'],
                    'TotalVideos': item['contentDetails']['itemCount']
                }
                all_data.append(data)
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
    
    pd.set_option('display.max_columns', None)
    df = pd.DataFrame(all_data)
    print(df)

# Call the function to get output
get_channel_playlists(api_key, channel_ids)
