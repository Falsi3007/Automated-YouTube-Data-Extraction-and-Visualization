from googleapiclient.discovery import build
import pandas as pd
 
api_key = 'AIzaSyC41Mb1yBOBA6qX8ahORhWNhMkInIGjCgE'
 
channel_ids = ['UCsvqVGtbbyHaMoevxPAq9Fg', 'UCnfZSN7A09wNwYiUoincXZg']
 
api_service_name = "youtube"
api_version = "v3"
 
youtube = build(api_service_name, api_version, developerKey=api_key)
 
def get_channel_playlists(youtube, channel_ids):
    all_data = []
   
    for channel_id in channel_ids:
        next_page_token = None
       
        while True:
            response = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                pageToken=next_page_token
            ).execute()
           
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
   
get_channel_playlists(youtube, channel_ids)
