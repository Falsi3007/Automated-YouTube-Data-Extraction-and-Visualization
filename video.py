from googleapiclient.discovery import build
import pandas as pd

api_key = 'AIzaSyC41Mb1yBOBA6qX8ahORhWNhMkInIGjCgE'

video_ids = ['xxkcqWK1FeI', 'ZTPrbAKmcdo', 'FVWCCmtJ46A', 'PQBzmddEao4', 'qQV2rc_klQo', 
             'EZSjU33FKiU', 'GR4qF-skGa0', 'NHoKRqrNTyA', 'ZFf_WpQzQ7Q']

api_service_name = "youtube"
api_version = "v3"

# Get credentials and create an API client
youtube = build(api_service_name, api_version, developerKey=api_key)

def get_video_stats(youtube, video_ids):    
    all_data = []
    
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=','.join(video_ids)
    )
    response = request.execute()

    # Loop through items
    for item in response.get('items', []):  # Use .get() to avoid KeyError
        data = {
            'videoId': item['id'],
            'publishedAt': item['snippet']['publishedAt'],
            'channelTitle': item['snippet']['channelTitle'],
            'channelId': item['snippet']['channelId'],
            'videoTitle': item['snippet']['title'],
            'views': item['statistics'].get('viewCount', '0'),
            'likes': item['statistics'].get('likeCount', '0'),
            'comments': item['statistics'].get('commentCount', '0'),
            # 'duration': item['contentDetails']['duration'],
        }
        all_data.append(data)
    
    # Convert to DataFrame and print
    pd.set_option('display.max_columns', None)
    df = pd.DataFrame(all_data)
    print(df)

# Call the function to get output
get_video_stats(youtube, video_ids)
