import pandas as pd
import time
from googleapiclient.errors import HttpError
import logging
from datetime import datetime
from googleapiclient.discovery import build

def setup_logging():
    logging.basicConfig(
        filename='youtube_api.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def load_progress():
    try:
        return pd.read_csv('./data/progress.csv')
    except FileNotFoundError:
        return pd.DataFrame(columns=['channel_name', 'playlist_id', 'processed_at'])

def save_progress(channel_name, playlist_id):
    progress_df = load_progress()
    new_row = {
        'channel_name': channel_name,
        'playlist_id': playlist_id,
        'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    progress_df = pd.concat([progress_df, pd.DataFrame([new_row])], ignore_index=True)
    progress_df.to_csv('./data/progress.csv', index=False)

def get_uploads_playlist_id(api_key, channel_name, max_retries=5):
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    for attempt in range(max_retries):
        try:
            # Search for the channel
            request = youtube.search().list(
                part="id",
                q=channel_name,
                type="channel",
                maxResults=1
            )
            response = request.execute()
            
            if not response.get('items'):
                logging.warning(f"Channel not found: {channel_name}")
                return None
                
            channel_id = response['items'][0]['id']['channelId']
            
            # Get uploads playlist ID
            request = youtube.channels().list(
                part="contentDetails",
                id=channel_id
            )
            response = request.execute()
            
            if not response['items']:
                logging.warning(f"No content details found for channel: {channel_name}")
                return None
                
            return response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            
        # need to extend or revise rate limits youtube v3 api
        except HttpError as e:
            if e.resp.status in [429, 403]:  # Quota exceeded or rate limit
                if attempt == max_retries - 1:
                    logging.error(f"Max retries reached for {channel_name}: {str(e)}")
                    raise
                wait_time = (2 ** attempt) * 60  # Exponential backoff
                logging.info(f"Rate limit hit. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"Error processing {channel_name}: {str(e)}")
                raise

if __name__ == "__main__":
    setup_logging()
    api_key = "API KEY"
    
    # Load channel names
    channels_df = pd.read_csv('./data/youtube_channels_1M_clean.csv')
    progress_df = load_progress()
    
    # Filter out already processed channels
    processed_channels = set(progress_df['channel_name'])
    channels_to_process = channels_df[~channels_df['channel_name'].isin(processed_channels)]
    
    for _, row in channels_to_process.iterrows():
        channel_name = row['channel_name']
        try:
            uploads_playlist_id = get_uploads_playlist_id(api_key, channel_name)
            if uploads_playlist_id:
                save_progress(channel_name, uploads_playlist_id)
                logging.info(f"Successfully processed {channel_name}")
        except Exception as e:
            logging.error(f"Failed to process {channel_name}: {str(e)}")
            # If quota exceeded, stop processing
            if isinstance(e, HttpError) and e.resp.status in [429, 403]:
                logging.error("Daily quota exceeded. Stopping processing.")
                break
            continue