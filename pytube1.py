import logging
import pandas as pd
import os
import yt_dlp
import time
import re
from urllib.parse import quote, unquote

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def sanitize_channel_name(channel_name):
    """Clean channel name for URL construction"""
    # Remove special characters except alphanumeric, @, and -
    clean_name = re.sub(r'[^\w\s@-]', '', channel_name)
    # Replace spaces and & with empty string
    clean_name = clean_name.strip().replace(' ', '').replace('&', '')
    return clean_name

def get_channel_playlists(channel_name, max_retries=3, retry_delay=5):
    """Get playlist information with improved error handling"""
    try:
        clean_channel = sanitize_channel_name(channel_name)
        
        ydl_opts = {
            'quiet': True,
            'extract_flat': True,
            'force_generic_extractor': True,
            'ignoreerrors': True,
            'sleep_interval': 1,  # Rate limiting
            'max_sleep_interval': 5
        }
        
        # Simplified URL patterns
        urls_to_try = [
            f"https://www.youtube.com/@{clean_channel}",  # Handle
            f"https://www.youtube.com/channel/{clean_channel}"  # Channel ID
        ]
        
        for retry in range(max_retries):
            for url in urls_to_try:
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        logging.info(f"Attempting to fetch: {url}")
                        channel_info = ydl.extract_info(url, download=False)
                        
                        if channel_info and 'entries' in channel_info:
                            logging.info(f"Successfully found channel: {channel_info.get('channel', 'Unknown')}")
                            return process_channel_videos(channel_info)
                            
                except Exception as e:
                    if '404' in str(e):
                        logging.warning(f"Channel not found: {url}")
                    elif '400' in str(e):
                        logging.warning(f"Invalid channel format: {url}")
                    continue
            
            if retry < max_retries - 1:
                wait_time = retry_delay * (2 ** retry)  # Exponential backoff
                logging.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                
        raise Exception(f"Failed to fetch channel data after {max_retries} retries")
        
    except Exception as e:
        logging.error(f"An error occurred while processing channel '{channel_name}': {str(e)}")
        return None

def process_channel_videos(channel_info):
    """Process videos from channel info"""
    videos = []
    for video in channel_info['entries'][:3]:
        video_info = {
            'channel_name': channel_info.get('channel', 'Unknown'),
            'video_id': video.get('id', 'Unknown'),
            'title': video.get('title', 'Unknown'),
            'url': video.get('url', 'Unknown')
        }
        videos.append(video_info)
    return videos

def append_to_csv(data, output_file):
    df = pd.DataFrame(data)
    if not os.path.isfile(output_file):
        df.to_csv(output_file, index=False)  # Write header if file doesn't exist
    else:
        df.to_csv(output_file, mode='a', header=False, index=False)  # Append without header

def get_processed_channels(output_dir="youtube_results"):
    """Get set of already processed channel names from existing CSVs"""
    processed = set()
    if not os.path.exists(output_dir):
        return processed
        
    for file in os.listdir(output_dir):
        if file.endswith('.csv'):
            try:
                df = pd.read_csv(os.path.join(output_dir, file))
                if 'channel_name' in df.columns:
                    processed.update(df['channel_name'].unique())
            except Exception as e:
                logging.warning(f"Error reading {file}: {e}")
    
    logging.info(f"Found {len(processed)} already processed channels")
    return processed

def process_channels(channel_list):
    """Process channels with skip for already processed"""
    processed_channels = get_processed_channels()
    all_videos = []
    success_count = 0
    
    results_dir = "youtube_results"
    os.makedirs(results_dir, exist_ok=True)
    
    for channel in channel_list:
        if channel in processed_channels:
            logging.info(f"Skipping already processed channel: {channel}")
            continue
            
        logging.info(f"Processing channel: {channel}")
        videos = get_channel_playlists(channel)
        
        if videos:
            all_videos.extend(videos)
            success_count += 1
            processed_channels.add(channel)
            
            # Save progress every 10 channels
            if success_count % 10 == 0:
                df = pd.DataFrame(all_videos)
                output_file = os.path.join(results_dir, f"youtube_playlists_{success_count}.csv")
                df.to_csv(output_file, index=False)
                logging.info(f"Saved interim results to {output_file}")
    
    # Save final results
    if all_videos:
        final_df = pd.DataFrame(all_videos)
        final_output = os.path.join(results_dir, "youtube_playlists_final.csv")
        final_df.to_csv(final_output, index=False)
        logging.info(f"Saved final results. Processed {success_count} new channels.")

if __name__ == "__main__":
    input_csv = './data/youtube_channels_1M_clean.csv'
    output_csv = './data/channel_playlists.csv'
    
    # Read channel names from CSV
    channels_df = pd.read_csv(input_csv)
    
    def clean_channel_name(x):
        """Clean and validate channel name"""
        if pd.isna(x):
            return None
        return str(x).strip().replace(" ", "")
    
    # Replace the problematic line with:
    channel_list = channels_df['channel_name'].apply(clean_channel_name).dropna().tolist()
    
    # Additional validation before processing
    if not channel_list:
        logging.error("No valid channel names found in the input data")
        exit(1)
    
    logging.info(f"Processing {len(channel_list)} valid channels")
    combined_videos = process_channels(channel_list)
    
    if not combined_videos.empty:
        append_to_csv(combined_videos, output_csv)
        logging.info("Appended data for all channels")