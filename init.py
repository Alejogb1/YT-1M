# building something today for fun lol

# https://www.youtube.com/@lmsys-org/videos

#This project is an automated YouTube data extraction and analysis tool designed to gather, process, and analyze large-scale YouTube channel data. It leverages the YouTube API to fetch "uploads" playlist IDs for channels, retrieves video IDs from these playlists, and extracts video transcripts. The tool handles API rate limits and quota management, ensuring continuous data collection. It stores the collected data in CSV files for easy access and further analysis. This project aims to provide researchers and developers with a robust framework for studying YouTube content trends, user behavior, and video metadata, enabling data-driven insights and applications.

import numpy as np
import pandas as pd
import os
import sys
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
import csv
import time, requests
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)

API_KEY = 'API KEY'

# Construct a Resource for interacting with an API
# https://stackoverflow.com/questions/46158127/youtube-api-get-upload-playlistid-for-youtube-channel 
youtube = build('youtube', 'v3', developerKey=API_KEY)



# Change these if you want different file names.
CHANNELS_CSV    = "./data/youtube_channels_1M_clean.csv"
PLAYLISTS_CSV   = "./data/upload_playlists.csv"
VIDEOIDS_CSV    = "./data/video_ids.csv"
TRANSCRIPTS_CSV = "./data/transcripts.csv"


# =================================================
# === STEP 1: Get "Uploads" playlist IDs per channel
# =================================================

# last processed instead of channel name somethgin else like number
def get_last_processed(csv_file):
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        last_row = None
        for row in reader:
            last_row = row
        return last_row

class QuotaHandler:
    def __init__(self):
        self.quota_exceeded = False
        self.last_quota_reset = datetime.now()
        
    def handle_quota_exceeded(self):
        self.quota_exceeded = True
        # Calculate time until next quota reset (3 AM PST)
        now = datetime.now()
        next_reset = now.replace(hour=3, minute=0, second=0, microsecond=0)
        if now.hour >= 3:
            next_reset += timedelta(days=1)
        
        wait_seconds = (next_reset - now).total_seconds()
        return wait_seconds

def get_uploads_playlist_id(youtube, channel_name):
    """
    Returns the 'uploads' playlist ID of a given channel_name. 
    channel_name can be either:
      - A channel ID that starts with 'UC' (use 'id' parameter), or
      - A channel username (use 'forUsername' parameter).
    """
    try:
        search_response = youtube.search().list(part='snippet', q=channel_name, type='channel', maxResults=1).execute()
        if 'error' in search_response:
            if search_response['error']['reason'] == 'quotaExceeded':
                return None
        if search_response['items']:
            channel_id = search_response['items'][0]['id']['channelId']
            request = youtube.channels().list(part='contentDetails', id=channel_id)
            response = request.execute()
            if 'items' in response:
                uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                logging.info(f"Playlist ID: {uploads_playlist_id}")
                return uploads_playlist_id
            else:
                logging.warning("No items found in response")
        else:
            logging.warning("No items found in search response")
            return None
    except Exception as e:
        if 'quota' in str(e).lower():
            return None
        logging.error(f"Error fetching playlist ID for {channel_name}: {e}")
        return None

def ensure_directory_exists():
    """Create data directory if it doesn't exist"""
    os.makedirs('./data', exist_ok=True)

def append_to_playlist_csv(channel_name, playlist_id, playlists_csv):
    """Append new channel data to playlist CSV"""
    mode = 'a' if os.path.exists(playlists_csv) else 'w'
    header = not os.path.exists(playlists_csv)
    
    df = pd.DataFrame([[channel_name, playlist_id]], 
                     columns=['channel_name', 'uploads_playlist_id'])
    df.to_csv(playlists_csv, mode=mode, header=header, index=False)

def update_last_processed(channel_name, last_processed_file):
    """Update last processed channel file"""
    with open(last_processed_file, 'a', encoding='utf-8') as f:
        f.write(f"{channel_name}\n")

def step1_get_playlists(youtube, channels_csv, playlists_csv, last_processed_file):
    quota_handler = QuotaHandler()
    ensure_directory_exists()
    
    while True:
        try:
            df = pd.read_csv(channels_csv)
            processed_channels = set()
            
            if os.path.exists(last_processed_file):
                with open(last_processed_file, 'r') as f:
                    processed_channels = set(line.strip() for line in f)
            
            for idx, row in df.iterrows():
                channel_name = str(row['channel_name']).strip()
                
                if channel_name in processed_channels:
                    continue
                    
                pl_id = get_uploads_playlist_id(youtube, channel_name)
                
                if pl_id is None and quota_handler.quota_exceeded:
                    wait_time = quota_handler.handle_quota_exceeded()
                    logging.info(f"Quota exceeded. Waiting {wait_time/3600:.1f} hours until reset")
                    time.sleep(wait_time)
                    break
                
                if pl_id:
                    append_to_playlist_csv(channel_name, pl_id, playlists_csv)
                    update_last_processed(channel_name, last_processed_file)
                    processed_channels.add(channel_name)
                
            if quota_handler.quota_exceeded:
                continue
                
            break
            
        except Exception as e:
            logging.error(f"Error in processing: {e}")
            break


# =====================================================
# === STEP 2: Get video IDs from each "uploads" playlist
# =====================================================

def get_video_ids_from_playlist(playlist_id):
    """
    Returns all video IDs in a playlist via pagination.
    """
    video_ids = []
    next_page_token = None
    while True:
        try:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=5000,
                pageToken=next_page_token
            )
            response = request.execute()
            items = response.get('items', [])
            if not items:
                break

            for item in items:
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

        except Exception as e:
            print(f"[get_video_ids_from_playlist] Error for playlist '{playlist_id}': {e}")
            break

    return video_ids


def step2_get_video_ids():
    """
    Reads PLAYLISTS_CSV, fetches all videos for each playlist,
    and writes out VIDEOIDS_CSV with columns:
      channel_name, playlist_id, video_id
    """
    print("=== STEP 2: Getting Video IDs from Playlists ===")
    df = pd.read_csv(PLAYLISTS_CSV,)

    all_videos = []
    for idx, row in df.iterrows():
        channel_name = row['channel_name']
        playlist_id  = row['uploads_playlist_id']

        vids = get_video_ids_from_playlist(playlist_id)
        print(f"[step2_get_video_ids] Found {len(vids)} videos for channel '{channel_name}'")

        for vid in vids:
            all_videos.append({
                'channel_name': channel_name,
                'playlist_id': playlist_id,
                'video_id': vid
            })

    video_df = pd.DataFrame(all_videos)
    video_df.to_csv(VIDEOIDS_CSV, index=False)
    print(f"[step2_get_video_ids] Created '{VIDEOIDS_CSV}' with {len(all_videos)} rows.")


# ========================================================
# === STEP 3: Fetch transcripts for each retrieved video ID
# ========================================================

def get_transcript_text(video_id):
    """
    Returns the concatenated transcript string for a video
    using YouTubeTranscriptApi.
    """
    try:
        transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
        transcript_text = " ".join([entry['text'] for entry in transcript_list])
        return transcript_text
    except Exception as e:
        print(f"[get_transcript_text] Could not retrieve transcript for video ID '{video_id}': {e}")
        return None


def step3_get_transcripts():
    """
    Reads VIDEOIDS_CSV, attempts to fetch transcripts,
    and writes TRANSCRIPTS_CSV with columns:
      channel_name, playlist_id, video_id, transcript
    """
    print("=== STEP 3: Getting Transcripts for Each Video ID ===")
    df = pd.read_csv(VIDEOIDS_CSV)

    results = []
    for idx, row in df.iterrows():
        channel_name = row['channel_name']
        playlist_id  = row['playlist_id']
        video_id     = row['video_id']

        t_text = get_transcript_text(video_id)
        if t_text:
            results.append({
                'channel_name': channel_name,
                'playlist_id': playlist_id,
                'video_id': video_id,
                'transcript': t_text
            })

    out_df = pd.DataFrame(results)
    out_df.to_csv(TRANSCRIPTS_CSV, index=False)
    print(f"[step3_get_transcripts] Created '{TRANSCRIPTS_CSV}' with {len(results)} transcripts.")


# ============================
# === MAIN / ENTRY POINT  ===
# ============================

if __name__ == "__main__":
    """
    Usage:
      python onefile_script.py step1
      python onefile_script.py step2
      python onefile_script.py step3
    """
    if len(sys.argv) < 2:
        print("Please specify which step to run: step1, step2, or step3")
        sys.exit(1)

    step = sys.argv[1].lower().strip()
    if step == "step1":
        step1_get_playlists(youtube, CHANNELS_CSV, PLAYLISTS_CSV, 'last_processed_channels.txt')
    elif step == "step2":
        step2_get_video_ids()
    elif step == "step3":
        step3_get_transcripts()
    else:
        print("Unknown step. Use 'step1', 'step2', or 'step3'.")
        sys.exit(1)
