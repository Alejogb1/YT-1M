# building something today for fun lol

# https://www.youtube.com/@lmsys-org/videos


import numpy as np
import pandas as pd
import os

from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi

import time, requests

API_KEY = 'API KEY'

# Construct a Resource for interacting with an API
# https://stackoverflow.com/questions/46158127/youtube-api-get-upload-playlistid-for-youtube-channel 
youtube = build('youtube', 'v3', developerKey=API_KEY)



dataset_path = './data/youtube_channels_1M_clean.csv'
df = pd.read_csv(dataset_path)

""" # get uploads playlist id 
def get_playlist_id(channel_name):
    try:
        search_response = youtube.search().list(part='snippet',
                                                q=channel_name,
                                                type='channel',
                                                maxResults=1).execute()
        if search_response['items']:
            channel_id = search_response['items'][0]['id']['channelId']
            request = youtube.channels().list(part='contentDetails', id=channel_id)
            response = request.execute()
            if 'items' in response and response['items']:
                uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                return uploads_playlist_id
            else:
                print("No items found for the retrieved channel ID.")
        else:
            print("No items found in the search response.")
    except Exception as e:
        print("An error occurred:", str(e))

# get video ids

def get_video_ids(playlist_id):
    try:
        request = youtube.playlistItems().list(part='snippet', playlistId=playlist_id)
        response = request.execute()
        if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 403:
            print("\n playlist response:", response.json())
            time.sleep(60)
            return get_video_ids(playlist_id)
        print("continue to get video ids...")
        video_ids = [item['snippet']['resourceId']['videoId'] for item in response['items']]
        return video_ids
    except Exception as e:
        print("An error occurred:", str(e))
        return []
 """

# fetching transcripts (from each video id)

# total_videos = df['total_videos'].sum()
#print("\n total_videos (sum from csv):", total_videos)
# 453246133
top_50_channels = df.nlargest(50, 'total_videos')

# removing top 50 channels with too many videos (more than 100000)
channels_to_remove = top_50_channels[top_50_channels['total_videos'] > 100000]
# print("\n channels to remove because they have too many videos:")
# print(channels_to_remove)
print("\n amount of channels before removing:", len(df))

df = df[~df['channel_name'].isin(channels_to_remove['channel_name'])]
df['channel_name'] = df['channel_name'].fillna('')

# removing channels with keywords 'india', 'hindi', 'telugu', 'tamil', 'malayalam'
keywords_to_remove = ['india', 'hindi', 'telugu', 'tamil', 'malayalam']
channels_to_remove = df[df['channel_name'].str.lower().str.contains('|'.join(keywords_to_remove))]
# print(channels_to_remove)
df = df[~df['channel_name'].str.lower().str.contains('|'.join(keywords_to_remove))]

# removing channels with more than 10000 videos
df = df[df['total_videos'] <= 10000]

# removing channels with 0 videos
df = df[df['total_videos'] > 0]

# total amount of channels & videos
print("\n amount of channels after removing:", len(df))
print(df['total_videos']/100000, 'M videos')


# 18 M videos top 50
print(df.nlargest(50, 'total_videos'))

print(top_50_channels['total_videos'])

print(top_50_channels['total_videos'].sum())

output_path = "./data/youtube_transcripts.csv"

# Function to get the 'uploads' playlist ID
def get_uploads_playlist_id(channel_name):
    try:
        request = youtube.channels().list(
            part="contentDetails",
            id=channel_name if channel_name.startswith("UC") else None,
            forUsername=channel_name if not channel_name.startswith("UC") else None
        )
        response = request.execute()
        uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return uploads_playlist_id
    except Exception as e:
        print(f"Error fetching playlist ID for {channel_name}: {e}")
        return None

# Function to fetch all video IDs from a playlist
def get_video_ids_from_playlist(playlist_id):
    video_ids = []
    next_page_token = None
    while True:
        try:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            video_ids.extend([item['contentDetails']['videoId'] for item in response['items']])
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        except Exception as e:
            print(f"Error fetching video IDs for playlist {playlist_id}: {e}")
            break
    return video_ids

# Function to fetch transcripts for a list of video IDs
def get_transcripts(video_ids):
    transcripts = {}
    for video_id in video_ids:
        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id)
            transcripts[video_id] = " ".join([entry['text'] for entry in transcript])
        except Exception as e:
            print(f"Could not retrieve transcript for video ID {video_id}: {e}")
    return transcripts

# Function to append processed data to CSV
def append_to_csv(data, output_file):
    df = pd.DataFrame(data)
    if not os.path.isfile(output_file):
        df.to_csv(output_file, index=False)  # Write header if file doesn't exist
    else:
        df.to_csv(output_file, mode='a', header=False, index=False)  # Append without header

# Main logic
last_processed_video_id = None  # Track the last processed video ID

for index, row in df.iterrows():
    channel_name = row['channel_name']
    print(f"Processing channel {channel_name} ({channel_name})...")

    # Step 1: Get 'uploads' playlist ID
    uploads_playlist_id = get_uploads_playlist_id(channel_name)
    if not uploads_playlist_id:
        continue

    # Step 2: Fetch video IDs from the playlist
    video_ids = get_video_ids_from_playlist(uploads_playlist_id)
    print(f"Found {len(video_ids)} videos for channel {channel_name}")

    # Step 3: Fetch transcripts for all videos
    for video_id in video_ids:
        # Skip already processed videos
        if last_processed_video_id and video_id <= last_processed_video_id:
            continue

        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id)
            transcript_text = " ".join([entry['text'] for entry in transcript])

            # Append data to CSV
            append_to_csv([{
                'Channel Name': channel_name,
                'Channel Identifier': channel_name,
                'Playlist ID': uploads_playlist_id,
                'Video ID': video_id,
                'Transcript': transcript_text
            }], output_path)

            # Update the last processed video ID
            last_processed_video_id = video_id
            print(f"Successfully processed video ID {video_id}")

        except Exception as e:
            print(f"Error processing video ID {video_id}: {e}")
            continue
    

# print(df.head())




"""

def step1_get_playlists():
    print("=== STEP 1: Getting Uploads Playlist IDs ===")

    # Load existing playlists CSV if it exists
    if os.path.exists(PLAYLISTS_CSV):
        existing_df = pd.read_csv(PLAYLISTS_CSV)
        processed_channels = set(existing_df['channel_name'])
    else:
        existing_df = pd.DataFrame(columns=['channel_name', 'uploads_playlist_id'])
        processed_channels = set()

    # Load channels data
    df = pd.read_csv(CHANNELS_CSV)
    print("DF columns:", df.columns)

    # Filter channels (e.g., clean, remove large/irrelevant channels)
    df['channel_name'] = df['channel_name'].fillna('')
    df = df[~df['channel_name'].isin(processed_channels)]  # Skip already processed channels

    # Example additional filtering (customize as needed)
    df = df[df['total_videos'] > 0]  # Skip channels with no videos
    df = df[df['total_videos'] <= 10000]  # Skip channels with too many videos

    print(f"Processing {len(df)} channels after filtering.")

    # Track results
    results = []
    for idx, row in df.iterrows():
        channel_name = str(row['channel_name']).strip()
        if not channel_name:
            continue

        print(f"[step1_get_playlists] Fetching uploads playlist for channel '{channel_name}'")
        pl_id = get_uploads_playlist_id(channel_name)

        if pl_id:
            results.append({
                'channel_name': channel_name,
                'uploads_playlist_id': pl_id
            })

            # Append to CSV immediately to ensure progress is saved
            with open(PLAYLISTS_CSV, 'a') as f:
                f.write(f"{channel_name},{pl_id}\n")

        # Track the last processed channel (optional)
        with open('last_processed_channel.txt', 'w') as f:
            f.write(channel_name)

    print(f"[step1_get_playlists] Added {len(results)} new playlists to '{PLAYLISTS_CSV}'.")

    if not results:
        print("No new playlists were added.")

"""