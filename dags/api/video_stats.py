import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime
from airflow.decorators import task
load_dotenv(dotenv_path="./.env")

api_key = os.getenv('API_KEY')
CHANNEL_Handle = os.getenv('CHANNEL_Handle')
max_results = 50

@task
def get_playlist_id(api_key, CHANNEL_Handle):
  try:
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_Handle}&key={api_key}"

    response = requests.get(url)
    data = response.json()
    #print(data)
    #print(json.dumps(data,indent=4))
    channel_items = data["items"][0]
    channel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
    print(f"Channel Playlist ID: {channel_playlist_id}")
    return channel_playlist_id
  except requests.exceptions.RequestException as e:
    raise e
    print("An error occurred while fetching the playlist ID:", str(e))
    return None
@task
def get_video_ids(playlist_id, api_key=api_key, max_results=50):
  video_ids = []
  pageToken = None

  base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={api_key}"
  try:
    while True:
      url = base_url
      if pageToken:
        url += f"&pageToken={pageToken}"

      response = requests.get(url)
      data = response.json()
      #print(json.dumps(data,indent=4))

      for item in data.get("items", []):
        video_id = item["contentDetails"]["videoId"]
        video_ids.append(video_id)

      pageToken = data.get("nextPageToken")
      if not pageToken:
        break

    

    print(f"Total Video IDs fetched: {len(video_ids)}")
    return video_ids
  except requests.exceptions.RequestException as e:
    raise e
    print("An error occurred while fetching video IDs:", str(e))
    return video_ids



@task
def extract_video_data(video_ids):
    all_video_data = []

    def batch_list(video_id_list, batch_size):
       for video_id in range(0, len(video_id_list), batch_size):
          yield video_id_list[video_id:video_id + batch_size]

    try:
       for batch in batch_list(video_ids, 50):
            video_ids_str = ",".join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={api_key}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            #print(json.dumps(data,indent=4))

            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]

                video_data = {
                    "video_id": item["id"],
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount", 0), 
                    "likeCount": statistics.get("likeCount", 0),
                    "commentCount": statistics.get("commentCount", 0),
                }
                all_video_data.append(video_data)
            return all_video_data
    
    except requests.exceptions.RequestException as e:
        raise e
        print("An error occurred while fetching video data:", str(e))
        return all_video_data
@task
def save_to_json(extracted_data):
    file_path = f"./data/video_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    with open(file_path, 'w', encoding="Utf-8") as json_file:
       json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
   

# url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id=0e3GP&key=AIzaSyChvlwPePW9z0TzBctC01XnXg8jzveqptk"

if __name__ == "__main__":
   playlistId = get_playlist_id(api_key, CHANNEL_Handle)
   video_ids = get_video_ids(playlistId)
   video_data = extract_video_data(video_ids)
   save_to_json(video_data)
   print(extract_video_data(video_ids))
  # print("Playlist ID:", get_video_ids(playlistId))