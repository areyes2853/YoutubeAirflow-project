import requests
import json
api_key = "AIzaSyChvlwPePW9z0TzBctC01XnXg8jzveqptk"
CHANNEL_Handle = "MrBeast"

def get_playlist_id(api_key, CHANNEL_Handle):
  try:
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_Handle}&key={api_key}"

    response = requests.get(url)
    data = response.json()
    #print(data)
    #print(json.dumps(data,indent=4))
    channel_items = data["items"][0]
    channel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
    #print(f"Channel Playlist ID: {channel_playlist_id}")
    return channel_playlist_id
  except requests.exceptions.RequestException as e:
    raise e
    print("An error occurred while fetching the playlist ID:", str(e))
    return None

if __name__ == "__main__":
    get_playlist_id(api_key, CHANNEL_Handle)