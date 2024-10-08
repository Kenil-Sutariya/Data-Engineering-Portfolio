import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_maanger = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager= client_credentials_maanger)
    
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"
    playlist_uri = playlist_link.split('/')[-1]   
    
    data = sp.playlist_tracks(playlist_uri)
    
    client = boto3.client('s3')
    
    filename = "spotify_row_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket = 'spotify-etl-project-kenil',
        Key = "raw_data/to_processed/" + filename,
        Body = json.dumps(data)
        )