import json
import boto3
from datetime import datetime
import pandas as pd
from io import StringIO

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-project-kenil'
    Key = 'raw_data/to_processed/'
    
    
    spotify_data = []
    spotify_data_keys = []
        
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_data_keys.append(file_key)
    

        
    # Creating Album List from Whole data
    def album(data):
        album_list = []
        
        for row in data['items']:
            album_id = row['track']['album']['id']
            album_name = row['track']['album']['name']
            album_release_date = row['track']['album']['release_date']
            album_total_tracks = row['track']['album']['total_tracks']
            album_url = row['track']['album']['external_urls']['spotify']
            album_element = {'id': album_id, 'name': album_name, 'release_date': album_release_date, 'total_tracks': album_total_tracks, 'url': album_url}
            album_list.append(album_element)
        return album_list
        
    # Creating Artists List from Whole data
    def artist(data):
        artists_list = []
        
        for row in data['items']:
            for key, value in row.items():
                if key == 'track':
                    for artists in value['artists']:
                        artist_dict = {'artist_id': artists['id'], 'artist_name': artists['name'], 'external_url': artists['href']}
                        artists_list.append(artist_dict)
        return artists_list
        
    # Creating Songs List from Whole data
    def song(data):
        
        song_list = []
        
        for row in data['items']:
            song_id = row['track']['id']
            song_name = row['track']['name']
            song_duration = row['track']['duration_ms']
            song_url = row['track']['external_urls']['spotify']
            song_popularity = row['track']['popularity']
            song_added = row['added_at']
            album_id = row['track']['album']['id']
            artist_id = row['track']['album']['artists'][0]['id']
            song_element = {'song_id': song_id, 'song_name': song_name, 'duration_ms': song_duration, 'url':song_url, 'popularity': song_popularity, 
                            'song_added':song_added, 'album_id': album_id, 'artist_id': artist_id}
            song_list.append(song_element)
        return song_list
        
    for data in spotify_data:
        album_data = album(data)
        artist_data = artist(data)
        song_data = song(data)
        
        album_df = pd.DataFrame.from_dict(album_data)
        album_df = album_df.drop_duplicates(subset=['id'])
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        
        artist_df = pd.DataFrame.from_dict(artist_data)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        song_df = pd.DataFrame.from_dict(song_data)
        song_df = song_df.drop_duplicates(subset=['song_id'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
        song_key = 'trasformed_data/song_data/song_transformed_' + str(datetime.now()) +'.csv'
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index = False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = song_key, Body = song_content)
        
        artist_key = 'trasformed_data/artist_data/artist_transformed_' + str(datetime.now()) +'.csv'
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index = False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = artist_key, Body = artist_content)
        
        album_key = 'trasformed_data/album_data/album_transformed_' + str(datetime.now()) +'.csv'
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index = False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = album_key, Body = album_content)
    
    s3_resource = boto3.resource('s3')
    for key in spotify_data_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()