from dotenv import load_dotenv
import pandas as pd
import requests
import pymongo
import random
import json
import time
import os

load_dotenv()

CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')

MONGODB_URI = os.getenv('MONGODB_URI')

#insert data to mongoDB(Many)
def insert_many_to_mongodb(database_name, collection, data_list):
    client = pymongo.MongoClient(MONGODB_URI)
    db = client.get_database(database_name)
    collection = db[collection]
    collection.insert_many(data_list)
    client.close()

#get data collection from mongoDB
def get_data_from_mongodb(database_name, collection):
    client = pymongo.MongoClient(MONGODB_URI)
    db = client.get_database(database_name)
    collection = db[collection]
    collec = collection.find({})
    data = []
    for document in collec:
        data.append(document)
    client.close()
    return data

def get_token():
    url = "https://id.twitch.tv/oauth2/token"
    
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }

    response = requests.post(url, data=data)
    
    if response.status_code == 200:
        access_token = response.json()["access_token"]
        return access_token
    else:
        print("Error:", response.text)
        return

def get_top_games(token, num_result):
    url = "https://api.twitch.tv/helix/games/top"

    params = {
        "first": num_result
    }

    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {token}"
    }
    response = requests.get(url, params=params, headers=headers)

    top_games_dict = []
    if response.status_code == 200:
        top_games = response.json()["data"]
        for game in top_games:
            top_games_dict.append({
                'game_name': game['name'],
                'id': game['id']
            })
    else:
        print("Error:", response.text)
        
    return top_games_dict

def get_video(token, num_result, gameID, cursor=None):
    url = "https://api.twitch.tv/helix/streams"
    
    params = {
        "first": num_result,
        "sort": "views",  
        "game_id": gameID,
    }
    
    if cursor:
        params["after"] = cursor
    
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, params=params, headers=headers)
    
    game_name = []
    video_id = []
    video_title = []
    live_broadcast_content = []
    views = []
    
    if response.status_code == 200:
        data = response.json()["data"]
        pagination = response.json().get('pagination', {})
        
        for item in data:
            if item['game_id'] == gameID:
                game_name.append(item['game_name'])
                video_id.append(item['id'])
                video_title.append(item['title'])
                live_broadcast_content.append(item['type'])
                views.append(item['viewer_count'])
       
        next_cursor = pagination.get('cursor')
    else:
        print("Error:", response.text)
    
    results = []
    for i in range(min(num_result, len(game_name))):
        results.append({
            'game_name': game_name[i],
            'video_id': video_id[i],
            'video_title': video_title[i],
            'live_broadcast_content': live_broadcast_content[i],
            'views': views[i],
        })


    return results, next_cursor

token = get_token()

query = []

#get Collection from Database
collec = get_data_from_mongodb('LIVE_DATA', 'GAME_CATEGORIES')
for data in collec:
    query.append(data['id'])

#loop for request for data
complete_result = [] 
for game_name in query:
    all_results = []
    cursor = None
    
    for i in range(2):
        delay = int(random.randrange(10, 30))
        results, next_cursor = get_video(game_name, 100, game_name, cursor)
        all_results.extend(results)
        cursor = next_cursor
        
        #Data frame
        df = pd.DataFrame(all_results)
        df.sort_values(by='views', inplace=True, ascending=False)
        df.reset_index(drop=True, inplace=True)
        print(df)
        
        #Completly results one loop
        complete_result.extend(all_results)
        time.sleep(delay)
        
complete_df = pd.DataFrame(complete_result)
complete_df.drop_duplicates(keep='first', inplace=True, ignore_index=True)
complete_df_dict = complete_df.to_dict(orient='records')
print(complete_df)

insert_many_to_mongodb('LIVE_DATA','GAME_LIVE_DATA_TWITCH', complete_df_dict)

