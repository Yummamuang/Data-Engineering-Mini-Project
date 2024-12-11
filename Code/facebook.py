from dotenv import load_dotenv
import pandas as pd
import requests
import pymongo
import random
import time
import os

load_dotenv()

CLIENT_ID = os.getenv('FACEBOOK_CLIENT_ID')
CLIENT_SECRET = os.getenv('FACEBOOK_CLIENT_SECRET')
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
    url = "https://graph.facebook.com/oauth/access_token"
    
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
    
def search_live_videos(query, token, limit, next_page):
    if next_page:
        url = next_page
    else:  
        url = f"https://graph.facebook.com/v12.0/search?type=live_videos&q={query}&fields=description,status&limit={limit}&access_token={token}"
    
    views = []
    video_id = []
    video_title = []

    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()["data"]
        
        if 'paging' in data and 'next' in data['paging']:
            next_page = data['paging']['next']
            
        for item in data:
            video_id.append(item['id'])
            video_title.append(item['title'])
    else:
        print("Error:", response.text)
        
    for vid_id in video_id:
        url = f"https://graph.facebook.com/{video_id}?fields=live_views&access_token={token}"
        response = requests.get(url)
        
        if response.status_code == 200:
            views.append(response.json().get("live_views", 0))
        else:
            print("Error:", response.text)
            return 0
    
    results = []
    for i in range(min(limit, len(video_id))):
        results.append({
            'game_name': query,
            'video_id': video_id[i],
            'video_title': video_title[i],
            'live_broadcast_content': 'live',
            'views': views[i],
        })
        
    return results, next_page
    

token = os.getenv('FACEBOOK_ACCESS_TOKEN')
collec = get_data_from_mongodb('LIVE_DATA', 'GAME_CATEGORIES')

#Categoreis from mongoDB
query = []
for doc in collec:
    query.append(doc['game_name'])
    
complete_result = [] 
for game_name in query:
    all_results = []
    page = None
    
    for i in range(2):
        delay = int(random.randrange(10, 30))
        results, next_page = search_live_videos(game_name, token, 25, page)
        all_results.extend(results)
        page = next_page

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

insert_many_to_mongodb('LIVE_DATA','GAME_LIVE_DATA_FACEBOOK', complete_df_dict)
live_videos = search_live_videos(query, token)
print(live_videos)