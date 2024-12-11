from googleapiclient.discovery import build
from dotenv import load_dotenv
import pandas as pd
import pymongo
import random
import time
import json
import os

#Load .env
load_dotenv()

#Ladd data from .env
API_KEY = os.getenv('YOUTUBE_API_KEY')
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

#get categoriesID from YouTube
def get_categoreis_id(Name):
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    request = youtube.videoCategories().list(
        part="snippet",
        regionCode="TH"
    )
    response = request.execute()
    
    results = dict()
    for i in response['items']:
        if i['snippet']['title'] == Name:
            results = {
                'id': i['id'],
                'title': i['snippet']['title']
            }
            break

    return results

#Search Video in YouTube
def search_videos(query, num_result, category_id, page_token=None):
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    
    if page_token:
        page_token = page_token
    
    request = youtube.search().list(
        q=query,
        part='snippet', 
        type='video',
        eventType='live',
        order='viewCount',
        maxResults=num_result,
        pageToken=page_token,
        videoCategoryId=category_id,
    )
    response = request.execute()
    
    #get page token
    page_token = response.get('nextPageToken')
    
    video_id = []
    video_title = []
    live_broadcast_content = []
    views = []
    video_response = []
    
    for item in response['items']:
        video_id.append(item['id']['videoId'])
        video_title.append(item['snippet']['title'])
        live_broadcast_content.append(str(item['snippet']['liveBroadcastContent']))

    for vid_id in video_id:
        video_request = youtube.videos().list(
            part="statistics",
            id=vid_id
        )
        #Create temp for search videoID
        temp = video_request.execute()
        
        #insert temp to video_respose
        if 'items' in temp:
            video_response.extend(temp['items'])
        else:
            video_response.append({})

    #insert data in to views and likes
    for item in video_response:
        if 'statistics' in item:
            if 'viewCount' in item['statistics']:
                views.append(int(item['statistics']['viewCount']))
            else:
                views.append(0)
        else:
            views.append(0)
    
    #combind data to result
    results = []
    for i in range(min(num_result, len(video_id))):
        results.append({
            'game_name': query,
            'video_id': video_id[i],
            'video_title': video_title[i],
            'live_broadcast_content': live_broadcast_content[i],
            'views': views[i],
        })

    return results, page_token


categories_id = get_categoreis_id("Gaming")
MaxResult = 50
collec = get_data_from_mongodb('LIVE_DATA', 'GAME_CATEGORIES')

#Categoreis from mongoDB
query = []
for doc in collec:
    query.append(doc['game_name'])

#loop for request for data
complete_result = [] 
for game_name in query:
    all_results = []
    page_token = None
    
    for i in range(2):
        delay = int(random.randrange(10, 30))
        results, next_page = search_videos(game_name, MaxResult, categories_id, page_token)
        all_results.extend(results)
        page_token = next_page
        
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

insert_many_to_mongodb('LIVE_DATA','GAME_LIVE_DATA_YT', complete_df_dict)