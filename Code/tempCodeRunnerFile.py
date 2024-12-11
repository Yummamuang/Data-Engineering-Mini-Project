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