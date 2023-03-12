import pymongo
import snscrape.modules.twitter as sntwitter
import numpy as np
import streamlit as st
import pandas as pd
import json
from datetime import date, timedelta

# Setting variables to be used below
hashtag = st.text_input("Enter Hashtag or Keyword Here ")
hashtag = str(hashtag).strip()
maxTweets = st.number_input("Enter Max Tweets to be loaded ")

# Start Date and EndDate

today = date.today()

default_date_20dayBack = today - timedelta(days=20)
startDate = st.date_input("Enter Start Date ", default_date_20dayBack)
startYear = str(startDate.year)
startDay = str(startDate.day)
startMonth = str(startDate.month)
endDate = st.date_input("Enter End Date ")
endYear = str(endDate.year)
endDay = str(endDate.day)
endMonth = str(endDate.month)

# Creating list to append tweet data
tweets_list1 = []
search = f"{hashtag} since:{startYear}-{startMonth}-{startDay} until:{endYear}-{endMonth}-{endDay}"
# LIC India Forever since:2022-10-14 until:2023-01-22'
# Using TwitterSearchScraper to scrape data and append tweets to list
for i,tweet in enumerate(sntwitter.TwitterSearchScraper(search).get_items()):
  if i>maxTweets:
    break
  tweets_list1.append([tweet.date, tweet.id, tweet.rawContent, 
                       tweet.user.username, tweet.replyCount, 
                       tweet.retweetCount, tweet.lang, tweet.sourceUrl,
                       tweet.likeCount])
  

# Mongo Db Connection and inserting into Database
arr1 = np.array(tweets_list1)
# print(arr1.shape[1])
if arr1.size > 0:
    tweet_data = {'Date Posted': arr1[:,0],'Twitter Id': arr1[:,1], 
                    'Tweet': arr1[:,2], 'Username': arr1[:,3], 
                    'Reply Count': arr1[:,4],'ReTweet Count': arr1[:,5], 
                    'Language': arr1[:,6], 'Source URL': arr1[:,7], 
                    'Like Count': arr1[:,8]}
    # Creating a dataframe from the twitter data
    df1 = pd.DataFrame(tweet_data)

    # Mongo Db Connection Setup

    client = pymongo.MongoClient("mongodb+srv://uma8331:1234@cluster0.q6okzia.mongodb.net/?retryWrites=true&w=majority")
    # Selecting a db or creating a new Database if given database not exist
    db = client.TwitterScrap
    # Creating a collection if not exist otherwise it will select the givne database
    record = db.LICTw

    if st.button("Upload to DB"):
        st.write("Record written to Database")
        for i in df1.index:
            record.insert_one({df1.columns[1]: df1.iloc[i,1], 
                                df1.columns[0]: df1.iloc[i,0],
                                "scraped Data": [{
                                df1.columns[2]: df1.iloc[i,2],
                                df1.columns[3]: df1.iloc[i,3],
                                df1.columns[4]: df1.iloc[i,4],
                                df1.columns[5]: df1.iloc[i,5],
                                df1.columns[6]: df1.iloc[i,6],
                                df1.columns[7]: df1.iloc[i,7],
                                df1.columns[8]: df1.iloc[i,8]
                                }
                                ]
                                })
    
    @st.cache_data
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode('utf-8')

    def displayDownload(lab, file, type, sdata):
        st.download_button(label=lab, file_name=file, mime=type, data=sdata,)

    download_option = st.selectbox("Download ", ["CSV", "Json"], index=1)
    if str(download_option).strip().lower()== 'json':
        st.write(download_option)
        json_string = df1.to_json()

    #    st.json(json_string, expanded=True)

        displayDownload("Download JSON","data.json","application/json",json_string)

    elif str(download_option).strip().lower() =='csv':
        st.write(download_option)
        csv = convert_df(df1)
        displayDownload("Download data as CSV",'large_df.csv',"text/csv",csv)
