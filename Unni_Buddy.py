#!/usr/bin/env python
# coding: utf-8

# In[1]:


import tweepy #Library required for Twitter API
import csv
import pandas as pd
import os
import pandas as pd
import numpy as np

#!pip install wget
import wget
#import mysql-connector as sql


# In[2]:


pip install PyMySQL


# In[3]:


import pymysql


# In[4]:


consumer_key="iIpARRXWJNQBSKaussghVotpp"  #API key
consumer_secret="QGSDxQxoCI28EywSDrmqYzlHdeD1Qok1p9YCB0sBDHZgv0Zui9"  # API Key Secret
access_token="1590179103881109504-7kJQ6y8jcNNEzbbTfJrZRHdAG11qM5"
access_token_secret="M8k3kgD4ngav5328JMKgxmaIk1KXI82EyrgWOVz7Ic4em"


# In[5]:


tweets =pd.DataFrame(columns=["user_id","twitter_handle","user_name","profile_image_url","tweet_description","friends_count","followers_count","user_account_created"])
tweets_account =pd.DataFrame(columns=["user_id","twitter_handle"])


# In[6]:


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)
outtweets_all =[] #list to store all tweets data


# In[7]:


for tweets in tweepy.Cursor(api.search_tweets,q="#GRE",count=100, #The q variable holds the hashtag 
                           lang="en",
                           since="2017-04-19").items():
    if(tweets.entities.get('media',[])) : #This condition appends only those tweets to the list which have image URL's
        media = tweets.entities.get('media')
        outtweets_all.append([tweets.id_str,tweets.user.id_str,tweets.user.name,tweets.user.screen_name,tweets.user.profile_image_url_https,tweets.user.description,tweets.user.friends_count,tweets.user.followers_count,tweets.user.created_at,tweets.text,tweets.created_at,tweets.favorite_count,tweets.entities['hashtags'],tweets.entities['user_mentions']])
print(outtweets_all) 


# In[8]:


tweetdf_all = pd.DataFrame(outtweets_all,columns = ["tweet_id","user_id","user_name","twitter_handle","profile_image_url","tweet_description","friends_count","followers_count","user_account_created","tweet_text","created_at","tweet_likes","tags",'target_user'])
tweetdf_all.to_csv(r'C:\\Users\\Lenovo\\Downloads\\Twitter_Data\\GRE_data.csv')
tweetdf_all


# In[9]:


tweetdf_user = tweetdf_all[["user_id","twitter_handle","user_name","profile_image_url","tweet_description","friends_count","followers_count","user_account_created"]]
tweetdf_user = tweetdf_user.drop_duplicates('user_id') #drop duplicates
tweetdf_user


# In[10]:


tweetdf_tweet = tweetdf_all[["tweet_id","user_id","tweet_text","created_at","tweet_likes"]]
tweetdf_tweet = tweetdf_tweet.drop_duplicates('tweet_id')
tweetdf_tweet


# In[11]:


tweetdf_tags = tweetdf_all[["tweet_id","user_id","tags"]]
new_tags =pd.DataFrame(columns=["tweet_id","user_id","tags"])

for i,row in tweetdf_tags.iterrows():
    for values in row['tags']:
        temp=[row['tweet_id'], row['user_id'],values['text']]
        new_tags.loc[len(new_tags)] = temp
new_tags.i=np.arange(1, len(new_tags) + 1)
new_tags['tag_id']=new_tags.i
new_tags=new_tags[['tag_id','user_id','tags','tweet_id']]
new_tags


# In[12]:


tweetdf_mentions = tweetdf_all[["tweet_id","user_id","target_user"]]
new_mentions =pd.DataFrame(columns=["tweet_id","user_id","target_user"])
            
for i,row in tweetdf_mentions.iterrows():
    if row['target_user']!=None:
        if len(row['target_user'])!=0:
            for values in row['target_user']:
                temp=[row['tweet_id'], row['user_id'],values['screen_name']]
                new_mentions.loc[len(new_mentions)] = temp
new_mentions.i=np.arange(1, len(new_mentions) + 1)
new_mentions['mention_id']=new_mentions.index
new_mentions=new_mentions[['mention_id','tweet_id','user_id','target_user']]
new_mentions


# In[13]:


import mysql.connector


# In[14]:


uni_recomm = pymysql.connect(host='localhost', user='root', passwd='RootPassword123', database='university_recommendation')


# In[15]:


cursor= uni_recomm.cursor()


# In[16]:


for i,row in tweetdf_user.iterrows():
    cursor.execute("INSERT INTO User values (%s,%s,%s,%s,%s,%s,%s,%s)", (int(row['user_id']),row['twitter_handle'],row['user_name'],row['profile_image_url'],row['tweet_description'],row['friends_count'],row['followers_count'],row['user_account_created']))
uni_recomm.commit()

cursor.execute("SELECT * from User")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[18]:


for i,row in new_tags.iterrows():
    cursor.execute("INSERT INTO Tweet_Tags values (%s,%s,%s,%s)", (int(row['tag_id']),row['user_id'],row['tags'],row['tweet_id']))
uni_recomm.commit()
cursor.execute("SELECT * from Tweet_Tags")
records=cursor.fetchall()
print(records)
uni_recomm.commit()


# In[19]:


for i,row in new_mentions.iterrows():
    cursor.execute("INSERT INTO Tweet_Mentions values (%s,%s,%s,%s)", (int(row['mention_id']),row['tweet_id'],row['user_id'],row['target_user']))
uni_recomm.commit()
cursor.execute("SELECT * from Tweet_Mentions")
records=cursor.fetchall()
print(records)
uni_recomm.commit()


# In[20]:


for i,row in tweetdf_tweet.iterrows():
    cursor.execute("INSERT INTO Tweets values (%s,%s,%s,%s,%s)", (int(row['tweet_id']),row['user_id'],row['tweet_text'],row['created_at'],row['tweet_likes']))
uni_recomm.commit()
cursor.execute("SELECT * from Tweets")
records=cursor.fetchall()
print(records)
uni_recomm.commit()


# In[21]:


for tweets in tweepy.Cursor(api.search_tweets,q="#Toefl",count=100, #The q variable holds the hashtag 
                           lang="en",
                           since="2017-04-19").items():
    if(tweets.entities.get('media',[])) : #This condition appends only those tweets to the list which have image URL's
        media = tweets.entities.get('media')
        outtweets_all.append([tweets.id_str,tweets.user.id_str,tweets.user.name,tweets.user.screen_name,tweets.user.profile_image_url_https,tweets.user.description,tweets.user.friends_count,tweets.user.followers_count,tweets.user.created_at,tweets.text,tweets.created_at,tweets.favorite_count,tweets.entities['hashtags'],tweets.entities['user_mentions']])
print(outtweets_all) 


# In[22]:


tweetdf_all = pd.DataFrame(outtweets_all,columns = ["tweet_id","user_id","user_name","twitter_handle","profile_image_url","tweet_description","friends_count","followers_count","user_account_created","tweet_text","created_at","tweet_likes","tags",'target_user'])
tweetdf_all.to_csv(r'C:\\Users\\Lenovo\\Downloads\\Twitter_Data\\Toefl_data.csv')
tweetdf_all


# In[23]:


tweetdf_user = tweetdf_all[["user_id","twitter_handle","user_name","profile_image_url","tweet_description","friends_count","followers_count","user_account_created"]]
tweetdf_user = tweetdf_user.drop_duplicates('user_id') #drop duplicates
tweetdf_user


# In[24]:


tweetdf_tweet = tweetdf_all[["tweet_id","user_id","tweet_text","created_at","tweet_likes"]]
tweetdf_tweet = tweetdf_tweet.drop_duplicates('tweet_id')
tweetdf_tweet


# In[25]:


tweetdf_tags = tweetdf_all[["tweet_id","user_id","tags"]]
new_tags =pd.DataFrame(columns=["tweet_id","user_id","tags"])

for i,row in tweetdf_tags.iterrows():
    for values in row['tags']:
        temp=[row['tweet_id'], row['user_id'],values['text']]
        new_tags.loc[len(new_tags)] = temp
new_tags.i=np.arange(1, len(new_tags) + 1)
new_tags['tag_id']=new_tags.i
new_tags=new_tags[['tag_id','user_id','tags','tweet_id']]
new_tags


# In[26]:


tweetdf_mentions = tweetdf_all[["tweet_id","user_id","target_user"]]
new_mentions =pd.DataFrame(columns=["tweet_id","user_id","target_user"])
            
for i,row in tweetdf_mentions.iterrows():
    if row['target_user']!=None:
        if len(row['target_user'])!=0:
            for values in row['target_user']:
                temp=[row['tweet_id'], row['user_id'],values['screen_name']]
                new_mentions.loc[len(new_mentions)] = temp
new_mentions.i=np.arange(1, len(new_mentions) + 1)
new_mentions['mention_id']=new_mentions.index
new_mentions=new_mentions[['mention_id','tweet_id','user_id','target_user']]
new_mentions


# In[58]:


for i,row in tweetdf_user.iterrows():
    cursor.execute("INSERT INTO User values (%s,%s,%s,%s,%s,%s,%s,%s)", (int(row['user_id']),row['twitter_handle'],row['user_name'],row['profile_image_url'],row['tweet_description'],row['friends_count'],row['followers_count'],row['user_account_created']))
uni_recomm.commit()

cursor.execute("SELECT * from User")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[77]:


for i,row in new_tags.iterrows():
    cursor.execute("INSERT INTO Tweet_Tags values (%s,%s,%s,%s)", (int(row['tag_id']),row['user_id'],row['tags'],row['tweet_id']))
uni_recomm.commit()
cursor.execute("SELECT * from Tweet_Tags")
records=cursor.fetchall()
print(records)
uni_recomm.commit()


# In[80]:


for i,row in new_mentions.iterrows():
    cursor.execute("INSERT INTO Tweet_Mentions values (%s,%s,%s,%s)", (int(row['mention_id']),row['tweet_id'],row['user_id'],row['target_user']))
uni_recomm.commit()
cursor.execute("SELECT * from Tweet_Mentions")
records=cursor.fetchall()
print(records)
uni_recomm.commit()


# In[120]:


for i,row in tweetdf_tweet.iterrows():
    cursor.execute("INSERT INTO Tweets values (%s,%s,%s,%s,%s)", (int(row['tweet_id']),row['user_id'],row['tweet_text'],row['created_at'],row['tweet_likes']))
uni_recomm.commit()
cursor.execute("SELECT * from Tweets")
records=cursor.fetchall()
print(records)
uni_recomm.commit()

