#### imports #### 

import tweepy
import json
import pandas as pd
from datetime import datetime
from datetime import timedelta
# pip install tweet-preprocessor
import preprocessor as pre  
import regex as re
import time
import configparser



#### Functions #### 
# export these into another file later 
print('tweepy version: ' + tweepy.__version__)

# Function: preprocess tweet text
def cleantweet(tweet):
    #https://towardsdatascience.com/basic-tweet-preprocessing-in-python-efd8360d529e
    #additional cleaning and parsing can be done 
    cleantweet = pre.clean(tweet)
    cleantweet = cleantweet.lower()
    cleantweet = re.sub('\d+', '', cleantweet)
    cleantweet = re.sub(r'[^\w\s]', '', cleantweet)  
    return cleantweet


# Function: identify tweet quality
def tweetquality(user_verified, favorite_count, retweet_count):
    if user_verified == True or favorite_count > 100 or retweet_count > 10:
        return True
    else:
        return False

# Function: twitter search pagination and rate limit handling
def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except StopIteration:
            break
        except tweepy.error:
            print('Reached rate limit. Sleeping for >15 minutes')
            time.sleep(15 * 61)



# Function: obtain query list from ticker list
def getqueries(tickers): #returns a list of query strings
    queries = []
    for ticker in tickers:
        queries.append('$'+ticker + ' -filter:retweets')
    return queries



def get_tweets(query, since_id, until_date, max_tweets):
    search = limit_handled(tweepy.Cursor(api.search_tweets,
                                            q = query, 
                                            count = 100,
                                            tweet_mode='extended',
                                            lang='en',
                                            result_type="recent",
                                            ).items(max_tweets))

    dftweets = pd.DataFrame()
    #cycle through generator 
    for tweet in search:
        dftweets = pd.concat([dftweets, pd.json_normalize(tweet._json)])

    print(query,'\n','# tweets collected:', len(dftweets), '\n')

    try:
        dftweets['full_text_preprocessed'] = dftweets.apply(lambda row : cleantweet(row['full_text']), axis = 1)
        dftweets['quality'] = dftweets.apply(lambda row : tweetquality(row['user.verified'], row['favorite_count'], row['retweet_count']), axis = 1)
        dftweets['num_cashtags'] = dftweets.apply(lambda row : str(row['entities.symbols']).count('text'), axis = 1)
        dftweets['ticker'] = dftweets.apply(lambda row : query.split()[0], axis = 1)
        dftweets['query_params'] = dftweets.apply(lambda row : 'query:'+query+' since_id:'+str(since_id)+' until_date:'+str(until_date)+' max_tweets:'+str(max_tweets), axis = 1)
        #apply filter
        dftweets = dftweets[dftweets.num_cashtags == 1]
        return dftweets
            
    except Exception:
        #for debugging purposes
        print('\n', 'preprocessing broke!!!!!', '\n')
    finally:
        print('# tweets (filtered):',len(dftweets),'\n') 


#### Data Extraction ####


#will be provided by scoping algorithm 
tickers = [
'AMZN',
'MSFT',
'SNAP',
'HOOD',
'WMT',
'GOOG',
'PTON',
'CPRX',
'TSLA',
'AAPL',
'F',
'AMC',
'SNDL',
'DIS',
'NIO',
'META',
'NFLX',
'LCID',
'NVDA',
'VOO',
'SPY',
'GOOGL',
'GPRO',
'PFE',
'PLUG',
'AAL',
'CCL',
'BABA',
'BAC',
'RIVN',
'SBUX',
'PLTR',
'DAL',
'AMD',
'NOK',
'GME',
'KO',
'TLRY',
'COIN',
'VTI',
'T',
'CGC',
'PYPL',
'SPCE',
'UBER',
'GM',
'MRNA',
'BB'
]


#parameters
max_tweets = 2000 #max per ticker 

print('tickers:', tickers)
print('max_tweets:', max_tweets, '\n')

# Set date/time parameters
cur_time_utc = datetime.utcnow().replace(microsecond=0)
until_date = cur_time_utc.strftime("%Y-%m-%d") #"2022-04-03"#"2022-04-25"#
from_date =  cur_time_utc - timedelta(days=1) #"2022-04-02"#"2022-04-24"#
from_date = from_date.strftime("%Y-%m-%d")

print('from date:', from_date) 
print('until date:', until_date, '\n')

#Config
config = configparser.ConfigParser()
config_path = r"C:\Users\Dennis\Desktop\Wisdm\wisdmai\Data\.Archive\Twitter\config.ini"
config.read(config_path)

api_key = config['twitter']['api_key']
api_key_secret = config['twitter']['api_key_secret']

access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']


# API Authentication
auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)

#wrapper for Twitter API 
api = tweepy.API(auth, wait_on_rate_limit=True)

#get ticker queries 
queries = getqueries(tickers)
print('first queury:', queries[0], '\n')


# Find the last tweet id for from_date (need this to filter on from_date)
search_since_id = limit_handled(tweepy.Cursor(api.search_tweets, 
                                                q = 'A', #query does not matter
                                                tweet_mode = 'extended', 
                                                lang = 'en', 
                                                result_type = 'recent', 
                                                until = from_date
                                                ).items(1))

since_id  = [tweet._json['id'] for tweet in search_since_id][0]
print('since last tweet id:', since_id, '\n')

#tweets dataframe for all tweets (unfiltered) - might be able to store seperately 
dftweets = pd.DataFrame()
#pulling tweets 


for query in queries:
    new_tweets = get_tweets(query, since_id, until_date, max_tweets)
    dftweets = pd.concat([dftweets, new_tweets], axis = 0, ignore_index = True)



#### Data Exportation #### 
# change code to send to database once developed 

export_df = dftweets[['ticker',
            'created_at',
            'full_text_preprocessed',
            'user.verified',
            'favorite_count',
            'retweet_count',
            'quality',
            'entities.symbols',
            'num_cashtags'
            ,'query_params', 
            'user.followers_count'
            ]].copy() 


export_path = r"C:\Users\Dennis\Desktop\dftweets.csv"
export_df.to_csv(export_path)


