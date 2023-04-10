#### imports ####

import praw
import pandas as pd
from datetime import datetime
from praw.models import MoreComments
import preprocessor as pre
import regex as re


reddit = praw.Reddit(
    client_id="auD_kIwyQ1r3hfxTQEYuGw",
    client_secret="XyLGeMB1mJqoaB0lCXrk4Jtmy515AA",
    password="wisdmai1234",
    user_agent="wisdm",
    username="Dramatic-Ad-9651",
    check_for_async=False
)

# make sure we're connected to the api
print(reddit.user.me())


#### Data Extraction #### 

posts = pd.DataFrame()
comments = pd.DataFrame()

tickers = ['GME', 'TSLA']
subreddits = ['wallstreetbets', 'stocks']


for ticker in tickers: 
    for sub in subreddits: 
        subreddit = reddit.subreddit(sub)
        for post in subreddit.search(ticker.lower(), sort = 'new', time_filter = 'day', limit = None):
            #check if title has stock ticker 
            if ticker.lower() not in post.title.lower(): 
                #print(post.title)
                continue 
            #check if author is not banned 
            if hasattr(post.author, 'is_suspended'):
                #print(post.author.is_suspended)
                continue
            
            try: 
                #collect desired values 
                title_instance = {
                    'ticker': ticker, 
                    'subreddit': post.subreddit,
                    'content': post.title, 
                    'upvotes': post.score, 
                    'upvote_ratio': post.upvote_ratio,
                    'num_comments': post.num_comments, 
                    'author_karma': post.author.comment_karma, 
                    'author_verified': post.author.has_verified_email, 
                    'time': datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d'),
                }
            except:
                #Untested edge case: author deletes account > unable to collect user data from author 
                print(Exception)
                pass

            #create row and concat it to the df
            row = pd.DataFrame([title_instance])
            posts = pd.concat([posts, row], axis = 0, ignore_index = True)

            #checking comments
            for comment in post.comments:
                #do not want sub comments of comments 
                if isinstance(comment, MoreComments):
                    continue
                #remove user reports
                if 'user report' in comment.body.lower(): 
                    continue
                

                try: 
                    comment_instance = {
                        'ticker': ticker, 
                        'subreddit': post.subreddit, 
                        # optional can remove if no grouping by title is needed 
                        'post_title': post.title,
                        'content': comment.body, 
                        'upvotes': comment.score, 
                        'replies': comment.replies.__len__(), 
                        'sticked': comment.stickied,
                        'author_karma': comment.author.comment_karma, 
                        'author_verified': comment.author.has_verified_email, 
                        'time': datetime.fromtimestamp(comment.created_utc).strftime('%Y-%m-%d'),
                    }
                    
                    row = pd.DataFrame([comment_instance])
                    comments = pd.concat([comments, row], axis = 0, ignore_index = True)
                except: 
                    #edge case: user deletes comment > unable to retrieve user karma > exception
                    # edge case: comment removed by moderator

                    print(Exception)
                    pass



#### Data Export #### 


comments_df = comments.copy()
posts_df = posts.copy()




comments_export_path = r"comments.csv"
comments_df.to_csv(comments_export_path)

posts_export_path = r"posts.csv"
posts_df.to_csv(posts_export_path)

print('Data Exported')
print("Posts", len(posts_df))
print("Comments:", len(comments_df))