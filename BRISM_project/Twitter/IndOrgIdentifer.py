import pandas as pd
import zipfile
import io
import ast
from demographer.indorg_neural import NeuralOrganizationDemographer
from demographer.indorg import IndividualOrgDemographer
import json
from demographer import process_tweet
import re
import os
print('Current root directory: ',os.getcwd())
# tweet_path = '/Users/jie/phd_project/BRISM_project/Twitter/data/all_tweets.csv.zip'
# user_path = '/Users/jie/phd_project/BRISM_project/Twitter/data/user_tweet.csv.zip'
sea_bri_path = '/BRISM_project/Twitter/data/China_SEA_tagged.csv.zip'

class IndOrgIdentifier(object):
    def __init__(self, sea_bri_path):
        self.data = pd.read_csv(sea_bri_path,compression='zip',lineterminator='\n')

    def get_tweet_id_for_scrapy_user(self,df,status,existing_scraped_path=None):

        users_to_scrape = list(df[df.Status == status].associated_tweets.unique())
        users_to_scrape = [t[1:-1] for t in users_to_scrape]
        if os.path.isfile(existing_scraped_path):
            with open(existing_scraped_path) as f:
                existing_scraped = [str(line.rstrip('\n')) for line in f]
            users_to_scrape = [user for user in users_to_scrape if user not in existing_scraped]
        return users_to_scrape
    def save_txt(self,li,file_path):
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        with open(file_path, 'a') as f:
            for line in li:
                f.write(f"{line}\n")
    def construct_data(self, associated_authors_path, df):
        associated_tweets_users = pd.read_csv(associated_authors_path)
        #construct in_reply_to_user_id_str, in_reply_to_screen_name
        look_up_authorid = dict(zip(associated_tweets_users.tweet_id, associated_tweets_users.author_id))
        look_up_username = dict(zip(associated_tweets_users.tweet_id, associated_tweets_users.username))
        for tweet_id, authord_id in  look_up_authorid.items():
            df.loc[(df.Status == 'replied_to') & (df.id==tweet_id), 'in_reply_to_user_id_str'] =authord_id
        for tweet_id, username in look_up_username.items():
            df.loc[(df.Status == 'replied_to') & (df.id == tweet_id), 'in_reply_to_screen_name'] = username
        #construct retweeted
        df.loc[df.Status == 'retweeted', 'retweeted'] = True
        df.loc[df.Status != 'retweeted', 'retweeted'] = False
        #construct truncated
        pattern = re.compile(r"(\.{3}\s?http)")
        def finddots(text):
            return len(re.findall(pattern,text))
        df.loc[df.text.map(lambda x: finddots(x)>0), 'truncated'] = True
        df.loc[df.truncated.isnull()==True, 'truncated'] = False
        return df

    def IndOrdDict(self, df):
        pass

if __name__ == '__main__':
  indorg = IndOrgIdentifier(sea_bri_path)
  tweets_users = indorg.subset(tweets_users,'author_id')
  tweets_users_ = indorg.assign_tweet_type(tweets_users)
  """Getting tweet ids of retweeted and replied_to tweets and save txt file for searching for their authors"""
  # replied_to_tweets = indorg.get_tweet_id_for_scrapy_user(tweets_users_, 'replied_to')
  # retweeted_tweets = indorg.get_tweet_id_for_scrapy_user(tweets_users_, 'retweeted')
  # indorg.save_txt(replied_to_tweets,'/Users/jie/phd_project/BRISM_project/Twitter/data/replied_to_tweets.txt')
  # indorg.save_txt(retweeted_tweets, '/Users/jie/phd_project/BRISM_project/Twitter/data/retweeted_tweets.txt')
  """Construct properties for IndOrg identifier"""
  tweets_users_ = indorg.construct_data('/Users/jie/phd_project/BRISM_project/Twitter/data/associated_authors.csv',tweets_users_)




# identifier = IndividualOrgDemographer(setup='balanced')

# identifier.process_tweet(new_tweet)
