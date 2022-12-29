import pandas as pd
import zipfile
import io
import ast
from demographer.indorg_neural import NeuralOrganizationDemographer
from demographer.indorg import IndividualOrgDemographer
import json
from demographer import process_tweet
import re

tweet_path = '/Users/jie/phd_project/BRISM_project/Twitter/data/all_tweets.csv.zip'
user_path = '/Users/jie/phd_project/BRISM_project/Twitter/data/user_tweet.csv.zip'

class IndOrgIdentifier(object):
    def __init__(self, user_path, tweet_path):
        with zipfile.ZipFile(tweet_path) as zip_archive:
            with zip_archive.open('all_tweets.csv') as f:
                self.tweets = pd.read_csv(io.StringIO(f.read().decode()))
        with zipfile.ZipFile(user_path) as zip_archive:
            with zip_archive.open('user_tweet.csv') as f:
                self.users = pd.read_csv(io.StringIO(f.read().decode()))
    def merge(self):
        all_tweets_users = pd.merge(self.tweets,self.users, how ='left', left_on='id', right_on='tweet_id')
        return all_tweets_users

    def _tweet_type(self,df):
        df.loc[df.referenced_tweets.str.contains('retweeted') == True, 'Status'] = 'retweeted'
        df.loc[df.referenced_tweets.str.contains('replied_to') == True, 'Status'] = 'replied_to'
        df.loc[df.referenced_tweets.str.contains('quoted') == True, 'Status'] = 'quoted'
        df['associated_tweets'] = [
            'a' + str(list(ast.literal_eval(t).keys())[0]) + 'b' if str(t) != 'nan' and str(
                t) != 'None' and t != None
            else None for t in df.referenced_tweets]
        return df

    def assign_tweet_type(self,df):
        new_df = self._tweet_type(df)
        return new_df



    def subset(self,df,col):
        df = df[df[col].isnull()==False]
        return df
    def get_tweet_id_for_scrapy_user(self,df,status):
        replied_to_tweets = list(df[df.Status == status].associated_tweets.unique())
        replied_to_tweets = [t[1:-1] for t in replied_to_tweets]
        return replied_to_tweets
    def save_txt(self,li,file_path):
        with open(file_path, 'w') as f:
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
  indorg = IndOrgIdentifier(user_path,tweet_path)
  tweets_users = indorg.merge()
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
