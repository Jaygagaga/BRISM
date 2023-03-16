import tweepy
import requests
import pandas as pd
import os.path
import json
from Twitter.twitter_authentication import bearer_token
import time

"""Researching for author info for each retweeter/liking_user from retweeter/liking_user df
   input: retweeter_user_bri / liking_user_bri
   output: retweeter_user_bri_new / liking_user_bri
           propertites: username', 'description', 'verified', 'location', 'profile_image_url',
                        author_id', 'name', 'url', 'protected', 'followers_count', 'following_count',
                        'tweet_count', 'listed_count'
"""
author_profile_path = '/Users/jie/BRISM_project/BRISM_project/Twitter/data/authors.csv'
# import argparse

# parser = argparse.ArgumentParser()
# parser.add_argument("--file","-l", nargs="+",required=True)
# parser.add_argument("--outfile", "-o", type=str, required=False)
# parser.add_argument("--tab_result", "-tr", type=str, required=False)
# args = parser.parse_args()


#assign value too variables
# file_path = args.file
# outfile = args.outfile
# file_path = '/Users/jie/Library/Mobile Documents/com~apple~CloudDocs/NUS/BRISM/data/retweeter_user_merge.csv'
# output_path = '/Users/jie/Library/Mobile Documents/com~apple~CloudDocs/NUS/BRISM/data/retweeter_user_merge_new.csv'
# file_path = '/Users/jie/Library/Mobile Documents/com~apple~CloudDocs/NUS/BRISM/data/liking_user/liking_user_bri.csv'
# output_path = '/Users/jie/Library/Mobile Documents/com~apple~CloudDocs/NUS/BRISM/data/liking_user_bri_new.csv'
# file_path = '/Users/jie/Library/Mobile Documents/com~apple~CloudDocs/NUS/BRISM/data/retweeter/retweeter_user_bri.csv'
# output_path = '/Users/jie/Library/Mobile Documents/com~apple~CloudDocs/NUS/BRISM/data/retweeter_user_info.csv'
output_path = './data/authors.csv'
txt_path = './data/search_author_id_theme_.txt'
# json_path = '/Users/jie/phd_project/brism/TwitterNetwork/data/user_info.json'

class SearchAuthorInfo(object):
    def __init__(self, file_path,output_path):
        if file_path:

            self.df = pd.read_csv(file_path)
            if 'tweet_id' not in self.df.columns and 'id' in self.df.columns:
                self.df.rename(columns={'id':'tweet_id'},inplace=True)
                tweet_ids = list(self.df.tweet_id.unique())
                self.tweet_ids = [str(i) for i in tweet_ids]
                self.df= self.df[['tweet_id','username']]
                self.df.username = self.df.username.astype(str)
                self.usernames =list(self.df.username.unique())
            else:
                self.df.tweet_id = self.df.tweet_id.astype(str)
                tweet_ids = list(self.df[self.df.author_id.isnull() == True].tweet_id.unique())
                self.tweet_ids = [str(i) for i in tweet_ids]
                self.rest_df =self.df[self.df.author_id.isnull() == True]
        else:
            self.usernames = None
            self.tweet_ids = None
        self.client =tweepy.Client(bearer_token, wait_on_rate_limit=True)
        self.tweet_fields = "tweet.fields=id,author_id"#,created_at,geo,public_metrics,text,lang,referenced_tweets,conversation_id"
        self.user_fields = "user.fields=username,id,name,public_metrics,description,location,profile_image_url,protected,url,verified"
        self.output_path = output_path
        # self.json_path = json_path

    @staticmethod
    def bearer_oauth(r):
        """
        Method required by bearer token authentication.
        """

        r.headers["Authorization"] = f"Bearer {bearer_token}"
        r.headers["User-Agent"] = "v2UserLookupPython"
        return r


    def connect_to_endpoint(self,url):
        response = requests.request("GET", url, auth=self.bearer_oauth)
        print(response.status_code)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 400:
            return None
        else:
            time.sleep(120)
            self.connect_to_endpoint(url)


    def get_author_id(self,tweet_ids):
        print('Total tweet_ids: {}'.format(tweet_ids))
        author_tweet_tuple = []
        for num, i in enumerate(tweet_ids):
            # param = "ids={}".format(id)
            url = "https://api.twitter.com/2/tweets?ids={}&{}".format(i, self.tweet_fields)
            json_response = self.connect_to_endpoint(url)
            # time.sleep(1)
            if json_response:
                try:
                    print('Getting author of tweet No.{}'.format(num))
                    author_id = json_response['data'][0]['author_id']
                    author_tweet_tuple.append((str(i),str(author_id)))
                except:
                    continue
        self.author_tweet_tuple = author_tweet_tuple
        return author_tweet_tuple

    def get_user_name(self,author_ids):
        usernames_tuple = []
        for num, author in enumerate(author_ids):
            response = self.client.get_user(id=author)
            if response.data:
                print('Getting username of user No.{}'.format(num))
                username = response.data.username
                print(username)

                usernames_tuple.append((author,username))
        self.usernames_tuple = usernames_tuple
        return usernames_tuple


    # usernames = "usernames=TwitterDev,TwitterAPI"


    # schema = {'username':str, 'description':str, 'verified':str, 'location':str,
    #           'profile_image_url':str,'author_id':str, 'name':str, 'url':str,
    #           'protected':str,'followers_count':int, 'following_count':int, 'tweet_count':int, 'listed_count':int}

    def get_user_info(self,usernames):
        print('Total users:{}'.format(len(usernames)))
        user_info = pd.DataFrame(columns=['username', 'description', 'verified', 'location', 'profile_image_url',
                                          'author_id', 'name', 'url', 'protected', 'followers_count', 'following_count',
                                          'tweet_count', 'listed_count'
                                          ])
        for num, username in enumerate(usernames):
            query = "https://api.twitter.com/2/users/by?usernames={}&{}".format(username, self.user_fields)
            json_response = self.connect_to_endpoint(query)

            if json_response:

                print('Getting user info of user No.{}'.format(num))
                try:
                    data = json_response['data'][0]
                    #Intended to save json file
                    # a = []
                    # if not os.path.isfile(self.json_path):
                    #     a.append(data)
                    #     with open(self.json_path, mode='w') as f:
                    #         f.write(json.dumps(a, indent=2))
                    # else:
                    #     with open(self.json_path) as feedsjson:
                    #         feeds = json.load(feedsjson)
                    #
                    #     feeds.append(data)
                    #     with open(self.json_path, mode='w') as f:
                    #         f.write(json.dumps(feeds, indent=2))

                    username = [data['username']] if 'username' in data else [None]
                    description = [data['description']] if 'description' in data  else [None]
                    verified = [data['verified']] if 'verified' in data else [None]
                    location = [data['location']] if 'location' in data else [None]
                    profile_image_url = [data['profile_image_url']] if 'profile_image_url' in data else [None]
                    author_id = [str(data['id'])] if 'id' in data else [None]
                    name = [data['name']] if 'name' in data else [None]
                    url = [data['url']] if 'url' in data else [None]
                    protected = [data['protected']] if 'protected' in data else [None]
                    followers_count = [data['public_metrics']['followers_count']] if 'public_metrics' in data else [None]
                    following_count = [data['public_metrics']['following_count']] if 'public_metrics' in data  else [None]
                    tweet_count = [data['public_metrics']['tweet_count']] if 'public_metrics' in data else [None]
                    listed_count = [data['public_metrics']['listed_count']] if 'public_metrics' in data else [None]
                    df =pd.DataFrame(list(zip(username,description,verified,location,profile_image_url,author_id,name,
                                              url,protected,followers_count,following_count,tweet_count,listed_count)),
                                     columns =['username', 'description', 'verified', 'location', 'profile_image_url',
                                               'author_id', 'name', 'url', 'protected','followers_count', 'following_count',
                                               'tweet_count', 'listed_count'
                                                            ])
                    # df['author_id'] = df['author_id'].map(lambda x: 'a'+x+'b')
                    user_info = pd.concat([user_info,df])
                except:
                    pass
        return user_info

    def search_user(self, author_ids,author_tweet_tuple):
        usernames_tuple = self.get_user_name(author_ids)
        usernames = [i[1] for i in usernames_tuple]
        usernames = list(set(usernames))
        # usernames = usernames
        print('Sleeping')
        time.sleep(60)
        user_info = self.get_user_info(usernames)
        user_info['author_id'] = user_info['author_id'].map(lambda x: 'a' + x + 'b')
        # user_info['author_id'] = user_info['author_id'].map(lambda x: 'a' + x + 'b')
        # print(user_info.columns)
        author_tweet_df = pd.DataFrame(author_tweet_tuple, columns=['tweet_id', 'author_id'])
        author_tweet_df['author_id'] = author_tweet_df['author_id'].map(lambda x: 'a' + x + 'b')
        author_tweet_df['tweet_id'] = author_tweet_df['tweet_id'].map(lambda x: 'a' + x + 'b')
        # print(author_tweet_df.author_id.iloc[0])
        user_info = author_tweet_df.merge(user_info, how='left', on='author_id')
        return user_info,author_tweet_df
    def run(self):
        if not self.usernames:
            # author_tweet_tuple = self.get_author_id(self.tweet_ids)
            if len(self.tweet_ids) < 100:
                # pass
                author_tweet_tuple = self.get_author_id(self.tweet_ids)
                author_ids = [i[1] for i in author_tweet_tuple]
                user_info, author_tweet_df = self.search_user(author_ids, author_tweet_tuple)
                self.save_file(user_info)
            else:
                for num, tu in enumerate(self.tweet_ids):
                    if num % 100 == 0 and num != 0:
                        author_tweet_tuple = self.get_author_id(self.tweet_ids[num-100:num])
                        print('No. {} batch of 100 records'.format(num))
                        author_ids = [i[1] for i in author_tweet_tuple]
                        # print('Sleeping')
                        # time.sleep(60)
                        user_info,author_tweet_df  = self.search_user(author_ids, author_tweet_tuple)

                        self.save_file(user_info)
                        print('Saved use info csv')

        if self.usernames:
            user_info = self.get_user_info(self.usernames)
            # print('2',self.user_info)
            user_info = self.df.merge(user_info,how = 'left', on = 'username')
            self.save_file(user_info)
            # self.author_tweet_df = author_tweet_df.merge(user_info, how='left', on='author_id')
        # self.rest_df = self.rest_df.merge(self.author_tweet_df, how='left', on='tweet_id')
        # self.new_df =

    def save_file(self,user_info):
        if os.path.isfile(self.output_path):
            user_info.to_csv(self.output_path, mode='a', header=False)
        else:
            user_info.to_csv(self.output_path)









# user_info = get_user_info(usernames)


#36169
if __name__ == '__main__':
    search_author = SearchAuthorInfo(file_path=None,output_path=output_path)
    #if run before, load exisiting csv
    author_profile = pd.read_csv(author_profile_path, lineterminator='\n')
    # last_tweet = author_profile.tweet_id.unique()[-1][1:-1]
    author_profile = author_profile.drop_duplicates(['tweet_id'])
    # author_profile = author_profile.drop_duplicates(['username'])
    if "listed_count\r" in author_profile.columns:
        author_profile.rename(columns = {"listed_count\r": "listed_count"}, inplace=True)
    author_profile = author_profile[['tweet_id', 'author_id', 'username',
       'description', 'verified', 'location', 'profile_image_url', 'name',
       'url', 'protected', 'followers_count', 'following_count', 'tweet_count','listed_count']]
    author_profile.to_csv('/Users/jie/BRISM_project/BRISM_project/Twitter/data/authors.csv')
    with open(txt_path) as f:
        needed = f.readlines()
    needed = [s.replace('\n', '') for s in needed]
    # if run before,
    needed= needed[30000:]
    needed = [i for i in needed if i not in author_profile.tweet_id.map(lambda x: str(x)[1:-1]).to_list()]
    # index =needed.index(last_tweet)
    # needed = needed[index:]
    print('Need to find author for a total of {} tweets'.format(len(needed)))
    search_author.tweet_ids =needed
    # now = time.localtime(time.time()).tm_min
    search_author.run()
    # search_author.save_file()


