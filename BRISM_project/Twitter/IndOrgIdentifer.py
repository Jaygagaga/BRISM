"""Individual-Organization identification would require several features:
   1) IndividualOrgDemographer from demographer (adopted from https://bitbucket.org/mdredze/demographer/src)
   2) Defined roles based on themes from AssignTheme.py
   3) User descriptions and user names
"""
import pandas as pd
import zipfile
import io
import ast
from demographer.indorg_neural import NeuralOrganizationDemographer
from demographer.indorg import IndividualOrgDemographer
from AddEntity import intersection,save_zip
# from AddEntity import save_zip, AddEntity, merged_data_path,entity_path,intersection
# from AssignTheme import AssignThemes,theme_path
import json
from demographer import process_tweet
from nltk.stem import WordNetLemmatizer
lemmatizer = WordNetLemmatizer()
from nltk.stem.snowball import SnowballStemmer
snowBallStemmer = SnowballStemmer("english")
import re
import os
print('Current root directory: ',os.getcwd())
# tweet_path = '/Users/jie/phd_project/BRISM_project/Twitter/data/all_tweets.csv.zip'
# user_path = '/Users/jie/phd_project/BRISM_project/Twitter/data/user_tweet.csv.zip'
# sea_bri_path = './bri_sea_cn.zip'
# sea_bri_path = './data/normalized_bri.zip'

description_roles = {'Government' :['republic', 'embassy', 'council','parliament','civil servant','ambassador','national spokesperson',
                                    'secretary','minister','ministry','office','committee','governor','governer','ministry of foreign affairs'],
                     'Education' : ['university','college','school','training','diplomacy','institute','programme','degree','education'],
                     'Agency':['scholarship','admission','application','fellowship', 'study abroad','consultancy','consulting','consultant','coach',
                               'visa','education adviser','education advisory','education consultancy','service',
                               'entry requirements','admission requirements','language requirement',
                               'entrance exam','enrollment','TOEFL','international student enrollment',
                               'international student admissions','IELTS'],
                     'Student':['student','undergrad','freshmen','masters','alumnus','graduate','phd candidate',
                                'ph.d','trainee','phd','doctoral student'],
                     'Researcher':['professor','prof','educator','dr.','lecturer','scholar','scholiast','researcher'
                                  ,'research fellow','expert','analyst','postdoc','research interest', 'research','fellowship',
                                   'research grant'],
                     'Media': ['media','news','information','press','china daily','platform','newsroom','newspaper'],
                     'Business':['company','enterprise','start-up','ceo','founder','entrepreneur','ltd','firm'],
                     'Organization':['organization','association','ngo','conference','think tank']
                     }

description_roles ={key:[snowBallStemmer.stem(v) if len(v.split()) ==1 else ' '.join([snowBallStemmer.stem(j) for j in v.split()]) for v in values]
                         for key, values in description_roles.items()}
#'Individual': ['fellow','director','']

class IndOrgIdentifier(object):
    def __init__(self, df=None,sea_bri_path=None,user_path1=None,user_path2=None,
                 tweets_cols=None,users_cols=None, assign_theme = True,):
        """

        :param tweets_cols: columns in tweet dataset (make up for missed authors)
        :param users_cols: columns in use dataset (make up for missed authors)
        :param sea_bri_path: path to filtered dataset with SEA and China location tags
        :param user_path: newly scraped user info for tweets with missed authors
        """
        self.data = pd.read_csv(sea_bri_path,compression='zip',lineterminator='\n')
        if 'origin_text' in self.data.columns:
            origin_text = self.data[['id', 'origin_text']]
        else:
            origin_text=None
            print('No column of origin_text exists')
        self.data = self.data.where(pd.notnull(self.data),None)
        self.data['origin_text_low'] = self.data.origin_text.str.lower()
        self.data = self.data[self.data.origin_text_low.str.contains('belt and road|silk road|one belt one road') == True]
        self.data = self.data.drop_duplicates(subset = ['id'])

        self.data = self.data[list(set(tweets_cols+users_cols))] if 'China_SEA' in self.data.columns else self.data[[j for j in list(set(tweets_cols + users_cols)) if j != 'China_SEA']]

        print('Number of datapoints: {}'.format(len(self.data)))
        print('Dataframe has columns: {}'.format(self.data.columns))
        if user_path1 and user_path2:
            user_info1 = pd.read_csv(user_path1,lineterminator='\n')
            user_info2 = pd.read_csv(user_path2,lineterminator='\n')
            user_info = pd.concat([user_info1,user_info2])
            data1 = self.data[self.data.author_id.isnull()==False]
            data2 = self.data[self.data.author_id.isnull()==True]
            data2 = data2[tweets_cols] if 'China_SEA' in self.data.columns else  data2[[j for j in tweets_cols if j != 'China_SEA']]
            data2 = data2.merge(user_info,how='left', left_on='id',right_on = 'tweet_id')
            data2 = data2[list(set(tweets_cols+users_cols))] if 'China_SEA' in self.data.columns else data2[[j for j in list(set(tweets_cols + users_cols)) if j != 'China_SEA']]

            assert len(data1.columns) == len(data2.columns) , "Numbers of columns are not matched!"

            self.data  = pd.concat([data1,data2])
            if origin_text is not None:
                self.data = self.data.merge(origin_text,how = 'left', on = 'id')
            self.data = self.data.where(pd.notnull(self.data), None)
            #remove datapoints without user name and a
            self.df = self.data[(self.data.author_id.isnull()==False) & (self.data.name.isnull()==False)]
            print('Dataframe columns are: ', self.df.columns)
            print('Number of datapoints is: ', len(self.df))
        # if assign_theme:
        #     self.themes = AssignThemes(theme_path)



    def process(self, df):
        df = df.where(pd.notnull(df), None)
        df['hastags'] = [ast.literal_eval(t) if t else None for t in df.hastags]
        df['mentions'] = [ast.literal_eval(t) if t else None for t in df.mentions]
        df['url_text'] = [ast.literal_eval(t) if t else None for t in df.url_text]
        # df = df.mask(df.applymap(str).eq('[]'))
        return df
    @staticmethod
    def get_mention_username(df,file_path=None):
        usernames = [i for i in df.mentions if i]
        usernames = list(set([i for sub in usernames for i in sub]))
        if file_path:
            with open(file_path, 'w') as f:
                for line in usernames:
                    f.write(f"{line}\n")
        return usernames



    def get_tweet_id_for_scrapy_user(self,df,status,existing_scraped_path=None,author=False):
        if author:
            #if this function is used to scrapy missed authors for the tweets
            users_to_scrape = list(df[df[status].isnull()==True]['id'].unique())
            users_to_scrape = [t[1:-1] for t in users_to_scrape]

        else:
            # if this function is used to scrapy authors for the associated tweets (retweeted, replied_to)
            users_to_scrape = list(df[df.Status == status].associated_tweets.unique())
            users_to_scrape = [t[1:-1] for t in users_to_scrape]
        if existing_scraped_path and existing_scraped_path.split('.')[-1]=='txt':
            with open(existing_scraped_path) as f:
                existing_scraped = [str(line.rstrip('\n')) for line in f]
        if existing_scraped_path and existing_scraped_path.split('.')[-1]=='csv':
            existing = pd.read_csv(existing_scraped_path,lineterminator='\n')
            existing_scraped = existing['tweet_id'].map(lambda x:x[1:-1]).to_list()
        users_to_scrape = [user for user in users_to_scrape if user not in existing_scraped]
        return users_to_scrape
    def save_txt(self,li,file_path=None):
        # if author:
        #     file_path = './data/search_author_id.txt'
        with open(file_path, 'a') as f:
            for line in li:
                f.write(f"{line}\n")
    def construct_data(self, associated_authors_path, df):
        """To be used as input for IndOrg indentifier."""
        associated_tweets_users = pd.read_csv(associated_authors_path)
        #construct in_reply_to_user_id_str, in_reply_to_screen_name
        look_up_authorid = dict(zip(associated_tweets_users.tweet_id, associated_tweets_users.author_id))
        look_up_username = dict(zip(associated_tweets_users.tweet_id, associated_tweets_users.username))
        for tweet_id, authord_id in look_up_authorid.items():
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
    def construct_json(self,df):
        """To be used as input for IndOrg indentifier."""
        tweets_txt= []
        for i in range(len(df)):
            if i % 100 == 0:
                print('Construct json for datapoint No. {}'.format(i))
            tweet_dict = {}
            tweet_dict['contributors'] = None
            tweet_dict['in_reply_to_status_id'] = None
            tweet_dict['favorite_count'] = None
            tweet_dict['source'] = None
            tweet_dict['coordinates'] = None
            tweet_dict['entities'] = None
            tweet_dict['favorited'] = None
            tweet_dict['truncated'] = False if df.truncated.iloc[i] == False else True
            tweet_dict['text'] =  df.origin_text.iloc[i] if df.origin_text.iloc[i] else None
            tweet_dict['id_str'] = df['id'].iloc[i][1:-1] if df['id'].iloc[i] else None
            tweet_dict['retweeted'] = False if df['retweeted'].iloc[i] else True
            tweet_dict['in_reply_to_screen_name'] = df['in_reply_to_screen_name'].iloc[i] if df['in_reply_to_screen_name'].iloc[i] else None
            tweet_dict['retweet_count'] = df['retweet_count'].iloc[i] if df['retweet_count'].iloc[i] else None
            tweet_dict['in_reply_to_user_id_str'] = df['in_reply_to_user_id_str'].iloc[i][1:-1]  if df['in_reply_to_user_id_str'].iloc[i] else None
            user_dict = {}
            user_dict['follow_request_sent'] = 0
            user_dict['profile_use_background_image'] = None
            user_dict['default_profile_image'] = None
            user_dict['profile_sidebar_fill_color']= None
            user_dict['profile_text_color'] = None
            user_dict['profile_sidebar_border_color'] = None
            user_dict['profile_background_color'] = None
            user_dict['profile_background_image_url_https'] = None
            user_dict['utc_offset'] = 0
            user_dict['statuses_count'] = 0
            user_dict['friends_count'] = 0
            user_dict['profile_link_color'] = None
            user_dict['profile_image_url_https'] = None
            user_dict['geo_enabled'] = None
            user_dict['profile_background_image_url'] = None
            user_dict['lang'] = None
            user_dict['profile_background_tile'] = None
            user_dict['favourites_count'] = 0
            user_dict['notifications'] = None
            user_dict['created_at'] = 0
            user_dict['contributors_enabled'] = None
            user_dict['time_zone'] = None
            user_dict['protected'] = None
            user_dict['default_profile'] = None
            user_dict['is_translator'] = None
            user_dict['id_str'] = df['author_id'].iloc[i][1:-1] if df['author_id'].iloc[i] else None
            user_dict['profile_image_url'] = df['profile_image_url'].iloc[i] if df['profile_image_url'].iloc[i] else None
            user_dict['verified'] = df['verified'].iloc[i] if df['verified'].iloc[i] else 0
            user_dict['followers_count'] = df['followers_count'].iloc[i] if df['followers_count'].iloc[i] else 0
            user_dict['listed_count'] = df['listed_count'].iloc[i] if df['listed_count'].iloc[i] else 0
            user_dict['description'] = df['description'].iloc[i] if df['description'].iloc[i] else None
            # user_dict['friends_count'] = df['friends_count'].iloc[i] if df['friends_count'].iloc[i] else None
            user_dict['following'] = df['following_count'].iloc[i] if df['following_count'].iloc[i] else 0
            user_dict['name'] = df['name'].iloc[i] if df['name'].iloc[i] else ''
            user_dict['screen_name'] = df['username'].iloc[i] if df['username'].iloc[i] else ''
            user_dict['url'] = df['url'].iloc[i] if df['url'].iloc[i] else None

            tweet_dict['entities'] = {}
            tweet_dict['entities']['symbols'] = []
            tweet_dict['entities']['urls'] = df.url_text.iloc[i] if df.url_text.iloc[i] else []
            tweet_dict['entities']['hashtags'] = df['hastags'].iloc[i] if df['hastags'].iloc[i] else []

            if df['mentions'].iloc[i] and len(df['mentions'].iloc[i]) > 0:
                tweet_dict['entities']['user_mentions'] = []
                if len(df['mentions'].iloc[i]) == 1 and df.retweeted.iloc[i] == True:
                    tweet_dict['entities']['user_mentions'] = []
                else:
                    tweet_dict['entities']['user_mentions'].append({'screen_name':j} for j in df['mentions'].iloc[i][1:])


            tweet_dict['user'] = user_dict
            # tweet_dict['user'] = { k: (0 if v is None else v) for k, v in tweet_dict['user'].items()}
            tweets_txt.append(tweet_dict)

        assert len(tweets_txt) == len(df), "List length must be equal to dataframe length!"
        return tweets_txt

    def IndOrdScore(self, txt):
        identifier = IndividualOrgDemographer(setup='balanced')
        identified_indorg = []
        for num, t in enumerate(txt):
            try:
                if t['user']['verified']=='False':
                    print("Need to modify 'verified'.")
                    t['user']['verified']=False
                if t['user']['verified']=='True':
                    print("Need to modify 'verified'.")
                    t['user']['verified'] = True
                a = [(k,v) for k, v in identifier.process_tweet(t)['indorg_balanced']['scores'].items()]
                identified_indorg.append(a[0])
            except:
                print('Unsuccessful identification of individual VS. organization in No.{}.'.format(num))
                break
                identified_indorg.append(None)
        return identified_indorg
    def save_attribute(self, identified_indorg,df,filename):
        assert len(identified_indorg) == len(df), "The length of the extracted IndOrg attributes list must be equal to that of dataframe!"

        df['identified_indorg'] = identified_indorg
        subset =df[['id','identified_indorg']]
        if filename:
            save_zip(subset,filename)
    # def assign_theme(self,df,col): #col='lowered_norm_text'
    #     theme_df =self.themes.assign_themes(df,col)
    #     return theme_df
    def roles(self,theme_df,description_roles):
        theme_df['follower_following_ratio'] = theme_df.followers_count/theme_df.following_count

        # Round1: identify defined roles from user descriptions and user names
        identified_roles = []
        theme_df.description = theme_df.description.astype(str)
        theme_df.name = theme_df.name.astype(str)
        for j,k in zip(theme_df.description.to_list(), theme_df.name.to_list()):
            # print(j,k)
            identified = []
            text = [lemmatizer.lemmatize(j.lower()) for j in j.split()] if j else ['']
            name = [lemmatizer.lemmatize(k.lower()) for k in k.split()] if k else ['']
            for key, value in description_roles.items():
                if 'professor' in text:
                    print(text)
                if len(intersection(value, text)) > 0 and any(ext in ' '.join(text) for ext in value):
                    # print('True')
                    identified.append(key)
                if len(intersection(value, name)) > 0 and any(ext in ' '.join(name) for ext in value):
                    identified.append(key)
            if len(identified) != 0:
                identified_roles.append(list(set(identified)))
            else:
                identified_roles.append(None)
        theme_df['identified_roles'] = identified_roles

        #Round 2: determine individual VS. organization based on identified_indorg scores
        ind_org = []
        assert 'identified_indorg' in theme_df.columns, "identified_indorg attributes do not exist!"
        ind_list = ['director','manager','photographer','writer','editor', 'journalist', 'consultant',
              'content creator']
        for i in range(len(theme_df)):
            # print(theme_df.identified_indorg.iloc[i][0],theme_df['follower_following_ratio'].iloc[i],theme_df.description.iloc[i])
            if theme_df.identified_indorg.iloc[i][0] == 'org' and  theme_df.identified_indorg.iloc[i][1] <= 1.5 and theme_df['follower_following_ratio'].iloc[i]<1:
                ind_org.append('Individual')
            elif theme_df.identified_indorg.iloc[i][0] == 'org' and  theme_df.identified_indorg.iloc[i][1] >=2:
                ind_org.append('Organization')
            elif theme_df.identified_indorg.iloc[i][0] == 'org' and  theme_df.identified_indorg.iloc[i][1] >=2:
                ind_org.append('Organization')
            elif theme_df.identified_indorg.iloc[i][0] == 'org' and 'found' in theme_df.description.iloc[i].lower():
                ind_org.append('Organization')
            elif theme_df.description.iloc[i] and 'official' in theme_df.description.iloc[i].lower():
                ind_org.append('Organization')
            elif theme_df.description.iloc[i] and 'head of' in theme_df.description.iloc[i].lower():
                ind_org.append('Individual')
            elif theme_df.identified_indorg.iloc[i][0] == 'ind' or any(en in theme_df.description.iloc[i].lower() for en in ind_list):
                ind_org.append('Individual')
            else:
                ind_org.append(None)


        theme_df['ind_org'] = ind_org
        theme_df = theme_df.where(pd.notnull(theme_df), None)

        #Round 3: Comfirm roles by matching two attributes from round 1 and 2
        theme_df.loc[(theme_df.identified_roles.map(lambda x: x != None and len(intersection(x,['Student','Professor']))>0)) &
                     (theme_df.ind_org=='Individual'),'confirmed_roles']='confirmed'
        theme_df.loc[(theme_df.identified_roles.map(lambda x: x != None and len(intersection(x,['University','Government','Agency','Media','Organization','Business'])) > 0 )) &
                     (theme_df.ind_org=='Organization'), 'confirmed_roles'] = 'confirmed'
        #Round 4: make up
        theme_df.loc[(theme_df.description.str.contains('students|resource|job|opportunit') == True) &
                     (theme_df.description.str.contains(
                         'offer|help|provide|network|update') == True), 'identified_roles'] = 'Agency'
        # theme_df.loc[(theme_df.description.str.contains('researcher|Researcher') == True) & (theme_df.identified_roles.isnull()==True),
        #              'identified_roles'] = 'Researcher'
        # theme_df.loc[(theme_df.description.str.contains('Professor') == True) & (
        #         theme_df.identified_roles.isnull() == True),
        #            'identified_roles'] = 'Researcher'
        theme_df.loc[(theme_df.description.str.contains('Institute') == True) & (
                    theme_df.identified_roles.isnull() == True),
                     'identified_roles'] = 'Organization'
        return theme_df
    def additionally_associated(self, df,origin_data_path=None):
        origin_data = pd.read_csv(origin_data_path,compression='zip',lineterminator='\n')
        associated_tweets = df[
            (df.extraction_coverage == 'Y') & (df.associated_tweets.isnull() == False)].associated_tweets.unique()
        associated_tweets = [a for a in associated_tweets if a not in df['id'].to_list()]
        addition_data = origin_data[origin_data['id'].isin(associated_tweets)]
        return addition_data






# def save_zip(df, filename):
#     compression_options = dict(method='zip', archive_name=f'{filename}.csv')
#     df.to_csv(f'{filename}.zip', compression=compression_options)


















#

# identifier.process_tweet(new_tweet)
