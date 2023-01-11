from AssignTheme import AssignThemes,theme_path
from AddEntity import save_zip, AddEntity, merged_data_path,entity_path,intersection
from IndOrgIdentifer import IndOrgIdentifier,description_roles
import pandas as pd
from AssignSentiment import AssignSentiment
user_path = './data/associated_authors.csv'
# theme_path = './helper_data/Theme_Keywords_new.xlsx'
origin_data = './data/normalized_bri.zip'

tweets_cols = ['id','text','created_at', 'retweet_count', 'reply_count', 'like_count',
               'quote_count', 'geo', 'lang', 'conversation_id', 'referenced_tweets',
               'keyword', 'Status', 'associated_tweets', 'hastags', 'mentions', 'url_text',
               'normalized_text', 'lowered_norm_text', 'China_SEA']
users_cols = ['author_id','username', 'description',
       'verified', 'location', 'profile_image_url', 'name', 'url',
       'followers_count', 'following_count', 'tweet_count', 'listed_count']
if __name__ == '__main__':
  # """Getting subet of data, this step can be done in AddEntity.py"""

  print('merged_data_path: ',merged_data_path)
  #AddEntity's parameter - merged_data_path can be overlooked
  add_entity = AddEntity(entity_path,merged_data_path=merged_data_path)
  # add_entity.subset(add_entity.data, 'China_SEA', subsetRule_path='./data/China_SEA_tagged.csv',filename='bri_sea_cn')
  sea_bri_path = './bri_sea_cn.zip'
  indorg = IndOrgIdentifier(sea_bri_path=sea_bri_path, user_path1='./data/authors.csv',
                            user_path2='./data/associated_authors.csv',
                            tweets_cols=tweets_cols,
                            users_cols=users_cols)
  """Getting tweet ids of retweeted and replied_to tweets and save txt file for searching for their authors"""
  # replied_to_tweets = indorg.get_tweet_id_for_scrapy_user(indorg.data, 'replied_to', './data/replied_to_tweets.txt')
  # retweeted_tweets = indorg.get_tweet_id_for_scrapy_user(indorg.data, 'retweeted','./data/retweeted_tweets.txt')

  # # indorg.save_txt(replied_to_tweets,'./data/replied_to_tweets1.txt')
  # # indorg.save_txt(retweeted_tweets, './data/retweeted_tweets1.txt')

  print('Assigning themes based on texts...')
  assign_theme = AssignThemes(theme_path=theme_path)
  theme_df = assign_theme.assign_themes(indorg.df,'lowered_norm_text')
  #indorg.df is the subset of data which does not have na values in username columns
  theme_df = assign_theme.extraction_coverage(theme_df)
  save_zip(theme_df, 'sea_bri_themes')
  # print('Getting tweet ids which do not have authors information...')
  # author_tweets = indorg.get_tweet_id_for_scrapy_user(theme_df, 'author_id', './data/authors.csv'
  #                                                     , author=True)
  # print('Saving tweet ids which do not have authors information...')
  #Then send to search_author_id.py
  # indorg.save_txt(author_tweets,file_path='./data/search_author_id_theme.txt')
  """Construct properties for IndOrg identifier"""
  tweets_users = indorg.process(theme_df)
  # # mention_usernames = indorg.get_mention_username(tweets_users,file_path='./data/mention_usernames.txt')
  tweets_users = indorg.construct_data('./data/associated_authors.csv',tweets_users)
  tweets_users = tweets_users.where(pd.notnull(tweets_users), None)
  # print('Columns of new dataframe: ', tweets_users.columns)
  print('Number of datapoints in new dataframe: ', len(tweets_users))
  tweets_txt = indorg.construct_json(tweets_users)
  identified_indorg = indorg.IndOrdScore(tweets_txt)
  print('Saving identified_indorg attributes...')
  # indorg.save_attribute(identified_indorg, indorg.df, filename=None)
  print('Adding identified_indorg attributes to dataframe...')
  tweets_users['identified_indorg'] = identified_indorg
  print('Getting capitalized entities including university names from texts...')
  uni_names, captialized_entity = add_entity.get_uni_names(tweets_users, col='lowered_norm_text',file_path=None)
  # print('Adding capitalized entities to dataframe...')
  # if 'uni_entities_x' in tweets_users.columns:
  #     tweets_users = tweets_users.drop(['uni_entities_x',
  #          'captialized_entities_x', 'uni_entities_y', 'captialized_entities_y'], axis=1)
  print('Getting organization abbreviations from user descriptions...')
  orgs = add_entity.get_org_name(tweets_users,col='description')
  print("Adding identified university names,captialized entities, user's organization abbreviation attributes to dataframe...")
  tweets_users['uni_names'] = uni_names
  tweets_users['captialized_entity'] = captialized_entity
  tweets_users['user_orgs'] = orgs
  # tweets_users = tweets_users.merge(uni_df,how = 'left', on = 'id')
  print('Assigning roles based on user descriptions, user names, and IndOrg scores...')
  new_df = indorg.roles(tweets_users,description_roles)
  # new_df = add_entity.data
  assign_senti = AssignSentiment()
  sentiments = assign_senti.assign_sentiment(new_df)
  new_df['sentiments_score'] = sentiments
  new_df = new_df.where(pd.notnull(new_df), None)
  print('Theme extraction coverage:', new_df[new_df['extraction_coverage']=='Y'].count()[0]/len(new_df))
  print('Coverage of identified roles: ',len(new_df[new_df['identified_roles'].isnull() == False])/len(new_df))
  print('Coverage of confirmed roles: ', len(new_df[new_df['confirmed_roles']=='confirmed'])/len(new_df))
  # new_df['identified_roles'] = new_df['identified_roles'].fillna(new_df['ind_org'])
  print('Coverage of identified roles after replacing null values with ind_org values: ', len(new_df[new_df['identified_roles'].isnull() == False])/len(new_df))
  save_zip(new_df, 'sea_theme_roles')

  """There are related tweets (retweeted, replied_to, quoted) to the tweets assigned with themes, see we can extract such tweets by the 'associated_tweets' property"""
  # additional_data = indorg.additionally_associated(new_df,origin_data_path='./data/normalized_bri.zip')
  # add_entity1 = AddEntity(entity_path, df = additional_data)
  # indorg = IndOrgIdentifier(df = additional_data,sea_bri_path=None, user_path1='./data/authors.csv',
  #                           user_path2='./data/associated_authors.csv',
  #                           tweets_cols=tweets_cols,
  #                           users_cols=users_cols,
  #                           theme_path=theme_path)