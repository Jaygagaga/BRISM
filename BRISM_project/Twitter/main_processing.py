import zipfile
import io
import pandas as pd
import ast
import re
import numpy as np
ast.literal_eval("{'muffin' : 'lolz', 'foo' : 'kitty'}")
import nltk
import ssl
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context
nltk.download()
tokenizer = nltk.RegexpTokenizer(r"\w+")
nltk.download('averaged_perceptron_tagger')
import spacy
nlp = spacy.load('en_core_web_sm')
nltk.download('wordnet')
from nltk.stem import WordNetLemmatizer
lemmatizer = WordNetLemmatizer()
puncs = "!$%&'()*+, -./:;<=>?@[\]^_`{|}~"
class Processing(object):
  def __init__(self, merged_data_path= None, tweets_path = None, user_path = None):
    if merged_data_path:
      data = pd.read_csv(merged_data_path,compression='zip')
    if tweets_path and user_path:
      tweets = self.read_zip(tweets_path,'all_tweets.csv','id')
      users = self.read_zip(user_path,'all_users.csv', 'author_id')



  def _tweet_type(self, df):
    df.loc[df.referenced_tweets.str.contains('retweeted') == True, 'Status'] = 'retweeted'
    df.loc[df.referenced_tweets.str.contains('replied_to') == True, 'Status'] = 'replied_to'
    df.loc[df.referenced_tweets.str.contains('quoted') == True, 'Status'] = 'quoted'
    df['associated_tweets'] = [
      'a' + str(list(ast.literal_eval(t).keys())[0]) + 'b' if str(t) != 'nan' and str(
        t) != 'None' and t != None
      else None for t in df.referenced_tweets]
    return df

  def assign_tweet_type(self, df):
    new_df = self._tweet_type(df)
    return new_df
  def read_zip(self, file_path, file_name, duplicate_col):
    with zipfile.ZipFile(file_path) as zip_archive:
      with zip_archive.open('{}.csv'.format(file_name)) as f:
        data = pd.read_csv(io.StringIO(f.read().decode()), lineterminator='\n')
        data = data.where(pd.notnull(twitter), None)
        data = data.drop_duplicates(subset = duplicate_col)
    return data
  def extact_mentions(self, df,col, puncs,symbol):
    mentions_list = []
    for words in df[col]:
      mentions = [w for w in words.split() if w[0] == symbol and w[-1] not in puncs]
      if len(mentions) != 0:
        mentions_list.append(mentions)
      else:
        mentions_list.append(None)
    return mentions_list
  def add_mentions(self,df, mentions_list, col_name):
    assert len(mentions_list) == len(df), "Length of mentions_list is not the same with that of original dataset."
    df[col_name] = mentions_list
    return df




    hashtags = []
    mentions = []
    # splitting the text into words
    for words in df[col]:
      for word in words:
        hashtag_list = []
        mentions_list = []
        # checking the first character of every word
        if word[0] == '#':
          # adding the word to the hashtag_list
          if word[-1] == '.':
            hashtag_list.append(word[1:-1])
          else:
            hashtag_list.append(word[1:])
        if word[0] == '@':
          if word[-1] == '.':
            mentions_list.append(word[1:-1])
          else:
            mentions_list.append(word[1:])
      if len(hashtag_list) ==0:
        hashtags.append(None)
      else:
        hashtags.append(hashtag_list)
      if len(mentions_list) == 0:
        mentions.append(None)
      else:
        mentions.append(mentions_list)
    df['hashtags'] = hashtags
    df['mentions'] = mentions









    def get_url(self, df, col):
      urls = []
      posts = [re.split(' |\n', t) for t in df[col]]
      for post in posts:
        url = []
        for token in post:
          # lowercased_token = token.lower()
          if token.startswith("http") or token.startswith("www"):
            url.append(token)
          else:
            pass
        if len(url) != 0:
          urls.append(url)
        else:
          urls.append(None)
      return urls



if '__name__'=='__main__':
  process = Processing()








