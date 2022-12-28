import pandas as pd
import zipfile
import io
from demographer.indorg_neural import NeuralOrganizationDemographer
from demographer.indorg import IndividualOrgDemographer
import json
from demographer import process_tweet

tweet_path = '/Users/jie/phd_project/BRISM_project/Twitter/all_tweets.csv.zip'
class ConstrcutData(object):
    def __init__(self, user_path, tweet_path):
        users = pd.read_csv(user_path)
        with zipfile.ZipFile(tweet_path) as zip_archive:
            with zip_archive.open('all_tweets.csv') as f:
                tweets = pd.read_csv(io.StringIO(f.read().decode()))


identifier = IndividualOrgDemographer(setup='balanced')

identifier.process_tweet(new_tweet)
