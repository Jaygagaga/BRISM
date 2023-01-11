import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import multiprocessing as mp
import tqdm
class AssignSentiment(object):
    # def __init__(self, df=None, file_path = None):
    #     if df:
    #         self.df = df
    @staticmethod
    def vader(i):  # adopted from https://github.com/edmangog/The-BRI-on-Twitter/blob/master/3.NLP/3.Sentiment%20Analysis.py
        analyser = SentimentIntensityAnalyzer()
        result = analyser.polarity_scores(i).get('compound')
        return result
    def assign_sentiment(self,df):
        pool = mp.Pool()
        sentiment_list = list(tqdm.tqdm(pool.imap(self.vader, df['origin_text'].fillna('')),
                                        total=len(df['origin_text'].fillna(''))))
        # df['sentiment'] = sentiment_list
        return sentiment_list