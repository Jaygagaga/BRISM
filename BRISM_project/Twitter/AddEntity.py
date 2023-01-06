import os
import math
print('Current root directory: ',os.getcwd())
import zipfile
import locationtagger
import io
import pandas as pd
import ast
import time
import re
import numpy as np
ast.literal_eval("{'muffin' : 'lolz', 'foo' : 'kitty'}")
import nltk
import ssl
import json
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

tokenizer = nltk.RegexpTokenizer(r"\w+")
nltk.download('averaged_perceptron_tagger')
import spacy
# nlp = spacy.load('en_core_web_sm')
# nltk.download('wordnet')
from nltk.stem import WordNetLemmatizer
lemmatizer = WordNetLemmatizer()
merged_data_path = './data/all_tweets_users.zip'
sea_en = ['Thailand', 'SEA','Singapore', 'Malaysia', 'Philippines','Vietnam', 'Myanmar',
          'Brunei','Indonesia','Laos','ASEAN', 'Cambodia']
entity_path = '../helper_data/entity_lookup.csv'
china_province = ['Heilongjiang', 'Hainan', 'Fujian', 'Henan', 'Shanghai', 'Jiangxi',
                  'Guangdong', 'Shandong', 'Ningxia', 'Shanxi', 'Yunnan', 'Liaoning',
                  'Zhejiang', 'Neimenggu', 'Xinjiang', 'Sichuan', 'Anhui', 'Hebei',
                  'Guizhou', 'Gansu', 'Beijing', 'Guangxi', 'Hubei', 'Jiangsu', 'Jilin',
                  'Tianjin', 'Hunan', 'Xizang', 'Qinghai', 'Chongqing', 'Taiwan', 'Hong Kong',
                  'Macao', 'China']
# dict_path = 'BRISM_project/helper_data/..json'
class AddEntity(object):
    def __init__(self, merged_data_path,entity_path,sea_en=None, china_province=None, fortest=True):
        self.data = pd.read_csv(merged_data_path,compression='zip',lineterminator='\n')
        if fortest:
            self.data = self.data.iloc[:20000]
        self.entity = pd.read_csv(entity_path)
        print('Dataframe columns: ',self.data.columns)
        self.sea_en = sea_en
        self.china_province = china_province

    def creat_entity_dict(self, col, ChinaSEA='China',identify_province=True,  save_dict = True, dict_path =None):  # entity df col= entity_en/entity_cn

        self.entity = self.entity.where(pd.notnull(self.entity), None)
        entity_subset = self.entity[self.entity[col].isnull() == False]
        entity_subset = entity_subset[['entity_cn', 'country', 'entity_en']]
        if col == 'entity_cn' and identify_province:
            # 对于Country==China的entity_cn 有两种country识别方式，一种仅识别国家中国，一种识别省份
            # head(1): entity_cn = '三明',country = Fujian
            entity_subset = self.entity.groupby('entity_cn').head(1)
        if col == 'entity_cn' and not identify_province:
            # tail(1): entity_cn = '三明',country = China
            entity_subset = self.entity.groupby('entity_cn').tail(1)
        if col == 'entity_en':

            """Only for extracting Chinese and SEA locations and entities"""
            entity_subset = self.entity[(self.entity[col].isnull() == False) & (self.entity[col] != 'Nan')]
            #remove country values with lowecased capital word
            entity_subset = entity_subset[entity_subset.country.map(lambda x: True if x[0].isupper() else False)]
            #lowercase values in entity_en/entity_cn columns
            entity_subset[col] = [x.lower() for x in entity_subset[col]]
            if ChinaSEA == 'China':
                entity_subset = entity_subset[~entity_subset.country.isin(self.sea_en+['China'])]
                entity_ = entity_subset.groupby('country')[col].agg('|'.join).reset_index()
                if identify_province == True:
                    #If distingish china provinces
                    entity_dict = dict(zip(entity_.country, entity_[col]))
                else:
                    # if treat all locations in china as one place "China"
                    entity_['country'] = ['China'] * len(entity_)
                    entity_dict = dict(zip(entity_.country, entity_[col]))
                #add key to value
                entity_dict_final = {key: value+'|{}'.format(key) for key, value in entity_dict.items()}
                if save_dict and dict_path:
                    self.save_dict(entity_dict_final,dict_path)
                return entity_dict_final

            if ChinaSEA == 'SEA':
                entity_subset = entity_subset[entity_subset.country.isin(self.sea_en)]

            if ChinaSEA =='World':
                entity_subset = entity_subset[~entity_subset.country.isin(self.sea_en + ['China'] + self.china_province)]
            entity_ = entity_subset.groupby('country')[col].agg('|'.join).reset_index()
            entity_dict = dict(zip(entity_.country, entity_[col]))
            if save_dict and dict_path:
                self.save_dict(entity_dict, dict_path)
            return entity_dict

    def add_country(self,df,col,colname, china_dict=None,sea_dict=None,
                    world_dict=None, batch_size= 1000,file_path=None):  # col = sentences
        # create country column denoting identified places from texts
        #Only allow two cases: 1) identify China & SEA 2) identify worldwide locations except for China & SEA
        # if batch_size != None and file_path != None:
        for batch_idx in range(math.ceil(len(df) / batch_size)):
            data_batch = df[['id',col]].iloc[batch_size * batch_idx:batch_size * (batch_idx + 1)]
            if china_dict and sea_dict:
                # Extract China and SEA locations and entities at the same time and save them to Country column
                data_batch[colname] = ''  #ChinaSEA
                country_dict = china_dict.update(sea_dict)
            if world_dict:
                data_batch[colname] = '' #Worldwide
                country_dict = world_dict
            for k, v in country_dict.items():
                # print(k,v)
                data_batch.loc[(data_batch[col].str.contains(v) == True), [colname]] += ',{}'.format(k)
            data_batch[colname] = data_batch[colname].map(
                lambda x: list(set(x.split(',')[1:])) if len(list(set(x.split(',')[1:]))) != 0 else None)
            data_batch = data_batch[['id',colname]]
            self.save_batch(data_batch,file_path)
            print('Extracted entities for data batch No.{} and saved batch'.format(batch_idx))
            time.sleep(10)


        # return df[['id',colname]]
    def save_batch(self,data_batch,file_path):
        if os.path.isfile(file_path) == False:
            data_batch.to_csv(file_path)
        else:
            existing = pd.read_csv(file_path, dtype=str, lineterminator='\n')
            if data_batch['id'].tolist()[0] not in existing['id'].tolist() and data_batch['id'].tolist()[-1] not in \
                    existing['id'].tolist():
                data_batch.to_csv(file_path, mode='a', header=False)

    def save_dict(self,dic, dict_path):
        with open(dict_path, "w") as fp:
            json.dump(dic, fp)
            print("Saved json file! to {}".format(dict_path))
    @staticmethod
    def _tagger(i):
        """Adapted from https://github.com/edmangog/The-BRI-on-Twitter/blob/master/3.NLP/5.Nanmed%20Entities%20Recognition.py"""
        try:
            result = locationtagger.find_locations(text=i)
            result = result.cities, result.regions, result.countries
        except:
            result = ''
        return result
    def useLocationTagger(self, df,col,batch_size,file_path):
        """See if it can capture additional locations beyond our look-up table."""
        for batch_idx in range(math.ceil(len(df) / batch_size)):
            data_batch = df[['id',col]].iloc[batch_size * batch_idx:batch_size * (batch_idx + 1)]
            data_batch['locationTagger'] = data_batch[col].map(lambda x: self._tagger(x)).map(lambda x: self._tagger(x))
            data_batch = data_batch[['id','locationTagger']]
            self.save_batch(self, data_batch, file_path)









if __name__ == '__main__':
    add_entity = AddEntity(merged_data_path,entity_path,sea_en=sea_en, china_province=china_province)
    china_dict = add_entity.creat_entity_dict('entity_en', ChinaSEA='China',identify_province=True,  save_dict = True,
                                              dict_path ='../helper_data/china_dict.json')
    sea_dict = add_entity.creat_entity_dict('entity_en', ChinaSEA='SEA',identify_province=False,  save_dict = True,
                                              dict_path ='../helper_data/sea_dict.json')
    world_dict = add_entity.creat_entity_dict('entity_en', ChinaSEA='World', identify_province=False, save_dict=True,
                                            dict_path='../helper_data/world_dict.json')
    # CnSEAsubset = add_entity.add_country(add_entity.data,col='text',colname='China_SEA', china_dict=china_dict,sea_dict=sea_dict,world_dict=None)



















