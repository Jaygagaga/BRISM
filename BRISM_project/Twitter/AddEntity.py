import os
import math
print('Current root directory: ',os.getcwd())
import pyarrow
import zipfile
import locationtagger
import io
import pandas as pd
import ast
import time
import re
import polars as pl
import numpy as np
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import tqdm
import multiprocessing as mp

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
merged_data_path = './data/normalized_bri.zip'
entity_path = '../helper_data/entity_lookup.csv'
sea_en = ['Thailand', 'SEA','Singapore', 'Malaysia', 'Philippines','Vietnam', 'Myanmar',
          'Brunei','Indonesia','Laos','ASEAN', 'Cambodia']

china_province = ['Heilongjiang', 'Hainan', 'Fujian', 'Henan', 'Shanghai', 'Jiangxi',
                  'Guangdong', 'Shandong', 'Ningxia', 'Shanxi', 'Yunnan', 'Liaoning',
                  'Zhejiang', 'Neimenggu', 'Xinjiang', 'Sichuan', 'Anhui', 'Hebei',
                  'Guizhou', 'Gansu', 'Beijing', 'Guangxi', 'Hubei', 'Jiangsu', 'Jilin',
                  'Tianjin', 'Hunan', 'Xizang', 'Qinghai', 'Chongqing', 'Taiwan', 'Hong Kong',
                  'Macao', 'China']
# dict_path = 'BRISM_project/helper_data/..json'
def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3

# def save_zip(df,filename):
#     compression_options = dict(method='zip', archive_name=f'{filename}.csv')
#     df.to_csv(f'{filename}.zip', compression=compression_options)

class AddEntity(object):
    def __init__(self, entity_path,df=None,merged_data_path=None,sea_en=None, china_province=None, fortest=False,index= 0):
        """

        :param merged_data_path: path to the data with tweets and author infos
        :param entity_path: entity look-up table
        :param sea_en: list
        :param china_province: list
        :param fortest: selecting a small number of dataset for test
        :param index: continuing running the script from defined index to the dataset

        return:
              A dataframe with columns 1) tweet_id 2) extracted location entities from look-up table
        """
        if merged_data_path:
            self.data = pd.read_csv(merged_data_path,compression='zip',lineterminator='\n')
        if df is not None:
            self.data = df
            self.index = index
            if fortest:
                self.data = self.data.iloc[:20000]
            else:
                self.data = self.data.iloc[self.index:]
            print('Dataframe columns: ', self.data.columns)
            if 'lowered_norm_text' in self.data.columns:
                self.data['lowered_norm_text'] = self.data['lowered_norm_text'].astype('str')
        self.entity = pd.read_csv(entity_path)
        self.sea_en = sea_en
        self.china_province = china_province


    def creat_entity_dict(self, col, ChinaSEA='China',identify_province=True,  save_dict = True, dict_path =None):  # entity df col= entity_en/entity_cn

        self.entity = self.entity.where(pd.notnull(self.entity), None)
        entity_subset = self.entity[self.entity[col].isnull() == False]
        entity_subset = entity_subset[['entity_cn', 'country', 'entity_en']]
        if col == 'entity_cn' and identify_province:
            # 对于Country==China的entity_cn 有两种country识别方式，一种仅识别国家中国，一种识别省份
            # head(1): entity_cn = '三明',country = Fujian
            entity_subset = entity_subset.groupby('entity_cn').head(1)
        if col == 'entity_cn' and not identify_province:
            # tail(1): entity_cn = '三明',country = China
            entity_subset = entity_subset.groupby('entity_cn').tail(1)
        if col == 'entity_en':

            """Only for extracting Chinese and SEA locations and entities"""
            entity_subset = self.entity[(self.entity[col].isnull() == False) & (self.entity[col] != 'Nan')]
            #remove country values with lowecased capital word
            entity_subset = entity_subset[entity_subset.country.map(lambda x: x[0].isupper())]
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

    # @staticmethod


    def add_country(self, df, col, colname, china_dict_path=None, sea_dict_path=None,
                    world_dict_path=None, batch_size= 1000, file_path=None, zip_path=None):  # col = sentences
        # create country column denoting ideget_uni_namesntified places from texts
        #Only allow two cases: 1) identify China & SEA 2) identify worldwide locations except for China & SEA
        # if batch_size != None and file_path != None:
        df[col] = df[col].str.lower()
        country_dict = {}
        if china_dict_path and os.path.isfile(china_dict_path):
            # Extract China and SEA locations and entities at the same time and save them to Country column
            with open(china_dict_path) as json_file1:
                china_dict = json.load(json_file1)
                for k, v in china_dict.items():
                    country_dict[k] = v
        if sea_dict_path and os.path.isfile(sea_dict_path):
            with open(sea_dict_path) as json_file2:
                sea_dict = json.load(json_file2)
                for k, v in sea_dict.items():
                    country_dict[k] = v
        if world_dict_path and os.path.isfile(world_dict_path):
            with open(world_dict_path) as json_file3:
                world_dict = json.load(json_file3)
                for k, v in world_dict.items():
                    country_dict[k] = v
        else:
            print('Run creat_entity_dict first')


        for batch_idx in range(math.ceil(len(df) / batch_size)):
            data_batch = df[['id',col]].iloc[batch_size * batch_idx:batch_size * (batch_idx + 1)]
            data_batch[colname] = ''
            #Assigning locations
            identified_loc = []
            for j in data_batch[col]:
                identified = []
                for k, v in country_dict.items():
                    locs=country_dict[k].split('|')
                    locs = [l.lower() for l in locs]
                    if len(intersection(locs, j.split())) > 0 and any(ext in j for ext in locs):
                        identified.append(k)
                if len(identified) != 0:
                    identified_loc.append(list(set(identified)))
                else:
                    identified_loc.append(None)

                # print(k,v)
                # data_batch.loc[(data_batch[col].str.contains(v) == True), [colname]] += ',{}'.format(k)
                # data_batch[colname] = data_batch[colname].map(
                #     lambda x: list(set(x.split(',')[1:])) if len(list(set(x.split(',')[1:]))) != 0 else None)
            assert len(identified_loc) == len(data_batch), 'Lengths are not matched!'
            data_batch[colname] = identified_loc
            data_batch = data_batch[['id',colname]]
            self.save_batch(data_batch,file_path=file_path,zip_path=zip_path)
            print('Extracted entities for data batch No.{} and saved batch!'.format(batch_idx))
            time.sleep(10)


        # return df[['id',colname]]
    def save_batch(self,data_batch,file_path=None, zip_path=None):
        if zip_path:
            # NotImplementedError
            try:
                existing = pd.read_csv('{}.zip'.format(zip_path),compression='zip',lineterminator='\n')
                print('Read saved zip file and saving to zip file...')
                existing = existing[data_batch.columns]
                if data_batch['id'].tolist()[0] not in existing['id'].tolist() and data_batch['id'].tolist()[-1] not in existing['id'].tolist():
                    all_df = pd.concat([existing,data_batch])
                    self.save_zip(all_df, zip_path)
                    time.sleep(20)

            except:
                self.save_zip(data_batch, zip_path)
                print('No available zip file. Saving the first batch file')
                time.sleep(20)
        else:

            if os.path.isfile(file_path) == False:
                data_batch.to_csv(file_path)
            else:
                existing = pd.read_csv(file_path, dtype=str, lineterminator='\n')
                if data_batch['id'].tolist()[0] not in existing['id'].tolist() and data_batch['id'].tolist()[-1] not in \
                        existing['id'].tolist():
                    data_batch.to_csv(file_path, mode='a', header=False)


    @staticmethod
    def save_dict(dic, dict_path):
        with open(dict_path, "w") as fp:
            json.dump(dic, fp)
            # print("Saved json file! to {}".format(dict_path))
    @staticmethod
    def _tagger(i):
        """Adapted from https://github.com/edmangog/The-BRI-on-Twitter/blob/master/3.NLP/5.Nanmed%20Entities%20Recognition.py"""
        try:
            result = locationtagger.find_locations(text=i)
            result = result.cities, result.regions, result.countries
        except:
            result = ''
        return result
    def useLocationTagger(self, df,col,batch_size=1000,file_path=None,zip_path=None):
        """See if it can capture additional locations beyond our look-up table."""

        def Remove(tuples):
            tuples = [t for t in tuples if t]
            return tuples
        for batch_idx in range(math.ceil(len(df) / batch_size)):
            data_batch = df[['id',col]].iloc[batch_size * batch_idx:batch_size * (batch_idx + 1)]
            data_batch['locationTagger'] = data_batch[col].map(lambda x: self._tagger(x))
            data_batch['locationTagger'] = data_batch['locationTagger'].map(lambda x: Remove(x))
            data_batch['locationTagger'] = data_batch['locationTagger'].map(lambda x: list(set([l for sublist in x for l in sublist])) if len(x) >0 else None)
            data_batch['locationTagger'] = data_batch['locationTagger'].map(lambda x: list(set([i.capitalize() for i in x])) if x else None)
            # data_batch['locationTagger'] = data_batch['locationTagger'].map(lambda x: x if len(x)>0 else None)
            data_batch = data_batch[['id','locationTagger']]
            if file_path:
                self.save_batch(data_batch, file_path,zip_path)
                print('Extracted entities for data batch No.{} and saved batch!'.format(batch_idx))
            time.sleep(10)
    @staticmethod
    def _extract_org(df,col): #col='origin_text
        # def get_entity(col):
        pattern = re.compile(r'([A-Z][a-z]+(?=\s[A-Z])(?:\s[A-Z][a-z]+)+)')

        captialized_entity = [re.findall(pattern, i) for i in df[col]]
        captialized_entity = [[en for en in entity if not any(e in en for e in ['Silk Road','Road Initiative'])]
                              for entity in captialized_entity]
        return captialized_entity

    def get_uni_names(self,df,col, file_path=None):
        captialized_entity = self._extract_org(df,col)
        assert len(captialized_entity) == len(df), "Lengths are not matched!"
        uni_names = []
        keywords = ['University', 'School','College','Research']
        for entity in captialized_entity:
            if any(ele in entity for ele in keywords):
                uni_names.append(entity)
            else:
                uni_names.append(None)
            # scholarship_name = [i for i in entity if 'scholarship' in i]
            # scholarship_names.append(scholarship_name)
        assert len(uni_names) == len(df), "Lengths are not matched!"

        if file_path:
            df['uni_entities'] = uni_names
            df['captialized_entities'] = captialized_entity
            subset = df[['id','uni_entities','captialized_entities']]
            subset.to_csv(file_path)
        return uni_names, captialized_entity

    def get_org_name(self,df,col):
        pattern = re.compile(r'([A-Z]+)\s')
        df[col] = df[col].astype(str)
        orgs = [re.findall(pattern,o) for o in df[col]]
        orgs = [o if len(o) != 0 else None for o in orgs]
        return orgs






    def subset(self, df,col,subsetRule_path = '', zip_path=None, batch_size=1000): # col = labelled attribute/column in subsetRule_path
        if subsetRule_path.split('.')[-1] =='zip':
            subsetAttribute = pd.read_csv(subsetRule_path, compression='zip', lineterminator='\n')
        if subsetRule_path.split('.')[-1] =='csv':
            subsetAttribute = pd.read_csv(subsetRule_path) #[id, locationTaggedProperty]
        subsetAttribute = subsetAttribute.where(pd.notnull(subsetAttribute), None)
        # assert len(df) == len(subsetAttribute), "Numbers of data points do not match!"
        for batch_idx in range(math.ceil(len(subsetAttribute)/batch_size)):
            attribut_batch = subsetAttribute.iloc[batch_idx*batch_size:batch_size*(batch_idx+1)]
            df_batch = df.iloc[batch_idx*batch_size:batch_size*(batch_idx+1)]
            df_batch = df_batch[['sentences', 'doc_id', 'id', 'Unnamed: 0.1', 'file_name', 'date',
                     'agency', 'publication', 'body', 'geo', 'subject', 'industry', 'content']]
            merged_df = df_batch.merge(attribut_batch, how='left', on='id')
            merged_df = merged_df[merged_df[col].isnull() == False]
            merged_df = merged_df.where(pd.notnull(merged_df), None)
            # return only sentences assigned with China SEA locations
            merged_df = merged_df[merged_df.China_SEA.isnull() == False]
            self.save_batch(merged_df,file_path=None, zip_path=zip_path)
            time.sleep(20)

        # subsetAttribute = pl.DataFrame(subsetAttribute)
        # df = df[['sentences', 'doc_id', 'id', 'Unnamed: 0.1', 'file_name', 'date',
        #          'agency', 'publication', 'body', 'geo', 'subject', 'industry','content']]
        # df = pl.DataFrame(df)
        # merged_df = df.join(subsetAttribute, on="id", how="left")
        # # merged_df = df.merge(subsetAttribute, how = 'left', on = 'id')
        # merged_df = merged_df.filter([pl.col(col).is_null()==False,
        #                               pl.col('China_SEA').is_null()==False])
        # # merged_df = merged_df.filter(pl.col('China_SEA') != None)
        # # merged_df = merged_df[merged_df[col].isnull()==False]
        # merged_df = merged_df.select(df.columns.to_list()+['China_SEA'])
        # merged_df = merged_df.to_pandas()
        # if zip_path:
        #     self.save_zip(merged_df,zip_path)
        #     print('Saved dataset with labelled attribute!')
        # merged_df = merged_df.where(pd.notnull(merged_df), None)
        # #return only sentences assigned with China SEA locations
        # merged_df = merged_df[merged_df.China_SEA.isnull() == False]
        # return merged_df

    @staticmethod
    def append_zip(df, filename):
        df.to_csv('{}.csv'.format(filename), mode='a', compression='gzip')
    @staticmethod
    def save_zip(df, zip_path):
        compression_options = dict(method='zip', archive_name=f'{zip_path}.csv')
        df.to_csv(f'{zip_path}.zip', compression=compression_options)

        # new_df = pd.read_csv('test.csv', compression='gzip')







def save_zip(df, zip_path):
    compression_options = dict(method='zip', archive_name=f'{zip_path}.csv')
    df.to_csv(f'{zip_path}.zip', compression=compression_options)


#
#
# if __name__ == '__main__':
#     add_entity = AddEntity(merged_data_path,entity_path,sea_en=sea_en, china_province=china_province,fortest=False,index= 0)
#
#     # china_dict = add_entity.creat_entity_dict('entity_en', ChinaSEA='China',identify_province=True,  save_dict = True,dict_path ='../helper_data/china_dict.json')
#     # sea_dict = add_entity.creat_entity_dict('entity_en', ChinaSEA='SEA',identify_province=False,  save_dict = True, dict_path ='../helper_data/sea_dict.json')
#     # world_dict = add_entity.creat_entity_dict('entity_en', ChinaSEA='World', identify_province=False, save_dict=True,
#     #                                         dict_path='../helper_data/world_dict.json')
#     print('Tagging China and SEA locations....')
#     CnSEAsubset = add_entity.add_country(add_entity.data,col='lowered_norm_text',colname='China_SEA',
#                                          china_dict_path='../helper_data/china_dict.json',sea_dict_path='../helper_data/sea_dict.json',world_dict=None,
#                                          batch_size= 1000,file_path='./data/China_SEA_tagged.csv',zip_path=None)
#
#     print('Tagging locations using locationtagger....')
#     LocTaggerSubset = add_entity.useLocationTagger(add_entity.data,col = 'lowered_norm_text',
#                                                    batch_size=1000,file_path='./data/LocTagged.csv')
#
#     merged_df = add_entity.subset(add_entity.data, 'China_SEA', subsetRule_path='./data/China_SEA_tagged.csv',filename='bri_sea_cn')
#     merged_df = add_entity.subset(merged_df, 'LocTag', subsetRule_path='./data/LocTagged.csv',
#                       filename='bri_sea_cn')
#     print('Extracting capitalized entities....')
#     uni_df = add_entity.get_uni_names(merged_df,'lowered_norm_text','./data/capitalized_entities.csv')

#


















