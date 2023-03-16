import ast

import pandas as pd
import zipfile
file_path = '/Users/jie/BRISM_project/BRISM_project/News/data/Guangxi_sentence.zip'
import io
import re
from jieba import analyse
from News.AssignTheme import AssignThemes, theme_path
from Twitter.AddEntity import AddEntity, entity_path, china_province, sea_en,intersection
import numpy as np
from zhon.hanzi import punctuation
import jieba
import paddle
paddle.enable_static()
jieba.enable_paddle()
import jieba.posseg as pseg
jieba.enable_parallel()
import math
import nltk
import time
nltk.download('stopwords')
from nltk.corpus import stopwords
stop_words = stopwords.words('english')
stopwords_path='../helper_data/stopwords_cn_en.txt'
with open(stopwords_path) as f:
    stopwords=f.readlines()
stopwords = [s.replace('\n','') for s in stopwords]
stop_words += stopwords
class ForSTM(object):
    def __init__(self, file_path=None, filename=None,df=None,stop_words=stop_words):
        if file_path:
            self.data = self.read_zip(file_path, filename)
            self.data['id'] = self.data.reset_index().index
        if df is not None:
            self.data = df #Assume df have id column
        self.AssignTheme = AssignThemes(theme_path)
        self.AddEntity = AddEntity(entity_path,merged_data_path=None, sea_en=sea_en, china_province=china_province,
                               fortest=False, index=0)
        self.stopwords =stop_words

    def read_zip(self, file_path=None, file_name=None): #duplicate_col):
        with zipfile.ZipFile(file_path) as zip_archive:
            with zip_archive.open('{}.csv'.format(file_name)) as f:
                data = pd.read_csv(io.StringIO(f.read().decode()), lineterminator='\n')
                data = data.where(pd.notnull(data), None)
                data = data[data.columns.drop(list(data.filter(regex='Unnamed')))]

                # data = data.drop_duplicates(subset=duplicate_col)
        return data

    """Theme Assignment"""
    def assign_theme(self):
        df = self.AssignTheme.assign_themes(self.data, 'sentences',zip_path='./data/Guangxi_themes')
        return df

    """Location Tagging"""
    def assign_entity(self, df):
        china_dict = self.AddEntity.creat_entity_dict('entity_cn', ChinaSEA='China', identify_province=True, save_dict=True,
                                                  dict_path='../helper_data/china_dict_cn.json')
        CnSEAsubset = self.AddEntity.add_country(df, col='sentences', colname='China_SEA',
                                                 china_dict_path='../helper_data/china_dict_cn.json',sea_dict_path=None,world_dict_path=None,
                                                 batch_size= 1000,file_path=None,zip_path='./data/Guangxi_China_SEA')
        return CnSEAsubset

    """Merging values to keys"""
    def merge_texts(self, df):
        self.AssignTheme.subtheme_dict_new()
        for key, value in self.AssignTheme.subtheme_new.items():
            for k, v in value.items():
                # print(k,v)
                for l in v:
                    # print(l,v)
                    df.sentences = df.sentences.map(lambda x: x.replace(l, k))
        return df

    """Processing chinese"""
    @staticmethod
    #reference: https://stackoverflow.com/questions/2644221/how-can-i-convert-this-string-to-list-of-lists
    def regex_change(line):
        line = re.sub("[%s]+" % punctuation, "", line)
        # 前缀的正则
        username_regex = re.compile(r"^\d+::")
        # URL，为了防止对中文的过滤，所以使用[a-zA-Z0-9]而不是\w
        url_regex = re.compile(r"""
            (https?://)?
            ([a-zA-Z0-9]+)
            (\.[a-zA-Z0-9]+)
            (\.[a-zA-Z0-9]+)*
            (/[a-zA-Z0-9]+)*
        """, re.VERBOSE | re.IGNORECASE)
        # 剔除日期
        data_regex = re.compile(u"""        #utf-8编码
            年 |
            月 |
            日 |
            (周一) |
            (周二) | 
            (周三) | 
            (周四) | 
            (周五) | 
            (周六)
        """, re.VERBOSE)
        # 剔除所有数字
        decimal_regex = re.compile(r"[^a-zA-Z]\d+")
        # 剔除空格
        space_regex = re.compile(r"\s+")

        line = username_regex.sub(r"", line)
        line = url_regex.sub(r"", line)
        line = data_regex.sub(r"", line)
        line = decimal_regex.sub(r"", line)
        line = space_regex.sub(r"", line)

        return line
    def process_chinese(self,df,col):
        df['processed_sents'] = df[col].map(lambda x: self.regex_change(x))
        df.drop('sentences',axis = 1,inplace= True)
        return df




    """Text Segmentation"""
    @staticmethod
    def jieba_sent(sentence):
        word_tag = []
        words = pseg.cut(sentence,use_paddle=True)
        for word, tag in words:
            word_tag.append((word, tag))
        return word_tag

    def seg(self,df,col,batch_size=1000,zip_path=None):
        print('Starting segmentation process......')
        #
        #
        for batch_idx in range(math.ceil(len(df) / batch_size)):
            print('Assigning themes ba the batch No.{}'.format(batch_idx))
            df_ = df.iloc[batch_idx*batch_size:batch_size*(batch_idx+1)]
            jieba_sentences = []
            jieba_tags = []
            for id, sent in zip(df_['id'],df_[col]):

                segmented_tuples = self.jieba_sent(sent)
                jieba_tags.append(segmented_tuples)
                segmented = [s[0] for s in segmented_tuples if s[1] in ['nt', 'LOC', 'ORG', 'n', 'vn', 'nz', 'ns', 'c', 'v', 'a', 'vd', 'vg', 'vi',
                                      'ad', 'ag', 'an', 'nz', 'nrt']  and s[0] not in self.stopwords and s[0] != None
                             ]
                jieba_sentences.append(segmented)
            batch = pd.DataFrame(list(zip(df_['id'].to_list(), jieba_tags, jieba_sentences)), columns = ['id', 'jieba_tags', 'jieba_sentences'])
            self.save_batch(batch, zip_path=zip_path)
            print('No.{}'.format(batch_idx) + ' completed')

        print('Text segmentation completed with a total of {}'.format(len(df)))
        # return jieba_sentences, jieba_tag
    def save_batch(self, data_batch,zip_path=None):
        if zip_path:
            # NotImplementedError
            try:
                existing = pd.read_csv('{}.zip'.format(zip_path), compression='zip', lineterminator='\n')
                print('Read saved zip file and saving to zip file...')
                existing = existing[data_batch.columns]
                if data_batch['id'].tolist()[0] not in existing['id'].tolist() and data_batch['id'].tolist()[-1] not in \
                        existing['id'].tolist():
                    all_df = pd.concat([existing, data_batch])
                    self.save_zip(all_df, zip_path)
                    time.sleep(1)

            except:
                self.save_zip(data_batch, zip_path)
                print('No available zip file. Saving the first batch file')
                # time.sleep(20)

    @staticmethod
    def save_zip(df, zip_path):
        compression_options = dict(method='zip', archive_name=f'{zip_path}.csv')
        df.to_csv(f'{zip_path}.zip', compression=compression_options)

    # def jieba_seg(self,df, col):
    #     jieba_sentences, jieba_tags = self.seg(df,col)
    #     print(jieba_tags[0])
    #     df['jieba_tags'] = jieba_tags
    #     return df

    # def _get_seg_uni(self):
    #     extracted_uni_entities = []
    #     for idx, sent in enumerate(self.df.jieba_tags):
    #         if idx % 100 == 0:
    #             print('Get entities from jieba No.{}'.format(idx))
    #         uni_list = []
    #         get_uni(sent, uni_list, self.stopwords)
    #         extracted_uni_entities.append(uni_list)
    #     return extracted_uni_entities

theme_cols = ['Academic Exchange','Institutational Cooperation', 'Language Learning', 'MOU','Research','Employment',
              'Enrollment','Cultural Events','Scholarship', 'Student', 'Study', 'Teacher', 'Teaching', 'Training']
def education(df):
    df['education_coverage'] = ['Y' if x > 1 else 'N' for x in np.sum(df[theme_cols].values != None, 1)]
    print('education coverage:', df[df['education_coverage'] == 'Y'].count()[0] / len(df))
    df_edu =df[df.education_coverage=='Y']
    return df_edu

def get_keyphrase(sentence):
  keyphrases =[]
  for s in sentence:
    sent_keyphrase = []
    # for string in s:
    keyphrase = analyse.textrank(s, topK=20, withWeight=False, allowPOS=('ns', 'n', 'vn', 'v'))
    sent_keyphrase.append(keyphrase)
    sent_keyphrase = [l for sublist in sent_keyphrase for l in sublist]
    if len(sent_keyphrase)!= 0:
      keyphrases.append(sent_keyphrase)
    else:
      keyphrases.append(None)
  keyphrases = [k for k in keyphrases if k]
  return keyphrases




def converge_edu(df,groupby_col,text_col,stopwords, docu_level=False, docu_topic=False): # =doc_id, text_col=sentences/jieba_sentences
    def delete_single(data,col):
        data[col] = data[col].map(lambda x: ' '.join([token for token in x if len(token) != 1 and token not in stopwords and '大学' not in token and '学院' not in token]))

    df =df.where(pd.notnull(df), None)
    if docu_level:
        df_ = df.groupby(groupby_col)[text_col].apply(list).reset_index(name='edu_sentences')
        return df_
    if docu_topic:
        grouped_sent_df = pd.DataFrame(columns=['doc_id', 'edu_sentences','theme'])
        for col in theme_cols:
            df_ = df[df[col].isnull()==False]
            delete_single(df_, text_col)
            df_group = df_.groupby(groupby_col)[text_col].apply(list).reset_index(name='edu_sentences')
            df_group['theme'] = [col]*len(df_group)
            df_group.edu_sentences = df_group.edu_sentences.map(lambda x: ','.join(x))
            grouped_sent_df = pd.concat([grouped_sent_df, df_group])
        return grouped_sent_df





if __name__ == '__main__':
    # # forSTM = ForSTM(file_path='/Users/jie/BRISM_project/BRISM_project/News/data/Guangxi_sentence.zip', filename= 'Guangxi_sentence')
    # # df = forSTM.assign_theme()
    # data_path = './data/Guangxi_China_SEA.zip'
    # data =  pd.read_csv(data_path,compression='zip',lineterminator='\n')
    # data_edu = education(data)
    #

    # # df =df.iloc[974:]
    # # # # df = forSTM.assign_entity(df)
    # data = data[data.columns.drop(list(data.filter(regex='Unnamed')))]
    # df = df.merge(data, how = 'left', on = 'id')
    #
    # forSTM = ForSTM(df=df)
    # forSTM.data = forSTM.data.iloc[974:]
    # """Processing chinese"""
    # df = forSTM.process_chinese(forSTM.data, 'sentences')
    # # # print(forSTM.data.columns)
    # # print('Text segmentation using jieba')
    # """Text segmentation"""
    # if 'processed_sents' in df.columns:
    #     print('Segemtation for processed_sents')
    #     forSTM.seg(df,'processed_sents',batch_size=1000,zip_path='./data/Guangxi_theme_CN_SEA_jiaba') #with theme key_value_merged
    # else:
    #     print('Segemtation for original sentences')
    #     forSTM.seg(df, 'sentences', batch_size=1000,
    #                zip_path='./data/Guangxi_theme_CN_SEA_jiaba')  # with theme key_value_merged
    # # """Merge values with keys in Theme dicts"""
    # # df_ = forSTM.merge_texts(forSTM.data)
    # # forSTM.AssignTheme.save_zip(df_, zip_path='./data/Guangxi_themes_key_value_merge.zip')
    #
    # # data_path = './data/Guangxi_China_SEA.zip'
    # print('Integrating jieba sentences with original data')
    data = pd.read_csv('./data/Guangxi_theme_CN_SEA_jiaba.zip', compression='zip', lineterminator='\n')
    df = pd.read_csv('./data/Guangxi_themes_key_value_merge.zip', compression='zip', lineterminator='\n')
    df_jieba = data.merge(df, how = 'left', on = 'id')
    # print(df_jieba.columns)
    # # # df_jieba['jieba_phrases'] = df_jieba.sentences.map(lambda x: get_keyphrase(x))
    # print('Converging jieba sentences by theme or/and by documents')
    # import ast
    # df_jieba['jieba_sentences'] = df_jieba['jieba_sentences'].map(lambda x: ast.literal_eval(x))
    #
    #
    df_jieba.jieba_sentences = df_jieba.jieba_sentences.map(lambda x: ast.literal_eval(x))
    df_new = converge_edu(df_jieba, 'doc_id','jieba_sentences', stopwords, docu_level=False, docu_topic=True) #jieba_phrases
    #dd


    # df = pd.read_csv('./data/Guangxi_edu_converge_docu_jieba.csv')

    # df_new= pd.read_csv('./data/Guangxi_edu_converge_docu_jieba.csv')
    # edu_sentences = []
    # for sent in df_new.edu_sentences:
    #     new_sents = []
    #     for s in sent.split('=='): #
    #         new_sent = [i for i in s.split() if i not in intersection(s, stopwords)]
    #         new_sents.append(new_sent)
    #         print(new_sent)
    #     edu_sentences.append(new_sents)
    # df_new['edu_sentences_list'] = edu_sentences

    file_path = './data/Guangxi_themes_key_value_merge.zip'  # with China_SEA
    df1 = pd.read_csv(file_path, compression='zip', lineterminator='\n')
    df1=df1[['title', 'keyword','date', 'China_SEA', 'Uni_type', 'doc_id']]
    df1= df1.where(pd.notnull(df1), None)
    import ast
    df1.China_SEA =df1.China_SEA.map(lambda x: ast.literal_eval(x) if x != None else x)
    df2 = df1[df1.China_SEA.isnull()==False]
    df_loc = df2.groupby('doc_id')['China_SEA'].apply(sum).reset_index(name='Geo_locations')
    df1.drop('China_SEA',axis=1, inplace=True)
    df1 = df1.iloc[974:]
    #Merging with geo locations and uni_type, theme and other properties
    df_new=df_new.merge(df_loc, how = 'left', on = 'doc_id')
    df_new_ = df_new.merge(df1, how='left', on='doc_id').drop_duplicates('edu_sentences')
    theme_dict = {}
    for num, i in enumerate(list(df_new.theme.unique())):
        theme_dict[i] = num


    df_new_['theme_id'] = df_new_.theme.map(lambda x: theme_dict[x])
    df_new_['uni_id'] = df_new_.Uni_type.map(lambda x: 1 if x=='Regular' else 0)
    dates =df1[['doc_id','date']]
    df_new_.drop('date', axis =1, inplace=True)
    df_new_= df_new_.merge(dates, how = 'left', on = 'doc_id')
    df_new_ = df_new_.drop_duplicates('edu_sentences')

    df_new_.to_csv('./data/Guangxi_edu_converge_docu_jieba.csv')
    """Get link to articles without date"""
    # li = [str(i) for i in list(df_new[df_new.date.isnull() == True].doc_id.unique())]
    # new= df[df.doc_id.isin(li)][['doc_id', 'keyword', 'link']].drop_duplicates('link')
    # pattern = re.compile(r'://(.*)(?=\.com|\.cn)')
    # lost = df[df.doc_id.isin(li)][['doc_id', 'keyword', 'link']].drop_duplicates('link')['link'].map(
    #     lambda x: ','.join(re.findall(pattern, x)))
    # new['url_root'] = lost
    # new.to_csv('./data/date_lost.csv')














