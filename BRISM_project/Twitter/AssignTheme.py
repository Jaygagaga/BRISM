import pandas as pd
from Twitter.AddEntity import intersection
import numpy as np
import math
import time
import os
theme_path = './helper_data/Theme_Keywords_NEW_300123.xlsx'
# from nltk.stem import WordNetLemmatizer
# lemmatizer = WordNetLemmatizer()
from nltk.stem.snowball import SnowballStemmer
snowBallStemmer = SnowballStemmer("english")
from nltk import word_tokenize

# sea_bri_path = './data/normalized_bri.zip'
class AssignThemes(object):
    def __init__(self,theme_path,sea_bri_path=None,addition_sheet = 'English_NEW'):
        theme_dict,self.theme_stemm_dict = self.read_clean(theme_path,sheet_name='English',groupby='Theme',addition_sheet = 'English_NEW')
        subtheme_dict,self.subtheme_stemm_dict = self.read_clean(theme_path, sheet_name='English_Subthemes', groupby='Subtheme')
        # self.themes_dict = themƒes.groupby('Theme')['Keywords'].agg('|'.join).reset_index()
        self.theme = self.clear(theme_dict)
        self.subtheme = self.clear(subtheme_dict)
        if sea_bri_path:
            self.data = pd.read_csv(sea_bri_path,compression='zip',lineterminator='\n')
        # self.data= self.data.iloc[:10000]
    def read_clean(self,theme_path,sheet_name='English', groupby='Theme', addition_sheet = 'English_NEW'):#sheet_name='English' / English_Subthemes
        themes = pd.read_excel(theme_path, sheet_name=sheet_name)
        themes.iloc[:,0] = themes.iloc[:,0].map(lambda x: x.strip())
        themes.iloc[:,1] = themes.iloc[:,1].map(lambda x: x.strip())
        themes = themes[themes.Keywords.isnull() == False]
        if addition_sheet:
            addition_themes = pd.read_excel(theme_path, sheet_name=addition_sheet)
            addition_themes.iloc[:, 0] = addition_themes.iloc[:, 0].map(lambda x: x.strip())
            addition_themes.iloc[:, 1] = addition_themes.iloc[:, 1].map(lambda x: x.strip())
            addition_themes = addition_themes[addition_themes.Keywords.isnull() == False]
            themes = pd.concat([themes, addition_themes])
            themes = themes.drop_duplicates('Keywords')
        themes['Keywords_stemmed'] = [snowBallStemmer.stem(v) if len(v.split()) == 1 else ' '.join([snowBallStemmer.stem(j) for j in v.split()])
             for v in themes['Keywords']]
        # themes['Keywords_stemmed'] = themes['Keywords'].map(lambda x: self.stemming(x))
        themes= themes.drop_duplicates(subset = ['Keywords_stemmed'])
        themes_dict = themes.groupby([groupby]).apply(lambda x: x['Keywords_stemmed'].tolist()).to_dict()
        origin_stemm_dict = dict(zip(themes.Keywords_stemmed,themes.Keywords))
        return themes_dict,origin_stemm_dict
    # @staticmethod
    # def stemming(string):
    #     if len(string.split()) >=2:
    #         stemmed = ' '.join([snowBallStemmer.stem(i) for i in string])
    #     else:
    #         stemmed= snowBallStemmer.stem(string)

    @staticmethod
    def clear(dict):
        new = {}
        for k, v in dict.items():
            new[k] = [i for i in v if str(i) != 'nan']
        return new

    def extend_themes(self):
        self.extended_themes = {}
        for k, value in self.theme.items():
            self.extended_themes[k] = value
            for v in value:
                # if v in extended_themes[k]:
                #     extended_themes[k] += [v]
                if v in self.subtheme:
                    self.extended_themes[k] += self.subtheme[v]

            self.extended_themes[k] += [j for j in value if j not in self.subtheme]
            self.extended_themes[k] = [m for m in self.extended_themes[k] if str(m) != 'nan']
        return self.extended_themes


    """Assign theme to subtheme dictionary"""
    def subtheme_dict_new(self):
        self.subtheme_new = {}
        for key, value in self.theme.items():
            self.subtheme_new[key] = {}
            for k, v in self.subtheme.items():
                if k in value:
                    self.subtheme_new[key][k] = self.subtheme[k]

    def assign_themes(self,df, col,col1,batch_size = 1000,zip_path=None): #col = lowered_norm_text
        #Stemming normalized texts
        # df['lemmatized']= [[lemmatizer.lemmatize(i) for i in l] for l in [w.split() for w in df[col]]]
        self.subtheme_dict_new()
        print(df.columns)
        for batch_idx in range(math.ceil(len(df) / batch_size)):
            print('Assigning themes ba the batch No.{}'.format(batch_idx))
            df_ = df.iloc[batch_idx*batch_size:batch_size*(batch_idx+1)]
            # df_ = df_[['sentences', 'doc_id', 'file_name','date', 'agency', 'publication', 'body', 'geo', 'subject', 'industry',
            #         'file_content','id']]
            stemmed = []
            for sent in df_[col]:
                token_list = word_tokenize(sent)
                token_stemmed = [snowBallStemmer.stem(word) for word in token_list]
                stemmed.append(token_stemmed)
            df_['stemmed'] = stemmed
            # df['stemmed']= [[snowBallStemmer.stemmedem(word)] for word in [word_tokenize(sent) for sent in df[col]]]

            #Assign theme first

            for key, value in self.theme.items():
                df_[key] = ''
                # words = v.split('|')
                # words = [l.lower() for l in value]
                for v in value:
                    # if v == 'forum':
                    #     print(v)
                    if len(v.split()) >=2:
                        print('two words', v)
                        df_.loc[df_['stemmed'].map(lambda x: x != None and v in ' '.join(x)), key] += ',{}'.format(self.theme_stemm_dict[v])
                    else:
                        print('one word', v)
                        df_.loc[df_['stemmed'].map(lambda x: x != None and v in x), key] += ',{}'.format(self.theme_stemm_dict[v])
            #Assign subthemes
            for key, value in self.subtheme_new.items():
                for k, v in value.items():
                    # print(k,v)
                    for l in v:
                        # print(l)
                        # if l =='forum':
                        #     # print('True')

                        if len(l.split()) >=2 :
                            print('two words',l)
                            df_.loc[(df_['stemmed'].map(lambda x: x != None and l in ' '.join(x))) &
                                   (df_[key].str.contains(k) == False), key] += ',{}'.format(self.theme_stemm_dict[k])
                        else:
                            print('one word', l)
                            df_.loc[(df_['stemmed'].map(lambda x: x != None and l in x)) &
                                    (df_[key].str.contains(k) == False), key] += ',{}'.format(self.theme_stemm_dict[k])
                df_[key] = df_[key].map(
                        lambda x: list(set(x.split(',')[1:])) if len(list(set(x.split(',')[1:]))) != 0 else None)
            #make up
            df_.loc[df_[col1].str.contains('universit|Universit|college|College|academy|Academy')==True,'University']='university'
            df_.loc[df_[col1].str.contains(
                'TOEFL|IELTS') == True, 'Admission'] = 'TOEFL|IELTS'
            df_ = df_.where(pd.notnull(df_), None)
            self.save_batch(df_,zip_path=zip_path)
        # return df

    def save_batch(self, data_batch, file_path=None, zip_path=None):
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
    def save_zip(df, zip_path):
        compression_options = dict(method='zip', archive_name=f'{zip_path}.csv')
        df.to_csv(f'{zip_path}.zip', compression=compression_options)
    def extraction_coverage(self,df):
        theme_cols = list(self.theme.keys())
        df['extraction_coverage'] = ['Y' if x > 1 else 'N' for x in
                                          np.sum(df[theme_cols].values != None, 1)]
        print('extraction coverage:', df[df['extraction_coverage']=='Y'].count()[0]/len(df))
        return df


#republic, ambassy, government,







#
# if __name__ == '__main__':
#     sea_bri_path = './bri_sea_cn.zip'
#     assign_theme = AssignThemes(theme_path, sea_bri_path= sea_bri_path)

#     df = assign_theme.assign_themes(assign_theme.data,'lowered_norm_text')


