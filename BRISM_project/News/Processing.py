import os
os.chdir('/Users/jie/BRISM_project/BRISM_project')
import os
print('Current root directory: ',os.getcwd())
from Twitter.AddEntity import AddEntity,sea_en,china_province,intersection
from Twitter.AssignTheme import AssignThemes
import pandas as pd
import re
class SimpleProcess(object):
    def __init__(self, merged_data_path = None, df =None):
        if merged_data_path:
            self.data = pd.read_csv(merged_data_path, compression='zip', lineterminator='\n')

        if df:
            self.data = df

    def doc_id(self, df, col='body'):  # col1 =keyword, col2 = content
        # combined_bri.keyword = [re.sub('\d', '', string) for string in combined_bri[col1]]
        # df.rename(columns={'Unnamed: 0': 'id'}, inplace=True)
        df['lower_body'] = df.body.str.lower()
        lens= len(df)
        df = df[df.lower_body.str.contains('belt and road|silk road|one belt one road')==True]
        print('Number of news about BRI: {}, about {} of the total dataset'.format(len(df),len(df)/lens))
        df['doc_id'] = range(0, len(df))
        df['content'] = [i.strip('\n') for i in df[col]]
        df = df.drop(['lower_body'], axis = 1)
        return df

    def split_sentence(self, df, col1='body'):  # col1 = content, col2 = sentences
        if 'doc_id' in df.columns:
            spilt_sentence_df = pd.DataFrame(columns=['sentences', 'doc_id'])
        else:
            df['doc_id'] = range(0, len(df))
            spilt_sentence_df = pd.DataFrame(columns=['sentences', 'doc_id'])
        for i in range(len(df)):
            sentences = [j.strip() for j in re.split('\.|!|;|\r', df[col1].iloc[i])]  # add more splitter
            # sentences = [j.strip() for j in self.combined_bri.content.iloc[i].split('。')]
            doc_id = [df.doc_id.iloc[i]] * len(sentences)

            new = pd.DataFrame(list(zip(sentences, doc_id)), columns=['sentences', 'doc_id'])
            spilt_sentence_df = pd.concat([spilt_sentence_df, new], axis=0)
        spilt_sentence_df = spilt_sentence_df.merge(df, how='left', on='doc_id')
        # Remove sentence with na value or duplicated
        spilt_sentence_df = spilt_sentence_df[spilt_sentence_df.sentences != '']
        spilt_sentence_df = spilt_sentence_df.drop_duplicates(subset=['sentences'])
        spilt_sentence_df['id'] = range(0, len(spilt_sentence_df))
        print(spilt_sentence_df[['id','doc_id','sentences']].head())
        return spilt_sentence_df

if __name__ == '__main__':
    #
    merged_data_path = './News/data/nexis_data.zip'
    processing = SimpleProcess(merged_data_path = merged_data_path)
    df = processing.doc_id(processing.data)
    split_df = processing.split_sentence(df)
    add_entity = AddEntity(entity_path = './helper_data/entity_lookup.csv', df=split_df)
    # china_dict = add_entity.creat_entity_dict('entity_en', ChinaSEA='China',identify_province=True,  save_dict = True,dict_path ='./helper_data/china_dict.json')
    # sea_dict = add_entity.creat_entity_dict('entity_en', ChinaSEA='SEA',identify_province=False,  save_dict = True, dict_path ='./helper_data/sea_dict.json')
    # print('Tagging China and SEA locations....')
    # add_entity.add_country(add_entity.data,col='sentences',colname='China_SEA',
    #                                          china_dict_path='./helper_data/china_dict.json',sea_dict_path='./helper_data/sea_dict.json',world_dict=None,
    #                                          batch_size= 1000,file_path=None,zip_path='./News/data/China_SEA_tagged_nexis')
    # # # print('Number of datapoints', len(CnSEAsubset))
    # print('Merging identified China_SEA tags with original dataframe')
    # add_entity.subset(add_entity.data, 'China_SEA', subsetRule_path='./News/data/China_SEA_tagged_nexis.zip',
    #                   zip_path='./News/data/bri_sea_cn_nexis')
    # bri_sea_cn_nexis = pd.read_csv('./News/data/bri_sea_cn_nexis.zip', compression='zip',lineterminator='\n')
    #
    # bri_sea_cn_nexis = bri_sea_cn_nexis.where(pd.notnull(bri_sea_cn_nexis),None)
    # bri_sea_cn_nexis = bri_sea_cn_nexis[bri_sea_cn_nexis.China_SEA.isnull()==False]
    # print('Number of sentences assigned with China & SEA locations:',len(bri_sea_cn_nexis))
    # print('Getting capitalized entities including university names from texts...')
    #
    # uni_names, captialized_entity = add_entity.get_uni_names(bri_sea_cn_nexis, col='sentence', file_path=None)
    # print('Getting organization abbreviations from user descriptions...')
    # # orgs = add_entity.get_org_name(bri_sea_cn_nexis, col='sentences')
    #
    # orgs = add_entity.get_org_name(add_entity.data, col='sentences')
    # bri_sea_cn_nexis['uni_names'] = uni_names
    # bri_sea_cn_nexis['captialized_entity'] = captialized_entity
    # bri_sea_cn_nexis['user_orgs'] = orgs
    # add_entity.save_zip(bri_sea_cn_nexis,'./News/data/China_SEA_nexis')
    print('Assigning themes and combine sentences by themes')
    theme_path = './helper_data/Theme_Keywords_NEW_300123.xlsx'
    assign_theme = AssignThemes(theme_path=theme_path, addition_sheet='English_NEW')
    theme_df = assign_theme.assign_themes(split_df, col='sentences',col1 = 'sentences',batch_size = 1000,zip_path='./News/data/bri_themes_nexis')
    theme_df = assign_theme.extraction_coverage(theme_df)
    add_entity.save_zip(theme_df, 'sea_bri_themes_nexis')




