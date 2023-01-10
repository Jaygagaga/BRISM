import pandas as pd
from AddEntity import intersection, save_zip
import numpy as np
theme_path = '../helper_data/Theme_Keywords_new.xlsx'
# sea_bri_path = './data/normalized_bri.zip'
class AssignThemes(object):
    def __init__(self,theme_path,sea_bri_path=None,df=None):
        themes = pd.read_excel(theme_path,sheet_name='English')
        themes = themes[themes.Keywords.isnull() == False]
        themes_dict = themes.groupby(['Theme']).apply(lambda x: x['Keywords'].tolist()).to_dict()
        subtheme = pd.read_excel(theme_path, sheet_name='English_Subthemes')
        subtheme = subtheme[subtheme.Keywords.isnull() == False]
        subtheme_dict = subtheme.groupby(['Subtheme']).apply(lambda x: x['Keywords'].tolist()).to_dict()

        # self.themes_dict = themes.groupby('Theme')['Keywords'].agg('|'.join).reset_index()
        self.subtheme = self.clear(subtheme_dict)
        self.theme = self.clear(themes_dict)
        if sea_bri_path:
            self.data = pd.read_csv(sea_bri_path,compression='zip',lineterminator='\n')
        else:
            self.data = df
        # self.data= self.data.iloc[:10000]
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


    def assign_themes(self,df, col): #col = sentences
        #Assign theme first
        self.subtheme_dict_new()
        for key, value in self.theme.items():
            df[key] = ''
            # words = v.split('|')
            # words = [l.lower() for l in value]
            for v in value:
                # print(v)
                df.loc[df[col].map(lambda x: True if v in x.split() else False), key] += ',{}'.format(v)
        #Assign subthemes
        for key, value in self.subtheme_new.items():
            for k, v in value.items():
                # print(k,v)
                df.loc[(df[col].map(lambda x: True if v in x.split() else False)) &
                        (df[key].str.contains(k) == False), key] += ',{}'.format(k)
            df[key] = df[key].map(
                    lambda x: list(set(x.split(',')[1:])) if len(list(set(x.split(',')[1:]))) != 0 else None)
        df = df.where(pd.notnull(df), None)
        return df
    def save_attribute(self,df,colnames,filename):
        subset = df[['id',colnames]]
        save_zip(subset,filename)
    def extraction_coverage(self,df):
        theme_cols = list(self.theme.keys())
        df['extraction_coverage'] = ['Y' if x > 1 else 'N' for x in
                                          np.sum(df[theme_cols].values != None, 1)]
        print('extraction coverage:', df[df['extraction_coverage']=='Y'].count()[0]/len(df))
        return df

#republic, ambassy, government,








# if __name__ == '__main__':
#     assign_theme = AssignThemes(theme_path, sea_bri_path= sea_bri_path)
#     df = assign_theme.assign_themes(assign_theme.data,'lowered_norm_text')


