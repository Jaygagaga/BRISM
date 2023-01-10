sea_bri_theme_roles_path = './Twitter/bri_sea_cn.zip'
import pandas as pd

class DataAnalysis(object):
    def __init__(self, sea_bri_theme_roles_path):
        self.data = pd.read_csv(sea_bri_theme_roles_path, compression='zip', lineterminator='\n')
    def theme_roles(self):

