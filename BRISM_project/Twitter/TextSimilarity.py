import pandas as pd

sea_bri_themes = './sea_bri_themes.zip'
class TextSimilarity(object):
    def __init__(self, file_path =None):
        if file_path:
            data = pd.read_csv(file_path,compression='zip',lineterminator='\n')

