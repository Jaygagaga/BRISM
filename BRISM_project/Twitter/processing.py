import zipfile
import io
import pandas as pd
mport ast
import numpy as np
ast.literal_eval("{'muffin' : 'lolz', 'foo' : 'kitty'}")
import nltk
tokenizer = nltk.RegexpTokenizer(r"\w+")
nltk.download('averaged_perceptron_tagger')
import spacy
nlp = spacy.load('en_core_web_sm')
nltk.download('wordnet')
from nltk.stem import WordNetLemmatizer
lemmatizer = WordNetLemmatizer()
with zipfile.ZipFile('/Users/jie/phd_project/BRISM_project/Twitter/all_tweets.csv.zip') as zip_archive:
  with zip_archive.open('all_tweets.csv') as f:
    data = pd.read_csv(io.StringIO(f.read().decode()))


