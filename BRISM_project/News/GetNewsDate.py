import pandas as pd
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import os
from time import sleep

options = webdriver.ChromeOptions()
options.add_experimental_option('excludeSwitches', ['enable-automation'])
options.add_argument('--incognito')  # 隐身模式（无痕模式）
options.add_argument("--disable-blink-features")
options.add_argument("--disable-blink-features=AutomationControlled")
driver = webdriver.Chrome(executable_path="/Users/jie/Downloads/chromedriver1", chrome_options=options)
# driver.maximize_window()
# driver = webdriver.Chrome(executable_path="/Users/jie/Downloads/chromedriver", chrome_options=options)



data = pd.read_csv('/Users/jie/BRISM_project/BRISM_project/News/data/date_lost.csv')
data_ = data[data.url_root == 'www.wzzyedu']
link_date = {}
link_date['gxsdxy'] = ['.//span[@class="date-display-single"]','.//div[@id="info"]']
link_date['www.lcvc.edu'] = ['.//div[@class="info"]/span[2]']
link_date['www.gxibvc.net'] = ['.//li[@class="nlb"][3]/span']
link_date['www.gxjrxy'] = ['.//div[@class="ny_fbt"]']
link_date['www.gxyhxx'] =['.//div[@class="ny_fbt"]']
link_date['gjc.guat.edu'] = ['.//div[@class="content-title fl"]/i']
link_date['news.hcnu.edu'] = ['.//div[@class="news-msg txtcenter"]/span[4]']
link_date['politics.people.com'] =[ './/div[@class="col-1-1"]']
link_date['news.hcnu.edu'] = ['.//div[@class="news-msg txtcenter"]/span[4]']
link_date['news.gxau.edu'] = ['.//div[@class="property"]']
link_date['msjy.gxau.edu'] = ['.//div[@class="property"]']
link_date['yyxy.gxau.edu'] =['.//div[@class="property"]']
link_date['zxys.gxau.edu'] =['.//div[@class="property"]']
link_date['ysyjy.gxau.edu'] =['.//span[@class="s_date"]']
link_date['fz.gxau.edu'] =['.//div[@class="property"]']
link_date['ysyjy.gxau.edu'] =['.//div[@class="property"]']
link_date['zghxy.gxau.edu'] =['.//div[@class="property"]']
link_date['news.gxau.edu'] =['.//div[@class="property"]']
link_date['gjjy.gxau.edu'] = ['.//div[@class="property"]']
link_date['zyjs.gxau.edu'] = ['.//div[@class="property"]']
link_date['ggk.gxau.edu'] = ['.//div[@class="property"]']
link_date['gjjy.gxau.edu'] = ['.//div[@class="property"]']
link_date['mgmt.glmc.edu'] =['.//div[@class="show01"]//i[2]']
link_date['www.glmc.edu'] =['.//li[@class="nlb"][3]']
link_date['news.gxnu.edu'] =['.//div[@class="content_left_list_s"]/div/p[2]']
link_date['www.gxtcmu.edu'] = ['.//div[@class="property"]']
link_date['www.gxust.edu'] =['.//div[@class="article-number text-center"]/span[4]']
link_date['www.gxgsxy'] =['.//div[@id="info"]']
link_date['www.wzzyedu'] =['.//div[@align="center"]']
link_date['www.gxxd.net'] = ['.//div[@class="property"]']
link_date['www.gxibvc.net'] = ['.//div[@class="title txtcenter"]/p/span[3]']
link_date['www.glnc.edu']  = ['.//li[@class="nlb"][3]/span']
link_date['www.cice.gxnu.edu'] = ['.//div[@frag="窗口22"]/p/small']
import re

def filter_time(combined_bri, col):
    pattern = re.compile(r'(\d+(?:-|\/|年)\d+(?:-|\/|月)\d+日?)')
    reformated_time = [re.findall(pattern, str(i)) for i in combined_bri[col].to_list()]
    reformated_time = [i[0] if i else None for i in reformated_time]
    reformated_time = [re.sub(r'年|月|/', '-', str(i)) for i in reformated_time]
    reformated_time = [re.sub(r'日', '', str(i)) for i in reformated_time]
    return reformated_time

data.loc[data.url_root =='www.gxjmzy', 'date'] = None
dates = pd.DataFrame(columns=['doc_id',  'date'])
data_ = data[data.url_root == 'www.cice.gxnu.edu']
data = data_
for doc_id, link, root in zip(data.doc_id, data.link, data.url_root):
    if root in link_date:
        # print('%s' % link_date[root])
        # break
        try:
            driver.get(link)
            sleep(6)
            if len(link_date[root]) >1:
                try:
                    date = driver.find_element(By.XPATH,'%s' % link_date[root][0]).text

                except:
                    try:
                        date = driver.find_element(By.XPATH,'%s' % link_date[root][1]).text

                    except:
                        date = None
            else:
                try:
                    date = driver.find_element(By.XPATH, '%s' % link_date[root][0]).text
                except:
                    date = None
            if date != None:
                new = {}
                new['date'] = date
                new['doc_id'] = doc_id
                date_df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in new.items()]))
                dates = pd.concat([dates, date_df])

            else:
                print('No date for doc {}'.format(doc_id))

        except:
            print('Cannot get date for link {}, root {}'.format(link, root))

    else:
        print('root_url not in dict for link {}, root {}'.format(link, root))

dates.to_csv('/Users/jie/BRISM_project/BRISM_project/News/data/additional_dates.csv')
dates['date'] = filter_time(dates, 'date')
new_dates = filter_time(dates, 'date')
dates['date'] = new_dates
df = pd.read_csv('/Users/jie/BRISM_project/BRISM_project/News/data/Guangxi_edu_converge_docu_jieba.csv')
df =df.where(pd.notnull(df), None)
no_dates = df[(df.date.isnull()==True) | (df.date == 'None') ]
no_dates.drop('date', axis =1, inplace=True)
no_dates = no_dates.merge(dates, how = 'left', on = 'doc_id')
rest = df[~df.doc_id.isin(no_dates.doc_id.to_list())]
df_ = pd.concat([rest, no_dates],axis=0)
