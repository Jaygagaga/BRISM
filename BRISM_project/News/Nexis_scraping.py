import argparse
import re
# parser = argparse.ArgumentParser()
# parser.add_argument("--keywords", default='southeast asia', type=str)
# parser.add_argument("--filename", default='sea file', type=str)
# args = parser.parse_args()
# sea_keywords = args.keywords
# filename = args.filename
"""Need to do the following before running the main codes:
1. log into nus library
2. key in searching keywords "belt AND road"
3. specify searching period
 
"""

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import os

def download(i):
    # Check all results on the page
    check_box = WebDriverWait(driver, 20).until(
        EC.visibility_of_element_located((By.XPATH,
                                          './/div[@id = "results-list-toolbar-gvs"]/ul/li/input[@type = "checkbox"]')))
    check_box.click()
    sleep(2)
    # Download button
    driver.find_element(By.XPATH, './/button[@data-label="Download "]').click()
    # sleep(3)
    file = WebDriverWait(driver, 20).until(
        EC.visibility_of_element_located((By.XPATH, './/label[@for="Rtf"]')))
    # Select file format
    # driver.find_element(By.XPATH, './/label[@for="Rtf"]').click()
    file.click()
    # Select saving individual files
    driver.find_element(By.XPATH, './/label[@for="SeparateFiles"]').click()
    # Name file
    name = driver.find_element(By.XPATH, './/input[@id="FileName"]')
    name.clear()
    name.send_keys('{}'.format('_'.join(sea_keywords.split() + [str(i)])))
    sleep(2)
    # Downloading
    driver.find_element(By.XPATH, './/button[@data-action="download"]').click()
    sleep(30)



if __name__ == '__main__':
    import pandas as pd
    from time import sleep

    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_argument('--incognito')  # 隐身模式（无痕模式）
    options.add_argument("--disable-blink-features")
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(executable_path="/Users/jie/Downloads/chromedriver1", chrome_options=options)
    # driver.maximize_window()
    # driver = webdriver.Chrome(executable_path="/Users/jie/Downloads/chromedriver", chrome_options=options)
    sleep(3)

    driver.get('https://advance-lexis-com.libproxy1.nus.edu.sg/bisacademicresearchhome/?pdmfid=1516831&identityprofileid=RHB5HS58401&crid=f6742964-86fe-4b79-8135-db1443cfd85a')
    sleep(100)
    WebDriverWait(driver, 20).until(
        EC.visibility_of_element_located((By.XPATH, './/nav[@class="pagination "]')))
    # driver.find_element_by_xpath('//input[@name="username"]').click()

    MAX_WAIT = 20
    wait = WebDriverWait(driver, MAX_WAIT)

    sea_keywords = 'southeast asia'
    # Narrowing down searching scope by specifing location
    narrow_keywords = driver.find_element(By.XPATH, './/textarea[@class="search expandingTextarea"]')
    narrow_keywords.clear()
    narrow_keywords.send_keys(sea_keywords)
    driver.find_element(By.XPATH, './/button[@aria-label="Search within results"]').click()
    # Get number of total pages
    sleep(2)
    pagination = WebDriverWait(driver, 20).until(
        EC.visibility_of_element_located((By.XPATH, './/nav[@class="pagination "]')))
    pagenum = pagination.text.split('\n')[-1]
    unfound = []
    for i in range(254,2089):
        if i == 1:
            download(i)
            sleep(30)
        if i>1:
            print(i)
            # New page
            nav = driver.find_element(By.XPATH, './/nav[@class="pagination "]')
            # print(nav)
            nav.find_element(By.XPATH, ".//ol/li/a[@data-value='%s']" % str(i)).click()
            sleep(5)
            WebDriverWait(driver, 20).until(
                EC.visibility_of_element_located(
                    (By.XPATH, './/span[@class="showResultListPanelNexisUni btnPreview "]')))
            try:
                download(i)
                sleep(30)
                print('Downloaded news articles on page {}'.format(i))
            except:
                print('Downloading is failed on page {}'.format(i))
                unfound.append(i)
                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[1])
                driver.get(
                    'https://advance-lexis-com.libproxy1.nus.edu.sg/bisacademicresearchhome/?pdmfid=1516831&identityprofileid=RHB5HS58401&crid=f6742964-86fe-4b79-8135-db1443cfd85a')

            # if 'exceeded the number of delivery request' in driver.page_source:
            #     sleep(700)
            #     driver.get(
            #         'https://advance-lexis-com.libproxy1.nus.edu.sg/bisacademicresearchhome/?pdmfid=1516831&identityprofileid=RHB5HS58401&crid=f6742964-86fe-4b79-8135-db1443cfd85a')
            #     print('Reloading the index page. Need to key in searching criteria')
            #     i = i-1





#https://advance-lexis-com.libproxy1.nus.edu.sg/search/?pdmfid=1516831&crid=cb230527-3e77-42d7-a311-6e183c7dc163&pdsearchtype=SearchBox&pdtypeofsearch=searchboxclick&pdstartin=&pdsearchterms=belt+AND+road&pdtimeline=1%2F1%2F2013+to+12%2F1%2F2022%7Cbetween%7CMM%2FDD%2FYYYY&pdpsf=&pdquerytemplateid=&pdsf=&ecomp=3bJgkgk&prid=f6742964-86fe-4b79-8135-db1443cfd85a
#https://advance-lexis-com.libproxy1.nus.edu.sg/search/?pdmfid=1516831&crid=32ea98bc-5bb9-4724-bd32-8982dda2cb0a&pdsearchtype=SearchBox&pdtypeofsearch=searchboxclick&pdstartin=&pdsearchterms=belt+AND+road&pdtimeline=1%2F1%2F2013+to+12%2F1%2F2022%7Cbetween%7CMM%2FDD%2FYYYY&pdpsf=&pdquerytemplateid=&pdsf=&ecomp=3bJgkgk&prid=f6742964-86fe-4b79-8135-db1443cfd85a