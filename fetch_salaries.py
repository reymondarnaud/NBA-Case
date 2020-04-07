import http.client
import mimetypes

from bs4 import BeautifulSoup
from lxml import etree
import pandas as pd
import time
import requests

full_data = []
years = [2000 + i for i in range(0,19)]

url_list = ['https://hoopshype.com/salaries/{}-{}/'.format(year,year+1) for year in years]

for url in url_list:
        print(url)
        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        html = response.text.encode('utf8')
        time.sleep(30)

        try:
            soup = BeautifulSoup(html, "lxml")

            chart = soup.find('div',{'class':'hh-salaries-ranking'}).find('tbody')
            children = chart.find_all("tr")
            full_data = []
            for elem in children:
                href = elem.find('a')['href'].replace('https://hoopshype.com/salaries/','').rstrip('/').split('/')
                team = href[0]
                date = href[1]
                data = [team,date] +[item.text for item in elem.find_all('td')]
                full_data.append(data)

            full_data = pd.DataFrame(full_data, columns=['date','team','rank','state','payroll','payroll_adjusted'])

            full_data.to_csv('data/hoopshype/salaries/salaries_{}.csv'.format(date), sep='|', index=False)
        except Exception as e:
            print('Error for {} with error {}'.format(url, e))




