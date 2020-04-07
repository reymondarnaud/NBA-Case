import http.client
import mimetypes

from bs4 import BeautifulSoup
from lxml import etree
import pandas as pd
import time
import requests

years = [str(2004 + i) for i in range(0,17)]
months = ['october',
          'november',
          'december',
          'january',
          'february',
          'march',
          'april',
          'may',
          'june'
          ]

full_data = []

for year in years:
    for month in months:
        url  =  'https://www.basketball-reference.com/leagues/NBA_{}_games-{}.html'.format(year,month)

        print(url)
        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        html = response.text.encode('utf8')
        time.sleep(30)
        'Average ticket price in the NBA  by team 2015/16'
        try:
            soup = BeautifulSoup(html, "lxml")

            chart = soup.find('table',{'id':'schedule'}).find('tbody')
            children = chart.find_all("tr")
            full_data = []
            for elem in children:
                data = [elem.find('th').text] + [item.text  for item in elem.find_all('td') ]
                full_data.append(data)

            full_data = pd.DataFrame(full_data, columns=['date','time','visitor_team','visitor_pts','home_team','home_pts','box','ot','attendance','notes'])

            full_data.to_csv('data/basketball-reference/schedule/attendance_and_scores_{}-{}.csv'.format(year,month), sep='|', index=False)
        except Exception as e:
            print('Error for {} with error {}'.format(url, e))




