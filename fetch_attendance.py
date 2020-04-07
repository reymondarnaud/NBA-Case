import http.client
import mimetypes

from bs4 import BeautifulSoup
from lxml import etree
import pandas as pd
import time


#attendance for both home and away game
years = []
base_url = 'http://www.espn.com/nba/attendance/_/year/'

import requests

payload = {}
headers = {
  'Cookie': 'country=ch; connectionspeed=full; edition=espn-en-us; edition-view=espn-en-us; region=emea; _dcf=0; SWID=55C04F45-D829-4F35-CF9A-0941E00C78BB'
}

response = requests.request("GET", base_url, headers=headers, data = payload)
html = response.text.encode('utf8')

soup = BeautifulSoup(html, "lxml")

chart = soup.find("select", {"class": "tablesm"})
dates = [item.text for item in chart.find_all('option')]

full_data = []

for date in dates:
    url = base_url + date
    print(url)

    response = requests.request("GET", url, headers=headers, data=payload)
    html = response.text.encode('utf8')
    soup = BeautifulSoup(html, "lxml")

    chart = soup.find("table", {"class": "tablehead"})

    children = chart.find_all("tr")[2:]
    for elem in children:
        data = [date] + [item.text for item in elem.find_all('td')]
        full_data.append(data)
    print(full_data)

full_data = pd.DataFrame(full_data, columns=['date', 'rank', 'team', 'home_gms', 'home_total', 'home_avg', 'home_pct', 'road_gms', 'road_avg', 'road_pct', 'overall_gms', 'overall_avg', 'overall_pct'])
print(full_data)
full_data.to_csv('data/attendance.csv', sep='|', index=False)



