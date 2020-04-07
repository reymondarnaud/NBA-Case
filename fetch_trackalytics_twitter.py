import http.client
import mimetypes

from bs4 import BeautifulSoup
from lxml import etree
import pandas as pd
import time
import requests

twitter_list = [
        '@okcthunder',
        '@DetroitPistons',
        '@spurs',
        '@NYKnicks',
        '@Pacers',
        '@cavs',
        '@Suns',
        '@UtahJazz',
        '@OrlandoMagic',
        '@nuggets',
        '@celtics',
        '@Hornets',
        '@WashWizards',
        '@BrooklynNets',
        '@ChicagoBulls',
        '@DallasMavs',
        '@MemGrizz',
        '@Warriors',
        '@PelicansNBA',
        '@Raptors',
        '@MiamiHEAT',
        '@Timberwolves',
        '@Lakers',
        '@HoustonRockets',
        '@LAClippers',
        '@ATLHawks',
        '@trailblazers',
        '@Bucks',
        '@Sixers',
        '@SacramentoKings'
]

# driver = webdriver.PhantomJS()

full_data = []

for acc in twitter_list:
    url=  'https://www.trackalytics.com/twitter/profile/{}/'.format(acc.replace('@',''))
    print(url)

    payload = {}
    headers = {
        'Cookie': 'PHPSESSID=c44236d42ef847aa7d7242eedb010cab'
    }
    response = requests.request("GET", url, headers=headers, data=payload)

    html = response.text.encode("utf-8")
    time.sleep(30)
    try:
        soup = BeautifulSoup(html, "lxml")
        # soup2 = BeautifulSoup(html, "html.parser")
        chart = soup.find('tbody')
        team = soup.find('h1', {'class': 'post-title'}).text.replace(acc,'').replace(acc.lower(),'').replace('(','').replace(')','').lstrip(' ')
        children = chart.find_all("tr")
        full_data = []
        for elem in children:
            data = [acc,team] + [item.text for item in elem.find_all('td')[1:]]
            full_data.append(data)

        full_data = pd.DataFrame(full_data, columns=['account','team', 'date', 'followers', 'following', 'tweets','lists','favourites'])

        full_data.to_csv('data/trackalytics/twitter/trackalytics_twitter_history_{}.csv'.format(team.replace(' ', '-')), sep='|', index=False)
    except Exception as e:
        print('Error for {} with error {}'.format(url, e))




