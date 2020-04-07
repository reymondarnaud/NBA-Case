import http.client
import mimetypes

from bs4 import BeautifulSoup
from lxml import etree
import pandas as pd
import time
import requests

url_list = [
       'https://www.trackalytics.com/facebook/fanpage/warriors/',
        'https://www.trackalytics.com/facebook/fanpage/losangeleslakers/',
        'https://www.trackalytics.com/facebook/fanpage/NYKnicks/',
        'https://www.trackalytics.com/facebook/fanpage/chicagobulls/',
        'https://www.trackalytics.com/facebook/fanpage/bostonceltics/',
        'https://www.trackalytics.com/facebook/fanpage/LAClippers/',
        'https://www.trackalytics.com/facebook/fanpage/BrooklynNets/',
        'https://www.trackalytics.com/facebook/fanpage/houstonrockets/',
        'https://www.trackalytics.com/facebook/fanpage/dallasmavs/',
        'https://www.trackalytics.com/facebook/fanpage/MiamiHeat/',
        'https://www.trackalytics.com/facebook/fanpage/Spurs/',
        'https://www.trackalytics.com/facebook/fanpage/Cavs/',
        'https://www.trackalytics.com/facebook/fanpage/suns/',
        'https://www.trackalytics.com/facebook/fanpage/TorontoRaptors/',
        'https://www.trackalytics.com/facebook/fanpage/trailblazers/',
        'https://www.trackalytics.com/facebook/fanpage/Wizards/',
        'https://www.trackalytics.com/facebook/fanpage/okcthunder/',
        'https://www.trackalytics.com/facebook/fanpage/sacramentokings/',
        'https://www.trackalytics.com/facebook/fanpage/OrlandoMagic/',
        'https://www.trackalytics.com/facebook/fanpage/utahjazz/',
        'https://www.trackalytics.com/facebook/fanpage/DenverNuggets/',
        'https://www.trackalytics.com/facebook/fanpage/detroitpistons/',
        'https://www.trackalytics.com/facebook/fanpage/pacers/',
        'https://www.trackalytics.com/facebook/fanpage/atlhawks/',
        'https://www.trackalytics.com/facebook/fanpage/MemphisGrizzlies/',
        'https://www.trackalytics.com/facebook/fanpage/hornets/',
        'https://www.trackalytics.com/facebook/fanpage/MNTimberwolves/',
        'https://www.trackalytics.com/facebook/fanpage/Sixers/',
        'https://www.trackalytics.com/facebook/fanpage/milwaukeebucks/',
        'https://www.trackalytics.com/facebook/fanpage/PelicansNBA/'

]


# driver = webdriver.PhantomJS()

full_data = []

for url in url_list:

    print(url)

    payload = {}
    headers = {
        'Cookie': 'PHPSESSID=c44236d42ef847aa7d7242eedb010cab'
    }
    response = requests.request("GET", url, headers=headers, data=payload)


    html =  response.text.encode("utf-8")
    time.sleep(30)
    try:
        soup = BeautifulSoup(html, "lxml")
        # soup2 = BeautifulSoup(html, "html.parser")
        chart = soup.find('tbody')
        team = soup.find('h1',{'class':'post-title'}).text
        children = chart.find_all("tr")
        full_data = []
        for elem in children:
            data = [team] + [item.text for item in elem.find_all('td')[1:]]
            full_data.append(data)

        full_data = pd.DataFrame(full_data, columns=['team','date', 'likes', 'talking_about', 'check_ins'])

        full_data.to_csv('data/trackalytics/facebook/trackalytics_fb_history_{}.csv'.format(team.replace(' ','-')), sep='|',index= False)
    except Exception as e:
        print('Error for {} with error {}'.format(url,e))




