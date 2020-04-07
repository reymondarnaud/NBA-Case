import http.client
import mimetypes

from bs4 import BeautifulSoup
from lxml import etree
import pandas as pd
import time
import requests

url_list = [
        'https://web.archive.org/web/20190320094410/https://www.statista.com/statistics/193720/average-ticket-price-in-the-nba-by-team-in-2010/',
'https://web.archive.org/web/20150303053704/https://www.statista.com/statistics/193720/average-ticket-price-in-the-nba-by-team-in-2010/',
    'https://web.archive.org/web/20140715151816/http://www.statista.com/statistics/193720/average-ticket-price-in-the-nba-by-team-in-2010/'

]


# driver = webdriver.PhantomJS()

full_data = []

for url in url_list:

    print(url)
    conn = http.client.HTTPSConnection("web.archive.org")
    payload = ''
    headers = {}
    conn.request("GET",
                 url,
                 payload, headers)
    res = conn.getresponse()
    data = res.read()
    html = data.decode("utf-8")
    time.sleep(30)
    'Average ticket price in the NBA  by team 2015/16'
    try:
        soup = BeautifulSoup(html, "lxml")
        date = soup.find('h1').text.replace('Average ticket price in the NBA  by team ','').replace('/','-')
        # soup2 = BeautifulSoup(html, "html.parser")
        try:
            chart = soup.find('table',{'id':'statTable--mobile'}).find('tbody')
        except:
            chart = soup.find('table', {'id': 'statTable'}).find('tbody')
        children = chart.find_all("tr")
        full_data = []
        for elem in children:
            data = [date]+ [item.text for item in elem.find_all('td')]
            full_data.append(data)

        full_data = pd.DataFrame(full_data, columns=['date','team','avg_price'])

        full_data.to_csv('data/statista/price/statista_price_{}.csv'.format(date), sep='|', index=False)
    except Exception as e:
        print('Error for {} with error {}'.format(url, e))




