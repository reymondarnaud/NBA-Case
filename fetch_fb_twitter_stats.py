import http.client
import mimetypes

from bs4 import BeautifulSoup
from lxml import etree
import pandas as pd
import time

url_list = [
    'https://web.archive.org/web/20110223165807/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20110514041831/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20110727135619/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20120603020836/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20120708072815/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20130222121039/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20130526013218/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20130807143527/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20130928004008/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20140109170014/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20140328170757/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20140803184325/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20150220061944/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20150502062732/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20150705175443/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20151023152230/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20160407083410/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20160507174246/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20160709123527/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20170121001335/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20170311142142/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20170608232111/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20171204144530/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20180326031332/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20180627192737/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20180830065356/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/',
    'https://web.archive.org/web/20180908053507/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/followers/'
]


# driver = webdriver.PhantomJS()

full_data = []

for url in url_list:

    print(url)
    date = url.replace('https://web.archive.org/web/', '').replace(
        '/http://fanpagelist.com/category/sports-teams/nba/view/list/sort/fans_today/', '')
    date = date[0:8]

    conn = http.client.HTTPSConnection("web.archive.org")
    payload = ''
    headers = {}
    conn.request("GET", url,payload, headers)
    res = conn.getresponse()
    data = res.read()


    html = data.decode("utf-8")
    time.sleep(30)
    soup = BeautifulSoup(html, "lxml")
    # soup2 = BeautifulSoup(html, "html.parser")
    chart = soup.find("div", {"class": "box-content"})

    data = {}
    children = chart.find_all("li")
    for elem in chart.find_all('li', {"class": "ranking_results"}):
        data[elem.find('div', {'class': 'listing_profile'}).a.text] = {
            'fb_stats': elem.find('div', {'class', 'fb_stats'}).text,
            'twitter_stats': elem.find('div', {'class', 'twitter_stats'}).contents[1]}
        team = elem.find('div', {'class': 'listing_profile'}).a.text
        fb_stats = elem.find('div', {'class', 'fb_stats'}).text
        twitter_stats = elem.find('div', {'class', 'twitter_stats'}).contents[1]
        full_data.append([date, team, fb_stats, twitter_stats])
    print(full_data)

full_data = pd.DataFrame(full_data, columns=['date', 'team', 'fb_stats', 'twitter_stats'])
print(full_data)
full_data.to_csv('data/fb_twitter_stats.csv', sep='|',index= False)



