


from bs4 import BeautifulSoup
from selenium import webdriver
from lxml import etree
import pandas as pd
import time

from bs4 import BeautifulSoup
from lxml import etree
import pandas as pd
import time

url_list = [
        'https://web.archive.org/web/20120315153738/http://www.forbes.com/nba-valuations/list/',
        'https://web.archive.org/web/20130302224508/http://www.forbes.com/nba-valuations/list/',
        'https://web.archive.org/web/20140322171836/http://www.forbes.com/nba-valuations/list/',
        'https://web.archive.org/web/20150626002411/http://www.forbes.com/nba-valuations/list/',
        'https://web.archive.org/web/20160715233800/http://www.forbes.com/nba-valuations/list/',
        'https://web.archive.org/web/20170706115913/https://www.forbes.com/nba-valuations/list/',
        'https://web.archive.org/web/20180729022200/http://www.forbes.com/nba-valuations/list/',
        'https://web.archive.org/web/20190705203655/https://www.forbes.com/nba-valuations/list/',
        'https://web.archive.org/web/20200229031947/http://www.forbes.com/nba-valuations/list/',
]


# driver = webdriver.PhantomJS()

full_data = []
driver = webdriver.Firefox()

for url in url_list:

    print(url)
    driver.get(url)
    driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    time.sleep(2)
    driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    time.sleep(30)
    html = driver.page_source

    soup = BeautifulSoup(html, "lxml")
    # soup2 = BeautifulSoup(html, "html.parser")

    chart = soup.find('tbody', {'id': 'listbody'})

    if chart is None:
        chart = soup.find('tbody', {'id': 'list-table-body'})
        date = soup.find('table', {'id': 'the_list'}).attrs['data-year']
        format_change= True
    else:
        date = soup.find('div', {'id': 'thelist'}).attrs['data-year']
        format_change = False

    for elem in chart.find_all('tr'):
        items = elem.find_all('td')
        if format_change:
            items = items[1:]
        data = [date] + [e.text for e in items]
        full_data.append(data)
    print(full_data)

full_data = pd.DataFrame(full_data, columns=['date','rank', 'team', 'current_value', '1_yr_value_change','debt_value','revenue_M','operating_income_M'])
print(full_data)
full_data.to_csv('data/revenues.csv', sep='|',index = False)
driver.quit()



