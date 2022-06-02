# -*- coding: utf-8 -*-
import codecs
import pandas as pd
# BeautifulSoup and requests needed for connection with webpage and read advertisements as html
from bs4 import BeautifulSoup
import requests
import csv
import re
import sqlalchemy as sqla
# RegEx needed for extracting necessary data from html element
import re
# matplotlib to prepare a chart with points at the end
import matplotlib.pyplot as plt

# choose Polish city without Polish special characters
city = 'lodz'
#Polish city with Polish letters
city_pl = 'Łódź'
# integer for distance in kilometres from city
distance = "0"
# choose "SECONDARY" or "PRIMARY" or "ALL"
market = "SECONDARY"
# you can provide multiple number of rooms - start from "%5B" (which means start),
# then use ONE, TWO etc.,
# use "%2C" (which means AND)
# "%5D" is the end
# everything is one string
rooms = "%5BTWO\
%2CTHREE\
%2CFOUR\
%2CONE\
%2CFIVE%5D"
price_meter_min = "2000"
price_meter_max = "10000"
build_year_min = "1960"
build_year_max = "2022"
extras = "%5BBALCONY%5D"

url = 'https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie/' \
      + city + '?distanceRadius=' + distance + '&page=1&limit=99999&market=' + market \
      + '&roomsNumber=' + rooms \
      + '&pricePerMeterMin=' + price_meter_min + '&pricePerMeterMax=' + price_meter_max \
      + '&buildYearMin=' + build_year_min + '&buildYearMax=' \
      + build_year_max + '&extras=' + extras + '&viewType=listing'

page = requests.get(url)
soup = BeautifulSoup(page.text, 'lxml')
# attributes of table to find it on webpage
tabletext = soup.find_all('a')

i = 0
results_table = pd.DataFrame()
for a in tabletext:
    if str(a.attrs).find('/pl/oferta') > 0:
        try:
            #use RegEx to extraxct necessary data from text - Location, Price, MetrePrice, Rooms, Space and Owner
            ap = list(re.findall(city_pl + ',(.*)(\d{3,4}.\d{3,4})\D*(\d*.?\d*).?zł\D*(\d*)\D*(\d*\.?\d?\d?)\s?m.(.*)[Zobacz\sogłoszenie]?', string=a.text)[0])
            apartment_info = {
                              'Full': a.text,
                              'Location': ap[0].strip(),
                              'Price': float(ap[1].replace('\xa0','')),
                              'MetrePrice': float(ap[2].replace('\xa0','')),
                              'Rooms': int(ap[3]),
                              'Space': float(ap[4]),
                              'Owner': ap[5],
                              'Market': market #taken from market variable defined at the beginning
                              }
            if apartment_info['Price'] * 1.05 < apartment_info['MetrePrice'] * apartment_info['Space'] and apartment_info['Location'][-1].isnumeric():
                apartment_info['Price'] = float(str(apartment_info['Location'][-1])+ str(apartment_info['Price']))
                apartment_info['Location'] = apartment_info['Location'][:-1]

            results_table = results_table.append(apartment_info,ignore_index=True)
        except:
            print(f'Issues with Regex for item: {a.text}. Row index: {i}.')
    i += 1

#Remove duplicates, because one advertisement on webpage can occur twice (if it is promoted)
results_table = results_table.drop_duplicates(subset='Full')

""""#use encoding to store Polish special letters
results_table.to_csv("Test.csv", encoding='utf-8-sig')
results_table.to_excel("Test.xlsx", encoding='utf-8-sig')"""

#Put data from AWS RDS after crearting PostgreSQL database - username, password, endpoint
engine = sqla.create_engine('postgresql://<USERNAME>:<PASSWORD>@<DATABASE_ENDPOINT>.amazonaws.com/postgres')

results_table.to_sql('apartments', engine, schema='public', if_exists='replace', index=False)
