import os
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup

import asyncio
from playwright.async_api import async_playwright
from playwright.sync_api import sync_playwright

import time
import nest_asyncio
import math
import datetime


def scrape_resou():
    urls, titles, dates = [], [], []    
    url = "https://s.weibo.com/top/summary"

    headers = {
        "Cookie": "SUB=_2AkMWJrkXf8NxqwJRmP8SxWjnaY12zwnEieKgekjMJRMxHRl-yj9jqmtbtRB6PaaX-IGp-AjmO6k5cS-OH2X9CayaTzVD"
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print("Failed to retrieve data from Weibo")
            return None
    
    except Exception as e:
        time.sleep(6000)
        print(e)
        response = requests.get(url, headers=headers)

    result = response.text
    regexp = r'href="(/weibo\?q=[^"]+)"[^>]*>([^<]+)</a>'

    matches = re.findall(regexp, result)
    for match in matches:
        # print(f"URL: {match[0]}, Text: {match[1]}")
        urls.append("https://s.weibo.com" + match[0])
        titles.append(match[1])
        dates.append(datetime.datetime.now().strftime("%Y-%m-%d"))
    
    file_name = output_path + datetime.datetime.now().strftime("%Y-%m-%d") + ".csv"
    if not os.path.exists(file_name):
        # os.makedirs(file_name)
        old_resou = pd.DataFrame(columns=["url", "title", "date"])

    else: old_resou = pd.read_csv(file_name)

    output = pd.concat([old_resou, pd.DataFrame({
            "url": urls,
            "title": titles,
            "date": dates
        })], ignore_index=True)

    output = output.drop_duplicates(subset=['title'], keep="first")

    output.to_csv(file_name, index=False)

    print(f"{len(output) - len(old_resou)} new data saved to {file_name}")


    return



if __name__ == "__main__":
    output_path = "/Users/yanans/Desktop/projects/weibo-trending-hot-search/resou/"
    scrape_resou()