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

# playwright
def get_ref_detail():

    storage_state_file = "account.json"
    save_state_file = 'account2_fixed.json'
    csv_filename = "modeling_data_true.csv"
    # each page
    page_number = 1
    # driver.get(url)
    # time.sleep(1)
    r_d = []

    modeling = pd.read_csv("modeling_data_true.csv", encoding='utf-8-sig')

    BASE_URL = "https://s.weibo.com/aisearchmore?q={topic}&res_type=ref_blog&page={page}"

    # change ' in save_state_file to ""
    save_state_file = save_state_file.replace("'", '"')

    with sync_playwright() as p:
        # p = sync_playwright().start()
        # browser = p.chromium.launch(headless=False)
        # page = browser.new_page()
        browser = p.chromium.launch(headless=False, executable_path="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",args=["--mute-audio"])
        context = browser.new_context(storage_state=save_state_file)
        page = context.new_page()
        page.goto("https://s.weibo.com/")
        page.wait_for_timeout(10000)
        page.pause()

        # cookies = context.cookies()
        # # Save cookies to a file
        # with open(save_state_file, 'w') as f:
        #     f.write(str(cookies))

        # scrape a hot topic
        for index, row in df.iterrows():
            
            topic = row['text']
            if topic in modeling['topic'].values:
                print(f"Topic {topic} already scraped. Skipping...")
                continue

            date = row['filename'][:-3]
            subject = row['subject']
            
            url = BASE_URL.format(topic=topic, page=1)

            page.goto(url)
            page.wait_for_timeout(10000)

            # close the previous page
            # all_pages = page.context.pages
            # if len(all_pages) > 1:
            #     all_pages[1].close()
                
            print(f"Fetching data for topic: {topic}")
            
            page_number = 1

            try:
                while True: 
                    print(f"Scraping page {page_number}...")
                    # wait for page to load
                    page.wait_for_selector('.card-wrap')
                    page.wait_for_timeout(20000)
                    
                    # Expand all collapsed posts
                    expand_buttons = page.query_selector_all("a[action-type='fl_unfold']")
                    for btn in expand_buttons:
                        try:
                            btn.click()
                            page.wait_for_timeout(300)
                        except:
                            continue

                    # Find all post cards
                    cards = page.query_selector_all("div.card-wrap")
                    
                    if len(cards) == 0:
                        print("No cards found on this page.")
                        r_d.append({
                            "account_name": "",
                            "account_link": "",
                            "post_date": "",
                            "forward": "",
                            "comment": "",
                            "like": "",
                            "text": "",
                            "image": "",
                            "video": "",
                            "vip": "",
                            "topic": topic,
                            "topic-date": date,
                            "subject": subject,
                            "referred": True
                        })
                        return r_d
                    
                    for card in cards:
                        try:
                            name_elem = card.query_selector("a.name")
                            account_name = name_elem.inner_text() if name_elem else ""
                            account_link = name_elem.get_attribute("href") if name_elem else ""

                            date_elem = card.query_selector(".from a[target]")
                            post_date = date_elem.inner_text() if date_elem else ""

                            forward_elem = card.query_selector("a[action-type='feed_list_forward']")
                            forward = forward_elem.inner_text() if forward_elem else "0"

                            comment_elem = card.query_selector("a[action-type*='feed_list_comment']")
                            comment = comment_elem.inner_text() if comment_elem else "0"

                            like_elem = card.query_selector("span.woo-like-count")
                            like = like_elem.inner_text() if like_elem else "0"

                            text_elem = card.query_selector("p[node-type='feed_list_content']")
                            text = text_elem.inner_text() if text_elem else ""

                            has_image = card.query_selector(".media img") is not None
                            has_video = card.query_selector("div.wbpv-poster") is not None
                            is_vip = card.query_selector(".user_vip_icon_container img") is not None
                        
                            r_d.append({
                                "account_name": account_name,
                                "account_link": account_link,
                                "post_date": post_date,
                                "forward": forward,
                                "comment": comment,
                                "like": like,
                                "text": text,
                                "image": has_image,
                                "video": has_video,
                                "vip": is_vip,
                                "topic": topic,
                                "topic-date": date,
                                "subject": subject,
                                "referred": True
                            })

                        except Exception as e:
                            print(f"Error on a card: {e}")
                            continue
                
                    # Try to click "下一页"
                    # Check for and click "下一页"
                    next_btn = page.query_selector("a.next")
                    if next_btn:
                        next_btn.click()
                        page_number += 1
                    else:
                        print(f"No more pages. Writing {topic} to csv file...")
                        
                        new_df = pd.DataFrame(r_d)

                        # write to file
                        if os.path.exists(csv_filename):
                            # new_df = pd.DataFrame(r_d)
                            existing_df = pd.read_csv(csv_filename, encoding='utf-8-sig')
                            # 从第二行开始插入新数据
                            result_df = pd.concat([new_df, existing_df[0:]], ignore_index=True)

                            # 将更新后的数据重新写入 CSV 文件
                            result_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                            r_d = []
                        else:
                            # 如果文件不存在，则创建新文件并写入表头
                            new_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                            r_d = []
                        
                        break
            except Exception as e:
                print(f"Error fetching data for topic {topic}: {e}")
        # browser.close()

def get_non_ref_detail():

    # show all results
    BASE_URL = "https://s.weibo.com/weibo?q={topic}&nodup=1"

    # topics data    
    topics = pd.read_csv("scraped_resou_final.csv", encoding='utf-8')
    
    # scraped data -- to be updated
    # csv_filename = "modeling_data_true.csv"
    # where to save
    csv_filename = "testing_modeling_data.csv"

    false = pd.read_csv("testing_modeling_data.csv", encoding='utf-8-sig')

    # true data to compare
    true = pd.read_csv("modeling_data_true.csv", encoding='utf-8-sig')
    
    save_state_file = 'account2_fixed.json'

    r_d = []

    with sync_playwright() as p:
        # p = sync_playwright().start()
        # browser = p.chromium.launch(headless=False)
        # page = browser.new_page()
        browser = p.chromium.launch(headless=False, executable_path="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",args=["--mute-audio"])
        context = browser.new_context(storage_state=save_state_file)
        page = context.new_page()
        page.goto("https://s.weibo.com/")
        page.wait_for_timeout(10000)
        page.pause()

        # scrape a topic
        for index, row in topics.iterrows():
            topic = row['text']
            # already scraped
            if false[false['topic'] == topic].shape[0] > 0:
                print(f"Topic {topic} already scraped. Skipping...")
                continue
            subject = row['subject']
            date = row['filename']

            # target_num = len(modeling[modeling['topic'] == topic])
            target_num = 200

            print(f"Target number of posts for {topic}: {target_num}")
            # go to page and wait for page num to load
            url = BASE_URL.format(topic=topic)

            page.goto(url)
            page.wait_for_timeout(10000)

            page_number = len(page.query_selector_all("ul.s-scroll > li"))     # start from the earliest page

            print(f"Fetching data for topic: {topic}")

            page.goto(url+f"&page={page_number}")

            scraped_num = 0
            try:
                # scrape data per page
                while scraped_num < target_num and page_number > 0: 

                    print(f"Scraping page {page_number}...")
                    # wait for page to load
                    page.wait_for_selector('.card')
                    page.wait_for_timeout(10000)
                    
                    # Expand all collapsed posts
                    expand_buttons = page.query_selector_all("a[action-type='fl_unfold']")
                    for btn in expand_buttons:
                        try:
                            btn.click()
                            page.wait_for_timeout(300)
                        except:
                            continue
                    
                    # Find all post cards
                    cards = page.query_selector_all("div.card-wrap")
                    
                    # No cards found
                    if len(cards) == 0:
                        print("No cards found on this page.")
                        

                    
                    # scrape data per card
                    else:
                        for card in cards:
                            try:
                                name_elem = card.query_selector("a.name")
                                account_name = name_elem.inner_text() if name_elem else ""
                                account_link = name_elem.get_attribute("href") if name_elem else ""

                                date_elem = card.query_selector(".from a[target]")
                                post_date = date_elem.inner_text() if date_elem else ""

                                forward_elem = card.query_selector("a[action-type='feed_list_forward']")
                                forward = forward_elem.inner_text() if forward_elem else "0"

                                comment_elem = card.query_selector("a[action-type*='feed_list_comment']")
                                comment = comment_elem.inner_text() if comment_elem else "0"

                                like_elem = card.query_selector("span.woo-like-count")
                                like = like_elem.inner_text() if like_elem else "0"

                                text_elem = card.query_selector("p[node-type='feed_list_content']")
                                text = text_elem.inner_text() if text_elem else ""

                                has_image = card.query_selector(".media img") is not None
                                has_video = card.query_selector("div.wbpv-poster") is not None
                                is_vip = card.query_selector(".user_vip_icon_container img") is not None
                            
                                # Check if the post is already scrape
                                if len(true[(true['topic'] == topic) & 
                                            (true['account_name'] ==account_name) &
                                            (true['text'] == text) &
                                            (true['post_date'] == post_date) &
                                            (true['image'] == has_image) &
                                            (true['video'] == has_video)]) > 0:
                                    continue
                                else:
                                    r_d.append({
                                    "account_name": account_name,
                                    "account_link": account_link,
                                    "post_date": post_date,
                                    "forward": forward,
                                    "comment": comment,
                                    "like": like,
                                    "text": text,
                                    "image": has_image,
                                    "video": has_video,
                                    "vip": is_vip,
                                    "topic": topic,
                                    "topic-date": date,
                                    "subject": subject,
                                    "referred": False
                                })
                                    scraped_num += 1

                            except Exception as e:
                                print(f"Error on a card: {e}")
                                continue

                    # Try to click "上一页"
                    # Check for and click "上一页"
                    # next_btn = page.query_selector("a.prev")
                    # if next_btn:
                    #     next_btn.click()
                    if page_number > 0:
                        if page_number > 20:
                            page_number -= 2
                        else: page_number -= 1
                        page.goto(url+f"&page={page_number}")

                    
                    else:
                        print(f"No more pages. Finished {topic}...")    
                        break
                # write to file
                new_df = pd.DataFrame(r_d)

                if os.path.exists(csv_filename):
                        # new_df = pd.DataFrame(r_d)
                    existing_df = pd.read_csv(csv_filename, encoding='utf-8-sig')
                    # 从第二行开始插入新数据
                    result_df = pd.concat([new_df, existing_df[0:]], ignore_index=True)

                    # 将更新后的数据重新写入 CSV 文件
                    result_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                else:
                    # 如果文件不存在，则创建新文件并写入表头
                    new_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                r_d = []

            except Exception as e:
                print(f"Error fetching data for topic {topic}: {e}") 


def get_follower_num():
    # get follower number
    # get the follower number of each account
    # save to a csv file

    # read the csv file

    # create a new column for follower number
    # df['follower_num'] = ""
    df["account_url"] = df["account_link"].str.replace(r"\?refer_flag=.*", "", regex=True)
    df["account_url"] = df["account_url"].fillna("")
    df["follower_num"] = df["follower_num"].fillna("")

    account_dict = {}
    follower_list = []
    with sync_playwright() as p:
        # p = sync_playwright().start()
        # browser = p.chromium.launch(headless=False)
        # page = browser.new_page()
        browser = p.chromium.launch(headless=False, executable_path="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",args=["--mute-audio"])
        context = browser.new_context()
        page = context.new_page()

        for index, row in df.iterrows():
            if row['follower_num'] != "":
                continue

            account_url = row['account_url']
            # print(account_url)
            if account_url != "":
                account_url = "https:" + account_url

                # already exists
                if account_url in account_dict.keys():
                    follower_num = account_dict[account_url]
                    df.at[index, 'follower_num'] = follower_num
                    # follower_list.append(follower_num)
                    continue
                # print(account_url)
                try:
                    page.wait_for_timeout(5000)
                    page.goto(account_url)
                    page.wait_for_selector('.ProfileHeader_h5_1XppQ')
                    
                    header = page.query_selector_all('span.ProfileHeader_h5_1XppQ')
                    # if len(header) == 3:
                    follower_num = header[0].query_selector('span').inner_text()
                    # follower_num = re.sub(r"[^\d]", "", follower_num)
                    df.at[index, 'follower_num'] = follower_num.strip()
                    # print(row['follower_num'])
                    # follower_list.append(follower_num.strip())
                    account_dict[account_url] = follower_num.strip()
                # print(follower_num.strip())
                except Exception as e:
                    print(f"Error fetching follower number for {account_url}: {e}")
                    df.at[index, 'follower_num'] = ""
                    # follower_list.append("")
                    continue
            else:
                print(f"Account URL is empty for index {index}.")
                # follower_list.append("")
    return follower_list


if __name__ == '__main__':

    BASE_URL = "https://s.weibo.com/aisearchmore?q={topic}&res_type=ref_blog&page={page}"
    # testing data
    # df = pd.read_csv("resou.csv", encoding='utf-8')

    # modeling data
    # df = pd.read_csv("modeling_data_true.csv", encoding='utf-8-sig')
    # try:
    # # get_ref_detail()
    # # get_non_ref_detail()

    #     get_follower_num()
    # except Exception as e:
    #     print(f"Error: {e}")
    
    # df.to_csv("modeling_data_true_final.csv", index=False, encoding='utf-8-sig')
    
    df = pd.read_csv('modeling_data_false_final_1.csv', encoding='utf-8-sig')

    try:
        get_follower_num()
    except Exception as e:
        print(f"Error: {e}")
    df.to_csv("modeling_data_false_final_2.csv", index=False, encoding='utf-8-sig')
    # ref_df = pd.DataFrame(ref_data)
    # ref_df.to_csv("ref_data.csv", index=False, encoding='utf-8-sig')
