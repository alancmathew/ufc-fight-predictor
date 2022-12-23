#!/usr/bin/env python
# coding: utf-8

# ## Data Collection

# In[1]:


import os
import time
import httpx
import random
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup


# In[2]:


lmap = lambda funcion, iterable: list(map(funcion, iterable))
lfilter = lambda funcion, iterable: list(filter(funcion, iterable))


# In[3]:


data_dir = "../data/"
raw_dir = os.path.join(data_dir, "raw")
raw_csv_dir = os.path.join(raw_dir, "csv")
html_dir = os.path.join(raw_dir, "html")
completed_html_dir = os.path.join(html_dir, "completed")
upcoming_html_dir = os.path.join(html_dir, "upcoming")
completed_eventlist_html_dir = os.path.join(completed_html_dir, "eventlist")
completed_events_html_dir = os.path.join(completed_html_dir, "events")
completed_fights_html_dir = os.path.join(completed_html_dir, "fights")
upcoming_eventlist_html_dir = os.path.join(upcoming_html_dir, "eventlist")
upcoming_events_html_dir = os.path.join(upcoming_html_dir, "events")
upcoming_fights_html_dir = os.path.join(upcoming_html_dir, "fights")


# In[4]:


dirs = [raw_csv_dir, completed_eventlist_html_dir, completed_events_html_dir,
        upcoming_eventlist_html_dir, upcoming_events_html_dir,
       completed_fights_html_dir, upcoming_fights_html_dir]

for folderpath in dirs:
    os.makedirs(folderpath, exist_ok=True)


# In[5]:


def save_html(html_str: str, filename: str, folderpath: str = html_dir) -> None:
    os.makedirs(folderpath, exist_ok=True)
    
    filepath = os.path.join(folderpath, filename)
    with open(filepath, "w") as f:
        f.write(html_str)


# In[6]:


def download_get_html(url: str, filename: str, folderpath: str = html_dir, session=None) -> str:
    if session:
        r = session.get(url)
    else:
        r = httpx.get(url)
        
    if r.status_code == 200:
        html_str = r.text
        save_html(html_str, filename, folderpath)
        return html_str
    else:
        print(f"Unable to get: {url}")


# ### Get Events list

# In[7]:


def download_sequential_eventlist_pages(first_url:str, folderpath: str = html_dir) -> None:
    with httpx.Client() as session:
        html = download_get_html(first_url, "page01.html", folderpath, session)
        soup = BeautifulSoup(html)

        page_links = soup.find_all("a", class_="b-statistics__paginate-link")

        page_nums = [a.text.strip() for a in page_links]

        for idx, num in enumerate(page_nums):
            try:
                page_nums[idx] = int(num)
            except ValueError:
                page_nums.remove(num)

        num_pages = max(page_nums)

        if num_pages != 1:
            for page_num in tqdm(range(2, num_pages+1)):
                url = f"{first_url}?page={page_num}"
                _ = download_get_html(url, f"page{page_num:02}.html", folderpath, session)
                time.sleep(random.randint(1, 3))


# #### Completed

# In[ ]:


completed_first_url = "http://ufcstats.com/statistics/events/completed"
# download_sequential_eventlist_pages(completed_first_url, completed_eventlist_html_dir)


# #### Upcoming

# In[ ]:


upcoming_first_url = "http://ufcstats.com/statistics/events/upcoming"
# download_sequential_eventlist_pages(upcoming_first_url, upcoming_eventlist_html_dir)


# ### Parse Events list

# In[8]:


def get_events_list(eventlist_html_dir: str) -> list[str]:
    
    def extract_event_data(row):
        features = {
            "name": None,
            "date": None,
            "location": None,
            "url": None
        }
        
        a_elem = row.find("a", class_="b-link b-link_style_black")
        features["name"], features["url"] = a_elem.text.strip(), a_elem["href"]
        features["date"] = row.find("span", class_="b-statistics__date").text.strip()
        features["location"] = row.find_all("td")[1].text.strip()
            
        return features
    
    events_list = []
    page_files = sorted(lfilter(lambda s: s.endswith(".html"), os.listdir(eventlist_html_dir)))
    for file in page_files:
        filepath = os.path.join(eventlist_html_dir, file)
        with open(filepath, "r") as f:
            eventlistpage_html = f.read()
            assert eventlistpage_html != ""
            soup = BeautifulSoup(eventlistpage_html)
            row_elems = soup.find("tbody").find_all("tr", class_="b-statistics__table-row")
            row_elems = lfilter(lambda r: len(r.find_all("a", class_="b-link b-link_style_black")) != 0, row_elems)
            events_sublist = lmap(extract_event_data, row_elems)
            events_list.extend(events_sublist)
            
    return events_list


# In[9]:


def write_list_to_file(thelist: list[str], filepath: str) -> None:
    with open(filepath, "w") as f:
        for item in thelist:
            f.write(f"{item}\n")


# In[10]:


def read_list_from_file(filepath: str) -> list[str]:
    with open(filepath, "r") as f:
        return f.read().splitlines()


# In[11]:


def save_events_df(eventlist_html_dir: str, outfilename: str) -> pd.DataFrame:
    events_list = get_events_list(eventlist_html_dir)
    print(f"Event list length: {len(events_list)}")
    events_df = pd.DataFrame(events_list)
    filepath = os.path.join(raw_csv_dir, outfilename)
    events_df.to_csv(filepath, index=False)
    return events_df


# #### Completed

# In[12]:


completed_events_df = save_events_df(completed_eventlist_html_dir, "completed_events.csv")


# #### Upcoming

# In[14]:


upcoming_events_df = save_events_df(upcoming_eventlist_html_dir, "upcoming_event.csv")


# ### Get Event pages

# In[15]:


def save_event_pages(event_urls: list[str], outfolderpath: str) -> None:
    with httpx.Client() as s:
        for url in event_urls:
            event_id = os.path.split(url)[1]
            if not os.path.exists(os.path.join(outfolderpath, f"{event_id}.html")):
                download_get_html(url, f"{event_id}.html", outfolderpath, s)
                time.sleep(random.randint(10000, 20000)/1000)


# #### Completed

# In[16]:


completed_event_urls = completed_events_df["url"].to_list()


# In[ ]:


# save_event_pages(completed_event_urls, completed_events_html_dir)


# #### Upcoming

# In[ ]:


upcoming_event_urls = upcoming_events_df["url"].to_list()


# In[ ]:


# save_event_pages(upcoming_event_urls, upcoming_events_html_dir)


# ### Parse Fights list

# In[21]:


def extract_fight_urls(event_html_filepath: str) -> list[str]:
    with open(event_html_filepath, "r") as f:
        html_str = f.read()
        
    soup = BeautifulSoup(html_str)
    rows = soup.find("tbody").find_all("tr")
    return lmap(lambda r: r["data-link"], rows)


# In[22]:


def save_fight_urls(events_html_dir: str, outfilename: str) -> list[str]:
    event_files = lfilter(lambda x: x.endswith(".html"), os.listdir(events_html_dir))
    
    fight_urls_list = []
    for filename in tqdm(event_files):
        filepath = os.path.join(events_html_dir, filename)
        fight_urls_sublist = extract_fight_urls(filepath)
        fight_urls_list.extend(fight_urls_sublist)
        
    print(f"Fight urls list length: {len(fight_urls_list)}")
    filepath = os.path.join(raw_csv_dir, outfilename)
    write_list_to_file(fight_urls_list, filepath)
    return fight_urls_list


# #### Completed

# In[23]:


completed_fight_urls_list = save_fight_urls(completed_events_html_dir, "completed_fight_urls.txt")


# #### Upcoming

# In[25]:


upcoming_fight_urls_list = save_fight_urls(upcoming_events_html_dir, "upcoming_fight_urls.txt")


# ### Get Fight pages

# In[27]:


filepath = os.path.join(raw_csv_dir, "completed_fight_urls.txt")
completed_fight_urls_list = read_list_from_file(filepath)

filepath = os.path.join(raw_csv_dir, "upcoming_fight_urls.txt")
upcoming_fight_urls_list = read_list_from_file(filepath)


# In[32]:


def save_fight_pages(fight_urls: list[str], outfolderdir: str) -> None:
    fail_score = 0
    reached_not_downloaded = False
    session = httpx.Client()
    for idx, url in enumerate(tqdm(fight_urls)):
        if reached_not_downloaded and (idx != 0) and (idx%25 == 0):
            session.close()
            time.sleep(random.randint(60000, 300000)/1000)
            print("Switching session")
            session = httpx.Client()
        fight_id = os.path.split(url)[1]
        if not os.path.exists(os.path.join(outfolderdir, f"{fight_id}.html")):
            reached_not_downloaded = True
            try:
                download_get_html(url, f"{fight_id}.html", outfolderdir, session)
                if fail_score > 0:
                    fail_score -= 1
            except:
                fail_score += 2
                print(f"Network request unsuccessful. Fail score: {fail_score}")
                session.close()
                
                if fail_score >= 10:
                    break
                session = httpx.Client()
            finally:
                time.sleep(random.randint(5000, 10000)/1000)
        else:
            reached_not_downloaded = False


# In[33]:


save_fight_pages(completed_fight_urls_list, completed_fights_html_dir)


# In[ ]:


save_fight_pages(upcoming_fight_urls_list, upcoming_fights_html_dir)


# In[ ]:




