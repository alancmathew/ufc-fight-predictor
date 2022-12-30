#!/usr/bin/env python
# coding: utf-8

# ## Data Collection

# In[1]:


import re
import os
import sys
import time
import httpx
import random
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup


# In[2]:


sys.path.append("../src/")


# In[3]:


from utilities import *


# ### Get Events list

# #### Completed

# In[4]:


# completed_first_url = "http://ufcstats.com/statistics/events/completed?page=1"
# download_sequential_pages(completed_first_url, dir_dict["completed_eventlist_html"])


# #### Upcoming

# In[6]:


# upcoming_first_url = "http://ufcstats.com/statistics/events/upcoming"
# download_sequential_pages(upcoming_first_url, dir_dict["upcoming_eventlist_html"])


# ### Parse Events list

# In[7]:


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
            soup = BeautifulSoup(eventlistpage_html, features="lxml")
            row_elems = soup.find("tbody").find_all("tr", class_="b-statistics__table-row")
            row_elems = lfilter(lambda r: len(r.find_all("a", class_="b-link b-link_style_black")) != 0, row_elems)
            events_sublist = lmap(extract_event_data, row_elems)
            events_list.extend(events_sublist)
            
    return events_list


# In[11]:


def save_events_df(eventlist_html_dir: str, outfilename: str) -> pd.DataFrame:
    events_list = get_events_list(eventlist_html_dir)
    print(f"Event list length: {len(events_list)}")
    events_df = pd.DataFrame(events_list)
    filepath = os.path.join(dir_dict["raw_csv"], outfilename)
    events_df.to_csv(filepath, index=False)
    return events_df


# #### Completed

# In[12]:


# completed_events_df = save_events_df(dir_dict["completed_eventlist_html"], "completed_events.csv")


# #### Upcoming

# In[13]:


# upcoming_events_df = save_events_df(dir_dict["upcoming_eventlist_html"], "upcoming_event.csv")


# ### Get Event pages

# In[14]:


def save_event_pages(event_urls: list[str], outfolderpath: str) -> None:
    with httpx.Client() as s:
        for url in event_urls:
            event_id = os.path.split(url)[1]
            if not os.path.exists(os.path.join(outfolderpath, f"{event_id}.html")):
                download_get_html(url, f"{event_id}.html", outfolderpath, s)
                time.sleep(random.randint(10000, 20000)/1000)


# #### Completed

# In[15]:


# completed_event_urls = completed_events_df["url"].to_list()


# In[17]:


# save_pages(completed_event_urls, dir_dict["completed_events_html"])


# #### Upcoming

# In[18]:


# upcoming_event_urls = upcoming_events_df["url"].to_list()


# In[19]:


# save_pages(upcoming_event_urls, dir_dict["upcoming_events_html"])


# ### Parse Fights list

# In[20]:


def extract_fight_urls(event_html_filepath: str):
    
    with open(event_html_filepath, "r") as f:
        html_str = f.read()
    
    soup = BeautifulSoup(html_str, features="lxml")
    table = soup.find("table")
    
    data_list = []
    
    headers = [key for key in map(lambda x: x.text.strip(), table.find("thead").find_all("th"))]

    rows = table.find("tbody").find_all("tr")

    
    for row in rows:
        for col, elem in zip(headers, row.find_all("td")):
            if col == "Weight class":
                p_elem = elem.find("p")
                val = p_elem.text.strip()
                data_list.append((row["data-link"], val))
    
    return data_list


# In[23]:


def save_fight_urls(events_html_dir: str, outfilename: str) -> list[str]:
    event_files = lfilter(lambda x: x.endswith(".html"), os.listdir(events_html_dir))
    
    fight_urls_list = []
    for filename in tqdm(event_files):
        filepath = os.path.join(events_html_dir, filename)
        fight_urls_sublist = extract_fight_urls(filepath)
        fight_urls_list.extend(fight_urls_sublist)
        
    print(f"Fight urls list length: {len(fight_urls_list)}")
    df = pd.DataFrame(fight_urls_list, columns=["Fight Url", "Weight Class"])
    filepath = os.path.join(dir_dict["raw_csv"], outfilename)
    # write_list_to_file(fight_urls_list, filepath)
    df.to_csv(filepath, index=False)
    return df["Fight Url"].to_list()


# #### Completed

# In[24]:


# completed_fight_urls_list = save_fight_urls(dir_dict["completed_events_html"], "completed_fight_urls.csv")


# #### Upcoming

# In[25]:


# upcoming_fight_urls_list = save_fight_urls(dir_dict["upcoming_events_html"], "upcoming_fight_urls.csv")


# ### Get Fight pages

# In[26]:


# filepath = os.path.join(dir_dict["raw_csv"], "completed_fight_urls.csv")
# completed_fight_urls_list = pd.read_csv(filepath)["Fight Url"]

# filepath = os.path.join(dir_dict["raw_csv"], "upcoming_fight_urls.csv")
# upcoming_fight_urls_list = pd.read_csv(filepath)["Fight Url"]


# In[27]:


# save_pages(completed_fight_urls_list, dir_dict["completed_fights_html"])


# # In[28]:


# save_pages(upcoming_fight_urls_list, dir_dict["upcoming_fights_html"])


# ### Get Fighter pages

# In[30]:


# for i in tqdm(range(97,123)):
#     fighterlist_page_first_url = f"http://ufcstats.com/statistics/fighters?char={chr(i)}"
#     download_sequential_pages(fighterlist_page_first_url, dir_dict["fighterlist_html"])
#     time.sleep(random.randint(2, 5))


# In[31]:


def extract_fighter_urls(fighterlist_html_filepath: str) -> list[str]:
    with open(fighterlist_html_filepath, "r") as f:
        html_str = f.read()
        
    soup = BeautifulSoup(html_str, features="lxml")
    rows = soup.find("tbody").find_all("tr")
    rows = filter(lambda r: r.find("a") != None, rows)
    return lmap(lambda r: r.find("a")["href"], rows)


# In[36]:


def save_fighter_urls(fighterlist_html_dir: str, outfilename: str) -> list[str]:
    fighterlist_files = lfilter(lambda x: x.endswith(".html"), os.listdir(fighterlist_html_dir))
    
    fighter_urls_list = []
    for filename in tqdm(fighterlist_files):
        filepath = os.path.join(fighterlist_html_dir, filename)
        fighter_urls_sublist = extract_fighter_urls(filepath)
        fighter_urls_list.extend(fighter_urls_sublist)
        
    print(f"Fighter urls list length: {len(fighter_urls_list)}")
    filepath = os.path.join(dir_dict["raw_csv"], outfilename)
    write_list_to_file(fighter_urls_list, filepath)
    return fighter_urls_list


# In[37]:


# fighters_url_list = save_fighter_urls(dir_dict["fighterlist_html"], "fighter_urls.txt")

filepath = os.path.join(dir_dict["raw_csv"], "fighter_urls.txt")
fighters_url_list = read_list_from_file(filepath)

# ### *Fights dataframe comes from future steps*

# In[39]:


filepath = os.path.join(dir_dict["mid"], "completed_fights.csv")
fights_df = pd.read_csv(filepath)


# In[42]:


fighters_id_in_fight_df = list(set(fights_df["Fighter1 ID"]).union(fights_df["Fighter2 ID"]))

fighters_urls_in_fight_df = lmap(lambda s: f"http://ufcstats.com/fighter-details/{s}", fighters_id_in_fight_df)

alread_downloaded = lmap(lambda s: f"http://ufcstats.com/fighter-details/{s.replace('.html','')}", 
                         os.listdir(dir_dict["fighters_html"]))
print(alread_downloaded[0])

fighters_url_list = set(fighters_url_list).difference(alread_downloaded)
print(len(fighters_url_list))
fighters_urls_in_fight_df = set(fighters_urls_in_fight_df).difference(alread_downloaded)

remaining_fighters_urls = list(set(fighters_url_list).difference(fighters_urls_in_fight_df))



save_pages(fighters_urls_in_fight_df, dir_dict["fighters_html"])

save_pages(remaining_fighters_urls, dir_dict["fighters_html"])


# In[ ]:




