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
mid_dir = os.path.join(data_dir, "mid")
raw_csv_dir = os.path.join(raw_dir, "csv")
html_dir = os.path.join(raw_dir, "html")
completed_html_dir = os.path.join(html_dir, "completed")
upcoming_html_dir = os.path.join(html_dir, "upcoming")
fighterlist_html_dir = os.path.join(html_dir, "fighterlist")
fighters_html_dir = os.path.join(html_dir, "fighters")
completed_eventlist_html_dir = os.path.join(completed_html_dir, "eventlist")
completed_events_html_dir = os.path.join(completed_html_dir, "events")
completed_fights_html_dir = os.path.join(completed_html_dir, "fights")
upcoming_eventlist_html_dir = os.path.join(upcoming_html_dir, "eventlist")
upcoming_events_html_dir = os.path.join(upcoming_html_dir, "events")
upcoming_fights_html_dir = os.path.join(upcoming_html_dir, "fights")


# In[ ]:


dirs = [raw_csv_dir, fighters_html_dir, fighterlist_html_dir,
        completed_eventlist_html_dir, 
        completed_events_html_dir,
        upcoming_eventlist_html_dir, upcoming_events_html_dir,
       completed_fights_html_dir, upcoming_fights_html_dir]

for folderpath in dirs:
    os.makedirs(folderpath, exist_ok=True)


# In[4]:


def save_html(html_str: str, filename: str, folderpath: str = html_dir) -> None:
    os.makedirs(folderpath, exist_ok=True)
    
    filepath = os.path.join(folderpath, filename)
    with open(filepath, "w") as f:
        f.write(html_str)


# In[5]:


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


# In[22]:


def download_sequential_pages(first_url:str, folderpath: str = html_dir) -> None:
    with httpx.Client() as session:
        filename = first_url.split("?")[1].replace("&","-").replace("=","")
        html = download_get_html(first_url, f"{filename}-page01.html", folderpath, session)
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
            inter = "&" if "?" in first_url else "?"
            for page_num in tqdm(range(2, num_pages+1)):
                url = f"{first_url}{inter}page={page_num:02}"
                filename = url.split("?")[1].replace("&","-").replace("=","")
                if not os.path.exists(os.path.join(folderpath, f"{filename}.html")):
                    _ = download_get_html(url, f"{filename}.html", folderpath, session)
                    time.sleep(random.randint(5000, 10000)/1000)


# In[7]:


def save_pages(urls: list[str], outfolderdir: str) -> None:
    fail_score = 0
    reached_not_downloaded = False
    session = httpx.Client()
    for idx, url in enumerate(tqdm(urls)):
        if reached_not_downloaded and (idx != 0) and (idx%25 == 0):
            session.close()
            time.sleep(random.randint(60000, 300000)/1000)
            print("Switching session")
            session = httpx.Client()
        unique_id = os.path.split(url)[1]
        if not os.path.exists(os.path.join(outfolderdir, f"{unique_id}.html")):
            reached_not_downloaded = True
            try:
                download_get_html(url, f"{unique_id}.html", outfolderdir, session)
                if fail_score > 0:
                    fail_score -= 1
            except:
                fail_score += 2
                print(f"Network request unsuccessful. Fail score: {fail_score}")
                session.close()
                
                if fail_score >= 20:
                    break
                session = httpx.Client()
            finally:
                time.sleep(random.randint(5000, 10000)/1000)
        else:
            reached_not_downloaded = False


# ### Get Events list

# #### Completed

# In[ ]:


completed_first_url = "http://ufcstats.com/statistics/events/completed"
# download_sequential_pages(completed_first_url, completed_eventlist_html_dir)


# #### Upcoming

# In[ ]:


upcoming_first_url = "http://ufcstats.com/statistics/events/upcoming"
# download_sequential_pages(upcoming_first_url, upcoming_eventlist_html_dir)


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

# In[ ]:


# completed_events_df = save_events_df(completed_eventlist_html_dir, "completed_events.csv")


# #### Upcoming

# In[ ]:


# upcoming_events_df = save_events_df(upcoming_eventlist_html_dir, "upcoming_event.csv")


# ### Get Event pages

# In[12]:


def save_event_pages(event_urls: list[str], outfolderpath: str) -> None:
    with httpx.Client() as s:
        for url in event_urls:
            event_id = os.path.split(url)[1]
            if not os.path.exists(os.path.join(outfolderpath, f"{event_id}.html")):
                download_get_html(url, f"{event_id}.html", outfolderpath, s)
                time.sleep(random.randint(10000, 20000)/1000)


# #### Completed

# In[ ]:


# completed_event_urls = completed_events_df["url"].to_list()


# In[ ]:


# save_pages(completed_event_urls, completed_events_html_dir)


# #### Upcoming

# In[ ]:


# upcoming_event_urls = upcoming_events_df["url"].to_list()


# In[ ]:


# save_pages(upcoming_event_urls, upcoming_events_html_dir)


# ### Parse Fights list

# In[13]:


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



# In[14]:


def save_fight_urls(events_html_dir: str, outfilename: str) -> list[str]:
    event_files = lfilter(lambda x: x.endswith(".html"), os.listdir(events_html_dir))
    
    fight_urls_list = []
    for filename in tqdm(event_files):
        filepath = os.path.join(events_html_dir, filename)
        fight_urls_sublist = extract_fight_urls(filepath)
        fight_urls_list.extend(fight_urls_sublist)
        
    print(f"Fight urls list length: {len(fight_urls_list)}")
    df = pd.DataFrame(fight_urls_list, columns=["Fight Url", "Weight Class"])
    filepath = os.path.join(raw_csv_dir, outfilename)
    # write_list_to_file(fight_urls_list, filepath)
    df.to_csv(filepath, index=False)
    return df["Fight Url"].to_list()


# #### Completed

# In[ ]:


# completed_fight_urls_list = save_fight_urls(completed_events_html_dir, "completed_fight_urls.csv")


# #### Upcoming

# In[ ]:


# upcoming_fight_urls_list = save_fight_urls(upcoming_events_html_dir, "upcoming_fight_urls.csv")


# ### Get Fight pages

# In[ ]:


# filepath = os.path.join(raw_csv_dir, "completed_fight_urls.csv")
# completed_fight_urls_list = pd.read_csv(filepath)["Fight Url"]

# filepath = os.path.join(raw_csv_dir, "upcoming_fight_urls.csv")
# upcoming_fight_urls_list = pd.read_csv(filepath)["Fight Url"]


# In[ ]:


# save_pages(completed_fight_urls_list, completed_fights_html_dir)


# # In[ ]:


# save_pages(upcoming_fight_urls_list, upcoming_fights_html_dir)


# ### Get Fighter pages

# In[23]:


# for i in tqdm(range(97,123)):
#     fighterlist_page_first_url = f"http://ufcstats.com/statistics/fighters?char={chr(i)}"
#     download_sequential_pages(fighterlist_page_first_url, fighterlist_html_dir)


# In[24]:


def extract_fighter_urls(fighterlist_html_filepath: str) -> list[str]:
    with open(fighterlist_html_filepath, "r") as f:
        html_str = f.read()
        
    soup = BeautifulSoup(html_str)
    rows = soup.find("tbody").find_all("tr")
    rows = filter(lambda r: r.find("a") != None, rows)
    return lmap(lambda r: r.find("a")["href"], rows)


# In[25]:


def save_fighter_urls(fighterlist_html_filepath: str, outfilename: str) -> list[str]:
    fighterlist_files = lfilter(lambda x: x.endswith(".html"), os.listdir(fighterlist_html_dir))
    
    fighter_urls_list = []
    for filename in tqdm(fighterlist_files):
        filepath = os.path.join(fighterlist_html_dir, filename)
        fighter_urls_sublist = extract_fighter_urls(filepath)
        fighter_urls_list.extend(fighter_urls_sublist)
        
    print(f"Fighter urls list length: {len(fighter_urls_list)}")
    filepath = os.path.join(raw_csv_dir, outfilename)
    write_list_to_file(fighter_urls_list, filepath)
    return fighter_urls_list


# In[26]:


fighters_url_list = save_fighter_urls(fighterlist_html_dir, "fighter_urls.txt")


filepath = os.path.join(mid_dir, "completed_fights.csv")
fights_df = pd.read_csv(filepath)

# In[27]:

fighters_id_in_fight_df = list(set(fights_df["Fighter1 ID"]).union(fights_df["Fighter2 ID"]))

fighters_urls_in_fight_df = lmap(lambda s: f"http://ufcstats.com/fighter-details/{s}", fighters_id_in_fight_df)

alread_downloaded = lmap(lambda s: f"http://ufcstats.com/fighter-details/{s.replace('.html','')}", 
                         os.listdir(fighters_html_dir))
print(alread_downloaded[0])

fighters_url_list = set(fighters_url_list).difference(alread_downloaded)
fighters_urls_in_fight_df = set(fighters_urls_in_fight_df).difference(alread_downloaded)

remaining_fighters_urls = list(set(fighters_url_list).difference(fighters_urls_in_fight_df))

save_pages(fighters_urls_in_fight_df, fighters_html_dir)

save_pages(remaining_fighters_urls, fighters_html_dir)


# In[ ]:




