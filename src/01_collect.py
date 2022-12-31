#!/usr/bin/env python
# coding: utf-8

# ## Data Collection

# In[1]:


import re
import os
import sys
import json
import time
import httpx
import random
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count


# In[2]:


sys.path.append("../src/")


# In[3]:


from utilities import *


# ## Events

# In[4]:


class EventsCollector:
    
    def __init__(self, conf):
        self.conf = conf
        
        
    def save_eventlist_pages(self):
        download_sequential_pages(self.conf["completed_first_url"], 
                                  dir_dict["completed_eventlist_html"])
        download_sequential_pages(self.conf["upcoming_first_url"], 
                                  dir_dict["upcoming_eventlist_html"])
         
    def get_events_list(self, eventlist_html_dir: str) -> list[str]:
    
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
        page_files = sorted(lfilter(lambda s: s.endswith(".html"), 
                                    os.listdir(eventlist_html_dir)))
        for file in page_files:
            filepath = os.path.join(eventlist_html_dir, file)
            with open(filepath, "r") as f:
                eventlistpage_html = f.read()
                assert eventlistpage_html != ""
                soup = BeautifulSoup(eventlistpage_html, features="lxml")
                row_elems = soup.find("tbody").find_all("tr", class_="b-statistics__table-row")
                row_elems = lfilter(lambda r: \
                                    len(r.find_all("a", 
                                                   class_="b-link b-link_style_black")) != 0, \
                                                row_elems)
                events_sublist = lmap(extract_event_data, row_elems)
                events_list.extend(events_sublist)

        return events_list
    
    def get_eventlist_df(self, eventlist_html_dir: str, outfilename: str) -> pd.DataFrame:
        events_list = self.get_events_list(eventlist_html_dir)
        print(f"Event list length: {len(events_list)}")
        events_df = pd.DataFrame(events_list)
        filepath = os.path.join(dir_dict["raw_csv"], outfilename)
        events_df.to_csv(filepath, index=False)
        return events_df
    
    
    def get_eventlist_dfs(self):
        self.completed_events_df = \
                self.get_eventlist_df(dir_dict["completed_eventlist_html"], 
                                      "completed_events.csv")
        
        self.upcoming_events_df = \
                self.get_eventlist_df(dir_dict["upcoming_eventlist_html"], 
                                      "upcoming_events.csv")

        
    def save_event_pages(self):
        completed_event_urls = self.completed_events_df["url"].to_list()
        save_pages(completed_event_urls, dir_dict["completed_events_html"])
        
        upcoming_event_urls = self.upcoming_events_df["url"].to_list()
        save_pages(upcoming_event_urls, dir_dict["upcoming_events_html"])
        
        
    def start(self):
        self.save_eventlist_pages()
        self.get_eventlist_dfs()
        self.save_event_pages()


# ## Fights

# In[5]:


class FightsCollector:
    
    def extract_fight_urls(self, event_html_filepath: str) -> list[str]:
    
        with open(event_html_filepath, "r") as f:
            html_str = f.read()

        soup = BeautifulSoup(html_str, features="lxml")
        table = soup.find("table")

        links_list = []

        headers = [key for key in map(lambda x: x.text.strip(), 
                                      table.find("thead").find_all("th"))]

        rows = table.find("tbody").find_all("tr")


        for row in rows:
            for col, elem in zip(headers, row.find_all("td")):
                if col == "Weight class":
                    p_elem = elem.find("p")
                    val = p_elem.text.strip()
                    links_list.append((row["data-link"], val))

        return links_list
    
    def save_fight_urls(self, events_html_dir: str, outfilename: str) -> list[str]:
        event_files = lfilter(lambda x: x.endswith(".html"), os.listdir(events_html_dir))

        fight_urls_list = []
        for filename in tqdm(event_files):
            filepath = os.path.join(events_html_dir, filename)
            fight_urls_sublist = self.extract_fight_urls(filepath)
            fight_urls_list.extend(fight_urls_sublist)

        print(f"Fight urls list length: {len(fight_urls_list)}")
        df = pd.DataFrame(fight_urls_list, columns=["Fight Url", "Weight Class"])
        filepath = os.path.join(dir_dict["raw_csv"], outfilename)
        df.to_csv(filepath, index=False)
        return df["Fight Url"].to_list()
    
    def get_fight_urls(self):
        self.completed_fight_urls_list = \
            self.save_fight_urls(dir_dict["completed_events_html"], 
                                 "completed_fight_urls_weightclasses.csv")
        
        self.upcoming_fight_urls_list = \
            self.save_fight_urls(dir_dict["upcoming_events_html"], 
                                 "upcoming_fight_urls_weightclasses.csv")
        
    def save_fight_pages(self):
        save_pages(self.completed_fight_urls_list, dir_dict["completed_fights_html"])
        save_pages(self.upcoming_fight_urls_list, dir_dict["upcoming_fights_html"])
        
    
    def indiv_fight_data_extractor(self, fight_id_html):
        fight_id, fight_html = fight_id_html 
        fight_dict = {}

        soup = BeautifulSoup(fight_html, features="lxml")
        title_a_elem = soup.find("h2", class_="b-content__title").find("a")
        fight_dict["Event Name"] = title_a_elem.text.strip()
        fight_dict["Event Url"] = title_a_elem["href"]
        fight_dict["Fight ID"] = fight_id
        fighters = soup.find_all("div", class_="b-fight-details__person")
        for idx, fighter in enumerate(fighters, start=1):
            fight_dict[f"Fighter{idx} Status"] = fighter.find("i", 
                                    class_="b-fight-details__person-status").text.strip()

            a_elem = fighter.find("a", class_="b-link b-fight-details__person-link")
            fight_dict[f"Fighter{idx} Name"] = a_elem.text.strip()
            fight_dict[f"Fighter{idx} Url"] = a_elem["href"]

        fight_dict["Bout"] = soup.find("i", class_="b-fight-details__fight-title").text.strip()

        fight_details_div = soup.find("div", class_="b-fight-details__content")

        method_elem = fight_details_div.find("p").find("i", 
                                                       class_="b-fight-details__text-item_first")

        def get_details(i_elem):
            i_text = i_elem.text.replace("\n", "").strip()
            m = re.search(r"(.*):\s+(.*)", i_text)
            if m:
                return (m.group(1), m.group(2))

        label_elems = fight_details_div.find_all("i", class_="b-fight-details__label")
        detail_elems = lmap(lambda e: e.parent, label_elems)
        detail_tups = lfilter(lambda t: t != None, map(get_details, detail_elems))

        for label, text in detail_tups:
            fight_dict[label] = text

        fight_dict

        details_text = fight_details_div.find_all("p")[1].text.replace("\n", "").strip()

        m = re.search(r"(.*):\s+(.*)", details_text)
        if m:
            fight_dict[m.group(1)] = m.group(2)

        fight_dict

        tables = soup.find_all("table")

        len(tables)

        def fight_tables_to_dicts(page_html: str):

            def extract_table(tables):
                data_dict = {key:[] for key in map(lambda x: x.text.strip(), \
                                                       tables[0].find("thead").find_all("th"))}
                data_dict["Round"] = []

                for i, table in enumerate(tables[:2]):
                    rows = table.find("tbody").find_all("tr")

                    for j, row in enumerate(rows, start=1):
                        for col, elem in zip(data_dict.keys(), row.find_all("td")):
                            if col == "Fighter":
                                for a_elem in elem.find_all("a"):
                                    data_dict[col].append(a_elem.text.strip())
                            else:
                                for p_elem in elem.find_all("p"):
                                    val = p_elem.text.strip()
                                    val = val if val != "---" else None
                                    data_dict[col].append(val)

                        if i == 0:
                            data_dict["Round"].extend(["Overall", "Overall"])
                        else:
                            data_dict["Round"].extend([f"Round {j}", f"Round {j}"])


                return data_dict


            soup = BeautifulSoup(page_html, features="lxml")
            tables = soup.find_all("table")

            return extract_table(tables[:2]), extract_table(tables[2:])


        try:
            fight_dict["Totals"], fight_dict["Significant Strikes"] = \
                                                    fight_tables_to_dicts(fight_html)
        except IndexError:
            print("Table IndexError")
            pass 

        return fight_dict
    
    def all_fight_data_extractor(self, fights_html_dir):
        fights_html_dict = {}

        fight_files = lfilter(lambda x: x.endswith(".html"), os.listdir(fights_html_dir))
        for filename in tqdm(fight_files):
            filepath = os.path.join(fights_html_dir, filename)
            with open(filepath, "r") as f:
                html_str = f.read()
                fight_id = filename.replace(".html","")
                fights_html_dict[fight_id] = html_str

        with Pool(cpu_count()) as p:
            fights_dict_list = p.map(self.indiv_fight_data_extractor, fights_html_dict.items())

        return fights_dict_list
        
    def get_fight_dict(self): 
        completed_fights_dict_list = \
                        self.all_fight_data_extractor(dir_dict["completed_fights_html"])
        filepath = os.path.join(dir_dict["raw_json"], "completed_fights.json")
        with open(filepath, "w") as f:
            json.dump(completed_fights_dict_list, f, indent=4)
            
        return completed_fights_dict_list
    
    
    def process_fight_dict(self, fight_dict):

        def merge_additional_data(data_header):
            additional_dict = fight_dict.pop(data_header)
            fighter_names = additional_dict["Fighter"]
            round_order = lmap(lambda x: x.replace(" ", ""), additional_dict["Round"])
            if (fighter_names[0] == fight_dict["Fighter1 Name"]) and \
                                (fighter_names[1] == fight_dict["Fighter2 Name"]):
                fighter_order = ["Fighter1", "Fighter2"]
            elif (fighter_names[0] == fight_dict["Fighter2 Name"]) and \
                                (fighter_names[1] == fight_dict["Fighter1 Name"]):
                fighter_order = ["Fighter1", "Fighter2"]
            else:
                raise Exception(f"Fighter names not congurent in {data_header} table")

            for key, lov in list(additional_dict.items())[1:-1]:
                for i, v in enumerate(lov):
                    var = f"{key}_SS" if data_header == "Significant Strikes" else key
                    fight_dict[f"{fighter_order[(i % 2)]}_{round_order[i]}_{var}"] = v

        fight_dict_keys = fight_dict.keys()
        additional_data_headers = ["Totals", "Significant Strikes"]
        for data_header in additional_data_headers:
            if data_header in fight_dict_keys:
                merge_additional_data(data_header)

        return fight_dict

    def fight_dicts_to_df(self, fight_dict_list):
        with Pool(cpu_count()) as p:
            res_fight_dict_list = p.map(self.process_fight_dict, fight_dict_list)

        return pd.DataFrame(res_fight_dict_list)

    
    def save_fight_df(self):
        completed_fights_lod = self.get_fight_dict()
        fights_df = self.fight_dicts_to_df(completed_fights_lod)
        filepath = os.path.join(dir_dict["raw_csv"], "completed_fights.csv")
        fights_df.to_csv(filepath, index=False)
    
    def start(self):
        self.get_fight_urls()
        self.save_fight_pages()
        self.save_fight_df()


# ## Fighters

# In[6]:


class FightersCollector:
        
    def save_fighterlist_pages(self):
        for i in tqdm(range(97,123)):
            fighterlist_page_first_url = f"http://ufcstats.com/statistics/fighters?char={chr(i)}"
            download_sequential_pages(fighterlist_page_first_url, dir_dict["fighterlist_html"])
        
    def extract_fighter_urls(self, fighterlist_html_filepath: str) -> list[str]:
        with open(fighterlist_html_filepath, "r") as f:
            html_str = f.read()

        soup = BeautifulSoup(html_str, features="lxml")
        rows = soup.find("tbody").find_all("tr")
        rows = filter(lambda r: r.find("a") != None, rows)
        return lmap(lambda r: r.find("a")["href"], rows)
    
    def save_fighter_urls(self, fighterlist_html_dir: str, outfilename: str) -> list[str]:
        fighterlist_files = lfilter(lambda x: x.endswith(".html"), 
                                    os.listdir(fighterlist_html_dir))

        fighter_urls_list = []
        for filename in tqdm(fighterlist_files):
            filepath = os.path.join(fighterlist_html_dir, filename)
            fighter_urls_sublist = self.extract_fighter_urls(filepath)
            fighter_urls_list.extend(fighter_urls_sublist)

        print(f"Fighter urls list length: {len(fighter_urls_list)}")
        filepath = os.path.join(dir_dict["raw_csv"], outfilename)
        write_list_to_file(fighter_urls_list, filepath)
        return fighter_urls_list
    
    def get_fighter_urls_list(self):
        self.fighters_url_list = self.save_fighter_urls(dir_dict["fighterlist_html"],
                                                        "fighter_urls.txt")
        
    def save_fighter_pages(self):
        alread_downloaded = lmap(lambda s: \
                                     f"http://ufcstats.com/fighter-details/{s.replace('.html','')}", 
                                 os.listdir(dir_dict["fighters_html"]))
        
        self.fighters_url_list = set(self.fighters_url_list).difference(alread_downloaded)
        
        save_pages(self.fighters_url_list, dir_dict["fighters_html"])
        
        
    def extract_fighter_data(self, fighter_page_filepath):

        with open(fighter_page_filepath, "r") as f:
            html_str = f.read()

        soup = BeautifulSoup(html_str, features="lxml")

        fighter_dict = dict()

        fighter_dict["Name"] = soup.find("span", 
                                         class_="b-content__title-highlight").text.strip() 

        fighter_dict["Fighter ID"] = os.path.split(fighter_page_filepath)[1].replace(".html","")

        fighter_dict["Record"] = soup.find("span", 
                        class_="b-content__title-record").text.replace("Record:","").strip() 

        nn = soup.find("p", class_="b-content__Nickname").text.strip()
        fighter_dict["Nickname"] = nn if nn else None

        fighter_dict

        uls = soup.find_all("ul", class_="b-list__box-list")

        info_list = [li.text.replace("\n","").strip() for ul in uls for li in ul.find_all("li")]

        info_list = lfilter(lambda s: s, info_list)

        for info in info_list:
            m = re.search(r"(.*):\s+(.*)", info)
            if m:
                key, val = m.group(1).strip(), m.group(2).strip()
                if key == "Height":
                    val = val.replace(" ","")
                if val == "--":
                    val = None
                fighter_dict[key] = val

        return fighter_dict
    
    def create_fighters_df(self, fighters_html_dir):
        fighter_files = os.listdir(fighters_html_dir)
        fighter_files = lfilter(lambda s: s.endswith(".html"), fighter_files)
        fighter_filepaths = lmap(lambda s: os.path.join(fighters_html_dir, s), fighter_files)

        with Pool(cpu_count()) as p:
            fighters_lod = p.map(self, extract_fighter_data, fighter_filepaths)

        return pd.DataFrame(fighters_lod)
    
    def save_fighters_df(self):
        fighters_df = self.create_fighters_df(dir_dict["fighters_html"])
        filepath = os.path.join(dir_dict["raw_csv"], "fighters_data.csv")
        fighters_df.to_csv(filepath, index=False)
        
    def start(self):
        self.save_fighterlist_pages()
        self.get_fighter_urls_list()
        self.save_fighter_pages()
        self.save_fighters_df()


# In[7]:


def start_collectors():
    events_conf = {
        "completed_first_url": "http://ufcstats.com/statistics/events/completed?page=1",
        "upcoming_first_url": "http://ufcstats.com/statistics/events/upcoming?page=1"
    }

    events_collector = EventsCollector(events_conf)

    events_collector.start()

    print("\nCompleted Events Collector\n")
    
    fights_collector = FightsCollector()

    fights_collector.start()
    
    print("\nCompleted Fights Collector\n")

    fighters_collector = FightersCollector()

    fighters_collector.start()
    
    print("\nCompleted Fighters Collector\n")


# In[8]:


def main():
    start_collectors()


# In[9]:


if __name__ == "__main__":
    main()


# In[ ]:




