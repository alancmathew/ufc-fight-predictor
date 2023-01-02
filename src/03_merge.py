#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, cpu_count


# In[2]:


sys.path.append("../src/")


# In[3]:


from utilities import *


# In[4]:


pd.set_option("display.max_columns", None)


# In[5]:


def read_parquet(identifier):
    filepath = os.path.join(dir_dict["clean"], f"{identifier}.parquet")
    return pd.read_parquet(filepath)


# In[6]:


def merge_datasets(identifier):
    events = read_parquet(f"{identifier}_events")
    fights = read_parquet(f"{identifier}_fights")
    fighters = read_parquet("fighters")
    
    df = events.merge(fights, on="event_id", how="right").merge(fighters, on="fighter_id", how="left")
    df = df.drop(["event_id"], axis=1)

    general_cols = [col for col in df.columns                     if not (col.startswith("fight_fighter_") or col.startswith("fighter_") or col == "fight_id")]

    df = df.merge(df.drop(general_cols, axis=1), on="fight_id", suffixes=("_A", "_B"))

    df = df.loc[df["fighter_id_A"] != df["fighter_id_B"]].reset_index(drop=True)

    if identifier == "completed":
        df["winner"] = df["fight_fighter_won_B"]
        df = df.drop(["fight_fighter_won_A", "fight_fighter_won_B"], axis=1)

    filepath = os.path.join(dir_dict["clean"], f"{identifier}_merged.parquet")
    df.to_parquet(filepath)
    
    return df


# In[7]:


def merge():
    completed = merge_datasets("completed")
    upcoming = merge_datasets("upcoming")


# In[ ]:


def main():        
    merge()


# In[ ]:


if __name__ == "__main__":
    main()

