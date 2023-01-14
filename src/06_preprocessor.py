#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys
import numpy as np
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


filepath = os.path.join(dir_dict["preprocessed"], f"agg_processed.parquet")
agg = pd.read_parquet(filepath)


# ## Completed

# In[6]:


completed_fights = agg[["event_date", "fight_id", "fighter_id", "opponent_id", "fight_fighter_win"]]


# In[20]:


def join_agg_data(df, agg):
    # Fighter Data
    df = df.merge(agg.drop(["opponent_id", "fight_fighter_win"], axis=1), how="left", on="fighter_id")
    
    df = df.loc[df["event_date_x"] > df["event_date_y"]].reset_index(drop=True)
    
    def get_latest_data(df):
        return df.loc[df["event_date_y"] == df["event_date_y"].max()]
    
    

    df = df.groupby(["fight_id_x", "fighter_id"]).apply(get_latest_data)

    df = df.reset_index([0, 1], drop=True).sort_index().reset_index(drop=True)

    df = df.drop(["event_date_y", "fight_id_y"], axis=1)            .rename({"event_date_x": "event_date", "fight_id_x":"fight_id"},axis=1)


    # Opponent Data
    opponent_renamer = lambda c: c.replace("fighter_", "opponent_")
    df = df.merge(agg.drop(["fight_fighter_win", "opponent_id"], axis=1)                       .rename(opponent_renamer, axis=1), how="left", on="opponent_id")

    df = df.dropna(axis=0)

    df = df.loc[df["event_date_x"] > df["event_date_y"]].reset_index(drop=True)

    def get_latest_data(df):
        return df.loc[df["event_date_y"] == df["event_date_y"].max()]

    df = df.groupby(["fight_id_x", "opponent_id"]).apply(get_latest_data)

    df = df.reset_index([0, 1], drop=True).sort_index().reset_index(drop=True)

    df = df.drop(["event_date_y", "fight_id_y"], axis=1)            .rename({"event_date_x": "event_date", "fight_id_x":"fight_id"},axis=1)
    
    to_drop = ["fighter_stance_sideways", "opponent_stance_sideways"]
    df = df.drop(to_drop, axis=1)
    
    return df


# In[8]:


df = join_agg_data(completed_fights, agg)


# In[9]:


filepath = os.path.join(dir_dict["preprocessed"], f"completed_processed.parquet")
df.to_parquet(filepath)


# ### Train/Test Split

# In[10]:


test_df = df.sample(frac=0.25, random_state=42)
df = df.loc[~df.index.isin(test_df.index)]


# In[11]:


filepath = os.path.join(dir_dict["train_test"], f"train.parquet")
df.to_parquet(filepath)


# In[12]:


filepath = os.path.join(dir_dict["train_test"], f"test.parquet")
test_df.to_parquet(filepath)


# ## Upcoming

# In[13]:


filepath = os.path.join(dir_dict["clean"], f"upcoming_merged.parquet")
cols = ["event_date", "fight_id", "fighter_id", "opponent_id"]
upcoming_fights = pd.read_parquet(filepath, columns=cols)


# In[ ]:


upcoming_fights


# In[17]:


upcoming_fights.shape


# In[21]:


df = join_agg_data(upcoming_fights, agg)


# In[15]:


filepath = os.path.join(dir_dict["preprocessed"], f"upcoming_processed.parquet")
df.to_parquet(filepath)

