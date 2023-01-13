#!/usr/bin/env python
# coding: utf-8

# # Preprocessing 

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


filepath = os.path.join(dir_dict["feature_engineered"], f"completed_feateng.parquet")
df = pd.read_parquet(filepath)


# In[6]:


df["fighter_name"] = df["fighter_name"].astype("category")


# In[7]:


df.select_dtypes(exclude=["number", "bool"])


# In[8]:


to_drop = ["event_name", "event_city", "event_state", "event_country",
           "fight_bout", "fight_details", "fighter_dob",  
           "fighter_name", "fighter_nickname"]


# In[9]:


df = df.drop(to_drop, axis=1)


# ### One Hot Encoding 

# In[10]:


cat_cols = df.select_dtypes(exclude=["number", "bool"]).columns.to_list()
cat_cols.remove("event_date")
cat_cols


# In[11]:


df[cat_cols] = df[cat_cols].astype("category")


# In[12]:


df = pd.concat([df, pd.get_dummies(df[cat_cols])], axis=1)


# In[13]:


df = df.drop(cat_cols, axis=1)


# In[14]:


bool_cols = df.select_dtypes("bool").columns
df[bool_cols] = df[bool_cols].astype("uint8")


# ### Drop Fight columns

# In[15]:


to_drop = [col for col in df.columns if "fight_" in col] 
to_drop.remove("fight_id")
to_drop.remove("fight_fighter_win")
df = df.drop(to_drop, axis=1)


# ### Deal with Null Values

# In[16]:


df.isna().sum().sort_values()


# In[17]:


null_counter = df.isna().sum()

null_pct = null_counter.sort_values() / df.shape[0]


# In[18]:


too_much_missing = null_pct[null_pct > 0.5].index.to_list()


# In[19]:


len(too_much_missing)


# Round4 or Round5 columns

# In[20]:


round_4_5_cols = [col for col in too_much_missing if "round4" in col or "round5" in col]
len(round_4_5_cols)


# In[21]:


df = df.drop(round_4_5_cols, axis=1)


# In[22]:


null_counter = df.isna().sum()

null_pct = null_counter.sort_values() / df.shape[0]


# In[23]:


null_pct


# In[24]:


desc_cols = ["fighter_height_inches", "fighter_age", "fighter_reach_inches"]
df = df.loc[df[desc_cols].notna().all(axis=1)].reset_index(drop=True)


# In[ ]:





# In[25]:


null_counter = df.isna().sum()

null_pct = null_counter.sort_values() / df.shape[0]

null_cols = null_pct[null_pct > 0].index.to_list()


# In[26]:


len(null_cols)


# In[27]:


len([col for col in null_cols if "cummean" in col])


# #### Fighter Stats Columns

# In[33]:


if null_cols:
    stat_cols = [col for col in null_cols if "fighter" in col]

    df[stat_cols] =         df.groupby("fighter_id")[stat_cols].transform(lambda x: x.fillna(method="ffill")                                                                          .fillna(method="bfill"))


# In[34]:


df.isna().sum().sort_values()


# In[35]:


if null_cols:
    df[stat_cols] = df[stat_cols].fillna(df[stat_cols].mean())


# In[36]:


df.isna().sum().sort_values()


# In[37]:


filepath = os.path.join(dir_dict["preprocessed"], f"agg_processed.parquet")
df.to_parquet(filepath)


# In[ ]:




