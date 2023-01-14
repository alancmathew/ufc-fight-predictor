#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys
import json
import time
import joblib
import warnings
import numpy as np
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
from multiprocessing import Pool, cpu_count


# In[2]:


sys.path.append("../src/")


# In[3]:


from utilities import *


# In[4]:


pd.set_option("display.max_columns", None)


# ### Load Pipelines

# In[5]:


pipe = joblib.load("../assets/model_training/trained_pipeline.joblib")


# ### Load Data

# In[50]:


filepath = os.path.join(dir_dict["preprocessed"], f"upcoming_processed.parquet")
df = pd.read_parquet(filepath)
print(df.shape)

to_drop = ["event_date", "fight_id", "fighter_id", "opponent_id"]
identifiers = df[to_drop]
df = df.drop(to_drop, axis=1)


# In[51]:


y_pred = pipe.predict(df)


# In[52]:


df["predicted_win"] = y_pred
df = pd.concat([identifiers, df], axis=1)


# In[53]:


df = df.drop_duplicates("fight_id", ignore_index=True)
df = df.sort_values("event_date")


# In[54]:


filepath = os.path.join(dir_dict["clean"], f"fighters.parquet")
fighters = pd.read_parquet(filepath)


# In[55]:


df = df.merge(fighters[["fighter_id", "fighter_name"]])
df = df.merge(fighters[["fighter_id", "fighter_name"]].rename(lambda s: s.replace("fighter", "opponent"), axis=1))


# In[57]:


df[["fighter_name", "opponent_name", "predicted_win"]]


# In[ ]:




