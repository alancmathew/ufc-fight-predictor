#!/usr/bin/env python
# coding: utf-8

# ## Final Results

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


# In[5]:


from sklearn.model_selection import cross_val_score


# ### Load Data

# In[6]:


filepath = os.path.join(dir_dict["train_test"], f"train.parquet")
train = pd.read_parquet(filepath)
filepath = os.path.join(dir_dict["train_test"], f"test.parquet")
test = pd.read_parquet(filepath)

df = pd.concat([train, test], axis=0)
to_drop = ["event_date", "fight_id", "fighter_id", "opponent_id"]
df = df.drop(to_drop, axis=1)

target = "fight_fighter_win"
X, y = df.drop(target, axis=1), df[target]


# ### Load Pipelines

# In[7]:


pipe = joblib.load("../assets/model_training/trained_pipeline_final.joblib")


# ## Feature Importances

# In[8]:


selector = pipe.named_steps["selector"]


# In[9]:


_ = selector.fit_transform(X, y)


# In[10]:


final_features = selector.get_feature_names_out()


# In[11]:


model = pipe.named_steps["model"]


# In[12]:


coefs = model.coef_[0]


# In[13]:


feature_coefs = pd.Series(coefs, index=final_features)


# In[14]:


simple_features = [col for col in feature_coefs.index if "cummean" not in col]
feature_coefs = feature_coefs[simple_features]


# In[15]:


pos_features = feature_coefs.sort_values(ascending=False)[:10]
empty_row = pd.Series(["..."], index=["..."])
neg_features = feature_coefs.sort_values()[:10].sort_values(ascending=False)
imp_features = pd.DataFrame(pd.concat([pos_features, empty_row, neg_features]), columns=["feature_coef"])


# In[16]:


imp_features.index.name = "feature"


# In[17]:


imp_features


# #### Younger Fighter

# In[27]:


younger_fighter_winloss = df.loc[df["fighter_age"] < df["opponent_age"], target]

younger_fighter_winloss.sum() / younger_fighter_winloss.shape[0]


# #### Reach

# In[28]:


reach_adv_fighter_winloss = df.loc[df["fighter_reach_inches"] > df["opponent_reach_inches"], target]

reach_adv_fighter_winloss.sum() / reach_adv_fighter_winloss.shape[0]


# In[31]:


corrs = df.corr()[target]


# In[34]:


corrs.sort_values(ascending=False)["fighter_win_streak"]

