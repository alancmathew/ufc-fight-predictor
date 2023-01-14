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
from multiprocessing import Pool, cpu_count


# In[2]:


sys.path.append("../src/")


# In[3]:


from utilities import *


# In[4]:


pd.set_option("display.max_columns", None)

os.environ["PYTHONWARNINGS"] = "ignore"


# In[5]:


from sklearn.model_selection import cross_val_score


# In[6]:


filepath = os.path.join(dir_dict["train_test"], f"train.parquet")
df = pd.read_parquet(filepath)

to_drop = ["event_date", "fight_id", "fighter_id", "opponent_id"]
df = df.drop(to_drop, axis=1)


# In[7]:


target = "fight_fighter_win"
X, y = df.drop(target, axis=1), df[target]


# In[8]:


def cross_validate(estimator, X=X, y=y, cv=5, **kwargs):
    return np.mean(cross_val_score(estimator, X, y, cv=cv, **kwargs))


# ### Initial Model

# In[9]:


from sklearn.linear_model import LogisticRegression


# In[10]:


lr = LogisticRegression(random_state=42)


# In[11]:


cross_validate(lr)


# ### Scale Data

# In[12]:


from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler


# In[13]:


pipe = Pipeline([
    ("scaler", MinMaxScaler()),
    ("lr", LogisticRegression(max_iter=500, random_state=42))
])
cross_validate(pipe)


# ## Model Selection (Hyperparameter Tuning & Feature Selection)

# In[14]:


from sklearn.feature_selection import SelectKBest, f_regression


# In[15]:


from sklearn.model_selection import RandomizedSearchCV, GridSearchCV


# In[16]:


sys.path.append("../assets/model_training")


# In[17]:


from clf_param_grid_list import clfs


# In[18]:


search_results = dict()
for name, est_dict in tqdm(clfs.items()):
    pipe = Pipeline([
        ("scaler", MinMaxScaler()),
        ("selector", SelectKBest(f_regression)),
        ("model", est_dict["model"])
    ])
    
    param_grid = {f"model__{k}":v for k, v in est_dict["param_grid"].items()}
    param_grid["selector__k"] = [i*10 for i in range(20,57)]
    
    search = RandomizedSearchCV(estimator=pipe, param_distributions=param_grid, n_iter=500, 
                                cv=3, n_jobs=int(cpu_count() / 2), scoring ="accuracy", verbose=5)
    
    search.fit(X, y)
    search_results[name] = {
        "best_params": search.best_params_,
        "best_score": search.best_score_
    }
    
    with open("../assets/model_training/search_results.json", "w") as fh:
        json.dump(search_results, fh)
        
    time.sleep(120)


# ## Saving Models

# In[22]:


with open("../assets/model_training/search_results.json", "r") as fh:
    search_results = json.load(fh)


# In[23]:


best_model_name, best_result = max(search_results.items(), key=lambda x: x[1]["best_score"])
best_model = clfs[best_model_name]["model"]
best_params = best_result["best_params"]

pipe = Pipeline([
    ("scaler", MinMaxScaler()),
    ("selector", SelectKBest(f_regression)),
    ("model", best_model)
])
pipe.set_params(**best_params)
print("accuracy:", cross_validate(pipe, cv=5, n_jobs=cpu_count()))


# In[21]:


pipe.fit(X, y)
joblib.dump(pipe, "../assets/model_training/trained_pipeline.joblib")


# In[ ]:




