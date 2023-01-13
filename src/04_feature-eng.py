#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys
import numpy as np
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from pandas.testing import assert_frame_equal


# In[2]:


sys.path.append("../src/")


# In[3]:


from utilities import *


# In[4]:


pd.set_option("display.max_columns", None)


# ### Initial Prediction Test

# In[5]:


from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score


# In[6]:


def model_performance(df):
    df_num = df.select_dtypes(["number", "bool"]).copy(deep=True)
    bool_cols = df_num.select_dtypes("bool").columns
    df_num[bool_cols] = df_num[bool_cols].astype("uint8")
    df_num = df_num.fillna(0)

    to_drop = [col for col in df_num.columns if "fight_" in col] + ["fighter_id", "opponent_id"]
    to_drop.remove("fight_fighter_win")
    df_num = df_num.drop(to_drop, axis=1)

    X, y = df_num.drop("fight_fighter_win", axis=1), df_num["fight_fighter_win"]

    return np.mean(cross_val_score(LogisticRegression(random_state=42), X, y, cv=5, n_jobs=cpu_count()))


# In[7]:


filepath = os.path.join(dir_dict["clean"], "completed_merged.parquet")
df = pd.read_parquet(filepath)


# In[8]:


model_performance(df)


# ## Feature Creation 

# In[9]:


df.tail()


# ### Fighter Age 

# In[10]:


df["fighter_age"] = ((df["event_date"] - df["fighter_dob"]).dt.days / 365.25).round()


# ### Height & Reach Combo Feature 

# In[11]:


df["fighter_height_reach_prod"] = df["fighter_height_inches"] * df["fighter_reach_inches"]


# ### Wins/Losses/Win Rate (Cumulative Sum)

# In[12]:


df["fighter_cumsum_wins"] = df.groupby("fighter_id")["fight_fighter_win"].cumsum()


# In[13]:


df["fighter_cumsum_losses"] = df.groupby("fighter_id")["fight_opponent_win"].cumsum()


# In[14]:


df["fighter_cumsum_winrate"] = df["fighter_cumsum_wins"] /                                 (df["fighter_cumsum_wins"] + df["fighter_cumsum_losses"])
df["fighter_cumsum_winrate"] = df["fighter_cumsum_winrate"].fillna(0.5)


# Drop fights that ended in draw

# In[15]:


df = df.loc[(df["fight_fighter_win"]) | (df["fight_opponent_win"])].reset_index(drop=True)


# ### Fight Stats (Cumulative Mean)
# Currently:
# - fighter's stats
# - fighter's all opponents' stats
# 
# After self-join:
# - opponent's stats
# - opponent's all opponents' stats

# In[16]:


fight_stats_cols = [col for col in df.columns if "fight_fighter_" in col or "fight_opponent_" in col]
for col in ["fight_fighter_win", "fight_opponent_win"]:
    fight_stats_cols.remove(col)                                                    


# In[17]:


fight_stats_cummean = df.groupby("fighter_id")[fight_stats_cols].expanding().mean().reset_index(0, drop=True).sort_index()


# In[18]:


col_names = [col.replace("fight_fighter","fighter_cummean").replace("fight_opponent", "fighter_opponents_cummean")
             for col in fight_stats_cummean.columns]
fight_stats_cummean.columns = col_names


# In[19]:


df = pd.concat([df, fight_stats_cummean], axis=1)


# In[20]:


df.head()


# ### Wins/Losses Per Round (Cumulative Sum)
# Currently:
# - fighter_round{r} wins/losses
# 
# After self-join:
# - opponent_round{r} wins/losses

# In[21]:


to_drop = []
for r in range(1, 6):
    new_col = f"fight_fighter_round{r}_win"
    df[new_col] = ((df["fight_fighter_win"]) & (df["fight_round"] == r))
    df[f"fighter_cumsum_round{r}_wins"] =             df.groupby("fighter_id")[f"fight_fighter_round{r}_win"].cumsum()
    to_drop.append(new_col)

    new_col = f"fight_fighter_round{r}_loss"
    df[new_col] = ((~df["fight_fighter_win"]) & (df["fight_round"] == r))
    df[f"fighter_cumsum_round{r}_losses"] =             df.groupby("fighter_id")[f"fight_fighter_round{r}_loss"].cumsum()
    to_drop.append(new_col)
    
    df[f"fighter_cumsum_round{r}_winrate"] =                     df[f"fighter_cumsum_round{r}_wins"] /                         (df[f"fighter_cumsum_round{r}_wins"] + df[f"fighter_cumsum_round{r}_losses"])
    df[f"fighter_cumsum_round{r}_winrate"] = df[f"fighter_cumsum_round{r}_winrate"].fillna(df["fighter_cumsum_winrate"])
    
df = df.drop(to_drop, axis=1)


# ### W/L/Wr (Wins/Losses/Win Rate) by Method

# In[22]:


method_dummies = pd.get_dummies(df["fight_method"], prefix="fight_method").astype("bool")
df = pd.concat([df, method_dummies], axis=1)


# In[23]:


method_dummy_cols = method_dummies.columns
to_drop = []
for col in method_dummy_cols:
    method = col.replace("fight_method_","")
    df[f"fight_fighter_method_{method}_win"] = (df["fight_fighter_win"]) & (df[col])
    df[f"fight_fighter_method_{method}_loss"] = (~df["fight_fighter_win"]) & (df[col])
    
    to_drop.extend([f"fight_fighter_method_{method}_win", 
                    f"fight_fighter_method_{method}_loss"])
    
    df[f"fighter_method_{method}_wins"] =         df.groupby("fighter_id")[f"fight_fighter_method_{method}_win"].cumsum()
    df[f"fighter_method_{method}_losses"] =         df.groupby("fighter_id")[f"fight_fighter_method_{method}_loss"].cumsum()
    df[f"fighter_method_{method}_winrate"] = df[f"fighter_method_{method}_wins"] /                                             (df[f"fighter_method_{method}_wins"] + df[f"fighter_method_{method}_losses"])
    df[f"fighter_method_{method}_winrate"] = df[f"fighter_method_{method}_winrate"].fillna(df["fighter_cumsum_winrate"])
    
df = df.drop(to_drop + list(method_dummy_cols), axis=1)


# In[24]:


df.iloc[:, -10:].head()


# ### W/L by Stance 

# In[25]:


opponent_stance_dummies = pd.get_dummies(df["opponent_stance"], prefix="fight_fighter_opponent").astype("bool")
df = pd.concat([df, opponent_stance_dummies], axis=1)


# In[26]:


stance_dummy_cols = opponent_stance_dummies.columns
to_drop = []
for col in stance_dummy_cols:
    stance = col.replace("fight_fighter_opponent_","")
    df[f"fight_fighter_opponent_stance_{stance}_win"] = (df["fight_fighter_win"]) & (df[col])
    df[f"fight_fighter_opponent_stance_{stance}_loss"] = (~df["fight_fighter_win"]) & (df[col])
    
    to_drop.extend([f"fight_fighter_opponent_stance_{stance}_win", 
                    f"fight_fighter_opponent_stance_{stance}_loss"])
    
    df[f"fighter_opponent_stance_{stance}_wins"] =                 df.groupby("fighter_id")[f"fight_fighter_opponent_stance_{stance}_win"].cumsum()
    df[f"fighter_opponent_stance_{stance}_losses"] =                 df.groupby("fighter_id")[f"fight_fighter_opponent_stance_{stance}_loss"].cumsum()
    df[f"fighter_opponent_stance_{stance}_winrate"] =                     df[f"fighter_opponent_stance_{stance}_wins"] /                     (df[f"fighter_opponent_stance_{stance}_wins"] + df[f"fighter_opponent_stance_{stance}_losses"])
    df[f"fighter_opponent_stance_{stance}_winrate"] =                     df[f"fighter_opponent_stance_{stance}_winrate"].fillna(df["fighter_cumsum_winrate"])
    
df = df.drop(to_drop + list(stance_dummy_cols), axis=1)


# In[27]:


df.iloc[:,-20:].head()


# ### W/L/Wr in last 1/3/5/10/20 fights (Rolling Sum)

# In[28]:


for w in [1, 3, 5, 10, 20]:
    df[f"fighter_rollsum{w}_wins"] = df.groupby("fighter_id")["fight_fighter_win"]                    .rolling(w, min_periods=0).sum().reset_index(0, drop=True).sort_index()
    
    df[f"fighter_rollsum{w}_losses"] = df.groupby("fighter_id")["fight_opponent_win"]                    .rolling(w, min_periods=0).sum().reset_index(0, drop=True).sort_index()
    
    df[f"fighter_rollsum{w}_winrate"] = df[f"fighter_rollsum{w}_wins"] /                                             (df[f"fighter_rollsum{w}_wins"] + df[f"fighter_rollsum{w}_losses"])
    df[f"fighter_rollsum{w}_winrate"] = df[f"fighter_rollsum{w}_winrate"].fillna(df["fighter_cumsum_winrate"])


# ### W/L Streak 

# In[29]:


def streak_finder(arr):
    for idx, x in enumerate(arr[::-1]):
        if x != 1:
            return idx
    return len(arr)


# In[30]:


df["fighter_win_streak"] = df.groupby("fighter_id")["fight_fighter_win"].expanding().apply(streak_finder)                                                                     .reset_index(0, drop=True).sort_index()


# In[31]:


df["fighter_loss_streak"] = df.groupby("fighter_id")["fight_opponent_win"].expanding().apply(streak_finder)                                                                     .reset_index(0, drop=True).sort_index()


# In[32]:


### Self Join Opponent Features 

# opponent_cols = lfilter(lambda c: c.startswith("opponent_") or c.startswith("fight_opponent_"), df.columns)
# opponent_cols.remove("opponent_id")

# df = df.drop(opponent_cols, axis=1)

# fighter_cols = lfilter(lambda c: c.startswith("fighter_") or c.startswith("fight_fighter_"), df.columns)

# general_cols = set(df.columns).difference(fighter_cols)

# to_drop = general_cols
# to_drop.remove("fight_id")
# df2 = df.drop(to_drop, axis=1)

# (pd.Series(df.columns) == "opponent_id").sum()

# (pd.Series(df2.columns) == "opponent_id").sum()

# df2.columns = [col.replace("fighter", "opponent") for col in df2.columns]

# df2

# df = df.merge(df2, on=["fight_id", "opponent_id"])


# In[33]:


df


# In[34]:


opponent_cols = [col for col in df.columns if "opponent_" in col]
opponent_cols.remove("opponent_id")
df = df.drop(opponent_cols, axis=1)


# In[35]:


filepath = os.path.join(dir_dict["feature_engineered"], f"completed_feateng.parquet")
df.to_parquet(filepath)

