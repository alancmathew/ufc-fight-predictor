#!/usr/bin/env python
# coding: utf-8

# ## Data Cleaning

# In[1]:


import os
import sys
import warnings
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, cpu_count


# In[2]:


sys.path.append("../src/")


# In[3]:


from utilities import *


# In[4]:


pd.set_option("display.max_columns", None)


# ### Events

# In[5]:


def clean_events(df):
    res = df["location"].str.extract(r"(?:(?P<city>.*), )?(?P<state>.*), (?P<country>.*)")
    df = pd.concat([df, res], axis=1)
    
    df["city"] = df["city"].fillna("Unspecified")
    
    df["name"] = df["name"].astype("string")
    df["date"] = pd.to_datetime(df["date"], format="%B %d, %Y", errors="coerce")                .fillna(pd.to_datetime(df["date"], format="%b %d, %Y", errors="coerce"))
    df["id"] = df["url"].map(lambda s: os.path.split(s)[1])                        .map(lambda s: int(str(s), 16))                        .astype("float")
    df["city"] = df["city"].astype("category")
    df["state"] = df["state"].astype("category")
    df["country"] = df["country"].astype("category")
    
    df = df.drop(["location", "url"], axis=1)
    
    df = df.rename({col:f"event_{col}" for col in df.columns}, axis=1)
    
    df = df.drop_duplicates(ignore_index=True)
    
    return df.reset_index(drop=True)


# ### Fights

# In[6]:


def join_weight_class(df, identifier):
    filepath = os.path.join(dir_dict["raw_csv"], f"{identifier}_fight_urls_weightclasses.csv")
    wc_df = pd.read_csv(filepath)
    wc_df["Fight ID"] =             wc_df["Fight Url"].map(lambda s: os.path.split(s)[1])
    df = df.merge(wc_df.drop("Fight Url", axis=1), on="Fight ID", how="left")
    
    return df


# In[7]:


def clean_fights(df, identifier):
    
    df = join_weight_class(df, identifier) 
        
    # Drop fights that ended in draw
    if identifier == "completed":
        df = df.loc[(df["Fighter1 Status"] == "W") | (df["Fighter2 Status"] == "W")].reset_index(drop=True)
    
    to_drop = [col for col in df.columns if "Details:" in col] + ["Event Name"]
    df = df.drop(to_drop, axis=1)
    
    fighter1_cols = [col for col in df.columns if "Fighter1" in col]
    fighter2_cols = [col for col in df.columns if "Fighter2" in col]
    general_cols = [col for col in df.columns                             if col not in set(fighter1_cols).union(fighter2_cols)]
    
    df2 = df.copy(deep=True)

    df = df.drop(fighter2_cols, axis=1)
    df2 = df2.drop(fighter1_cols, axis=1)

    # df.columns = lmap(lambda col: col.replace("Fighter1_",""), df.columns)
    df.columns = lmap(lambda col: col.replace("Fighter1","Fighter"), df.columns)

    # df2.columns = lmap(lambda col: col.replace("Fighter2_",""), df2.columns)
    df2.columns = lmap(lambda col: col.replace("Fighter2","Fighter"), df2.columns)

    df = pd.concat([df, df2], axis=0)
    
    df.columns = lmap(lambda col: col.lower().replace(" ","_").replace(".",""), df.columns)
    df = df.drop("fighter_name", axis=1)
    
    
    # ID columns
    url_cols = [col for col in df.columns if "url" in col]
    new_id_cols = lmap(lambda s: s.replace("url", "id"), url_cols)
    df[new_id_cols] = df[url_cols].applymap(lambda s: os.path.split(s)[1])
    
    id_cols = [col for col in df.columns if "id" in col]
    df.loc[:, id_cols] = df.loc[:,id_cols].applymap(lambda s: int(str(s), 16)).astype("float")
    
    df = df.drop(url_cols, axis=1)
    
    
        # Category columns
    cat_cols = ["bout", "method", "time_format", "weight_class"]
    for col in cat_cols:
        if col in df.columns:
            if col in ["method", "bout", "weight_class"]:
                method_formatter = lambda s: s.lower()                                                .replace(" - ","_")                                                .replace("/", "_")                                                .replace(" ", "_")                                                .replace("'","")
                df[col] = df[col].map(method_formatter)
            df[col] = df[col].astype("category")
    
    if identifier == "upcoming":
        return df.reset_index(drop=True)

    df["fighter_win"] = (df["fighter_status"] == "W")
    df = df.drop("fighter_status", axis=1)
    
    
    df = df.loc[df["fighter_overall_kd"].notnull()]
    
    # String columns
    str_cols = ["referee", "details"]
    df[str_cols] = df[str_cols].astype("string")
    
    
    
    # Percentage columns
    is_pct_col = df.select_dtypes("object").apply(lambda s: s.str.contains("%")).any()
    pct_cols = is_pct_col[is_pct_col == True].index.to_list()
    df[pct_cols] = df[pct_cols].apply(lambda s:                                         s.str.replace("%","").astype("float") / 100)
    
    
    
    # Out-of columns
    is_outof_col = df.select_dtypes("object").apply(lambda s:                                                     s.str.contains(r"\d+ of \d+")).any()
    outof_cols = is_outof_col[is_outof_col == True].index.to_list()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for col in outof_cols:
            df = pd.concat([df,                             df[col].str.extract(f"(?P<{col}_landed>\d+) of (?P<{col}_total>\d+)")],
                           axis=1)

    df = df.drop(columns=outof_cols)
    
    
    
    # Time columns
    is_time_col = df.select_dtypes("object").apply(lambda s: s.str.contains(r"\d+:\d+")).any()
    time_cols = is_time_col[is_time_col == True].index.to_list()

    def seconds_extrator(row):
        return row[0]*60 + row[1]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for col in time_cols:
            df[f"{col}_seconds"] =                         df[col].str.extract(r"(\d+):(\d+)").astype("float").apply(seconds_extrator, axis=1)
        
    df = df.drop(columns=time_cols)
    
    
    # Round column
    df["round"] = df["round"].astype("uint8")
    
    # Float columns
    float_cols = df.select_dtypes("object").columns.to_list()
    df[float_cols] = df[float_cols].astype("float")
    
    
    rename_dict = {col:f"fight_{col}"                    for col in df.columns                    if all([prefix not in col for prefix in ["event_", "fight_"]])
                      and col != "fighter_id"}
    
    df = df.rename(rename_dict, axis=1)
    
    return df.reset_index(drop=True)


# In[8]:


def clean_completed_fights(df):
    return clean_fights(df, "completed")
    
    
def clean_upcoming_fights(df):
    return clean_fights(df, "upcoming") 


# ### Fighters

# In[9]:


def clean_fighters(df):
    df.columns = lmap(lambda c: c.lower().replace(" ", "_").replace(".",""), df.columns)
    
    # String columns
    str_cols = ["name", "nickname"]
    df[str_cols] = df[str_cols].astype("string")
    
    # Category columns
    df["stance"] = df["stance"].str.lower().str.replace(" ","_")
    df["stance"] = df["stance"].astype("category")
    
    # ID columns
    df["fighter_id"] = df["fighter_id"].map(lambda s: int(str(s), 16)).astype("float")
    
    wlt = df["record"].str.extract(r"(?P<wins>\d+)-(?P<losses>\d+)-(?P<ties>\d+)").astype("uint8")
    df = pd.concat([df, wlt], axis=1)
    
    
    # Percentage columns
    is_pct_col = df.select_dtypes("object").apply(lambda s: s.str.contains("%")).any()
    pct_cols = is_pct_col[is_pct_col == True].index.to_list()
    df[pct_cols] = df[pct_cols].apply(lambda s: s.str.replace("%","").astype("float") / 100)
    
    # date column
    df["dob"] = pd.to_datetime(df["dob"], format="%b %d, %Y", errors="coerce")                        .fillna(pd.to_datetime(df["dob"], format="%B %d, %Y", errors="coerce"))
    
    # height column
    df["height_inches"] =             df["height"].str.extract("(\d+)'(\d+)\"").astype("float").apply(lambda row: row[0]*12 + row[1], axis=1)
    
    
    # reach column
    df["reach_inches"] =             df["reach"].str.extract("(\d+)\"").astype("float")
    
    # weight column
    df["weight_lbs"] = df["weight"].str.extract("(\d+) lbs.").astype("float")
    
    
    to_drop = ["record", "height", "reach", "weight"]
    df = df.drop(to_drop, axis=1)
    
    df.columns = [f"fighter_{col}" if col != "fighter_id" else col for col in df.columns]
    
    return df.reset_index(drop=True)


# In[10]:


def clean_dataset(filename, cleaner_func):
    
    filepath = os.path.join(dir_dict["raw_csv"], f"{filename}.csv")
    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = pd.read_csv(filepath)
        
    df = cleaner_func(df)
    
    filepath = os.path.join(dir_dict["clean"], f"{filename}.parquet")
    df.to_parquet(filepath)


# In[11]:


def cleanse():
    filename_cleaner_map = {
        "completed_events": clean_events, 
        "upcoming_events": clean_events, 
        "completed_fights": clean_completed_fights,
        "upcoming_fights": clean_upcoming_fights,
        "fighters": clean_fighters
    }
    
    for filename, cleaner_func in filename_cleaner_map.items():
        clean_dataset(filename, cleaner_func)
        print(f"Cleaned {filename}")


# In[12]:


def main():
    cleanse()


# In[13]:


if __name__ == "__main__":
    main()


# In[ ]:




