# UFC Fight Predictor

## Goal

Create a machine learning model that can predict fight results with a higher degree of accuracy than the baseline (50%).



Mertric of interest: **accuracy**

- since classes are balanced and incorrectly predicting either binary outcome would be equally undesirable

## Data Collection & Cleanse

The data for this project was scraped from [UFCStats](http://ufcstats.com), an unofficial record-keeping site for the UFC.

Once scraped, the data was normalized into three tables: **events**, **fights**, and **fighters**; which were then cleansed individually.

![](assets/imgs/schema_diagram.png)

Once clean, the tables where joined together to form a large table of fight details.