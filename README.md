# UFC Fight Predictor

![https://c4.wallpaperflare.com/wallpaper/556/917/227/arts-b-w-battle-battles-wallpaper-preview.jpg](assets/imgs/banner.jpg)

## Table of Contents <a name="toc"></a>

***[Goal](#goal)***

***[1. Data Collection & Cleanse](#1-collect)***

***[2. Feature Engineering](#2-fe)***

***[3. Model Training & Evaluation Method](#3-model)***

***[4. Final Results](#4-results)***

***[Conclusion](#conclusion)***

***[Appendix: Exploratory Analysis](#appendix)***

***[Sources](#sources)***

## Goal <a name="goal" href="toc">^</a>

Create a machine learning model that can predict fight results with a higher degree of correctness than the baseline.

Mertric of interest: **accuracy**

- since classes are balanced and incorrectly predicting either binary outcome would be equally undesirable

## Data Collection & Cleanse <a name="1-collect" href="#toc">^</a>

The data for this project was scraped from [UFCStats](http://ufcstats.com), an unofficial record-keeping site for the UFC.

Once scraped, the data was normalized into three tables: **events**, **fights**, and **fighters**; which were then cleansed individually.

![](assets/imgs/schema_diagram.png)

Once clean, the tables where joined together to form a large table of fight details.

## Sources <a name="sources" href="#toc">^</a>

- https://c4.wallpaperflare.com/wallpaper/556/917/227/arts-b-w-battle-battles-wallpaper-preview.jpg
- http://ufcstats.com/