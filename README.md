# UFC Fight Predictor

![https://c4.wallpaperflare.com/wallpaper/556/917/227/arts-b-w-battle-battles-wallpaper-preview.jpg](assets/imgs/banner.jpg)

## Table of Contents <a name="toc"></a>

***[Goal](#goal)***

***[1. Data Collection & Cleanse](#1-collect)***

***[2. Feature Engineering](#2-fe)***

***[3. Model Training & Evaluation](#3-model)***

***[4. Final Results](#4-results)***

***[Conclusion](#conclusion)***

***[Appendix: Exploratory Analysis](#appendix)***

***[Sources](#sources)***

## Goal <a name="goal" href="#toc">^</a>

Create a machine learning model that can predict fight results with a higher degree of correctness than the baseline.

Mertric of interest: **accuracy**

- since classes are balanced and incorrectly predicting either binary outcome would be equally undesirable

## 1. Data Collection & Cleanse <a name="1-collect" href="#toc">^</a>

### i. Scrape

The data for this project was scraped from [UFCStats](http://ufcstats.com), an unofficial record-keeping site for the UFC.

The scrape consisted of over 11,000 pages on the website.

### ii. Normalization & Joins

Once scraped, the data was normalized into three tables: **events**, **fights**, and **fighters**; which were then cleansed individually.

![Author: Alan Mathew](assets/imgs/schema_diagram.png "ER Diagram")

Once clean, the tables where joined together to form a large table of fight details.

### iii. Reshape

Since rows consist of each fight's details (`date`, `fighter_id`, `opponent_id`, `fight_fighter_win`, ...), to simplify further processing, the table was reshaped so that each row was split in two, one for each contestant's (fighter and opponent) fight details (`date`, `fighter_id`, `fight_fighter_win`, ...).

## 2. Feature Engineering <a name="2-fe" href="#toc">^</a>

### i. Cumulative sum of wins, losses, winrate

Since one might intuitively consider that the best predictor of a fighter's future performance is their past performance, the first set of features that were added were the cumulative wins, losses, and winrate of the fighter, up till the date of each fight.

### ii. By round, method, opponent's stance

In a similar vein, features consisting of cumulative wins, losses, and winrate by round, method (KO, TKO, Submission, ...), opponent's stance (othodox, southpaw, ...) were added.

### iii. Rolling sums of wins, losses, winrate

Additionally n-fight rolling sums of wins, losses, and winrate were added for `n in [1, 3, 5, 10, 20]` to measure the fighter's recent performance at the time of the fight.

### iv. Win/Lose streaks

Another measure of recent performance that was added is a fighter's winning or losing streak at the date of each fight.

### v. Cumulative means of fight stats

A number of per fight stats including number of knockdowns and significant strikes were also available. Using that, cumulative means of these fight stats were created to reflect each fighter's average performance up till the date of each fight.

## 3. Model Training & Evaluation <a name="3-model" href="#toc">^</a>

### i. Train/Test Split

Before model training, the data was split into train (*n=6860, p=0.75*) and test (*n=2286, 0.25*) sets. The train set will be used to train and optimize the model and the test set will be used to evaluate only the best model.

### ii. Model Selection (Feature Selection & Hyperparameter Tuning)

To find the optimal model, a number of difference machine learning algorithmns were tested. For each model, a pipeline was created to scale the data, select the k best features (where k is a hyperparameter to be optimized), and perform hyperparamater tuning. To perform hyperparameter tuning, the pipeline was subject to 3-fold cross validation, attempting to find the paramaters that maximized accuracy.

The best model was a **Logistic Regression** model with **360** **features**, which had a 5-fold cross validated accuracy of: **0.6089** (60.89%).

### iii. Evaluation

After having selected the best model, it was then evaluated using the train and test sets, which yielded the following confusion matrices:  

![Author: Alan Mathew](assets/imgs/train_confusion_matrix.png "Train Confusion Matrix")

![Author: Alan Mathew](assets/imgs/test_confusion_matrix.png "Test Confusion Matrix")

And produced the following table of metrics:

<table class="dataframe" border="1">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>accuracy</th>
      <th>f1_score</th>
      <th>precision</th>
      <th>recall</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>train</th>
      <td>0.6531</td>
      <td>0.6531</td>
      <td>0.6529</td>
      <td>0.6533</td>
    </tr>
    <tr>
      <th>test</th>
      <td>0.6234</td>
      <td>0.6144</td>
      <td>0.6299</td>
      <td>0.5997</td>
    </tr>
  </tbody>
</table>

Thus, based on unseen data, the model seems to have an accuracy of **0.6234** (62.34%).

### iv. Re-train

After evaluating the model with the test set, the model was re-trained using all the available data (train + test sets).

## Sources <a name="sources" href="#toc">^</a>

- https://c4.wallpaperflare.com/wallpaper/556/917/227/arts-b-w-battle-battles-wallpaper-preview.jpg
- http://ufcstats.com/