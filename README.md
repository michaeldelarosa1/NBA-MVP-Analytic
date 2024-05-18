|*********************************|
|PROJECT: MVP Prediction Algorithm|
|*********************************|


## Directory and File Description

For our project, we split up our directories into /data_ingest, /etl_code, /profiling_code, /data, and /ana_code.

/data_ingest
    General Description:
        These are the scala files that we used to load the data that we got from our datasources into spark scala
    Files:
        initial_setup.scala

/data
    General Desctiption:
        There are two subdirectories within this file, ./cleaned and ./raw. ./raw consists of the raw data files as we took from the internet,
        while ./cleaned consists of what the files should look like, and should be named after running the scala files within the ./cleaned seciton 
        of our project. 
    Files:
        ./raw
            nba.com_19_20.csv
            nba.com_20_21.csv
            nba.com_21_22.csv
            nba.com_22_23.csv
            nba.com_23_24.csv
            2019_Advanced.csv
            2019_Per_Game.csv
            2020_Advanced.csv
            2020_Per_Game.csv
            2021_Advanced.csv
            2021_Per_Game.csv
            2022_Advanced.csv
            2022_Per_Game.csv
            2023_Advanced.csv
            2023_Per_Game.csv
        ./cleaned
            bbref_cleaned_test.csv
            bbref_cleaned_train.csv
            cleaned_data_test.csv
            cleaned_data_train.csv
            

./etl_code
    General Description:
        This folder contains two subdirectories for each team member including the code that they wrote to clean their datasource, as well as the code written to merge the cleaned data
    Files:
        ./dl4457
            cleaning_nba.com.scala
        ./mfd9268
            cleaning_bbref.scala
        merge_cleaning.scala


./ana_code
    General Description:
        This seciton consists of the files that contain the model that we train and test this year's data on to predict who we beleive is the most valuable player in the NBA
    Files:
        regression_analytics.scala

./profiling_code
    General Description:
        This folder contains two subdirectories for each team member including the code that they wrote to profile their datasource
    Files:
        ./dl4457
            data_profiling_nba.com.scala
        ./mfd9268
            data_profiling_bbref.scala


## How we built our project

Part 1: Idea Generation 
    Because we were both had a keen intereset in basketball and the debates surrounding the sport, we knew we were going in that direction from the outset. One of the hotly contested debates 
    since the award was introduced is who the NBA MVP award should go to, or rather who will it go to. Due to the recent statistics revolution in basketball, ther is a wealth of various statistics 
    ranging anywhere from counting stats like points to advanced stats like Value Over Replacement Player (VORP). Therefore, we settled on using this abundance of statistics to predict who, based on 
    the numbers, will be the MVP this year.

Part 2: Gathering Data
    The NBA keeps a good record of various statistics on each player, including their data department's own advanced statistics. These are all available on the NBA's official website, NBA.com, which we
    used in our project. For our other data source, we decided to use the popular website Basketball Reference, who keeps track of advanced statistics outside the NBA's own stat room and has info on every player 
    in most seasons. Since we are predicting a MVP this year, we decided that we needed the last 5 years worth of data from these data sources. We would train our model on the 4 years prior to the current year, 
    and test it our on this years statistics, hopefully getting close to this years award.

Part 3: Initial Cleaning
    We decided to combine both datasets for each player, for each season in order to get their "full season's" worth of data. We dropped columns that were repeated between data sources so that they wouldn't be counted
    twice in our algorithm, as well as made all player names lowercase in order to make joining easier. We encountered the problem of accents interfering with our joining and dealt with that by creating a udf to filter out 
    the accents so we could smoothly join the two datasources. Since we decided on using Win Shares as our target variable, we removed all Win Share related features in our dataset that could interfere with our model's predictive 
    power. This included Offensive Win Shares, Defensive Win Shares, and Win Shares per 48 minutes. 

Part 4: Model Selection and Tuning
    We settled on a target variable of "Win Shares"(WS), which we decided captured the essence of what a most valuable player should be. We decided that it's best to start with linear regression and see how well the 
    model performs before moving onto more complicated models. After an initial run led to an extreemely good R^2, we we worried that the model was overfitting to the training dataset. As such, we decided to introduce some 
    regularization and ran a Ridge and Lasso regression in parallel with our default linear regression in order to compare the performances on the test sets and adjust accordingly. Since the performance across the board was so high 
    we decided to stick with linear/lasso/ridge regression as our model for our conclusions.

Part 5: Interpreting Results
    With the new rules in the NBA limiting the MVP award to players who play 65 games or more, we tested our model on the players who played more than 65 games this season. Our model predicted win shares on the next season with an 
    R^2 > than 0.95 almost every run that we conducted. This means that our model is very accurate in its predictions. Interestingly, we found that outside of the predictors that we expected to have high coefficients like VORP, stats like 
    freethrow rate and total rebounds seemed to have a higher than expected predictive effect on the win shares of a given player.

This year, our model predicts that Nikola Jokic will be the MVP.
    

## Steps to Replicate Analysis

Step 1: Getting and loading the data and files 
    The first step to replicate our data analytic is to download the data from theses sources 
        Basketball Reference
            Advanced Statistics: https://www.basketball-reference.com/leagues/NBA_2024_advanced.html
            Per Game Statistics: https://www.basketball-reference.com/leagues/NBA_2024_per_game.html
            INSTRUCTIONS:
                Select 'Get table as CSV (for excel)' from the 'Share & Export' dropdown and copy paste the data into an empty textfile to save as csv
                Do for all season for training as well as the season you want to test out! 
        NBA.com 
            Advanced Statistics: https://www.nba.com/stats/players/advanced
            INSTRUCTIONS: Highlight table and copy paste into an excel sheet to download as a csv (do for all pages)
        NOTE: Do for all season for training as well as the season you want to test out! (WE trained on 2019-2022 to test the current season's stats)
    
    Once you have these loaded, it is important that you name the files like shown in the ./data/raw folder
    To upload these files, simply upload like usual and use the 'hdfs dfs -put myFile.csv' file (replacing myFile.csv with desired file) to load into hadoop filesystem
    Once completed, do the same for all .scala files included in all directories

Step 2: Cleaning and Saving Cleaned Data 
    Start your spark shell by using the 'spark-shell --deploy-mode client' command
    Run the .scala files found in the ./data_ingest folder using the ':load myFile.scala' (replacing myFile.scala with desired file)
    To clean the data, follow these steps carefully:
        First, run the files in the ./etl_code and ./profiling_code folder associated with bbref consecutively in this order:
            1. cleaning_bbref.scala
            2. data_profiling_bbref.scala
        Once finished running BOTH of the files above, run the files in the ./etl_code and ./profiling_code folder folder associated with nba.com in this order:
            1. cleaning_nba.com.scala
            2. data_profiling.nba.scala
    
    Once finished, exit scala and download the cleaned data as instructed below:

    INSTRUCTIONS FOR DOWNLOAD AND STORAGE OF EACH CLEANED DATASET:
        Since we are running on spark scala, each output will be stored as a colleciton of files under a folder named after the desired .csv file 
        For each dataset, download the associated file using 'hdfs dfs -getmerge myFile1.csv myFile.csv' (replacing myFile.csv with desired file)
            myFile1.csv is the directory that the data was saved to and myFile.csv is what you want to merge the file as.
            This merges the outputfiles into a csv named whatever you put into myFile.csv and saves it into your local directory on dataproc

        These are the files you will need, which are the same name as the folders where the distributed csvs will be held:
            bbref_cleaned_test.csv
            bbref_cleaned_train.csv
            cleaned_data_test.csv
            cleaned_data_train.csv

        For example, you would use 'hdfs dfs -getmerge bbref_cleaned_test1.csv bbref_cleaned_test.csv'.
    
    Once finished, download the files into your local filesystem using the dataproc UI 

Step 3: Loading the Cleaned Data and Running the model!
    Begin this step by loading the cleaned data that you downloaded from step 2 into the hadoop filesystem 
        This is similar to step 1
        Make sure you're loading the 4 files above, they should not have the 1 in the name.
    Double check that both .scala files in the ./ana_code folder are loaded into your hdfs before continuing
    Restart your spark shell as you did above in step 2 (spark-shell --deploy-mode client)

    Once in the spark shell, run the file merge_cleaning.csv in the ./etl_code directory with ':load merge_cleaning.scala' to merge the cleaned data from the two datasources for use in the model
    Make sure to change the directory path in the file to whoever is running it, not dl4457/mfd9268.
    After that finishes running, finally run our model using the ':load regression_analytics.scala' command to train and exexute our model
    At the conclusion, you should have a table of the players predicted to have the top win contributions who have played at least 65 games in your terminal.
    as well as a table of the coefficients of each of the features in our dataset.
        The list of players corresponds to our model's rankings of who deserves MVP based on the statsitics we have gathered 
        The table of coefficients corresponds to the weight each feature has on the predictions of our model 
        
