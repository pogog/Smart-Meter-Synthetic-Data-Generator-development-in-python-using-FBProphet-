# Synthetic-data-generator
Data-science is a key component of Morden science since it fuels AI, data analytics, etc. As the electrical grid has been modernized into a smart grid, it has also become increasingly dependent on data science to monitor and control grid activity. Realistic data is essential to evaluating the algorithm's workability but it is difficult to obtain real smart meter data due to strict privacy and security policies of many countries. using the FBprophet library, we code and develop a prediction-based Synthetic Data Generator GUI, which generate the synthetic data sets. Using the FBprophet library synthetic data sets are generated in prediction-based Synthetic Data Generator GUI. For that source  CSV (real-time) file is used to generate synthetic data in CSV format depending upon the number meter and number days to be calculated. Using FBprophet, time series data can be forecast based on an additive model that integrates seasonality, yearly, weekly, and daily trends, as well as holiday effects into non-linear trends. The algorithm is most effective when there are several seasons of historical data and strong seasonal effects in the data series. FBprophet provide automated forecast for the time series data along with seasonality and trend removals.

# Setup, Configure, and Build

## Step 1
In order to run this generator first extract unzip the source file which contain original data set of consumers energy savings in to folder where python environment running. Caution!! Don't change the name of source the file keep LCL-June2015v2_19.csv as it is.
## Step 2
Run main.py script, it open GUI interface and asking for number of meters (n) and number of days. Enter the credential and submit it. Caution!! 20 extra csv files also generated in the name of consumer_1.csv to consumer_20.csv in same working environment. Don’t delete these files it required for successful running of the main.py script.
## Step 3
Based on the entered credential it will generate the CSV files with predicted output for energy consumption.
## Step 4
Two output file will be generated A) PredictedOutput_n.csv B) PredictedallOutput_n.csv which contain all the necessary details generated by FBprophet library.
