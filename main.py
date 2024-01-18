#!/usr/bin/env python
# coding: utf-8

# # PRE PROCESSING

# LIBRARY IMPORTING
import pandas as pd

# DATA LOAD AND VISUALIZATION
dat=pd.read_csv('LCL-June2015v2_19.csv')
dat.to_csv('consumer.csv', header=True, index=False)
dat= dat.drop(['stdorToU'], axis=1)
list(dat)

# # CLEANING RAW DATA

dat=dat.rename(columns = {'KWH per half hour ':'Energy_consumption_kwh'})
print(dat)

consumer_1= dat[(dat.LCLid == "MAC000674")]
consumer_2=dat[(dat.LCLid == "MAC000675")]
consumer_3=dat[(dat.LCLid == "MAC000676")]
consumer_4=dat[(dat.LCLid == "MAC000677")]
consumer_5=dat[(dat.LCLid == "MAC000678")]
consumer_6=dat[(dat.LCLid == "MAC000679")]
consumer_7=dat[(dat.LCLid == "MAC000682")]
consumer_8=dat[(dat.LCLid == "MAC000684")]
consumer_9=dat[(dat.LCLid == "MAC000685")]
consumer_10=dat[(dat.LCLid == "MAC000686")]
consumer_11=dat[(dat.LCLid == "MAC000695")]
consumer_12=dat[(dat.LCLid == "MAC000690")]
consumer_13=dat[(dat.LCLid == "MAC000694")]
consumer_14=dat[(dat.LCLid == "MAC000696")]
consumer_15=dat[(dat.LCLid == "MAC000691")]
consumer_16=dat[(dat.LCLid == "MAC000692")]
consumer_17=dat[(dat.LCLid == "MAC000701")]
consumer_18=dat[(dat.LCLid == "MAC000702")]
consumer_19=dat[(dat.LCLid == "MAC000697")]
consumer_20=dat[(dat.LCLid == "MAC000699")]
consumer_21=dat[(dat.LCLid == "MAC000700")]
consumer_22=dat[(dat.LCLid == "MAC000703")]
consumer_23=dat[(dat.LCLid == "MAC000698")]
consumer_24=dat[(dat.LCLid == "MAC000705")]
consumer_25=dat[(dat.LCLid == "MAC000711")]
consumer_26=dat[(dat.LCLid == "MAC000713")]
consumer_27=dat[(dat.LCLid == "MAC000707")]
consumer_28=dat[(dat.LCLid == "MAC000706")]
consumer_29=dat[(dat.LCLid == "MAC000712")]
consumer_30=dat[(dat.LCLid == "MAC000714")]


consumer_1=consumer_1.drop(['LCLid'],axis=1)
consumer_2=consumer_2.drop(['LCLid'],axis=1)
consumer_3=consumer_3.drop(['LCLid'],axis=1)
consumer_4=consumer_4.drop(['LCLid'],axis=1)
consumer_5=consumer_5.drop(['LCLid'],axis=1)
consumer_6=consumer_6.drop(['LCLid'],axis=1)
consumer_7=consumer_7.drop(['LCLid'],axis=1)
consumer_8=consumer_8.drop(['LCLid'],axis=1)
consumer_9=consumer_9.drop(['LCLid'],axis=1)
consumer_10=consumer_10.drop(['LCLid'],axis=1)
consumer_11=consumer_11.drop(['LCLid'],axis=1)
consumer_12=consumer_12.drop(['LCLid'],axis=1)
consumer_13=consumer_13.drop(['LCLid'],axis=1)
consumer_14=consumer_14.drop(['LCLid'],axis=1)
consumer_15=consumer_15.drop(['LCLid'],axis=1)
consumer_16=consumer_16.drop(['LCLid'],axis=1)
consumer_17=consumer_17.drop(['LCLid'],axis=1)
consumer_18=consumer_18.drop(['LCLid'],axis=1)
consumer_19=consumer_19.drop(['LCLid'],axis=1)
consumer_20=consumer_20.drop(['LCLid'],axis=1)


# Segrgate the consumer based on ID

consumer_11=consumer_1.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_22=consumer_2.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_33=consumer_3.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_44=consumer_4.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_55=consumer_5.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_66=consumer_6.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_77=consumer_7.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_88=consumer_8.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_99=consumer_9.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_1010=consumer_10.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_1111=consumer_11.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_1212=consumer_12.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_1313=consumer_13.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_1414=consumer_14.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_1515=consumer_15.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_1616=consumer_16.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_1717=consumer_17.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_1818=consumer_18.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_1919=consumer_19.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']
consumer_2020=consumer_20.loc['2014-01-01 00:00:00':'2021-01-08 00:00:00']

# Create CSV files for each consumers

consumer_1.to_csv('consumer_1.csv', header=True, index=False)
consumer_2.to_csv('consumer_2.csv', header=True, index=False)
consumer_3.to_csv('consumer_3.csv', header=True, index=False)
consumer_4.to_csv('consumer_4.csv', header=True, index=False)
consumer_5.to_csv('consumer_5.csv', header=True, index=False)
consumer_6.to_csv('consumer_6.csv', header=True, index=False)
consumer_7.to_csv('consumer_7.csv', header=True, index=False)
consumer_8.to_csv('consumer_8.csv', header=True, index=False)
consumer_9.to_csv('consumer_9.csv', header=True, index=False)
consumer_10.to_csv('consumer_10.csv', header=True, index=False)
consumer_11.to_csv('consumer_11.csv', header=True, index=False)
consumer_12.to_csv('consumer_12.csv', header=True, index=False)
consumer_13.to_csv('consumer_13.csv', header=True, index=False)
consumer_14.to_csv('consumer_14.csv', header=True, index=False)
consumer_15.to_csv('consumer_15.csv', header=True, index=False)
consumer_16.to_csv('consumer_16.csv', header=True, index=False)
consumer_17.to_csv('consumer_17.csv', header=True, index=False)
consumer_18.to_csv('consumer_18.csv', header=True, index=False)
consumer_19.to_csv('consumer_19.csv', header=True, index=False)
consumer_20.to_csv('consumer_20.csv', header=True, index=False)

# # FORECASTING

import pandas as pd
from fbprophet.plot import plot_plotly, plot_components_plotly
from fbprophet import Prophet
from tkinter import *

def add_numbers():
    v=int(e2.get())
    h=v+1
    for v in range(1,h):
        df=pd.read_csv('consumer_'+str(v)+'.csv')
        df.dropna(inplace= True)
        df.reset_index(drop=True, inplace=True)
        df.columns = ['ds','y']
        df['ds'] = pd.to_datetime(df['ds'])
        train = df.iloc[:len(df)-37]
        test = df.iloc[len(df)-37:]
        res=(int(e1.get())*48)
        m = Prophet()
        m = Prophet(changepoint_prior_scale=0.01).fit(df)
        future = m.make_future_dataframe(periods=res, freq='30min')
        forecast = m.predict(future)  
        forecast.to_csv('PredictedallOutput_'+str(v)+'.csv')
        df1 = pd.read_csv('PredictedallOutput_'+str(v)+'.csv')
        df1.dropna(inplace = True)
        df1.reset_index(drop = True, inplace = True)
        df1 = df1.drop(['trend','yhat_lower','yhat_upper','trend_lower','trend_upper','additive_terms','additive_terms_lower','additive_terms_upper','daily','daily_lower','daily_upper','multiplicative_terms_lower','multiplicative_terms_upper','multiplicative_terms'], axis = 1)
        d=len(df1)
        filtered= df1.iloc[d-res:]
        filtered=filtered.drop(['Unnamed: 0'],axis=1)
        filtered=filtered.rename(columns = {'ds':'DateTime'})
        filtered=filtered.rename(columns = {'yhat':'Predicted_consumption_Kwh'})
        filtered.to_csv('PredictedOutput_'+str(v)+'.csv', header=True, index=False)
    
root = Tk()
label_text=StringVar();
Label(root, text="Enter no of days to predict:").grid(row=0, sticky=W)
Label(root, text="Enter no of meters:").grid(row=1, sticky=W)
Label(root, text="Data will be predicted from reference data").grid(row=2, sticky=W)

e1 = Entry(root)
e2 = Entry(root)

e1.grid(row=0, column=1)
e2.grid(row=1, column=1)

b = Button(root, text="submit", command=add_numbers)
b.grid(row=0, column=2,columnspan=2, rowspan=2,sticky=W+E+N+S, padx=5, pady=5)

root.mainloop()

