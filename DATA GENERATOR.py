#!/usr/bin/env python
# coding: utf-8

# # SYNTHETIC DATA GENERATOR

# In[ ]:



def add_numbers():
   v=int(e2.get())
   for v in range(1,21):
       df=pd.read_csv('consumer_'+str(v)+'.csv')
       df.dropna(inplace= True)
       df.reset_index(drop=True, inplace=True)
       df.columns = ['ds','y']
       df['ds'] = pd.to_datetime(df['ds'])
       len(df)
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
       filtered= df1.iloc[len(df1)-337:]
       sparkDF = spark.createDataFrame(filtered)
       sparkDF = sparkDF.drop("Unnamed: 0")
       sparkDF = sparkDF.withColumnRenamed('ds', 'DateTime')
       sparkDF = sparkDF.withColumnRenamed('yhat','Predicted_consumption_Kwh')
       sparkDF_csv = sparkDF.toPandas()
       sparkDF_csv.to_csv('PredictedOutput_'+str(v)+'.csv', header=True, index=False)
       
   
window = Tk()
label_text=StringVar();
Label(window, text="Enter no of days to predict:").grid(row=0, sticky=W)
Label(window, text="Enter no of meters:").grid(row=1, sticky=W)
Label(window, text="Data will be predicted from reference data").grid(row=2, sticky=W)




e1 = Entry(window)
e2 = Entry(window)

e1.grid(row=0, column=1)
e2.grid(row=1, column=1)

b = Button(window, text="submit", command=add_numbers)
b.grid(row=0, column=2,columnspan=2, rowspan=2,sticky=W+E+N+S, padx=5, pady=5)


mainloop()


# In[ ]:




