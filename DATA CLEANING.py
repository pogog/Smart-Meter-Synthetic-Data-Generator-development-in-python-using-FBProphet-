#!/usr/bin/env python
# coding: utf-8

# # SYNTHETIC DATA GENERATOR

#  LIBRARY

# In[ ]:


import pandas as pd

import pyspark
from pyspark.sql.types import StringType, BooleanType, IntegerType,datetime,time,calendar,FloatType
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('sunthetic data generator').getOrCreate()

from fbprophet.plot import plot_plotly, plot_components_plotly
from fbprophet import Prophet

from tkinter import *



# DATA CLEANING

# In[ ]:


import pandas as pd
    from fbprophet.plot import plot_plotly, plot_components_plotly
    from fbprophet import Prophet
    
    file=spark.read.option('header','true').csv('LCL-June2015v2_19.csv')
    file=file.drop("stdorToU")
    file = file.withColumnRenamed('KWH/hh (per half hour) ', 'Energy_consumption_kwh')
    
    consumer_1=file.filter(file['LCLid']=='MAC000674')
    consumer_2=file.filter(file['LCLid']=='MAC000675')
    consumer_3=file.filter(file['LCLid']=='MAC000676')
    consumer_4=file.filter(file['LCLid']=='MAC000677')
    consumer_5=file.filter(file['LCLid']=='MAC000678')
    consumer_6=file.filter(file['LCLid']=='MAC000679')
    consumer_7=file.filter(file['LCLid']=='MAC000682')
    consumer_8=file.filter(file['LCLid']=='MAC000684')
    consumer_9=file.filter(file['LCLid']=='MAC000685')
    consumer_10=file.filter(file['LCLid']=='MAC000686')
    consumer_11=file.filter(file['LCLid']=='MAC000695')
    consumer_12=file.filter(file['LCLid']=='MAC000690')
    consumer_13=file.filter(file['LCLid']=='MAC000694')
    consumer_14=file.filter(file['LCLid']=='MAC000696')
    consumer_15=file.filter(file['LCLid']=='MAC000691')
    consumer_16=file.filter(file['LCLid']=='MAC000692')
    consumer_17=file.filter(file['LCLid']=='MAC000701')
    consumer_18=file.filter(file['LCLid']=='MAC000702')
    consumer_19=file.filter(file['LCLid']=='MAC000697')
    consumer_20=file.filter(file['LCLid']=='MAC000699')
    consumer_21=file.filter(file['LCLid']=='MAC000700')
    consumer_22=file.filter(file['LCLid']=='MAC000703')
    consumer_23=file.filter(file['LCLid']=='MAC000698')
    consumer_24=file.filter(file['LCLid']=='MAC000705')
    consumer_25=file.filter(file['LCLid']=='MAC000711')
    consumer_26=file.filter(file['LCLid']=='MAC000713')
    consumer_27=file.filter(file['LCLid']=='MAC000707')
    consumer_28=file.filter(file['LCLid']=='MAC000706')
    consumer_29=file.filter(file['LCLid']=='MAC000712')
    consumer_30=file.filter(file['LCLid']=='MAC000714')
    

    consumer_1=consumer_1.drop("LCLid")
    consumer_2=consumer_2.drop("LCLid")
    consumer_3=consumer_3.drop("LCLid")
    consumer_4=consumer_4.drop("LCLid")
    consumer_5=consumer_5.drop("LCLid")
    consumer_6=consumer_6.drop("LCLid")
    consumer_7=consumer_7.drop("LCLid")
    consumer_8=consumer_8.drop("LCLid")
    consumer_9=consumer_9.drop("LCLid")
    consumer_10=consumer_10.drop("LCLid")
    consumer_11=consumer_11.drop("LCLid")
    consumer_12=consumer_12.drop("LCLid")
    consumer_13=consumer_13.drop("LCLid")
    consumer_14=consumer_14.drop("LCLid")
    consumer_15=consumer_15.drop("LCLid")
    consumer_16=consumer_16.drop("LCLid")
    consumer_17=consumer_17.drop("LCLid")
    consumer_18=consumer_18.drop("LCLid")
    consumer_19=consumer_19.drop("LCLid")
    consumer_20=consumer_20.drop("LCLid")
    
    consumer_11=consumer_1.filter((consumer_1['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_1['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_22=consumer_2.filter((consumer_2['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_2['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_33=consumer_3.filter((consumer_3['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_1['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_44=consumer_4.filter((consumer_4['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_2['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_55=consumer_5.filter((consumer_5['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_1['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_66=consumer_6.filter((consumer_6['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_2['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_77=consumer_7.filter((consumer_7['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_1['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_88=consumer_8.filter((consumer_8['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_2['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_99=consumer_9.filter((consumer_9['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_1['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_1010=consumer_10.filter((consumer_10['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_2['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_1111=consumer_11.filter((consumer_11['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_1['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_1212=consumer_12.filter((consumer_12['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_2['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_1313=consumer_13.filter((consumer_13['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_1['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_1414=consumer_14.filter((consumer_14['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_2['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_1515=consumer_15.filter((consumer_15['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_1['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_1616=consumer_16.filter((consumer_16['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_2['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_1717=consumer_17.filter((consumer_17['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_1['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_1818=consumer_18.filter((consumer_18['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_2['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_1919=consumer_19.filter((consumer_19['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_1['DateTime']<='2014-01-08 00:00:00.0000000'))
    consumer_2020=consumer_20.filter((consumer_20['DateTime']>='2014-01-01 00:00:00.0000000')&(consumer_2['DateTime']<='2014-01-08 00:00:00.0000000'))
    
    consumer_1CSV = consumer_11.toPandas()
    consumer_1CSV.to_csv('consumer_1.csv', header=True, index=False)

    consumer_2CSV = consumer_22.toPandas()
    consumer_2CSV.to_csv('consumer_2.csv', header=True, index=False)

    consumer_3CSV = consumer_3.toPandas()
    consumer_3CSV.to_csv('consumer_3.csv', header=True, index=False)

    consumer_4CSV = consumer_4.toPandas()
    consumer_4CSV.to_csv('consumer_4.csv', header=True, index=False)

    consumer_5CSV = consumer_5.toPandas()
    consumer_5CSV.to_csv('consumer_5.csv', header=True, index=False)

    consumer_6CSV = consumer_6.toPandas()
    consumer_6CSV.to_csv('consumer_6.csv', header=True, index=False)

    consumer_7CSV = consumer_7.toPandas()
    consumer_7CSV.to_csv('consumer_7.csv', header=True, index=False)

    consumer_8CSV = consumer_8.toPandas()
    consumer_8CSV.to_csv('consumer_8.csv', header=True, index=False)

    consumer_9CSV = consumer_9.toPandas()
    consumer_9CSV.to_csv('consumer_9.csv', header=True, index=False)

    consumer_10CSV = consumer_10.toPandas()
    consumer_10CSV.to_csv('consumer_10.csv', header=True, index=False)
    consumer_11CSV = consumer_11.toPandas()
    consumer_11CSV.to_csv('consumer_11.csv', header=True, index=False)

    consumer_12CSV = consumer_12.toPandas()
    consumer_12CSV.to_csv('consumer_12.csv', header=True, index=False)

    consumer_13CSV = consumer_13.toPandas()
    consumer_13CSV.to_csv('consumer_13.csv', header=True, index=False)

    consumer_14CSV = consumer_14.toPandas()
    consumer_14CSV.to_csv('consumer_14.csv', header=True, index=False)

    consumer_15CSV = consumer_15.toPandas()
    consumer_15CSV.to_csv('consumer_15.csv', header=True, index=False)

    consumer_16CSV = consumer_16.toPandas()
    consumer_16CSV.to_csv('consumer_16.csv', header=True, index=False)

    consumer_17CSV = consumer_17.toPandas()
    consumer_17CSV.to_csv('consumer_17.csv', header=True, index=False)

    consumer_18CSV = consumer_18.toPandas()
    consumer_18CSV.to_csv('consumer_18.csv', header=True, index=False)

    consumer_19CSV = consumer_19.toPandas()
    consumer_19CSV.to_csv('consumer_19.csv', header=True, index=False)

    consumer_20CSV = consumer_20.toPandas()
    consumer_20CSV.to_csv('consumer_20.csv', header=True, index=False)


# In[ ]:




