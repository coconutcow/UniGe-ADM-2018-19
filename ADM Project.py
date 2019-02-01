#Author: Huumberto and Sanket Sabharwal

#Libraries
import numpy as np
import pandas as pd

#Importing the Dataframe
df = pd.read_csv('DataSet1.csv')
print("Before:\n",df.isnull().sum()/len(df)*100)

#Data Cleansing
df.replace('NA',np.NaN)
df['Country'].replace('NaN','Singapore',inplace=True)
df['Age'].fillna(df['Age'].mean(), inplace=True)
df['Height'].fillna(df['Height'].mean(), inplace=True)
df['Weight'].fillna(df['Weight'].mean(), inplace=True)
df['Country'].fillna('Singapore',inplace=True)
df['Medal'].fillna('Participated',inplace=True)

#Solve Query1
df1=df[['Country','Games','Medal']].copy()
df1=df1[df1.Medal!='Participated']
print(df1.groupby(['Country','Games','Medal']).size())

#Solve Query7
df1=df[['ID','Country','Year','Sport']].copy()
length1 = len(df1) 
df1.sort_values('ID',inplace=True)
df1.drop_duplicates(keep=False,inplace=True)
length2 = len(df1) 
print('Difference after removing duplicates:',' ',length1,' ',length2)
df2=df1.groupby(['Country']).size()
df2=df2.reset_index()
df2.columns=['Country','Representation']
print('Highest Representation so far:',df2.loc[df2['Representation'].idxmax()])
print('Lowest Representation so far:',df2.loc[df2['Representation'].idxmin()])

#Solve Query4
df1=df[['Country','Year','Medal']].copy()
df1=df1[df1.Medal !='Participated']
df1=df1.groupby(['Country']).size()
df1=df1.reset_index()
df1.columns=['Country','Distribution']
print('Highest Medals won so far:',df1.loc[df1['Distribution'].idxmax()])
print('Lowest Medals won so far:',df1.loc[df1['Distribution'].idxmin()])

#Solve Query3
df1=df[['Year','Medal']].copy()
df1=df1[df1.Medal !='Participated']
df1=df1.groupby(['Year']).size()
df1=df1.reset_index()
df1.columns=['Year','MedalsWon']
print(df1.sort_values(by='MedalsWon',ascending=False))







########## PySpark ##########

import numpy as np
import findspark
import matplotlib.pyplot as plt
findspark.init('C:/spark/')
from pyspark.sql import SparkSession

spark1 = SparkSession.builder.appName("test").getOrCreate()
df = spark1.read.csv("120-years-of-olympic-history-athletes-and-results/test.csv",inferSchema = True, header=True, sep = ";")

def plot_mf_partifipation(m,f,ye,title = ""):
    """
    Inputs, all numpy arrays
    m: list of year paired with male count [[1900,200],[1904,500],...[2016,2000]]
    f: list of year paired with female count
    ye: list of years [1900, 1904, ..., 2016]
    """
    
    y_males = np.array([i[0] for i in m])
    y_females = np.array([i[0] for i in f])
    c_males = np.array([i[1] for i in m])
    c_females = np.array([i[1] for i in f])
    
    count_males = np.zeros(ye.size)
    count_females = np.zeros(ye.size)

    i = 0
    for y in ye:
        index_males, = np.where(y_males == y)
        if index_males.size == 1:
            count_males[i] = c_males[index_males]
        index_females, = np.where(y_females == y)
        if index_females.size == 1:
            count_females[i] = c_females[index_females]
        i += 1

    N = ye.size
    menMeans = count_males
    womenMeans = count_females
    ind = np.arange(N)    # the x locations for the groups
    width = 0.35       # the width of the bars: can also be len(x) sequence

    plt.subplots(figsize=(20, 10))
    p1 = plt.bar(ind, menMeans, width)
    p2 = plt.bar(ind, womenMeans, width,
                 bottom=menMeans)

    plt.ylabel('Count')
    plt.title(title)
    plt.xticks(ind, ye)
    plt.legend((p1[0], p2[0]), ('Men', 'Women'))
    for i in ind:
        plt.text(i-width/2,menMeans[i]+womenMeans[i]+20,str(round(100*womenMeans[i]/(menMeans[i]+womenMeans[i]),2))+'%',fontsize=10)
    plt.show()
    return None

def plot_feature(data,yl = 'Features',xl = 'Feature measurements', t = 'Feature comparison'):
    """
    Inputs
    data: numpy array of form [[feature1,float_value1],[feature2,float_value2],....]
    yl: y axis label
    xl: x axis label
    t:  title
    """
    d_feature = data[:,0]
    d_value = data[:,1]
    
    ind = np.arange(len(d_feature))
    values = np.array([float(i) for i in d_value])
    w = 0.75
    
    plt.subplots(figsize=(20, 10))
    plt.bar(ind,values,align='center',width = w)
    plt.xticks(ind,d_feature)
    plt.ylim(np.min(values)-10,np.max(values)+5)
    plt.ylabel(yl)
    plt.xlabel(xl)
    plt.title(t)
    for i in ind:
        plt.text(i-w/3,values[i]+1,round(values[i],2),fontsize=16)
    plt.show
    
    return None

#Solve Query 5
count_by_year = np.array(df.groupBy("Sex","Year","Season").count().sort("Year","Sex").collect())
years = np.array(df.select("Year","Season").distinct().sort("Year").collect())

males_s = np.array([i for i in count_by_year if i[0] == 'M' and i[2] == 'Summer'])[:,1::2]
females_s = np.array([i for i in count_by_year if i[0] == 'F' and i[2] == 'Summer'])[:,1::2]
years_s = np.array([i[0] for i in years if i[1] == "Summer"])
plot_mf_partifipation(males_s,females_s,years_s,"Women to men participation ratio over the years (Summer)")

males_w = np.array([i for i in count_by_year if i[0] == 'M' and i[2] == 'Winter'])[:,1::2]
females_w = np.array([i for i in count_by_year if i[0] == 'F' and i[2] == 'Winter'])[:,1::2]
years_w = np.array([i[0] for i in years if i[1] == "Winter"])
plot_mf_partifipation(males_w,females_w,years_w,"Women to men participation ratio over the years (Winter)") 

#Solve Query 6
countries = np.array(df.groupBy("NOC").count().orderBy("count",ascending = False).limit(20).select("NOC").collect())
count_country = np.array(df.groupBy("Sex","NOC").count().sort("Sex").collect())
males_c = np.array([i for i in count_country if i[0] == 'M' and i[1] in countries])[:,1:3]
females_c = np.array([i for i in count_country if i[0] == 'F' and i[1] in countries])[:,1:3]
plot_mf_partifipation(males_c,females_c,countries,"Women to men participation ratio of the top 20")

#Solve Query 10
avg_weight = np.array(df.groupBy("NOC").agg(F.mean("Weight")).orderBy("avg(Weight)",ascending = False).limit(10).collect())
avg_height = np.array(df.groupBy("NOC").agg(F.mean("Height")).orderBy("avg(Height)",ascending = False).limit(10).collect())
plot_feature(avg_weight,"Weight (kg)","Countries","Top 10 average weights by country")
plot_feature(avg_height,"Height (cm)","Countries","Top 10 average heights by country")
