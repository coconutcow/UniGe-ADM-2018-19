#Author: Huumberto and Sanket Sabharwal

#Libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

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

#Solve Query2
df1=df[['Country','Season','Medal']].copy()
df1=df1[df1.Medal!='Participated']
df1=df1.groupby(['Country','Season']).size()
df1=df1.reset_index()
df1.columns=['Country','Season','MedalsWon']
dfsummer=df1[df1.Season != 'Winter']
dfwinter=df1[df1.Season != 'Summer']
print('Highest Medals won so far in Summer Games:','\n',dfsummer.loc[dfsummer['MedalsWon'].idxmax()])
print('Highest Medals won so far in Winter Games:','\n',dfwinter.loc[dfwinter['MedalsWon'].idxmax()])


#Solve Query3
df1=df[['Year','Medal']].copy()
df1=df1[df1.Medal !='Participated']
df1=df1.groupby(['Year']).size()
df1=df1.reset_index()
df1.columns=['Year','MedalsWon']
print(df1.sort_values(by='MedalsWon',ascending=False))

#Solve Query4
df1=df[['Country','Year','Medal']].copy()
df1=df1[df1.Medal !='Participated']
df1=df1.groupby(['Country']).size()
df1=df1.reset_index()
df1.columns=['Country','Distribution']
print('Highest Medals won so far:',df1.loc[df1['Distribution'].idxmax()])
print('Lowest Medals won so far:',df1.loc[df1['Distribution'].idxmin()])

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

#Solve Query9A
df1=df[['ID','Country','Season','Sport']].copy()
length1 = len(df1) 
df1.sort_values('ID',inplace=True)
df1.drop_duplicates(keep=False,inplace=True)
length2 = len(df1) 
print('Difference after removing duplicates:',' ',length1,' ',length2)
df2=df1.groupby(['Country','Season']).size()
df2=df2.reset_index()
df2.columns=['Country','Season','Representation']
dfsummer=df2[df2.Season != 'Winter']
dfwinter=df2[df2.Season != 'Summer']
dfsummer.sort_values('Representation',inplace=True,ascending=False)
dfsummer=dfsummer.head(10)
dfwinter.sort_values('Representation',inplace=True,ascending=False)
dfwinter=dfwinter.head(10)
dfsummer.plot(kind='bar',title='Bar Graph Summer: Representation of top 10 countries',x='Country',y='Representation',legend=False)
dfwinter.plot(kind='bar',title='Bar Graph Winter: Representation of top 10 countries',x='Country',y='Representation',legend=False)


#Solve Query9B
df1=df[['ID','Country','Year']].copy()
length1 = len(df1) 
df1.sort_values('ID',inplace=True)
df1.drop_duplicates(keep=False,inplace=True)
length2 = len(df1) 
print('Difference after removing duplicates:',' ',length1,' ',length2)
df1=df1.groupby(['Country']).size()
df1=df1.reset_index()
df1.columns=['Country','Representation']
df1.sort_values('Representation',inplace=True,ascending=False)
df1=df1.head(10)
df1.plot(kind='bar',title='Bar Graph: Representation of top 10 countries',x='Country',y='Representation',legend=False)

#Solve Query9C and 2
df1=df[['ID','Year']].copy()
length1 = len(df1) 
df1.sort_values('ID',inplace=True)
df1=df1.drop_duplicates(subset='ID')
length2 = len(df1) 
print('Difference after removing duplicates:',' ',length1,' ',length2)
df1=df1.groupby(['Year']).size()
df1=df1.reset_index()
df1.columns=['Year','Representation']
df1.sort_values('Representation',inplace=True,ascending=False)
df1=df1.head(10)
df1.sort_values('Year',inplace=True,ascending=True)
df1.plot(kind='bar',title='Bar Graph: Highest participation over the years',x='Year',y='Representation',legend=False)
df1.plot(kind='line',title='Bar Graph: Highest participation over the years',x='Year',y='Representation',legend=False)

#Solve Query15
df1=df[['Name','Sport']].copy()
df1=df1.groupby(['Name','Sport']).size()
df1=df1.groupby(['Name']).size()
df1=df1.reset_index()
df1.columns=['Name','SportsPlayed']
df1.sort_values('SportsPlayed',inplace=True,ascending=False)
df1=df1.head(10)

for i in df1.values:
    print(i[0]+':')
    print(', '.join(df.loc[df['Name']== i[0]]['Sport'].drop_duplicates().values))

df1.plot(kind='bar',title='Bar Graph: Highest participation over the years',x='Name',y='SportsPlayed',legend=False)

#Solve Query16
df1=df[['Name','Sport','Height','Medal']].copy()
df1=df1[(df1.Sport =='Swimming') & (df1.Medal != 'Participated')]
meanheight=df1['Height'].mean()
df1=df1[df1.Height>meanheight]
df1=df1.drop_duplicates(subset='Name')
df1.sort_values('Name',inplace=True,ascending=True)
print('Players with their individual height higher than the average height, and who have won medals:')
for i in df1.values:
        print(i[0])
        
#Solve Query17
df1=df[['Name','Sport','Weight','Medal']].copy()
df1=df1[(df1.Sport =='Weightlifting') & (df1.Medal != 'Participated')]
meanweight=df1['Weight'].mean()
df1=df1[df1.Weight>meanweight]
df1=df1.drop_duplicates(subset='Name')
df1.sort_values('Name',inplace=True,ascending=True)
print('Players with their individual weight higher than the average weight, and who have won medals:')
for i in df1.values:
        print(i[0])
        
#Solve Query18
df1=df[['City','Year']].copy()
df1=df1.groupby(['City','Year']).size()
df1=df1.groupby(['City']).size()
df1=df1.reset_index()
df1.columns=['City','Year']
df1.sort_values('Year',inplace=True,ascending=False)
df1=df1.head(10)
print('Cities where the olympics have been hosted the most number of times')

#Solve Query19
print('First year of event:',df.Year.min())
print('Last year of event:',df.Year.max())
print('Difference:',((df.Year.max())-(df.Year.min())))
df1=df[['ID','Name','Year','Height','Weight']].copy()
df1=df1.drop_duplicates(subset='ID')
df1=df1.drop_duplicates(subset='Name')
df1.sort_values('Name',inplace=True,ascending=True)
df10=df1[(df1.Year < 1906) & (df1.Year >= 1896)]
df20=df1[(df1.Year < 1916) & (df1.Year >= 1906)]
df30=df1[(df1.Year < 1926) & (df1.Year >= 1916)]
df40=df1[(df1.Year < 1936) & (df1.Year >= 1926)]
df50=df1[(df1.Year < 1946) & (df1.Year >= 1936)]
df60=df1[(df1.Year < 1956) & (df1.Year >= 1946)]
df70=df1[(df1.Year < 1966) & (df1.Year >= 1956)]
df80=df1[(df1.Year < 1976) & (df1.Year >= 1966)]
df90=df1[(df1.Year < 1986) & (df1.Year >= 1976)]
df100=df1[(df1.Year < 1996) & (df1.Year >= 1986)]
df110=df1[(df1.Year < 2006) & (df1.Year >= 1996)]
df120=df1[(df1.Year <= 2016) & (df1.Year >= 2006)]

print('Average Height over the years from 1896 to 2016 - intervals of 10 years:',int(df10.Height.mean()),'cm',int(df20.Height.mean()),'cm',int(df30.Height.mean()),'cm',int(df40.Height.mean()),'cm',int(df50.Height.mean()),'cm',int(df60.Height.mean()),'cm',int(df70.Height.mean()),'cm',int(df80.Height.mean()),'cm',int(df90.Height.mean()),'cm',int(df100.Height.mean()),'cm',int(df110.Height.mean()),'cm',int(df120.Height.mean()),'cm')
print('Average Weight over the years from 1896 to 2016 - intervals of 10 years:',int(df10.Weight.mean()),'kg',int(df20.Weight.mean()),'kg',int(df30.Weight.mean()),'kg',int(df40.Weight.mean()),'kg',int(df50.Weight.mean()),'kg',int(df60.Weight.mean()),'kg',int(df70.Weight.mean()),'kg',int(df80.Weight.mean()),'kg',int(df90.Weight.mean()),'kg',int(df100.Weight.mean()),'kg',int(df110.Weight.mean()),'kg',int(df120.Weight.mean()),'kg')









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

def plot_bsg_distribution(b,s,g,athletes):
    """
    Inputs, all numpy arrays
    b,s,g: bronze, silver, gold array of form [[athlete name, medal count],...]
    atheletes: list of atheletes [Phelps, Takashi Ono, ...]
    """
    
    y_bronze = np.array([i[0] for i in b])
    y_silver = np.array([i[0] for i in s])
    y_gold = np.array([i[0] for i in g])
    c_bronze = np.array([i[1] for i in b])
    c_silver = np.array([i[1] for i in s])
    c_gold = np.array([i[1] for i in g])
    
    count_bronze = np.zeros(athletes.size)
    count_silver = np.zeros(athletes.size)
    count_gold = np.zeros(athletes.size)

    i = 0
    for a in athletes:
        index_bronze, = np.where(y_bronze == a)
        if index_bronze.size == 1:
            count_bronze[i] = c_bronze[index_bronze]
        index_silver, = np.where(y_silver == a)
        if index_silver.size == 1:
            count_silver[i] = c_silver[index_silver]
        index_gold, = np.where(y_gold == a)
        if index_gold.size == 1:
            count_gold[i] = c_gold[index_gold]
        i += 1

    N = athletes.size
    ind = np.arange(N)    # the x locations for the groups
    width = 0.75       # the width of the bars: can also be len(x) sequence

    plt.subplots(figsize=(20, 10))
    p1 = plt.bar(ind, count_bronze, width, color = 'brown', alpha = 0.5)
    p2 = plt.bar(ind, count_silver, width, color = 'gray', alpha = 0.7,
                 bottom=count_bronze)
    p3 = plt.bar(ind, count_gold, width, color = 'gold', alpha = 0.5, 
                 bottom=count_silver+count_bronze)

    plt.ylabel('Count')
    plt.title('Medal count')
    plt.xticks(ind, athletes,rotation=10)
    plt.legend((p1[0], p2[0], p3[0]), ('Bronze', 'Silver', 'Gold'))
    plt.show()
    
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

#Solve Query 11
medals = ["Gold","Silver","Bronze"]
medals_total = np.array(df.filter(df.Medal.isin(medals)).groupBy("Name").count().orderBy("count",ascending = False).limit(10).collect())
top_athletes = [i for i in medals_total[:,0]]
medals = np.array(df.filter(df.Name.isin(top_athletes)).filter(df.Medal.isin(medals)).groupBy("Name","Medal").count().orderBy("count",ascending = False).collect())

gold_medals = np.array([i[::2] for i in medals if i[1] == "Gold"])
silver_medals = np.array([i[::2] for i in medals if i[1] == "Silver"])
bronze_medals = np.array([i[::2] for i in medals if i[1] == "Bronze"])

plot_bsg_distribution(bronze_medals,silver_medals,gold_medals,np.array(top_athletes))

#Solve Query 12
medals = ["Gold"]
gold_medals_total = np.array(df.filter(df.Medal.isin(medals)).groupBy("Name").count().orderBy("count",ascending = False).limit(10).collect())
print("Top 10 gold medal winners")
for i in range(gold_medals_total.shape[0]):
    print(str(i+1)+'.',gold_medals_total[i,0],":",gold_medals_total[i,1])
    
#Solve Query 13
top_years_participated = np.array(df.select("Name","Year").distinct().groupBy("Name").count().orderBy("count",ascending = False).limit(10).collect())
athletes = [i[0] for i in top_years_participated]
years_participated = df.select("Name","Year").filter(df.Name.isin(athletes)).distinct().groupBy('Name').agg(F.collect_list("Year")).collect()
athletes = [i[0] for i in years_participated]
years = [sorted(i[1]) for i in years_participated]

print("Top 10 athletes on years participated")
for i in range(top_years_participated.shape[0]):
    print(str(i+1)+".",top_years_participated[i,0]+',',top_years_participated[i,1]+',',years[athletes.index(top_years_participated[i,0])])
    
#Solve Query 14
dropAge = ['NA']
dropSport = ["Art Competitions"]
youngest_athletes = np.array(df.filter(~df.Age.isin(dropAge)).groupBy("Name","Age","Year","Sport").count().orderBy("Age", ascending = True).select("Name","Age","Year","Sport").limit(10).collect())
oldest_athletes = np.array(df.filter(~df.Age.isin(dropAge)).filter(~df.Sport.isin(dropSport)).groupBy("Name","Age","Year","Sport").count().orderBy("Age", ascending = False).select("Name","Age","Year","Sport").limit(10).collect())

print("Top 10 youngest athletes, year of participation and sport")
for i in range(youngest_athletes.shape[0]):
    print(str(i+1)+".",youngest_athletes[i,0]+", ",youngest_athletes[i,1]+", "+youngest_athletes[i,2] +", "+youngest_athletes[i,3])
    
print("Top 10 oldest athletes, year of participation and sport")
for i in range(oldest_athletes.shape[0]):
    print(str(i+1)+".",oldest_athletes[i,0]+", ",oldest_athletes[i,1]+", "+oldest_athletes[i,2]+", "+oldest_athletes[i,3])
