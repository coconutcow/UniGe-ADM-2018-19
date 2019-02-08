import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def plot_mf_partifipation(m,f,ye,title = "",xl = "" , yl ="Count"):
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
    width = 0.8       # the width of the bars: can also be len(x) sequence

    plt.subplots(figsize=(20, 10))
    p1 = plt.bar(ind, menMeans, width, alpha = 0.6)
    p2 = plt.bar(ind, womenMeans, width, color = "pink", alpha = 0.9,
                 bottom=menMeans)

    plt.ylabel(yl)
    plt.xlabel(xl)
    plt.title(title)
    plt.xticks(ind, ye,rotation=10)
    plt.legend((p1[0], p2[0]), ('Men', 'Women'))
    for i in ind:
        plt.text(i-width/2,menMeans[i]+womenMeans[i]+20,str(round(100*womenMeans[i]/(menMeans[i]+womenMeans[i]),2))+'%',fontsize=10)
    plt.show()
    return None

def plot_feature(data,yl = 'Features',xl = 'Feature measurements', t = 'Feature comparison',bar = True):
    """
    Inputs
    data: numpy array of form [[feature1,float_value1],[feature2,float_value2],....]
    yl: y axis label
    xl: x axis label
    t:  title
    """
    d_feature = data[:,0]
    d_value = data[:,1]
    
    N = len(d_feature)
    ind = np.arange(N)
    w = 0.75
    values = np.array([float(i) for i in d_value])
    
    plt.figure(figsize =(20,10))
    if bar:
        plt.bar(ind,values,align='center',width = w)
        for i in ind:
            plt.text(i-w/3,values[i],round(values[i],2),fontsize=16)
    else:
        plt.plot(ind,values)
    plt.xticks(ind,d_feature,rotation = 15)
    plt.ylabel(yl)
    plt.xlabel(xl)
    plt.title(t)
    #plt.show
    
    return None

def plot_bsg_distribution(b,s,g,athletes,t = ''):
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

    plt.ylabel('Medals')
    plt.title(t)
    plt.xticks(ind, athletes,rotation=10)
    plt.legend((p3[0], p2[0], p1[0]), ('Gold', 'Silver', 'Bronze'))
    plt.show()
    return None

def query1(df):
    print(query_text[1])
    q1 = df.select("Country","Games","Medal").filter(~df.Medal.isin(["Participated"]))
    countries = input("To filter by country enter comma separated countries, just enter otherwise:")
    years = input("To filter by year enter comma separated years, just press enter otherwise:")
    if countries != "":
        q1 = q1.filter(df.Country.isin(countries.split(",")))
    if years != "":
        q1 = q1.filter(df.Year.isin(years.split(",")))
    q1.groupBy("Country","Games","Medal").count().orderBy(["Country","Games","Medal"]).show(40)
    return None

def query2(df):
    print(query_text[2])
    q2 = df.select("Country","Season","Medal").filter(~df.Medal.isin(["Participated"])).groupBy("Country","Season").count().orderBy("count", ascending = False)
    print("Highest medal counts so far in Summer Games:")
    q2.filter(df.Season.isin(["Summer"])).limit(5).withColumnRenamed("count","Medal Count").show()
    print("Highest medal counts so far in Winter Games:")
    q2.filter(df.Season.isin(["Winter"])).limit(5).withColumnRenamed("count","Medal Count").show()
    return None

def query3(df):
    print(query_text[3])
    q3 = df.select("Year","Medal","Season").filter(~df.Medal.isin(["Participated"])).groupBy("Year","Season").count().withColumnRenamed("count","Medals won").orderBy("Year")
    print("Summer:")
    s = q3.filter(q3.Season == "Summer").select("Year","Medals won")
    s.show(s.count())
    print("Winter:")
    w = q3.filter(q3.Season == "Winter").select("Year","Medals won")
    w.show(w.count())
    return None

def query4(df):
    print(query_text[4])
    q4 = df.select("Country","Year","Medal").filter(~df.Medal.isin(["Participated"])).groupBy("Country").count()
    print("Highest medals won so far:")
    q4.orderBy("count", ascending = False).limit(5).show()
    print("Lowest medals won so far:")
    q4.orderBy("count", ascending = True).limit(5).show()
    return None

def query5(df):
    print(query_text[5])
    q5 = df.groupBy("Sex","Year","Season").count().sort("Year","Sex")
    years = df.select("Year","Season").distinct().sort("Year")
    
    males_s = np.array(q5.filter(q5.Sex.isin(["M"])).filter(q5.Season.isin("Summer")).select("Year","count").collect())
    females_s = np.array(q5.filter(q5.Sex.isin(["F"])).filter(q5.Season.isin("Summer")).select("Year","count").collect())
    years_s = np.array(years.filter(q5.Season.isin("Summer")).select("Year").collect())
    years_s = np.array([i[0] for i in years_s])
    plot_mf_partifipation(males_s,females_s,years_s,"Women to men participation ratio over the years (Summer)", xl = "Years")
    
    males_w = np.array(q5.filter(q5.Sex.isin(["M"])).filter(q5.Season.isin("Winter")).select("Year","count").collect())
    females_w = np.array(q5.filter(q5.Sex.isin(["F"])).filter(q5.Season.isin("Winter")).select("Year","count").collect())
    years_w = np.array(years.filter(q5.Season.isin("Winter")).select("Year").collect())
    years_w = np.array([i[0] for i in years_w])
    plot_mf_partifipation(males_w,females_w,years_w,"Women to men participation ratio over the years (Winter)",xl = "Years")
    return None

def query6(df):
    print(query_text[6])
    countries = np.array(df.groupBy("Country").count().orderBy("count",ascending = False).limit(20).select("Country").collect())
    countries = [i[0] for i in countries]
    q6 = df.groupBy("Sex","Country").count()
    
    males_c = np.array(q6.filter(q6.Sex.isin(["M"])).select("Country","count").filter(q6.Country.isin(countries)).collect())
    females_c = np.array(q6.filter(q6.Sex.isin(["F"])).select("Country","count").filter(q6.Country.isin(countries)).collect())
    plot_mf_partifipation(males_c,females_c,np.array(countries),"Women to men participation ratio of the top 20 participating countries", xl = "Countries")
    return None

def query7(df):
    print(query_text[7])
    q7 = df.select("ID","Country","Year","Sport")
    l1 = q7.count()
    q7 = q7.dropDuplicates()
    l2 = q7.count()
    print('Difference after removing duplicates:',' ',l1,' ',l2)
    q7 = q7.groupBy("Country").count()
    print("Highest representation so far:")
    q7.orderBy("count", ascending = False).limit(5).withColumnRenamed("count","Representation").show()
    print("Lowest representation so far:")
    q7.orderBy("count", ascending = True).limit(5).withColumnRenamed("count","Representation").show()
    return None
    
def query8(df):
    print(query_text[8])
    q8 = df.select("ID","Country","Season","Sport","Year").orderBy("ID")
    l1 = q8.count()
    q8 = q8.dropDuplicates()
    l2 = q8.count()
    print('Difference after removing duplicates:',' ',l1,' ',l2)
    q8 = q8.groupBy("Country","Season").count().withColumnRenamed("count","Representation")
    dfs = np.array(q8.filter(q8.Season == "Summer").orderBy("Representation", ascending = False).limit(10).select("Country","Representation").collect())
    dfw = np.array(q8.filter(q8.Season == "Winter").orderBy("Representation", ascending = False).limit(10).select("Country","Representation").collect())
    plot_feature(dfs,yl = 'Representation',xl = 'Country', t = 'Bar Graph Summer: Representation of top 10 countries')
    plt.show()
    plot_feature(dfw,yl = 'Representation',xl = 'Country', t = 'Bar Graph Winter: Representation of top 10 countries')
    plt.show()
    return None

def query9(df):
    print(query_text[9])
    q9 = df.select("ID","Country","Year","Sport")
    l1 = q9.count()
    q9 = q9.dropDuplicates()
    l2 = q9.count()
    print('Difference after removing duplicates:',' ',l1,' ',l2)
    q9 = np.array(q9.groupBy("Country").count().withColumnRenamed("count","Representation").orderBy("Representation", ascending = False).limit(10).collect())
    plot_feature(q9,yl = 'Representation',xl = 'Country', t = 'Bar Graph: Representation of top 10 countries')
    plt.show()
    return None

def query10(df):
    print(query_text[10])
    q10 = df.select("ID","Year","Season")
    l1 = q10.count()
    q10 = q10.dropDuplicates()
    l2 = q10.count()
    print('Difference after removing duplicates:',' ',l1,' ',l2)
    q10s = q10.filter(q10.Season == "Summer").groupBy("Year").count().withColumnRenamed("count","Representation")
    q10hs = np.array(q10s.orderBy("Representation", ascending = False).limit(10).orderBy("Year", ascending = True).collect())
    q10as = np.array(q10s.orderBy("Year", ascending = True).collect())
    plot_feature(q10hs,yl = 'Representation',xl = 'Year', t = 'Bar Graph: Highest participation over the years (Summer)')
    plt.show()
    plot_feature(q10as,yl = 'Representation',xl = 'Year', t = 'Line Graph: Participation over the years (Summer)',bar = False)
    plt.show()
    q10w = q10.filter(q10.Season == "Winter").groupBy("Year").count().withColumnRenamed("count","Representation")
    q10hw = np.array(q10w.orderBy("Representation", ascending = False).limit(10).orderBy("Year", ascending = True).collect())
    q10aw = np.array(q10w.orderBy("Year", ascending = True).collect())
    plot_feature(q10hw,yl = 'Representation',xl = 'Year', t = 'Bar Graph: Highest participation over the years (Winter)')
    plt.show()
    plot_feature(q10aw,yl = 'Representation',xl = 'Year', t = 'Line Graph: Participation over the years (Winter)',bar = False)
    plt.show()
    return None

def query11(df):
    print(query_text[11])
    q11 = df.groupBy("Country")
    w = q11.agg(F.mean("Weight"))
    h = q11.agg(F.mean("Height"))
    print("Highest average weights and heights:")
    wh = w.orderBy("avg(Weight)",ascending = False).limit(10)
    hh = h.orderBy("avg(Height)",ascending = False).limit(10)
    wh.select("Country",F.round(wh["avg(Weight)"],2).alias("Weight (kg)")).show()
    hh.select("Country",F.round(hh["avg(Height)"],2).alias("Height (cm)")).show()
    print("Lowest average weights and heights:")
    wl = w.orderBy("avg(Weight)",ascending = True).limit(10)
    hl = h.orderBy("avg(Height)",ascending = True).limit(10)
    wl.select("Country",F.round(wl["avg(Weight)"],2).alias("Weight (kg)")).show()
    hl.select("Country",F.round(hl["avg(Height)"],2).alias("Height (cm)")).show()
    return None

def query12(df):
    print(query_text[12])
    medals = ["Gold","Silver","Bronze"]
    q12 = df.filter(df.Medal.isin(medals))
    athletes = [i[0] for i in q12.groupBy("Name").count().orderBy("count",ascending = False).limit(10).select("Name").collect()]
    q12 = q12.filter(df.Name.isin(athletes)).groupBy("Name","Medal").count()
    q12g = np.array(q12.filter(df.Medal == medals[0]).select("Name","count").collect())
    q12s = np.array(q12.filter(df.Medal == medals[1]).select("Name","count").collect())
    q12b = np.array(q12.filter(df.Medal == medals[2]).select("Name","count").collect())
    plot_bsg_distribution(q12b,q12s,q12g,np.array(athletes),t = "Athletes with the highest medal count")
    return None

def query13(df):
    print(query_text[13])
    df.filter(df.Medal == "Gold").groupBy("Name").count().orderBy("count",ascending = False).limit(10).withColumnRenamed("count","Gold medals").show(truncate = False)
    return None

def query14(df):
    print(query_text[14])
    q14 = df.select("Name","Year")
    athletes = [i[0] for i in q14.dropDuplicates().groupBy("Name").count().orderBy("count",ascending = False).limit(10).collect()]
    q14 = q14.filter(df.Name.isin(athletes)).distinct().orderBy("Year").groupBy('Name').agg(F.collect_list("Year"))
    q14.orderBy(F.size("collect_list(Year)"), ascending = False).select("Name",F.size("collect_list(Year)").alias("Total years"),F.col("collect_list(Year)").alias("Years")).show(truncate = False)
    return None

def query15(df):
    print(query_text[15])
    dropSport = ["Art Competitions"]
    print("Top 10 youngest athletes")
    df.groupBy("Name","Age","Year","Sport").count().orderBy("Age", ascending = True).select("Name","Age","Year","Sport").limit(10).show(truncate = False)
    print("Top 10 oldest athletes")
    df.filter(~df.Sport.isin(dropSport)).groupBy("Name","Age","Year","Sport").count().orderBy("Age", ascending = False).select("Name","Age","Year","Sport").limit(10).show(truncate = False)
    return None

def query16(df):
    print(query_text[16])
    q14 = df.select("ID","Name","Sport")
    ids = [i[0] for i in q14.dropDuplicates().groupBy("ID").count().orderBy("count",ascending = False).limit(10).collect()]
    q14 = q14.filter(df.ID.isin(ids)).select("Name","Sport").distinct().orderBy("Sport").groupBy('Name').agg(F.collect_list("Sport"))
    q14 = q14.orderBy(F.size("collect_list(Sport)"), ascending = False).select("Name",F.size("collect_list(Sport)").alias("Total sports"),F.col("collect_list(Sport)").alias("Sports"))
    q14.show(truncate = False)
    plot_feature(np.array(q14.select("Name","Total sports").collect()),yl = 'Sports',xl = 'Athlete', t = 'Bar Graph: Athletes thats participated in the largest variety of sports')
    plt.show()
    return None

def query17(df):
    print(query_text[17])
    meanh = df.select(F.avg("Height")).collect()[0][0]
    q17 = df.select("Name","Sport","Height","Medal").filter(df.Sport == "Swimming").filter(~(df.Medal == "Participated")).dropDuplicates(["Name"])
    percentage = q17.count()
    q17 = q17.filter(df.Height > meanh).select("Name").orderBy("Name",ascending = True)
    percentage = round(q17.count()/percentage * 100,2)
    print(str(percentage)+"% of medal winners in swimming are higher than average")
    q17.show(truncate = False)
    return None

def query18(df):
    print(query_text[18])
    meanh = df.select(F.avg("Weight")).collect()[0][0]
    q17 = df.select("Name","Sport","Weight","Medal").filter(df.Sport == "Weightlifting").filter(~(df.Medal == "Participated")).dropDuplicates(["Name"])
    percentage = q17.count()
    q17 = q17.filter(df.Weight > meanh).select("Name").orderBy("Name",ascending = True)
    percentage = round(q17.count()/percentage * 100,2)
    print(str(percentage)+"% of medal winners in weightlifting are heavier than average")
    q17.show(truncate = False)
    return None

def query19(df):
    print(query_text[19])
    q19 = df.select("City","Year").dropDuplicates()
    cities = [i[0] for i in q19.groupBy("City").count().orderBy("count",ascending = False).limit(5).collect()]
    q19 = q19.filter(df.City.isin(cities)).orderBy("Year").groupBy('City').agg(F.collect_list("Year"))
    q19 = q19.orderBy(F.size("collect_list(Year)"), ascending = False).select("City",F.size("collect_list(Year)").alias("Times hosted"),F.col("collect_list(Year)").alias("Years"))
    q19.show(truncate = False)
    return None

def query20(df):
    print(query_text[20])
    f = df.select(F.min("Year")).collect()[0][0]
    l = df.select(F.max("Year")).collect()[0][0]
    print("First year of event:",f)
    print("Last year of event:",l)
    print('Difference:',l-f)
    q20 = df.select("ID","Year","Height","Weight").dropDuplicates(["ID"])
    years = np.linspace(1896,2016,13)
    heights = np.zeros(len(years)-1)
    weights = np.zeros(len(years)-1)
    
    for i in range(years.size-2):
        qy = q20.filter(q20.Year >= years[i]).filter(q20.Year < years[i+1])
        heights[i] = round(qy.select(F.avg("Height")).collect()[0][0],2)
        weights[i] = round(qy.select(F.avg("Weight")).collect()[0][0],2)
    qy = q20.filter(q20.Year >= years[11]).filter(q20.Year <= years[12])
    heights[11] = round(qy.select(F.avg("Height")).collect()[0][0],2)
    weights[11] = round(qy.select(F.avg("Weight")).collect()[0][0],2)
    
    h = ", ".join([str(i)+"cm" for i in heights])
    w = ", ".join([str(i)+"kg" for i in weights])
    print("Average heights over the years from 1896 to 2016 - interval of 10 years:",h)
    print("Average weights over the years from 1896 to 2016 - interval of 10 years:",w)
    return None

def query21(df):
    print(query_text[21])
    q21 = df.select("Country","Sport","Year","Medal").filter(df.Sport == "Art Competitions")
    q21 = q21.filter(~(df.Medal == "Participated"))
    
    print("Art competitions where held in the following years:\n"+", ".join([str(i[0]) for i in q21.select("Year").dropDuplicates().orderBy("Year").collect()]))
    print("\nTop 10 countries by medal count in art competitions:")
    q21.groupBy("Country").count().orderBy("count",ascending = False).limit(10).withColumnRenamed("count","Medals won").show()
    print("Medal distribution in 1936")
    
    q21 = q21.filter(df.Year == 1936)
    countries = [i[0] for i in q21.groupBy("Country").count().orderBy("count",ascending = False).limit(10).select("Country").collect()]
    q21 = q21.filter(q21.Country.isin(countries)).groupBy("Country","Medal").count()
    q21g = np.array(q21.filter(df.Medal == "Gold").select("Country","count").collect())
    q21s = np.array(q21.filter(df.Medal == "Silver").select("Country","count").collect())
    q21b = np.array(q21.filter(df.Medal == "Bronze").select("Country","count").collect())
    plot_bsg_distribution(q21b,q21s,q21g,np.array(countries),t = "Countries with the highest medal count in the 1936 Art Competitions")
    return None


def query22(df):
    print(query_text[22])
    q22 = df.filter(df.Season == "Summer").select("Year","Sport","Event")
    s = q22.select("Year","Sport").dropDuplicates().groupBy("Year").count().orderBy("Year").withColumnRenamed("count","Sports")
    e = q22.select("Year","Event").dropDuplicates().groupBy("Year").count().orderBy("Year").withColumnRenamed("count","Events").withColumnRenamed("Year","Y")
    s.join(e,s.Year == e.Y).select("Year","Sports","Events").show(s.count())
    plot_feature(np.array(s.collect()),yl = 'Sports',xl = 'Year', t = 'Line Graph: Sport variety over the years',bar = False)
    plt.show()
    plot_feature(np.array(e.collect()),yl = 'Events',xl = 'Year', t = 'Line Graph: Event variety over the years',bar = False)
    plt.show()
    return None

def query23(df):
    print(query_text[23])
    q23 = df.filter(df.Season == "Winter").select("Year","Sport","Event")
    s = q23.select("Year","Sport").dropDuplicates().groupBy("Year").count().orderBy("Year").withColumnRenamed("count","Sports")
    e = q23.select("Year","Event").dropDuplicates().groupBy("Year").count().orderBy("Year").withColumnRenamed("count","Events").withColumnRenamed("Year","Y")
    s.join(e,s.Year == e.Y).select("Year","Sports","Events").show(s.count())
    plot_feature(np.array(s.collect()),yl = 'Sports',xl = 'Year', t = 'Line Graph: Sport variety over the years',bar = False)
    plt.show()
    plot_feature(np.array(e.collect()),yl = 'Events',xl = 'Year', t = 'Line Graph: Event variety over the years',bar = False)
    plt.show()
    return None

"""
Next query workspace
"""

query_text = ["",
              "Query 1  - Enlist the number of medals won by each country by each session",
              "Query 2  - Highlight which countries dominate each of the two sessions",
              "Query 3  - Total medal distribution over the years",
              "Query 4  - Highest and lowest medal count so far by country",
              "Query 5  - Highlight the ratio of female to male participation over the years",
              "Query 6  - Highlight the all time ratio of female to male participation amongst top participating countries",
              "Query 7  - Representation of every country in each sport",
              "Query 8  - Bar Graph: Top 10 participating countries for each session",
              "Query 9  - Bar Graph: Top 10 participating countries all time",
              "Query 10 - Bar Graph: highest participation over the years, Line Graph: participation over the years",
              "Query 11 - Highest and lowest 10 (by country) average weights and heights",
              "Query 12 - Top 10 athletes by medal count, and medal breakdown",
              "Query 13 - Top 10 gold medal winners",
              "Query 14 - Top 10 athletes with most years participated",
              "Query 15 - Youngest and oldest athletes of all time",
              "Query 16 - Top 10 athletes with most number of sports participated",
              "Query 17 - Swimmers with higher than the average height who have won medals",
              'Query 18 - Weightlifters with higher than the average weight who have won medals',
              "Query 19 - Cities where the olympics have been hosted the most number of times",
              "Query 20 - Average height and weight of athletes over 10 year intervals",
              "Query 21 - Various details on olympic art competitions",
              "Query 22 - Summer olympics sport and event diversity through the years",
              "Query 23 - Winter olympics sport and event diversity through the years"]

query_dict = {'1':query1,  '2':query2,  '3':query3,  '4':query4,  '5':query5,
              '6':query6,  '7':query7,  '8':query8,  '9':query9,  '10':query10,
              '11':query11,'12':query12,'13':query13,'14':query14,'15':query15,
              '16':query16,'17':query17,'18':query18,'19':query19,'20':query20,
              '21':query21,'22':query22,'23':query23}

def main(df):

    while True:
        inp = input("Enter query #(1-23) to execute, 'h' to list the queries, or 'q' to quit:")
        if inp == 'q':
            break
        elif inp == 'h':
            for i in query_text:
                print(i)
        elif inp in query_dict:
            query_dict[inp](df)
    return None

#Start spark
spark = SparkSession.builder.appName("ADM").getOrCreate()

df = None 
#df = spark.read.csv("C:/Users/humbe/Desktop/ADM Final Project/DataSetFinal.csv",inferSchema = True, header=True, sep = ";")
#main(df)

while df is None:
    try:
        filepath = input("Enter filepath of dataset, quit with 'q':")
        if filepath == 'q':
            break
        df = spark.read.csv(filepath,inferSchema = True, header=True, sep = ";")
        main(df)
    except:
        print("Invalid filepath")
        pass

spark.stop()
