from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np
import sys

if __name__ == "__main__":
    # Check the number of arguments
    if len(sys.argv) != 2:
        print("Usage: 'ADM_Project_Final-UniversityCluster <query #>' to execute a query or 'ADM_Project_Final-UniversityCluster h' to list available queries", file=sys.stderr)
        exit(-1)

query_text = ["",
              "Query 1  - Enlist the number of medals won by each country by each session",
              "Query 2  - Highlight which countries dominate each of the two sessions",
              "Query 3  - Total medal distribution over the years",
              "Query 4  - Highest and lowest medal count so far by country",
              "Query 5  - Highlight the ratio of female to male participation over the years",
              "Query 6  - Highlight the all time ratio of female to male participation amongst top participating countries",
              "Query 7  - Representation of every country in each sport",
              "Query 8  - Top 10 participating countries for each session",
              "Query 9  - Top 10 participating countries all time",
              "Query 10 - Highest participation over the years, and participation over the years",
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

if sys.argv[1] == 'h':
	for i in query_text:
		print(i)
	exit(-1)

medals = ["Gold","Silver","Bronze"]			  

def query1(df):
	print(query_text[1])
	q1 = df.filter(df.Medal.isin(medals)).select("Country","Games","Medal")
	q1 = q1.groupBy("Country","Games","Medal").count().orderBy(["Country","Games","Medal"])
	q1.show(40)
	return q1

def query2(df):
	print(query_text[2])
	q2 = df.filter(df.Medal.isin(medals)).select("Country","Season","Medal").groupBy("Country","Season").count().orderBy("count", ascending = False)
	print("Highest medal counts so far in Summer Games:")
	q2s = q2.filter(df.Season.isin(["Summer"])).limit(5).withColumnRenamed("count","Medal Count")
	q2s.show()
	print("Highest medal counts so far in Winter Games")
	q2w = q2.filter(df.Season.isin(["Winter"])).limit(5).withColumnRenamed("count","Medal Count")
	q2w.show()
	return q2s,q2w
	
def query3(df):
	print(query_text[3])
	q3 = df.filter(df.Medal.isin(medals)).select("Year","Medal","Season").groupBy("Year","Season").count().withColumnRenamed("count","Medals won").orderBy("Year")
	print("Summer:")
	q3s = q3.filter(q3.Season == "Summer").select("Year","Medals won")
	q3s.show(q3s.count())
	print("Winter:")
	q3w = q3.filter(q3.Season == "Winter").select("Year","Medals won")
	q3w.show(q3w.count())
	return q3s,q3w

def query4(df):
	print(query_text[4])
	q4 = df.filter(df.Medal.isin(medals)).select("Country","Year","Medal").groupBy("Country").count()
	print("Highest medals won so far:")
	q4h = q4.orderBy("count", ascending = False).limit(5)
	print("Lowest medals won so far:")
	q4l = q4.orderBy("count", ascending = True).limit(5)
	return q4h,q4l

def query5(df):
	print(query_text[5])
	q5years = df.select("Year","Season").distinct().sort("Year")
	q5ys = [i[0] for i in q5years.filter(q5years.Season.isin("Summer")).select("Year").collect()]
	q5yw = [i[0] for i in q5years.filter(q5years.Season.isin("Winter")).select("Year").collect()]
	q5 = df.groupBy("Sex","Year","Season").count().sort("Year")
	q5m = q5.filter(q5.Sex == "M")
	q5f = q5.filter(q5.Sex == "F")
	print("Summer olympics female to male ratio through the years:")
	q5ms = q5m.filter(q5m.Season == "Summer").select("Year","count").withColumnRenamed("count","Total males")
	q5fs = q5f.filter(q5f.Season == "Summer").select("Year","count").withColumnRenamed("Year","Y").withColumnRenamed("count","Total females")
	q5s = q5ms.join(q5fs,q5ms.Year == q5fs.Y).select("Year","Total males","Total females",F.col("Total females")/F.col("Total males").alias("Female to male ratio"))
	q5s.show()
	print("Winter olympics female to male ratio through the years:")
	q5mw = q5m.filter(q5m.Season == "Winter").select("Year","count").withColumnRenamed("count","Total males")
	q5fw = q5f.filter(q5f.Season == "Winter").select("Year","count").withColumnRenamed("Year","Y").withColumnRenamed("count","Total females")
	q5w = q5mw.join(q5fw,q5mw.Year == q5fw.Y).select("Year","Total males","Total females",F.col("Total females")/F.col("Total males").alias("Female to male ratio"))
	q5w.show()
	return q5s,q5w

def query6(df):
	print(query_text[6])
	q6countries = [i[0] for i in df.groupBy("Country").count().orderBy("count",ascending = False).limit(20).select("Country").collect()]
	q6 = df.filter(df.Country.isin(q6countries)).groupBy("Sex","Country").count()
	q6m = q6.filter(q6.Sex.isin(["M"])).select("Country","count").withColumnRenamed("count","Total males")
	q6f = q6.filter(q6.Sex.isin(["F"])).select("Country","count").withColumnRenamed("Country","C").withColumnRenamed("count","Total females")
	q6 = q6m.join(q6f,q6m.Country == q6f.C).select("Country","Total males","Total females",F.col("Total females")/F.col("Total males").alias("Female to male ratio"))
	q6.show()
	return q6

def query7(df):
	print(query_text[7])
	q7 = df.select("ID","Country","Year","Sport").orderBy("ID")
	l1 = q7.count()
	q7 = q7.dropDuplicates()
	l2 = q7.count()
	print('Difference after removing duplicates:',' ',l1,' ',l2)
	q7 = q7.groupBy("Country").count()
	print("Highest representation so far:")
	q7h = q7.orderBy("count", ascending = False).limit(5).withColumnRenamed("count","Representation")
	q7h.show()
	print("Lowest representation so far:")
	q7l = q7.orderBy("count", ascending = True).limit(5).withColumnRenamed("count","Representation")
	q7l.show()
	return q7h,q7l

def query8(df):
	print(query_text[8])
	q8 = df.select("ID","Country","Season","Sport","Year").orderBy("ID")
	q8l1 = q8.count()
	q8 = q8.dropDuplicates()
	q8l2 = q8.count()
	print('Difference after removing duplicates:',' ',q8l1,' ',q8l2)
	q8 = q8.groupBy("Country","Season").count().withColumnRenamed("count","Representation")
	q8s = q8.filter(q8.Season == "Summer").orderBy("Representation", ascending = False).limit(10).select("Country","Representation")
	q8w = q8.filter(q8.Season == "Winter").orderBy("Representation", ascending = False).limit(10).select("Country","Representation")
	q8s.show()
	q8w.show()
	return q8s,q8w

def query9(df):	
	print(query_text[9])
	q9 = df.select("ID","Country","Year","Sport").orderBy("ID")
	l1 = q9.count()
	q9 = q9.dropDuplicates()
	l2 = q9.count()
	print('Difference after removing duplicates:',' ',l1,' ',l2)
	q9 = q9.groupBy("Country").count().withColumnRenamed("count","Representation").orderBy("Representation", ascending = False).limit(10)
	q9.show()
	return q9

def query10(df):
	print(query_text[10])
	q10 = df.select("ID","Year","Season")
	q10l1 = q10.count()
	q10 = q10.dropDuplicates()
	q10l2 = q10.count()
	print('Difference after removing duplicates:',' ',q10l1,' ',q10l2)
	q10s = q10.filter(q10.Season == "Summer").groupBy("Year").count().withColumnRenamed("count","Representation")
	print('Highest participation in summer olympics:')
	q10sh = q10s.orderBy("Representation", ascending = False).limit(10).orderBy("Year", ascending = True)
	q10sh.show()
	print('All time participation in summer olympics:')
	q10sa = q10s.orderBy("Year", ascending = True)
	q10sa.show()
	q10w = q10.filter(q10.Season == "Winter").groupBy("Year").count().withColumnRenamed("count","Representation")
	print('Highest participation in winter olympics:')
	q10wh = q10w.orderBy("Representation", ascending = False).limit(10).orderBy("Year", ascending = True)
	q10wh.show()
	print('All time participation in summer olympics:')
	q10wa = q10w.orderBy("Year", ascending = True)
	q10wa.show()
	return q10sh,q10sa,q10wh,q10wa

def query11(df):
	print(query_text[11])
	q11 = df.groupBy("Country")
	q11w = q11.agg(F.mean("Weight"))
	q11h = q11.agg(F.mean("Height"))
	print("Highest average weights and heights:")
	q11wh = q11w.orderBy("avg(Weight)",ascending = False).limit(10)
	q11hh = q11h.orderBy("avg(Height)",ascending = False).limit(10)
	q11wh = q11wh.select("Country",F.round(q11wh["avg(Weight)"],2).alias("Weight (kg)"))
	q11hh = q11hh.select("Country",F.round(q11hh["avg(Height)"],2).alias("Height (cm)"))
	q11wh.show()
	q11hh.show()
	print("Lowest average weights and heights:")
	q11wl = q11w.orderBy("avg(Weight)",ascending = True).limit(10)
	q11hl = q11h.orderBy("avg(Height)",ascending = True).limit(10)
	q11wl = q11wl.select("Country",F.round(q11wl["avg(Weight)"],2).alias("Weight (kg)"))
	q11hl = q11hl.select("Country",F.round(q11hl["avg(Height)"],2).alias("Height (cm)"))
	q11wl.show()
	q11hl.show()
	return q11wh,q11hh,q11wl,q11hl

def query12(df):
	q12 = df.filter(df.Medal.isin(medals))
	print(query_text[12])
	q12athletes = [i[0] for i in q12.groupBy("Name").count().orderBy("count",ascending = False).limit(10).select("Name").collect()]
	q12 = q12.filter(df.Name.isin(q12athletes)).groupBy("Name","Medal").count()
	print("Gold medal distribution:")
	q12g = q12.filter(df.Medal == medals[0]).select("Name","count").withColumnRenamed("count","Gold medals").orderBy("Gold medals",ascending = False)
	q12g.show()
	print("Silver medal distribution:")
	q12s = q12.filter(df.Medal == medals[1]).select("Name","count").withColumnRenamed("count","Silver medals").orderBy("Silver medals",ascending = False)
	q12s.show()
	print("Bronze medal distribution:")
	q12b = q12.filter(df.Medal == medals[2]).select("Name","count").withColumnRenamed("count","Bronze medals").orderBy("Bronze medals",ascending = False)
	q12b.show()
	return q12g,q12s,q12b

def query13(df):
	print(query_text[13])
	q13 = df.filter(df.Medal.isin(medals)).filter(df.Medal == "Gold").groupBy("Name").count().orderBy("count",ascending = False).limit(10).withColumnRenamed("count","Gold medals")
	q13.show(truncate = False)
	return q13

def query14(df):
	print(query_text[14])
	q14 = df.select("ID","Name","Year")
	q14ids = [i[0] for i in q14.dropDuplicates().groupBy("ID").count().orderBy("count",ascending = False).limit(10).collect()]
	q14 = q14.filter(df.ID.isin(q14ids)).select("Name","Year").distinct().orderBy("Year").groupBy("Name").agg(F.collect_list("Year"))
	q14 = q14.orderBy(F.size("collect_list(Year)"), ascending = False).select("Name",F.size("collect_list(Year)").alias("Total years"),F.col("collect_list(Year)").alias("Years"))
	q14.show(truncate = False)
	return q14

def query15(df):
	print(query_text[15])
	q14dropSport = ["Art Competitions"]
	print("Top 10 youngest athletes")
	q15y = df.groupBy("Name","Age","Year","Sport").count().orderBy("Age", ascending = True).select("Name","Age","Year","Sport").limit(10)
	q15y.show(truncate = False)
	print("Top 10 oldest athletes")
	q15o = df.filter(~df.Sport.isin(q14dropSport)).groupBy("Name","Age","Year","Sport").count().orderBy("Age", ascending = False).select("Name","Age","Year","Sport").limit(10)
	q15o.show(truncate = False)
	return q15y,q15o

def query16(df):
	print(query_text[16])
	q16 = df.select("ID","Name","Sport")
	q16ids = [i[0] for i in q16.dropDuplicates().groupBy("ID").count().orderBy("count",ascending = False).limit(10).collect()]
	q16 = q16.filter(df.ID.isin(q16ids)).select("Name","Sport").distinct().orderBy("Sport").groupBy('Name').agg(F.collect_list("Sport"))
	q16 = q16.orderBy(F.size("collect_list(Sport)"), ascending = False).select("Name",F.size("collect_list(Sport)").alias("Total sports"),F.col("collect_list(Sport)").alias("Sports"))
	q16.show(truncate = False)
	return q16

def query17(df):
	print(query_text[17])
	q17meanh = df.select(F.avg("Height")).collect()[0][0]
	q17 = df.select("Name","Sport","Height","Medal").filter(df.Sport == "Swimming").filter(~(df.Medal == "Participated")).dropDuplicates(["Name"])
	percentage = q17.count()
	q17 = q17.filter(df.Height > q17meanh).select("Name").orderBy("Name",ascending = True)
	percentage = round(q17.count()/percentage * 100,2)
	print(str(percentage)+"% of medal winners in swimming are taller than average")
	q17.show(truncate = False)
	return q17

def query18(df):
	print(query_text[18])
	q18meanw = df.select(F.avg("Weight")).collect()[0][0]
	q18 = df.select("Name","Sport","Weight","Medal").filter(df.Sport == "Weightlifting").filter(~(df.Medal == "Participated")).dropDuplicates(["Name"])
	percentage = q18.count()
	q18 = q18.filter(df.Weight > q18meanw).select("Name").orderBy("Name",ascending = True)
	percentage = round(q18.count()/percentage * 100,2)
	print(str(percentage)+"% of medal winners in weightlifting are heavier than average")
	q18.show(truncate = False)
	return q18

def query19(df):    
	print(query_text[19])
	q19 = df.select("City","Year").dropDuplicates()
	q19cities = [i[0] for i in q19.groupBy("City").count().orderBy("count",ascending = False).limit(5).collect()]
	q19 = q19.filter(df.City.isin(q19cities)).orderBy("Year").groupBy('City').agg(F.collect_list("Year"))
	q19 = q19.orderBy(F.size("collect_list(Year)"), ascending = False).select("City",F.size("collect_list(Year)").alias("Times hosted"),F.col("collect_list(Year)").alias("Years"))
	q19.show(truncate = False)
	return q19

def query20(df):
	print(query_text[20])
	q20f = df.select(F.min("Year")).collect()[0][0]
	q20l = df.select(F.max("Year")).collect()[0][0]
	print("First year of event:",q20f)
	print("Last year of event:",q20l)
	print('Difference:',q20l-q20f)
	q20 = df.select("ID","Year","Height","Weight").dropDuplicates(["ID"])
	q20years = np.linspace(1896,2016,13)
	q20heights = np.zeros(len(q20years)-1)
	q20weights = np.zeros(len(q20years)-1)
	for i in range(q20years.size-2):
		q20y = q20.filter(q20.Year >= q20years[i]).filter(q20.Year < q20years[i+1])
		q20heights[i] = round(q20y.select(F.avg("Height")).collect()[0][0],2)
		q20weights[i] = round(q20y.select(F.avg("Weight")).collect()[0][0],2)
	q20y = q20.filter(q20.Year >= q20years[11]).filter(q20.Year <= q20years[12])
	q20heights[11] = round(q20y.select(F.avg("Height")).collect()[0][0],2)
	q20weights[11] = round(q20y.select(F.avg("Weight")).collect()[0][0],2)
	q20h = ", ".join([str(i)+"cm" for i in q20heights])
	q20w = ", ".join([str(i)+"kg" for i in q20weights])
	q20ah = "Average heights over the years from 1896 to 2016 - interval of 10 years: "+q20h
	q20aw = "Average weights over the years from 1896 to 2016 - interval of 10 years: "+q20h
	print(q20ah)
	print(q20aw)
	return q20ah,q20aw

def query21(df):
	print(query_text[21])
	q21 = df.filter(df.Medal.isin(medals)).select("Country","Sport","Year","Medal").filter(df.Sport == "Art Competitions")
	q21y = ", ".join([str(i[0]) for i in q21.select("Year").dropDuplicates().orderBy("Year").collect()])
	print("Art competitions where held in the following years:\n"+q21y)
	
	print("\nTop 10 countries by medal count in art competitions:")
	q21t10 = q21.groupBy("Country").count().orderBy("count",ascending = False).limit(10).withColumnRenamed("count","Medals won")
	q21t10.show()
	
	print("Medal distribution in 1936")
	q21 = q21.filter(df.Year == 1936)
	countries = [i[0] for i in q21.groupBy("Country").count().orderBy("count",ascending = False).limit(10).select("Country").collect()]
	q21 = q21.filter(q21.Country.isin(countries)).groupBy("Country","Medal").count()
	print("Gold medal distribution:")
	q21g1936 = q21.filter(df.Medal == medals[0]).select("Country","count").withColumnRenamed("count","Gold medals").orderBy("Gold medals",ascending = False)
	q21g1936.show()
	print("Silver medal distribution:")
	q21s1936 = q21.filter(df.Medal == medals[1]).select("Country","count").withColumnRenamed("count","Silver medals").orderBy("Silver medals",ascending = False)
	q21g1936.show()
	print("Bronze medal distribution:")
	q21b1936 = q21.filter(df.Medal == medals[2]).select("Country","count").withColumnRenamed("count","Bronze medals").orderBy("Bronze medals",ascending = False)
	q21g1936.show()
	return q21y,q21t10,q21g1936,q21s1936,q21b1936
	
def query22(df):
	print(query_text[22])
	q22 = df.filter(df.Season == "Summer").select("Year","Sport","Event")
	q22s = q22.select("Year","Sport").dropDuplicates().groupBy("Year").count().orderBy("Year").withColumnRenamed("count","Sports")
	q22e = q22.select("Year","Event").dropDuplicates().groupBy("Year").count().orderBy("Year").withColumnRenamed("count","Events").withColumnRenamed("Year","Y")
	q22 = q22s.join(q22e,q22s.Year == q22e.Y).select("Year","Sports","Events")
	q22.show(q22.count())
	return q22

def query23(df):
	print(query_text[23])
	q23 = df.filter(df.Season == "Winter").select("Year","Sport","Event")
	q23s = q23.select("Year","Sport").dropDuplicates().groupBy("Year").count().orderBy("Year").withColumnRenamed("count","Sports")
	q23e = q23.select("Year","Event").dropDuplicates().groupBy("Year").count().orderBy("Year").withColumnRenamed("count","Events").withColumnRenamed("Year","Y")
	q23 = q23s.join(q23e,q23s.Year == q23e.Y).select("Year","Sports","Events")
	q23.show(q23.count())
	return q23
		
query_dict = {'1':query1,  '2':query2,  '3':query3,  '4':query4,  '5':query5,
              '6':query6,  '7':query7,  '8':query8,  '9':query9,  '10':query10,
              '11':query11,'12':query12,'13':query13,'14':query14,'15':query15,
              '16':query16,'17':query17,'18':query18,'19':query19,'20':query20,
              '21':query21,'22':query22,'23':query23}

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark Olympic Games Analysis") \
    .getOrCreate()
 
inputFile = "hdfs://master:9000/user/user9/olympics/input/DataSetFinal.csv"
 
# Create DataFrame from CSV file
df =  spark.read.format("csv").option("sep", ";").option("inferSchema", "true").option("header", "true").load(inputFile)

if sys.argv[1] in query_dict:
	query_dict[sys.argv[1]](df)
			  
# Stop the Spark Session
spark.stop()
