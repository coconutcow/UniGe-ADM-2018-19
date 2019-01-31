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

#Solve Query5
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

#Solve Query3
df1=df[['Country','Year','Medal']].copy()
df1=df1[df1.Medal !='Participated']
df1=df1.groupby(['Country']).size()
df1=df1.reset_index()
df1.columns=['Country','Distribution']
print('Highest Medals won so far:',df1.loc[df1['Distribution'].idxmax()])
print('Lowest Medals won so far:',df1.loc[df1['Distribution'].idxmin()])

#Solve Query4
df1=df[['Year','Medal']].copy()
df1=df1[df1.Medal !='Participated']
df1=df1.groupby(['Year']).size()
df1=df1.reset_index()
df1.columns=['Year','MedalsWon']
print(df1.sort_values(by='MedalsWon',ascending=False))

