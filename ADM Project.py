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
df['Country'].fillna('Singapore',inplace=True)
df['Medal'].fillna('Participated',inplace=True)

#Solve Query1
df1=df[['Country','Games','Medal']].copy()
df1=df1[df1.Medal!='Participated']
print(df1.groupby(['Country','Games','Medal']).size())