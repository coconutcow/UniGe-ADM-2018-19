# -*- coding: utf-8 -*-
"""
Created on Tue Jan 22 17:42:14 2019

@author: humbe
"""

import pandas as pd

df = pd.read_csv("athlete_events.csv")

print("Before:\n",df.isnull().sum()/len(df)*100)

col = ["Sex","NOC","Year","Sport"]

for i in range(len(col)):
    df["Height"].fillna(round(df.groupby(col[:len(col)-i])["Height"].transform("mean")), inplace=True)
df["Height"].fillna(round(df["Height"].mean()), inplace=True)

for i in range(len(col)):
    df["Weight"].fillna(round(df.groupby(col[:len(col)-i])["Weight"].transform("mean")), inplace=True)
df["Weight"].fillna(round(df["Weight"].mean()), inplace=True)

print("After:\n",df.isnull().sum()/len(df)*100)
#df.to_csv("example.csv")