Shareable drive link with the dataset
https://drive.google.com/open?id=1rcVxQrDHRemVmXrIE9BP7nX3MMdaet8V

For ADM_Project_Final.py:
Code utilizes findspark and pyspark and requires environmental variables for spark and java homes.
Developed on our local machines to utilize python libraries such as matplotlib.

Run ADM_Project_Final.py as all the required code is contained in this python file.

Enter the filepath of the given dataset 'DataSetFinal.csv' when requested. 
If issues concerning filepath format arise in the python file try adding the filepath as shown 
in the spark.read.csv function in line 487 and then running line 488.

Upon succesful filepath entry and dataframe creation when requested enter a query #(1-23) to execute the query,
'h' to list the queries, or 'q' to quit.





For olympics.py:
Code runs in the university cluster utilizing pyspark.

The filepath is set in line 361, currently set as hdfs://master:9000/user/user9/olympics/input/DataSetFinal.csv. 
Can be changed if desired but be sure to use the provided 'DataSetFinal.csv' file. 

To to display the available queries run the following command
spark-submit --master yarn olympics.py h

To execute a query run the following command
spark-submit --master yarn olympics.py <query #>

Example command and output:
spark-submit --master yarn olympics.py 13

+---------------------------------------------------+-----------+
|Name                                               |Gold medals|
+---------------------------------------------------+-----------+
|Michael Fred Phelps, II                            |23         |
|"Raymond Clarence ""Ray"" Ewry"                    |10         |
|Mark Andrew Spitz                                  |9          |
|Larysa Semenivna Latynina (Diriy-)                 |9          |
|"Frederick Carlton ""Carl"" Lewis"                 |9          |
|Paavo Johannes Nurmi                               |9          |
|Birgit Fischer-Schmidt                             |8          |
|Usain St. Leo Bolt                                 |8          |
|"Matthew Nicholas ""Matt"" Biondi"                 |8          |
|"Jennifer Elisabeth ""Jenny"" Thompson (-Cumpelik)"|8          |
+---------------------------------------------------+-----------+
