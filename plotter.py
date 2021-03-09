#!/usr/bin/env python3.8
import numpy as np 
import matplotlib.pyplot as plt 
 
# set width of bar 
barWidth = 0.25
fig = plt.subplots(figsize =(12, 8)) 

#Change here with outhput of coresponing run times
# set height of bar 
RDD = [7.69, 36.27, 29.67, 2.95, 2.27, 2.3] 
SQL_CSV = [7.69, 36.27, 29.67, 2.95, 2.27, 2.5] 
SQL_PARQUET = [7.69, 36.27, 29.67, 2.95, 2.27, 2.7] 
 
# Set position of bar on X axis 
br1 = np.arange(len(RDD)) 
br2 = [x + barWidth for x in br1] 
br3 = [x + barWidth for x in br2] 
 
# Make the plot
plt.bar(br1, RDD, color ='r', width = barWidth, 
        edgecolor ='grey', label ='RDD') 
plt.bar(br2, SQL_CSV, color ='g', width = barWidth, 
        edgecolor ='grey', label ='SQL_CSV') 
plt.bar(br3, SQL_PARQUET, color ='b', width = barWidth, 
        edgecolor ='grey', label ='SQL_Parquet') 
 
# Adding Xticks 
plt.xlabel('Queries', fontweight ='bold', fontsize = 15) 
plt.ylabel('Execution Time (Seconds)', fontweight ='bold', fontsize = 15) 
plt.xticks([r + barWidth for r in range(len(RDD))], 
        ['Q1', 'Q2', 'Q3', 'Q4', 'Q5','Q6'])
 
plt.legend()
plt.show() 
