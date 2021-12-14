'''
Average Time Difference
Average Speed
'''
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
import math
import os,sys
from scipy import stats
import numpy as np

'''
       


'''

'''
def choice(ans):

########### WORKS ###################
    if (ans=="timedif"):
        c=8
        sum_td=0
        td_el=0
        count=0
        for i in pr_split:
            if (count==c):
                print (i+"\n")
                ti=i.strip()
                fi=float(ti)
                sum_td=sum_td+fi
                td_el+=1
                c+=15
            count+=1
        print ("Average Time Difference = " + (sum_td/td_el))            
    if (ans=="speed"):
        c=6
        sum_sp=0
        sp_el=0
        count=0
        for i in pr_split:
            if (count==c):
                print (i+"\n")
                ti=i.strip()
                fi=float(ti)
                sum_sp=sum_sp+fi
                sp_el+=1
                c+=15
            count+=1
        print ("Average Speed = " + (sum_sp/sp_el))    

'''
def choice(ans):
    if (ans=="timedif"):
        c = 8+15
        sum_td=0
        td_el=0
        count = 0
        for i in pr_split:
            if (count==c):
                try:
                    print (i+"\n")
                    ti=i.strip()
                    fi=float(ti)
                    sum_td=sum_td+fi
                    td_el+=1
                    c+=15
                except:
                    sum_td=sum_td
                    td_el=td_el    
            count+=1
        print ("========================")
        print("\n")
        print("\n")
        print ("Average Time Difference = " + str(sum_td/td_el))
        print("\n")
        print("\n")
        print ("========================")
    if (ans=="speed"):
        c = 6+15
        sum_sp=0
        sp_el=0
        count = 0
        for i in pr_split:
            if (count==c):
                try:
                    print (i+"\n")
                    ti=i.strip()
                    fi=float(ti)
                    sum_sp=sum_sp+fi
                    sp_el+=1
                    c+=15
                except:
                    sum_sp=sum_sp
                    sp_el=sp_el
            count+=1  
        print ("========================")
        print("\n")
        print("\n")
        print ("Average Speed = " + str(sum_sp/sp_el))
        print("\n")
        print("\n")
        print ("========================")    
    print("==============================================================")
    
sc= SparkContext()
data = sc.textFile('curated_final.csv')
test=data.take(100000)

total=""
for i in test:
    total=total+i
pr_split=total.split(",")

#rdd_lon, rdd_lat,rdd_time = (raw.filter(f) for f in (lon,lat,time))

#print (data.take(100000)) #2209746

ans=""

while (ans!="exit"):
    ans=input("Choose Statistic (timedif/speed)...")
    choice(ans)

end = input("Success!")
