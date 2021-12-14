from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext

"""
def lat(i): return ::5 #5
def lon(i): return ::4 #4
def time(i): return ::2 #2
"""

'''

    lon :   i = 3 at the beggining
            i = i + 16

    lat :   i = 4 at the beggining
            i = i + 16

    time :  i = 1 at the beggining
            i = i + 16       


'''

def choice(ans):
    if (ans=="lat"):
        c = 4
        count = 0
        for i in pr_split:
            if (count==c):
                print (i+"\n")
                c+=15
            count+=1
    if (ans=="lon"):
        c = 3
        count = 0
        for i in pr_split:
            if (count==c):
                print (i+"\n")
                c+=15
            count+=1
    if (ans=="time"):
        c = 1
        count = 0
        for i in pr_split:
            if (count==c):
                print (i+"\n")
                c+=15
            count+=1  
    print ("==============================================================================================================")


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
    ans=input("Choose action (lon/lat/time)...")
    choice(ans)

end = input("Success!")