# Spark example to print the average tweet length using Spark
# PGT April 2016   
# To run, do: spark-submit --master yarn-client avgTweetLength.py hdfs://hadoop2-0-0/data/twitter/part-03212

from __future__ import print_function
import sys, json
from pyspark import SparkContext

# Given a full tweet object, return the text of the tweet
def getDay(line):
  try:
    js = json.loads(line)
    if js['user']['screen_name'] == 'PrezOno':
     day = js['created_at'].encode('ascii', 'ignore')[0:3]
     date = js['created_at'].encode('ascii', 'ignore')[4:10] 
     return [(day,date)]
  except Exception as a:
    return []

#def getDate(line):
#  try:
#    js = json.loads(line)
#    date = js['created_at'].encode('ascii', 'ignore')[4:10] 
 #   return [date]
 # except Exception as a:
 #   return []
    
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="onotweet")
  
  tweets = sc.textFile(sys.argv[1],)
  days = tweets.flatMap(getDay)
  pairs =[]
#  print(days)
#  pairs = sorted(days.groupByKey().mapValues(list).collect())
  pairs = days.groupByKey().map(lambda l: (l[0],list(l[1]))).collect()
#  print(len(pairs[0][1]))
#  print(len(set(pairs[0][1])))
  
  weekaverage={}
  for i in range(len(pairs)):
   weekaverage[float(len(pairs[i][1])) / float(len(set(pairs[i][1])))]=[(pairs[i][0])]
   
  print(weekaverage)
  print(weekaverage[max(weekaverage)],max(weekaverage))
   
#  print(weekaverage.stats())
  
#  weekaverage.saveAsTextFile("onoweekaverage") 
 
 
  sc.stop()
#  s = days.countByKey().items()
#  c = days.countByValue().items()
#  d = set(c)
#  avg = s / c 
#  dates = days.flatMap(lambda day: day[1]) 
#  dates = tweets.flatMap(getDate)
#  d = texts.distinct()
 # print(d.count())
 # print(texts[0:10])
#  print(days.take(5))
#  print(s[:len(s)][1])
#  print(s[]) 
#  print(c[0][1])
#  print(d)
#  print(len(d))
#  print(s["Wed"])
#  print(c["Sept 24"])
#  count = 0
#  week = ["Mon" , "Tue", "Wed", "Thu" , "Fri","Sat" , "Sun"]
#  for j in week:
#   summ = s[j][1]      
#   for i in range(len(c)):
#    if j == c[i][0][0]:
#     ss = c[i][1]
#     print(ss)
#     print(c[i][0][1])
#   if s[i][1] in c[] 
#  avg = s[1]
#  avg = s[0:len(s)][1] / c[0:len(c)][1]
 # print(avg)
 # lengths = texts.map(lambda l: len(l))
  
  # Just show 10 tweet lengths to validate this works
#  print(lengths.take(10))
  # Print out the stats
 # print(lengths.stats())
  
  # Save to your local HDFS folder
#  lengths.saveAsTextFile("lengths")
  
  
#  sc.stop()

  
