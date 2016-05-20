
from __future__ import print_function
import sys,string, json
from pyspark import SparkContext

def getDay(line):
  try:
    js = json.loads(line)
    tweeter = js['user']['screen_name']
    if tweeter == "PrezOno":
      d1 = js['created_at'].encode('ascii', 'ignore')[0:3]
      d2 = js['created_at'].encode('ascii', 'ignore')[4:10] 
      return [(d1,d2)]
  
  except Exception as a:
    return [] 
        

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="onotweet1")
  
  tweets = sc.textFile(sys.argv[1],)
  days = sorted(tweets.map(getDay).collect())
#  data = days.sortByKey()
#  data.foreach(avg)
 # print(days[0][1])
#  l = sorted(days)
#  l = days.sort()
#  print(l)
  count = 1.0
  avg = 0.0
  summ = 0.0
  day = None
  date = None 
    
  output = {}
  for lin in days:
 #    print(lin)
 #   (key,val) = lin.strip().split(',',1)
    key = lin[0]
    val = lin[1]
    if day != key:
       if day:
           avg = summ / count
           print('%s\t%s\t%s\t%s' % (day,avg,summ,count))
           output[avg]= day
           avg = 0.0
           summ = 0.0
           count = 1.0
    day = key
    try:
       summ = summ + 1
       if date != val:
         if date:
            count = count + 1
       date = val
    except:
       continue
  avg = summ / count
  print ('%s\t%s\t%s\t%s' % (day,avg,summ,count))
  output[avg] = day
  print ('PrezOno tweeted in the day %s with the maximum average of %s' % (output[max(output)],(max(output))))
  
  sc.stop()
    

