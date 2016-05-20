from __future__ import print_function
import sys, json
from pyspark import SparkContext

# Given a full tweet object, return the text of the tweet
def getText(line):
  try:
    js = json.loads(line)
    text = js['text'].encode('ascii', 'ignore')
    return [text]
  except Exception as a:
    return []

def returnTrueIfPrez(line):
  js = json.loads(line)
  user = js['user']['screen_name']
  if user == "PrezOno":
    return True
  else:
    return False

def returnTrueIfNotPrez(line):
  js = json.loads(line)
  user = js['user']['screen_name']
  if user == "PrezOno":
    return False
  else:
    return True


  
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="avgTweetLength")
  
  tweets = sc.textFile(sys.argv[1])

  prez_tweets=tweets.filter(returnTrueIfPrez)
  non_prez_tweets=tweets.filter(returnTrueIfNotPrez)

  prez_texts = prez_tweets.flatMap(getText)
  prez_lengths = prez_texts.map(lambda l: len(l))

  non_prez_texts = non_prez_tweets.flatMap(getText)
  non_prez_lengths = non_prez_texts.map(lambda l: len(l))

  # Just show 10 tweet lengths to validate this works
  #print(prez_lengths.take(10))
  # Print out the stats
  print("Mean is the average tweet length of PrezOno")
  print(prez_lengths.stats())


  # Just show 10 tweet lengths to validate this works
  #print(non_prez_lengths.take(10))
  # Print out the stats
  print("Mean is the average tweet length of others")
  print(non_prez_lengths.stats())


  # Save to your local HDFS folder
  #prez_lengths.saveAsTextFile("prez_lengths")
  #non_prez_lengths.saveAsTextFile("non_prez_lengths")
  
  
  sc.stop()

