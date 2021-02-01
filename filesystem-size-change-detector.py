from elasticsearch import Elasticsearch
import json, requests
import pandas as pd
import os,subprocess
import datetime
timestamp = datetime.datetime.utcnow()#.strftime("%Y-%m-%dT%H:%M:%S.%f")
d = timestamp - datetime.timedelta(hours=1,minutes=16)
d = d.strftime("%Y-%m-%dT%H:%M:%S.%f")

index_timestamp = datetime.datetime.now().strftime("%Y-%m")
esIndexName = "datamgmt-fs-usage*" #+str(index_timestamp)

es = Elasticsearch(hosts=['elastic-test.winstorm.nssl:9200'],timeout=30,http_auth=('user.name', 'password'))

response = es.search(index=esIndexName,body={"size":100,"query":{"range":{"observed":{"gte":d,"lte":timestamp}}}},scroll='2s',)
old_scroll_id = response['_scroll_id']

myList = []
while len(response['hits']['hits']):
	response = es.scroll(scroll_id=old_scroll_id,scroll='2s')
	if old_scroll_id != response['_scroll_id']:	
		print(response['_scroll_id'])
	old_scroll_id = response['_scroll_id']
	#print(response['_scroll_id'])
	for hit in response['hits']['hits']:
		#print(hit['_source'])
		myList.append(hit['_source'])
        
df = pd.DataFrame(myList)
#print(df)
df["observed"] = df["observed"].astype("datetime64")
df["localtime"] = df["localtime"].astype("datetime64")

for qtree in df['qtree'].unique():
	temp = df[df['qtree']==qtree].sort_values(by='observed')#.iloc[0:4]
	if temp.iloc[-1][5] > temp.iloc[0][5]:
		beforeSize = temp.iloc[0][5]
		afterSize = temp.iloc[-1][5]
		percentNowUsed = temp.iloc[-1][6]
		volumeQtree = temp.iloc[-1][2]
		increase = afterSize-beforeSize
		percentageIncrease = increase/beforeSize
		message = "The the size of "+volumeQtree+" has increased in the past hour. It has increased by "+str(percentageIncrease)+" percent. It is now at "+str(percentNowUsed)+" percent of its quota."
		#print(temp)
		#print(message,beforeSize,afterSize,round(percentNowUsed,2))
		os.system("""curl -X POST -H 'Content-type: application/json' --data '{"text":" """+message+""" "}' https://hooks.slack.com/services/x/x""")
	elif temp.iloc[-1][5] < temp.iloc[0][5]:
		beforeSize = temp.iloc[0][5]
		afterSize = temp.iloc[-1][5]
		percentNowUsed = temp.iloc[-1][6]
		volumeQtree = temp.iloc[-1][2]
		decrease = beforeSize-afterSize
		percentageDecrease = decrease/beforeSize
		message = "The the size of "+volumeQtree+" has decreased in the past hour. It has decreased by "+str(percentageDecrease)+" percent. It is now at "+str(percentNowUsed)+" percent of its quota."
		os.system("""curl -X POST -H 'Content-type: application/json' --data '{"text":" """+message+""" "}' https://hooks.slack.com/services/x/xx""")
		#print(message,beforeSize,afterSize,round(percentNowUsed,2))
