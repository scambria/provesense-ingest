from __future__ import print_function

import sys
import os 
import requests
import uuid

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def sendPartition(iter):
    print('w**********SENDPARTITION**********1')    
    headers = {'Content-Type': 'application/x-turtle '}
    print('w**********SENDPARTITION**********2')
    insert = '@prefix dc: <http://purl.org/dc/elements/1.1/> . dc:thing dc:test_errr2' + ' dc:test1 .'
    print('w**********SENDPARTITION**********3')    
    results = requests.post(os.getenv('SPARQL_ENDPOINT'), insert, headers=headers)    
    print('w**********SENDPARTITION**********4')
    i = 0
    for record in iter:
        print('w**********SENDPARTITION**********5')        
        try:
            record = record.rstrip('\n')                        
            print('w**********SENDPARTITION**********6')            
            headers = {'Content-Type': 'application/x-turtle '}
#            insert = '@prefix dc: <http://purl.org/dc/elements/1.1/> . dc:thing dc:test_' + str(i) + ' dc:test' + str(i) + ' .'
            print('w*********SENDING*********r' + str(record))
            print('w*********SENDING' + str(record))
            print(record)
            print('w*********SENDING2*********r2' + str(record))
            results = requests.post(os.getenv('SPARQL_ENDPOINT'), record, headers=headers)
            print('w**********SENT2BLAZE**********7 ' + str(results))
            print('w**********SEND2BLAZE**********text ' + str(results.text))
        except Exception as e:
            print('w********BLAZEERROR*****************' + str(e))
            pass

if __name__ == "__main__":
    
    try: 
        if len(sys.argv) != 3:
            print("Usage: direct_stream.py <broker_list> <topic>", file=sys.stderr)
            exit(-1)
    
        sc = SparkContext(appName="ProvesenseDirectStream")
        ssc = StreamingContext(sc, 2)

        brokers, topic = sys.argv[1:]
        kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
        print('******CREATED KVS********')
        headers = {'Content-Type': 'application/x-turtle '}
        insert = '@prefix dc: <http://purl.org/dc/elements/1.1/> . dc:thing dc:test_errr1' + ' dc:test1 .'
        results = requests.post(os.getenv('SPARQL_ENDPOINT'), insert, headers=headers)

    except Exception as e:
        print('******************MAIN ERROR!***********' + str(e))
 
    kvs.map(lambda x: x[1]).foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
    ssc.start()
    ssc.awaitTermination()

# @prefix dc: <http://purl.org/dc/elements/1.1/> . dc:thing dc:testspark5 dc:test1 .        
# >>> insert = '@prefix dc: <http://purl.org/dc/elements/1.1/> . dc:thing dc:test_py2 dc:test1 .' 
# >>> results = requests.post('http://blazegraph:9999/blazegraph/sparql', insert, headers=headers)
# >>> print(results.text)                                                                         
# <?xml version="1.0"?><data modified="1" milliseconds="48"/>
#curl -H 'Content-Type: text/turtle' -X POST -d 'PREFIX dc: <http://nonsense.org> INSERT {  dc:thing dc:test_py dc:test } WHERE {}' http://localhost:9090/blazegraph/sparql

