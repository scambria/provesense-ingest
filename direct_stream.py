from __future__ import print_function

import os 
import requests
import sys
import time 
import uuid

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def sendPartition(iter):
    for record in iter:
        try:
            log_msg = record.replace('\n', '/ \n')
            record = record.rstrip('\n')                        
            headers = {'Content-Type': 'application/x-turtle '}            
            print('PAYLOAD_START\n' + str(log_msg) + '\nPAYLOAD_END')
            results = requests.post(os.getenv('SPARQL_ENDPOINT'), record, headers=headers)
            print('RESULT_PAYLOAD_START\n' + str(results).replace('\n','/ \n') + '\nRESULT_PAYLOAD_END')
        except Exception as e:
            print('ERROR_PAYLOAD_START\n' + str(e).replace('\n', '/ \n') + '\nERROR_PAYLOAD_END')
            pass

if __name__ == "__main__":
    
    try: 
        if len(sys.argv) != 3:
            print("Usage: direct_stream.py <broker_list> <topic>", file=sys.stderr)
            exit(-1)
    
        sc = SparkContext(appName="ProvesenseDirectStream")
        ssc = StreamingContext(sc, 2)
        brokers, topic = sys.argv[1:]

        while True:
            try: 
                kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
                break
            except:
                print('Waiting on kafka...')
                time.sleep(1)
                pass

    except Exception as e:
        print('******************MAIN ERROR!***********' + str(e))
 
    kvs.map(lambda x: x[1]).foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
    ssc.start()
    ssc.awaitTermination()
