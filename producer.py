#!/usr/bin/env python

"""Generates a stream to Kafka from a time series csv file.
"""

import argparse
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket
import datetime


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print(f"Message produced: {msg.value()}")


static_mappings = {'916_Signal Controller_US1' : '916', '1430': '1430'}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    # parser.add_argument('filename', type=str,
    #                     help='Time series csv file.')
    # parser.add_argument('topic', type=str,
    #                     help='Name of the Kafka topic to stream.')
    # parser.add_argument('--speed', type=float, default=1, required=False,
    #                     help='Speed up time series by a given multiplicative factor.')
    # args = parser.parse_args()

    # futrue any new region comes, add below
    regions = ["Orlando"]
    # topic = 'topic_ATSPM_MOE_Orlando' 
    rdr = csv.reader(open('./916.csv')) # here they can upload file and reading will happen

    # p_key = args.filename
    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)

    next(rdr)  # Skip header
    firstline = True

    while True:

        try:

            if firstline is True:
                line1 = next(rdr, None)
                # timestamp, value = line1[0], (line1[1])
                isc_id, timestamp, eventcode, eventparam = line1[0], line1[1], line1[2], line1[4]
                # Convert csv columns to key value pair
                result = {}
                result["SignalID"] = isc_id
                dt = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') # akhil new change
                unix_time =  int(time.mktime(dt.timetuple()) * 1000)
                str_time = dt.strftime('%Y-%m-%d %H:%M:%S') # akhil new change
                result["ts"] = unix_time
                result["EventCode"] = eventcode
                result["EventParam"] = eventparam
                
                # Convert dict to json as message format
                jresult = json.dumps(result)
                firstline = False
                
                for i in regions:
                    producer.produce("topic_ATSPM_MOE_"+i, key=isc_id, value=jresult, callback=acked)

            else:
                line = next(rdr, None)
                # d1 = parse(timestamp)
                # d2 = parse(line[0])
                # diff = ((d2 - d1).total_seconds())/args.speed
                time.sleep(0.1)
                # timestamp, value = line[0], (line[1])
                isc_id, timestamp, eventcode, eventparam = line[0], line[1], line[2], line[4]
                # Convert csv columns to key value pair
                result = {}
                result["SignalID"] = isc_id
#                 result["ts"] = str(timestamp)
                dt = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') # akhil new change
                unix_time =  int(time.mktime(dt.timetuple()) * 1000)
                str_time = dt.strftime('%Y-%m-%d %H:%M:%S') # akhil new change
                result["ts"] = unix_time
                result["EventCode"] = eventcode
                result["EventParam"] = eventparam
                
                jresult = json.dumps(result)
                
                for i in regions:
                    producer.produce("topic_ATSPM_MOE_"+i, key=i+isc_id, value=jresult, callback=acked)

            producer.flush()

        except TypeError:
            sys.exit()


if __name__ == "__main__":
    main()
