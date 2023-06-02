from ksql import KSQLAPI
import socket
import json
import re
import pandas as pd
import time
import datetime
import csv
# from moe import processAtspmRow
from moe import AtsmProcessor
import threading
import os

client = KSQLAPI('http://localhost:8088')
x = {
    "ksql.streams.auto.offset.reset": "earliest",
    "ksql.query.pull.table.scan.enabled": "true"
}

def parse_columns(columns_str):
    regex = r"(?<!\<)`(?P<name>[A-Z_]+)` (?P<type>[A-z]+)[\<, \"](?!\>)"
    result = []

    matches = re.finditer(regex, columns_str)
    for matchNum, match in enumerate(matches, start=1):
        result.append({"name": match.group("name"), "type": match.group("type")})

    return result


def process_row(row, column_names):
    row = row.replace(",\n", "").replace("]\n", "").rstrip("]")
    row_obj = json.loads(row)
    if "finalMessage" in row_obj:
        return None
    column_values = row_obj["row"]["columns"]
    index = 0
    result = {}
    for column in column_values:
        result[column_names[index]["name"]] = column
        index += 1

    return result

def ts_unix_to_ts(time_unix):
    dt = datetime.datetime.fromtimestamp(time_unix/1000)
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')

def process_query_result(results, return_objects=None):
    if return_objects is None:
        yield from results

    # parse rows into objects
    processor = AtsmProcessor()

    try:
        header = next(results)
    except StopIteration:
        return
    columns = parse_columns(header)
    # newdf = pd.DataFrame(columns=['SignalID','System_time','CL','M_1','M_2','M_3','M_4','M_5','M_6','M_7','M_8','AOG_2','AOR_2','TOT_2','GOR_2','GOR_ADV_2','SUR_2', 'SUR_ADV_2','AOG_6','AOR_6','TOT_6','GOR_6','GOR_ADV_6','SUR_6', 'SUR_ADV_6','AOG_4','AOR_4','TOT_4','GOR_4','GOR_ADV_4','SUR_4', 'SUR_ADV_4','AOG_8','AOR_8','TOT_8','GOR_8','GOR_ADV_8','SUR_8', 'SUR_ADV_8','P_4','P_8','GT_1','RT_1','GT_2','RT_2','GT_3','RT_3','GT_4','RT_4','RT_5','GT_5','GT_6','RT_6','GT_7','RT_7','GT_8','RT_8'], index = range(50000))
    for result in results:
        row_obj = process_row(result, columns)
        if row_obj is None:
            return
        newdf = processor.processAtspmRow(row_obj['SIGNALID'], ts_unix_to_ts(int(row_obj['TS'])), int(row_obj['EVENTCODE']), int(row_obj['EVENTPARAM']))
        # print(type(newdf))
        if newdf:
            
            with open('file.csv', mode='a', newline='') as file:

                writer = csv.DictWriter(file, fieldnames=['SignalID','System_time','CL','M_1','M_2','M_3','M_4','M_5','M_6','M_7','M_8','AOG_2','AOR_2','TOT_2','GOR_2','GOR_ADV_2','SUR_2', 'SUR_ADV_2','AOG_6','AOR_6','TOT_6','GOR_6','GOR_ADV_6','SUR_6', 'SUR_ADV_6','AOG_4','AOR_4','TOT_4','GOR_4','GOR_ADV_4','SUR_4', 'SUR_ADV_4','AOG_8','AOR_8','TOT_8','GOR_8','GOR_ADV_8','SUR_8', 'SUR_ADV_8','P_4','P_8','GT_1','RT_1','GT_2','RT_2','GT_3','RT_3','GT_4','RT_4','RT_5','GT_5','GT_6','RT_6','GT_7','RT_7','GT_8','RT_8'])
                file.seek(0)
                # has_header = csv.Sniffer().has_header(file.read(1024))
                file.seek(0)

                # if not has_header:
                #     writer.writeheader()
                writer.writerow(newdf)

            # resultdf = newdf.dropna(axis=0, how='all')
            # resultdf = resultdf.fillna(0)
            # save_file = os.path.join("", f"moe_signal.csv")
            # resultdf.to_csv(save_file)

            # client.inserts_stream ('IntersectionMOE', list(newdf))
        yield row_obj

def return_query_df(ksql_string):
    query = client.query(ksql_string, stream_properties=streamProperties)
    tspm = list(process_query_result(query, return_objects=True))
    return pd.DataFrame(tspm)

def ts_str_to_unix(time_str):
    dt = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')
    return int(time.mktime(dt.timetuple()) * 1000)

# FUNCTION TO RETURN MOST RECENT TIMESTAMP FOR A INTERSECTION
def return_recent_data_ts():
    '''
    signalid: isc name string
    '''
    TABLE_NAME = 'ATSPM_MOE_stream_v3' #THIS IS A TABLE
    ksql_string = f'SELECT * from {TABLE_NAME}'
    print(ksql_string)
    df = return_query_df(ksql_string)
    return df

# df = return_recent_data_ts()
# print (df)

#======  AKhil new design changes
# archi - region -> interesection -> (moe) > output file
# each region - one topic and one consumer group( add any consumers)


def runner(region, all_objects):

    def consume_msg(local_consumer):
        local_consumer.subscribe(['topic_ATSPM_MOE_'+region])   
    
        while True:
            msg = local_consumer.poll()
            print(msg)
            row_obj={}
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached')
                else:
                    print('Error while consuming message: {}'.format(msg.error()))
            else:
                row_obj=json.loads(msg.value().decode('utf-8'))

            print(msg,region, region+static_mappings[row_obj['SignalID']])

            newdf = all_objects[region+static_mappings[row_obj['SignalID']]].processAtspmRow(static_mappings[row_obj['SignalID']], ts_unix_to_ts(int(row_obj['ts'])), int(row_obj['EventCode']), int(row_obj['EventParam']))

            if newdf:
                with open("results/"+region+"/" +static_mappings[row_obj['SignalID']]+'_signal.csv', mode='a', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=['SignalID','System_time','CL','M_1','M_2','M_3','M_4','M_5','M_6','M_7','M_8','AOG_2','AOR_2','TOT_2','GOR_2','GOR_ADV_2','SUR_2', 'SUR_ADV_2','AOG_6','AOR_6','TOT_6','GOR_6','GOR_ADV_6','SUR_6', 'SUR_ADV_6','AOG_4','AOR_4','TOT_4','GOR_4','GOR_ADV_4','SUR_4', 'SUR_ADV_4','AOG_8','AOR_8','TOT_8','GOR_8','GOR_ADV_8','SUR_8', 'SUR_ADV_8','P_4','P_8','GT_1','RT_1','GT_2','RT_2','GT_3','RT_3','GT_4','RT_4','RT_5','GT_5','GT_6','RT_6','GT_7','RT_7','GT_8','RT_8'])
                    file.seek(0)
                    writer.writerow(newdf)

     # Add as many consumers
    conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group'+region,
    'auto.offset.reset': 'earliest'
    }
    for i in range(1):
        consumer = Consumer(conf)
        internal_thread = threading.Thread(target=consume_msg, args=(consumer,))
        internal_thread.start()


from confluent_kafka import Consumer, KafkaError
import json

#TODO : add required to new region
regions = ["Orlando", "Ganisville", "FIU"]
region_intsersection_mapping = {"Orlando":['916', '1430'], 
                                "Ganisville":['916', '1430'],
                                "FIU":['916', '1430']
                                }
static_mappings = {'916_Signal Controller_US1' : '916', '1430': '1430'}

#TODO : y csv we are getting signalids in that way ; for it add for any region - one place
# can chnage accoriding to requirement


# for each region start a thread 
if not os.path.exists("results"):
    os.makedirs("results")

def caller(each_region):
    # process all intersections objects for a region in map
    #create dir for each region
    if not os.path.exists("results/"+each_region):
        os.makedirs("results/"+each_region)

    #TODO : add new intersections here
    intersections =  region_intsersection_mapping.get(each_region)
    intersections_objects = {}
    for intersection in intersections:
        intersections_objects[each_region+intersection] = AtsmProcessor(each_region,intersection)
    
    print(intersections_objects)
    runner(each_region, intersections_objects)

    # thread = threading.Thread(target=runner, args=(each_region,intersections_objects,))
    # thread.start()
        

for each_region in regions:
    thread = threading.Thread(target=caller, args=(each_region,))
    thread.start()


