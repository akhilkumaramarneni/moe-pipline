# coding: utf-8

# In[1]:


# ALL
# PHASE_DETCHANNEL_MAP = {
#     1: [1,2,3,4],
#     2: [5,6,7,8,11,12,15,16],
#     3: [17,18,19,20],
#     4: [21,22,23,24,26,27,28,29,30,31,32,49,50], # Why so many?
#     5: [33,34,36],
#     6: [37,38,39,40,43,44,47,48],
#     7: [], # Why zero? 
#     8: [53,54,55,56,58,60,61,62,63,64]
# }


# In[2]:

#TODO : furure based load once
# Only active
# Akhil change - get dumped data where mapping is done into dict, confrim once with Rahul/Tania
# Y ? static mappings so can use dump data
# load all regions
import pickle

all_regions_phase_detector_mappings={}
all_regions_mapped_phases={}
all_regions_advance_detecors_mappings={}
regions = ["Orlando", "Ganisville"]
for each in regions:
    try:
        with open('mappings/'+each+'/'+'phase_to_detector_mappings.pickle', 'rb') as ph_to_detector_pickle_file:
            all_regions_phase_detector_mappings[each] = pickle.load(ph_to_detector_pickle_file)

        with open('mappings/'+each+'/'+'mapped_phases.pickle', 'rb') as mappings_pickle_file:
            all_regions_mapped_phases[each] = pickle.load(mappings_pickle_file)

        with open('mappings/'+each+'/'+'advance_detector_mappings.pickle', 'rb') as advance_mappings_pickle_file:
            all_regions_advance_detecors_mappings[each] = pickle.load(advance_mappings_pickle_file)
    except:
        print("no mappings found for ",each)


# print(all_regions_phase_detector_mappings)
PHASE_DETCHANNEL_MAP_INT = all_regions_phase_detector_mappings
mapped_phases = all_regions_mapped_phases
# print(mapped_phases)
advancedDet_map = all_regions_advance_detecors_mappings


import pandas as pd
import numpy as np
import math
import sys
import datetime
from datetime import datetime
from datetime import timedelta
from pathlib import Path
import glob
import os
#from retrieval import RetrieveData
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# import psycopg2
# import pandas.io.sql as psql
import time
# Approach ID Count Lookup

def dictionarycreator(approach, AdvancedDetectorList):
    #df = pd.read_csv(file_to_open)
    df = approach
    s = df['Approach ID']
    d = {2: 0, 4:0, 6:0, 8:0}
    for index, row in df.iterrows():
        key = row['Protected Phase Number']
        if key in d:
            if row['Detector ID'] not in AdvancedDetectorList:
                d[key] += 1
    return d

#@dataclass
class PhaseCycle:
    def __init__(self, sTS, eTS, rTS, yTS, r, g1, g2, m, f, isG, ped):
        self.sTimestamp = sTS  #start timestamp
        self.eTimestamp = eTS  #end timestamp
        self.rTimestamp = rTS  #start of red timestamp
        self.yTimestamp = yTS  #start of red timestamp
        self.aor_adv = r           #number of arrivals on red for cycle
        self.aog_adv = g1           #number of arrivals on green for cycle
        self.aog_stop = g2           #number of arrivals on green for cycle
        self.isMaxed = m       #1 of the phase maxed out or was forced off
        self.isForced = f       #1 of the phase maxed out or was forced off
        self.isGreen = isG     #1 if phase is green, 0 otherwise
        self.pedstatus = ped   #1 if ped call recorded, 0 otherwise
        self.skipDetector = 0
        self.finish = 0
        self.sIndex = 0
        self.eIndex = 0
        self.cumaor = 0
        self.cumaog = 0
        self.gordict = {}

def initialize_dict(dictname):
    dictname['SignalID'] = 0
    dictname['System_time'] = 0
    dictname['CL'] = 0
    dictname['M_1'] = 0
    dictname['M_2'] = 0
    dictname['M_3'] = 0
    dictname['M_4'] = 0
    dictname['M_5'] = 0
    dictname['M_6'] = 0
    dictname['M_7'] = 0
    dictname['M_8'] = 0
    dictname['AOG_2'] = 0
    dictname['AOR_2'] = 0
    dictname['TOT_2'] = 0
    dictname['GOR_2'] = 0
    dictname['GOR_ADV_2'] = 0
    dictname['SUR_2'] = 0 
    dictname['SUR_ADV_2'] = 0
    dictname['AOG_6'] = 0
    dictname['AOR_6'] = 0
    dictname['TOT_6'] = 0
    dictname['GOR_6'] = 0
    dictname['GOR_ADV_6'] = 0
    dictname['SUR_6'] = 0 
    dictname['SUR_ADV_6'] = 0
    dictname['AOG_4'] = 0
    dictname['AOR_4'] = 0
    dictname['TOT_4'] = 0
    dictname['GOR_4'] = 0
    dictname['GOR_ADV_4'] = 0
    dictname['SUR_4'] = 0 
    dictname['SUR_ADV_4'] = 0
    dictname['AOG_8'] = 0
    dictname['AOR_8'] = 0
    dictname['TOT_8'] = 0
    dictname['GOR_8'] = 0
    dictname['GOR_ADV_8'] = 0
    dictname['SUR_8'] = 0 
    dictname['SUR_ADV_8'] = 0
    dictname['P_4'] = 0
    dictname['P_8'] = 0
    dictname['GT_1'] = 0
    dictname['RT_1'] = 0
    dictname['GT_2'] = 0
    dictname['RT_2'] = 0
    dictname['GT_3'] = 0
    dictname['RT_3'] = 0
    dictname['GT_4'] = 0
    dictname['RT_4'] = 0
    dictname['RT_5'] = 0
    dictname['GT_5'] = 0
    dictname['GT_6'] = 0
    dictname['RT_6'] = 0
    dictname['GT_7'] = 0
    dictname['RT_7'] = 0
    dictname['GT_8'] = 0
    dictname['RT_8'] = 0
    return

def sumTotalDetectorTime(gordict, phase, AdvancedDetectorList):
    gor = 0
    gor_adv = 0
    for key in gordict.keys():
        item = gordict[key]
        if item['Phase'] == phase:
            if key not in AdvancedDetectorList:
                gor = gor + item['totalTime']
            else:
                gor_adv = gor_adv + item['totalTime']
            print ("Partial GOR for detector {} is {}".format(key, item['totalTime']))
    return gor, gor_adv

def totalDetectors(gordict, phase, AdvancedDetectorList):
    dets = 0
    dets_adv = 0
    for key in gordict.keys():
        item = gordict[key]
        if item['Phase'] == phase:
            if key not in AdvancedDetectorList:
                dets = dets + 1
            else:
                dets_adv = dets_adv + 1
            print ("Detector {} is in phase {}".format(key, item['Phase']))
    return dets, dets_adv

reportTime = pd.to_datetime('today')
baseTime = pd.to_datetime('today')
startCycleTime = pd.to_datetime('today')
isBaseSet = 0
stepseconds = 60
step = timedelta(seconds=stepseconds)
i = 0
j = -1

phaseInfo = []
numPhases = 16
currentSignalState = [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1]
k = 0
while k <= numPhases:      #assign and initialize a phase cycle list for each phase
    cycleList = []
    phaseInfo.append(cycleList)
    k = k + 1

cycle_mark = -1
cycle_end = 0
lineno = 0
eTS = 0

class AtsmProcessor :
    def __init__(self, region, intersection):
        self.reportTime = pd.to_datetime('today')
        self.baseTime = pd.to_datetime('today')
        self.startCycleTime = pd.to_datetime('today')
        self.isBaseSet = 0
        self.stepseconds = 60
        self.step = timedelta(seconds=self.stepseconds)
        self.i = 0
        self.j = -1
        self.phaseInfo = phaseInfo
        self.cycle_mark = -1
        self.cycle_end = 0
        self.eTS = 0
        self.lineno = 0
        self.currentSignalState = [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1]
        self.region = region
        self.intersection = intersection
    
    def processAtspmRow(self, signalid, timeStamp, eventCode, eventParam):
        # global reportTime
        # global baseTime 
        # global startCycleTime 
        # global isBaseSet 
        # global stepseconds 
        # global step 
        # global i 
        # global j 
        # global phaseInfo 
        # global cycle_mark
        # global cycle_end
        # global eTS
        # lineno=0
        # global currentSignalState
        # print(PHASE_DETCHANNEL_MAP_INT)
        PHASE_DETCHANNEL_MAP = PHASE_DETCHANNEL_MAP_INT[self.region][signalid]
        # print(int(eventCode), type(eventCode), int(eventCode) ==1)
        # print(PHASE_DETCHANNEL_MAP)
        approaches = {
            "Approach ID": [k for k in mapped_phases[self.region][signalid] for d in PHASE_DETCHANNEL_MAP[k]],
            "Detector ID": [signalid+str(d).zfill(2) for k in mapped_phases[self.region][signalid] for d in PHASE_DETCHANNEL_MAP[k]],
            "Protected Phase Number": [k for k in mapped_phases[self.region][signalid] for d in PHASE_DETCHANNEL_MAP[k]],
        }
        AdvancedDetectorList = [ signalid+str(d).zfill(2) for d in advancedDet_map[self.region][signalid] ]
        approach = pd.DataFrame.from_dict(approaches)
        approach.index = approach.index.astype(int)
        newdf = {}
        #newdf = pd.DataFrame(columns=['System_time','CL','M_1','M_2','M_3','M_4','M_5','M_6','M_7','M_8','AOG_2','AOR_2','TOT_2','GOR_2','GOR_ADV_2','SUR_2', 'SUR_ADV_2','AOG_6','AOR_6','TOT_6','GOR_6','GOR_ADV_6','SUR_6', 'SUR_ADV_6','AOG_4','AOR_4','TOT_4','GOR_4','GOR_ADV_4','SUR_4', 'SUR_ADV_4','AOG_8','AOR_8','TOT_8','GOR_8','GOR_ADV_8','SUR_8', 'SUR_ADV_8','P_4','P_8','GT_1','RT_1','GT_2','RT_2','GT_3','RT_3','GT_4','RT_4','RT_5','GT_5','GT_6','RT_6','GT_7','RT_7','GT_8','RT_8'], index=range(1))
        currentTime = pd.to_datetime(timeStamp, infer_datetime_format=True)
        lookup = dictionarycreator(approach, AdvancedDetectorList)
        self.lineno = self.lineno + 1
        if (currentTime > self.reportTime): #time to dump data, one minute is over
            self.j = self.j + 1
            initialize_dict(newdf)
            newdf['SignalID'] = signalid
            newdf['System_time'] = self.reportTime
            newdf['CL'] = int(self.stepseconds)
            k = 1
            while k <= numPhases: #Iterate over phases to gather AOR/AOG/MaxOut/ForceOff
                #Remember: an intersection cycle may have multiple phase cycles
                #Sum up values from all cycles of phase k
                #The last cycle in the list is the oldest one, and the first is the newest
                aor_adv = 0
                aog_adv = 0
                aog_stop = 0
                gt = 0
                rt = 0
                yt = 0
                pedcall = 0
                isMaxed = 0
                isForced = 0
                accu_gor = 0
                gor = 0
                accu_gor_adv = 0
                gor_adv = 0
                dets = 0
                dets_adv = 0
                SAT_HEADWAY = 2 # seconds
                LOST_TIME = 2 # seconds
                length = len(self.phaseInfo[k])
                if (length == 0):
                    if (self.currentSignalState[k-1] == 0):
                        rt = self.stepseconds
                    else:
                        gt = self.stepseconds
                pIndex = 0
                while pIndex < length : #Iterate over phase cycles and keep removing them when done
                    currentCycle = self.phaseInfo[k][pIndex]
                    
                    self.eTS = pd.to_datetime(currentCycle.eTimestamp, infer_datetime_format=True)
                    gTS = pd.to_datetime(currentCycle.sTimestamp, infer_datetime_format=True)
                    rTS = pd.to_datetime(currentCycle.rTimestamp, infer_datetime_format=True)
                    # print(f"debug currentCycle {currentCycle.eTimestamp}")
                    if(currentCycle.eTimestamp!=0):
                        self.eTS = pd.to_datetime(currentCycle.eTimestamp, infer_datetime_format=True)
                    else:
                        self.eTS = pd.to_datetime(currentCycle.eTimestamp, infer_datetime_format=True)
                    if(currentCycle.sTimestamp!=0):
                        gTS = pd.to_datetime(currentCycle.sTimestamp, infer_datetime_format=True)
                    else:
                        gTS = pd.to_datetime(currentCycle.sTimestamp, infer_datetime_format=True)
                    if(currentCycle.rTimestamp!=0):
                        rTS = pd.to_datetime(currentCycle.rTimestamp, infer_datetime_format=True)
                    else:
                        rTS = pd.to_datetime(currentCycle.rTimestamp, infer_datetime_format=True)
                    if(currentCycle.yTimestamp!=0):
                        yTS = pd.to_datetime(currentCycle.yTimestamp, infer_datetime_format=True)
                    else:
                        yTS = pd.to_datetime(currentCycle.yTimestamp, infer_datetime_format=True)
                    
                    diffgt = 0
                    diffrt = 0
                    diffyt = 0
                    if ((self.reportTime - self.step) <= gTS and self.eTS <= self.reportTime and (self.reportTime - self.step) < self.eTS): #gTS, rTS and eTS lie in the same minute
                        diffgt = pd.Timedelta(rTS - gTS).seconds
                        diffyt = pd.Timedelta(rTS - yTS).seconds
                        currentCycle.sIndex = j
                        currentCycle.eIndex = j
                        currentCycle.finish = 1
                        #if (currentCycle.rTimestamp == 0 and currentCycle.sTimestamp != 0 and currentCycle.eTimestamp != 0):
                    #two events of Green/Red/End happen on the same minute
                    elif ((self.reportTime - self.step) <= gTS and rTS <= self.reportTime and (self.reportTime - self.step) < rTS) :
                        diffgt = pd.Timedelta(rTS - gTS).seconds
                        diffyt = pd.Timedelta(rTS - yTS).seconds
                        #if (eTS < reportTime and eTS > (reportTime - step)) :
                        #    diffrt = pd.Timedelta(eTS - rTS).seconds 
                        #else:
                        #    diffrt = pd.Timedelta(reportTime - rTS).seconds
                        currentCycle.sIndex = j
                        currentCycle.eIndex = j
                        currentCycle.finish = 1
                    elif ((self.reportTime - self.step) <= rTS and self.eTS <= self.reportTime and (self.reportTime - self.step) < self.eTS):
                        diffgt = pd.Timedelta(rTS - (self.reportTime - self.step)).seconds
                        diffyt = diffgt
                        #diffrt = pd.Timedelta(eTS - rTS).seconds 
                        currentCycle.eIndex = j
                        currentCycle.finish = 1
                        # there may be a Green at End, but that will be captured in the next cycle
                    elif ((self.reportTime - self.step) <= self.eTS and self.eTS < self.reportTime):
                        #ignore gt and rt and these will be set as part of the next cycle
                        diffgt = 0
                        #diffrt = pd.Timedelta(eTS - (reportTime - step)).seconds
                        currentCycle.eIndex = j
                        currentCycle.finish = 1
                    #only one event of G/R/E happen in the current minute
                    elif ((self.reportTime - self.step) <= rTS and rTS < self.reportTime):
                        diffgt = pd.Timedelta(rTS - (self.reportTime - self.step)).seconds
                        diffyt = diffgt
                    elif ((self.reportTime - self.step) <= gTS and gTS < self.reportTime):
                        diffgt = pd.Timedelta(self.reportTime - gTS).seconds
                        diffyt = pd.Timedelta(self.reportTime - yTS).seconds
                        currentCycle.sIndex = j
                    #No event of G/R/E happen in the current minute
                    elif (gTS < (self.reportTime - self.step) and gTS >= self.baseTime):
                        diffgt = self.stepseconds
                        diffyt = pd.Timedelta(rTS - yTS).seconds
                    elif (rTS < (self.reportTime - self.step) and rTS >= self.baseTime or self.eTS < (self.reportTime - self.step) and self.eTS >= self.baseTime):
                        diffgt = 0
                        diffyt = pd.Timedelta(rTS - yTS).seconds
                    else:
                        currentCycle.finish = 1
                    diffrt = self.stepseconds - diffgt
                    if (diffrt < 0):
                        logging.info("Check for inconsistency in signal times for signal", signalid, gTS, rTS, self.eTS, self.reportTime-self.step, self.reportTime, self.baseTime)
                        currentCycle.finish = 1
                    else:
                        gt += diffgt
                    if (diffyt > 0 and diffyt < 60):
                        yt += diffyt
                    gor_sample, gor_adv_sample = sumTotalDetectorTime(currentCycle.gordict, k, AdvancedDetectorList)
                    dets_sample, dets_adv_sample = totalDetectors(currentCycle.gordict, k, AdvancedDetectorList)
                    gor = gor + gor_sample
                    gor_adv = gor_adv + gor_adv_sample
                    dets = dets + dets_sample
                    dets_adv = dets_adv + dets_adv_sample
                    aor_adv = aor_adv + currentCycle.aor_adv #Sum up AOR/AOG for all cycles serviced for the phase
                    aog_adv = aog_adv + currentCycle.aog_adv 
                    aog_stop = aog_stop + currentCycle.aog_stop 
                    currentCycle.aor_adv = 0
                    currentCycle.aog_adv = 0
                    currentCycle.aog_stop = 0
                    if (currentCycle.pedstatus == 1):
                        pedcall = 1
                        currentCycle.pedstatus = 0
                    if (currentCycle.isMaxed == 1):
                        isMaxed = 1
                        currentCycle.isMaxed = 0
                    if (currentCycle.isForced == 1):
                        isForced = 1
                        currentCycle.isForced = 0
                    pIndex = pIndex + 1
                    if (currentCycle.finish == 1):
                        self.phaseInfo[k].remove(currentCycle)
                        length = length - 1
                        pIndex = pIndex - 1
                if k >= 1 and k <= 8:
                    if (isMaxed > 0):
                        newdf['M_'+str(k)] = 1
                    if (isForced > 0):
                        newdf['M_'+str(k)] = 2
                    if (isMaxed == 0 and isForced == 0):
                        newdf['M_'+str(k)] = 0
                #if (k == 2  or k == 4 or k == 6 or k == 8):
                if (gt > self.stepseconds):
                    gt = self.stepseconds
                if (dets*(gt-yt) > 0):
                    accu_gor = gor/(dets*(gt-yt))
                if (dets_adv*(gt-yt) > 0):
                    accu_gor_adv = gor_adv/(dets_adv*(gt-yt))
                effective_green = gt - LOST_TIME
                sur_stop = 0
                sur_adv = 0
                if (lookup.get(k)):
                    lanes = lookup[k]
                    if (effective_green > 0):
                        sur_stop = aog_stop/(effective_green/SAT_HEADWAY)/lanes
                        sur_adv = aog_adv/(effective_green/SAT_HEADWAY)/lanes
                if (k == 2 or k == 4 or k == 6 or k == 8):
                    print ("Effective green {}, AOG stop {}, AOG advanced {}, sur stop {}, sur adv {}".format(effective_green, aog_stop, aog_adv, sur_stop, sur_adv))
                    newdf['AOG_'+str(k)] = aog_adv
                    newdf['AOR_'+str(k)] = aor_adv
                    newdf['TOT_'+str(k)] = aog_adv + aor_adv
                    newdf['GOR_'+str(k)] = round(accu_gor,3)
                    newdf['GOR_ADV_'+str(k)] = round(accu_gor_adv,3)
                    newdf['SUR_'+str(k)] = round(sur_stop,3)
                    newdf['SUR_ADV_'+str(k)] = round(sur_adv,3)
                if (k == 4 or k == 8):
                    newdf['P_'+str(k)] = pedcall
                if k >= 1 and k <= 8:
                    newdf['GT_'+str(k)] = gt
                    newdf['RT_'+str(k)] = self.stepseconds - gt
                k = k+1     #Next phase
            self.reportTime = self.reportTime + timedelta(hours=1)
        if (eventCode == 1):     #Phase begin green
            if (self.isBaseSet == 0):
                self.baseTime = currentTime
                self.isBaseSet = 1
            if (self.i == 0 and (eventParam == 2 or eventParam == 6)):       #The first phase turns green in the CSV
                self.i = self.i+1
                self.cycle_mark = eventParam #Store the phase to keep track of the current intersection cycle
                self.startCycleTime = currentTime
            elif (self.i > 0 and eventParam== self.cycle_mark):
                self.stepseconds = (currentTime - self.startCycleTime).total_seconds()
                self.startCycleTime = currentTime
                self.step = timedelta(seconds=self.stepseconds)
                self.reportTime = currentTime
            setSignalState(self.currentSignalState, eventParam-1, 'green')
            if (len(self.phaseInfo[eventParam]) > 0):
                currentCycle = self.phaseInfo[eventParam][0]
                currentCycle.isGreen = 0
                currentCycle.eTimestamp = timeStamp #Record end time so if needed total phase cycle time may be computed as eTimestamp-sTimestamp
            currentCycle = PhaseCycle(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            self.phaseInfo[eventParam].insert(0, currentCycle) #Add a new phase cycle, which may be needed if there are multiple phase cycles
            currentCycle.sTimestamp = timeStamp    #Record phase begin green time stamp
            currentCycle.isGreen = 1
        elif (self.i==0):           #Have not yet encountered the first green in the CSV, so continue to next CSV row
            return newdf
        elif (eventCode == 82):   #Detector off
            detChannel = eventParam
            combined = str(signalid) + str(detChannel).zfill(2)
            try:
                phase = approach[approach['Detector ID'] == combined]['Protected Phase Number'].item()
                if (phase > 0 and phase <= 16): 
                    currentCycle = self.phaseInfo[phase][0]
                    if combined not in currentCycle.gordict:
                        currentCycle.gordict[combined] = {}
                        currentCycle.gordict[combined]['Phase'] = phase
                        currentCycle.gordict[combined]['totalTime'] = 0
                    currentCycle.gordict[combined]['startTime'] = currentTime
                else:
                    currentCycle.skipDetector = 1
            except:
                #print('Phase no. '+ combined + ' does not existL:')
                #phase = 2 #determine accurately later 
                a = 0
        elif (eventCode == 81):   #Detector off
            detChannel = eventParam
            combined = str(signalid) + str(detChannel).zfill(2)
            try:
                phase = approach[approach['Detector ID'] == combined]['Protected Phase Number'].item()
                if (phase > 0 and phase <= 16): 
                    currentCycle = self.phaseInfo[phase][0]
                    if (combined in AdvancedDetectorList):
                        if (currentCycle.isGreen): #Increment vehicle arrival count
                            currentCycle.aog_adv += 1
                            currentCycle.cumaog += 1
                            if (currentCycle.aog_adv > self.stepseconds):
                                aid = approach[approach['Detector ID'] == combined]['Approach ID'].item()
                                lanes = lookup[aid]
                                if (currentCycle.aog_adv/lanes > self.stepseconds):
                                    logging.info("Check arrivals on green ", currentCycle.aog_adv, " violates LOS", currentTime, phase, signalid, detChannel, reportTime)
                        else:
                            currentCycle.aor_adv += 1
                            currentCycle.cumaor += 1
                    else: #stopbar detector
                        if (currentCycle.isGreen):
                            currentCycle.aog_stop += 1
                    if combined in currentCycle.gordict:
                        if (currentCycle.gordict[combined]['startTime'] > pd.to_datetime(0)):
                            total_time = currentCycle.gordict[combined]['totalTime'] + (currentTime - currentCycle.gordict[combined]['startTime']).total_seconds()
                            #if (combined == '91653'):
                                #print (combined, phase, total_time, currentCycle.gordict[combined]['startTime'], currentTime)
                            currentCycle.gordict[combined]['Phase'] = phase
                            currentCycle.gordict[combined]['totalTime'] = total_time
                            currentCycle.gordict[combined]['startTime'] = pd.to_datetime(0)
                        else:
                            print (f"Detector {detChannel} went off (81), but never was on (82), timestamp {timeStamp}")
                else:
                    currentCycle.skipDetector = 1
            except:
                #print('Phase no. '+ combined + ' does not existL:')
                #phase = 2 #determine accurately later 
                a = 0
        
        elif (eventCode == 5): #Phase max out/phase force off
            if (len(self.phaseInfo[eventParam]) > 0):
                currentCycle = self.phaseInfo[eventParam][0]
                currentCycle.isMaxed = 1
                #if (eventParam==2 or eventParam==6):       #Record max out for phase 2/6 only if there were vehicles detected
                #                                #to isolate cases where signals in the primary directions max out by default
                #    if (currentCycle.cumaog > 10):     # > 1 is used to ignore one-off detector 82/81 events
                #        currentCycle.isMaxed = 1
                #    elif (currentCycle.skipDetector): #For example, check 1510.csv
                #        currentCycle.isMaxed = 1
                #else:
                #    currentCycle.isMaxed = 1       #Record max out unconditionally for all other phases
        elif (eventCode == 6): #Phase max out/phase force off
            if (len(self.phaseInfo[eventParam]) > 0):
                currentCycle = self.phaseInfo[eventParam][0]
                currentCycle.isForced = 1
        elif (eventCode == 21): #Ped event
            if (len(self.phaseInfo[eventParam]) > 0):
                currentCycle = self.phaseInfo[eventParam][0]
                if (eventParam==4 or eventParam==8):
                    currentCycle.pedstatus = 1
        elif (eventCode == 8):                   #Phase begin red clearence
            length = len(self.phaseInfo[eventParam])
            index = 0
            while (index < length):
                currentCycle = self.phaseInfo[eventParam][index]
                currentCycle.yTimestamp = timeStamp
                index = index+1 
        elif (eventCode == 10):                   #Phase begin red clearence
            setSignalState(self.currentSignalState, eventParam-1, 'red')
            length = len(self.phaseInfo[eventParam])
            index = 0
            while (index < length):
                currentCycle = self.phaseInfo[eventParam][index]
                if (currentCycle.eTimestamp == 0):
                    currentCycle.isGreen = 0
                    currentCycle.rTimestamp = timeStamp  #Mark timestamp so if needed green time may be computed as rTimestamp-sTimestamp
                    break
                else:
                    currentCycle.rTimestamp = currentCycle.eTimestamp
                index = index+1 
        elif (eventCode == 11 or eventCode == 12): #Phase end red clearence
            setSignalState(self.currentSignalState, eventParam-1, 'end')
            length = len(self.phaseInfo[eventParam])
            index = 0
            while (index < length):
                currentCycle = self.phaseInfo[eventParam][index]
                if (currentCycle.eTimestamp == 0):
                    currentCycle.isGreen = 0
                    currentCycle.eTimestamp = timeStamp   #Mark timestamp so if needed green time may be computed as rTimestamp-sTimestamp
                    break
                else:
                    currentCycle.rTimestamp = currentCycle.eTimestamp
                index = index+1 
        return newdf

def setSignalState(currentSignalState, eventParam, state):
    if (eventParam > 7):
        return

    if (state == 'green'):
        currentSignalState[eventParam] = 1
        currentSignalState[eventParam+8] = 0
    elif (state == 'red'):
        currentSignalState[eventParam] = 0
        currentSignalState[eventParam+8] = 1
    elif (state == 'end'):
        currentSignalState[eventParam+8] = 0
    return