# binge_drinking.py
# Anna Wong

import os
import csv
import time
import math
import random
import copy
from Tkinter import *
import tkMessageBox
import tkSimpleDialog
import sys
from scipy import stats
from math import radians, cos, sin, asin, sqrt
import datetime
from geopy.geocoders import Nominatim

################################################################################
# USAGE
"""
In order to extract features for accelerometer, gyroscope, blood-alcohol level,
calls, messages, and weather, all 6 of the raw data files must be in the same
directory, with the keywords AAC, Gyro, calls, messages, and weather respectively.
Enter the outermost directory name in the prompt, followed by segment size in seconds, 
window size in seconds, and weather segment in minutes. 
Approximate time for program to run:
Consolidate AAC- 10 min
Aggregate AAC- 1 min
Consolidate Gyro- 10 min
Aggregate Gyro- 1 min
Normalize EBAC- <1 min
Normalize weather <1 min
Agregate calls- <1 min
Aggregate message- <1min
Processing data- <1 min
Writing files- <1min
The files will write as one file per date. 
"""
################################################################################
accHeaderLine = ['day', 'time-of-day', 'acc-id', 'acc-timestamp', 'acc-device_id', 'acc-date_time', 
                    'acc-mag-min', 'acc-mag-max', 'acc-mag-stDev', 'acc-mag-median',  
                    'acc-mag-q1', 'acc-mag-q3', 'acc-mag-variance',
                    'acc-x-min', 'acc-x-max', 'acc-x-stDev', 'acc-x-median',
                    'acc-x-q1', 'acc-x-q3', 'aac-x-variance', 
                    'acc-y-min', 'acc-y-max', 'acc-y-stDev', 'acc-y-median',
                    'acc-y-q1', 'acc-y-q3', 'aac-y-variance', 
                    'acc-z-min', 'acc-z-max', 'acc-z-stDev', 'acc-z-median',
                    'acc-z-q1', 'acc-z-q3', 'aac-z-variance', 'acc-delta-min',
                    'acc-delta-max', 'acc-delta-stDev', 'acc-delta-median',    
                    'acc-delta-q1', 'acc-delta-q3', 'acc-delta-variance'] 
gyroHeaderLine = ['gyro-id', 'gyro-timestamp', 'gyro-device_id', 'gyro-date_time',
                    'gyro-mag-min', 'gyro-mag-max', 'gyro-mag-stDev', 
                    'gyro-mag-median', 'gyro-mag-q1', 'gyro-mag-q3', 'gyro-mag-variance',
                    'gyro-x-min', 'gyro-x-max', 'gyro-x-stDev', 'gyro-x-median',
                    'gyro-x-q1', 'gyro-x-q3', 'gyro-x-variance', 
                    'gyro-y-min', 'gyro-y-max', 'gyro-y-stDev', 'gyro-y-median',
                    'gyro-y-q1', 'gyro-y-q3', 'gyro-y-variance', 
                    'gyro-z-min', 'gyro-z-max', 'gyro-z-stDev', 'gyro-z-median',
                    'gyro-z-q1', 'gyro-z-q3', 'gyro-z-variance',    
                    'gyro-delta-min', 'gyro-delta-max', 'gyro-delta-stDev', 
                    'gyro-delta-median', 'gyro-delta-q1', 'gyro-delta-q3',
                    'gyro-delta-variance']
ebacHeaderLine = ['eBrAC', 'drink_code']
callHeaderLine = ['type-of-call','call-id']
messageHeaderLine = ['type-of-messages', 'message-id', 'number-of-messages']
weatherHeaderLine = ['timestamp', 'device_id', 'city', 'temp_max', 'temp_min', 
                    'weather_icon_id', 'weather_description', 'temperature',
                    'humidity', 'pressure', 'wind_speed', 'wind_degrees', 'cloudiness', 
                    'temperature-min', 'temperature-max', 'temperature-stDev', 'temperature-median',  
                    'temperature-q1', 'temperature-q3', 'temperature-variance',
                    'humidity-min', 'humidity-max', 'humidity-stDev', 'humidity-median',  
                    'humidity-q1', 'humidity-q3', 'humidity-variance',
                    'pressure-min', 'pressure-max', 'pressure-stDev', 'pressure-median',  
                    'pressure-q1', 'pressure-q3', 'pressure-variance',
                    'wind_speed-min', 'wind_speed-max', 'wind_speed-stDev', 'wind_speed-median',  
                    'wind_speed-q1', 'wind_speed-q3', 'wind_speed-variance',
                    'wind_degrees-min', 'wind_degrees-max', 'wind_degrees-stDev', 'wind_degrees-median',  
                    'wind_degrees-q1', 'wind_degrees-q3', 'wind_degrees-variance',
                    'cloudiness-min', 'cloudiness-max', 'cloudiness-stDev', 'cloudiness-median',  
                    'cloudiness-q1', 'cloudiness-q3', 'cloudiness-variance']
actHeaderLine = ['google-activity']
screenHeaderLine = ['screen-status','num-status-changes', 'count-off', 'count-on', 'time-off', 'time-on']
locHeaderLine = ['double_lat', 'double_long', 'dist_from_home', 'delta-dist', 'dist-home-min', 'dist-home-max', 
                    'dist-home-stDev', 'dist-home-median',  'dist-home-q1', 'dist-home-q3', 
                    'dist-home-variance','delta-dist-min', 'delta-dist-max', 'delta-dist-stDev', 
                    'delta-dist-median', 'delta-dist-q1', 'delta-dist-q3', 'delta-dist-variance',
                    'sig-loc']
################################################################################
# File IO functions
################################################################################
def listFiles(path):
    # from CMU course 15-112
    if (os.path.isdir(path) == False):
        # base case:  not a folder, but a file, so return singleton list with its path
        return [path]
    else:
        # recursive case: it's a folder, return list of all paths
        files = [ ]
        for filename in os.listdir(path):
            files += listFiles(path + "/" + filename)
        return files

def onlyCSV(files):
    result = []
    for item in files:
        if item.endswith('.csv'):
            result += [item]
    return result

# From CMU course 15-112, S15
def readFile(filename, mode="rt"):       # rt = "read text"
    with open(filename, mode) as fin:
        return fin.read()

def writeFile(fileName, headers, contents, mode="wt"):    # wt = "write text"
    with open(fileName, mode) as fout:
        writer = csv.writer(fout)
        writer.writerow(headers)
        for row in contents:
            writer.writerow(row)

################################################################################
# Statistics/ Calculating functions
################################################################################
def almostEqual(d1, d2, epsilon=10**-4):
    return (abs(d2 - d1) < epsilon)

def magnitude(x, y, z):
    x2 = x**2
    y2 = y**2
    z2 = z**2
    return math.sqrt(x2 + y2 + z2)

def calculateDelta(accel0, accel1):
    x = (accel1[0] - accel0[0]) ** 2
    y = (accel1[1] - accel0[1]) ** 2
    z = (accel1[2] - accel0[2]) ** 2
    return math.sqrt(x + y + z)

def quartiles(segmentAccel):
    sortSegAccel = sorted(segmentAccel)
    q1 = sortSegAccel[len(sortSegAccel) / 4]
    q3 = sortSegAccel[len(sortSegAccel) / 4 * 3]
    return (q1, q3)

def calcStats(segmentAccel):
    if segmentAccel == []:
        return ['no_val']*7
    # need to calculate:
    #'min', 'max', 'stDev', 'median', 'q1', 'q3', 'varaince'
    statistics = []
    # min
    statistics.append(min(segmentAccel))
    # max
    statistics.append(max(segmentAccel))
    # standard deviation
    if len(segmentAccel) == 1:
        statistics.append(0)
    else:
        statistics.append(stats.tstd(segmentAccel))
    # median
    statistics.append(sorted(segmentAccel)[len(segmentAccel)/2])
    # q1 and q3
    statistics.extend(quartiles(segmentAccel))
    # variance
    if len(segmentAccel) == 1:
        statistics.append(0)
    else:
        statistics.append(stats.tvar(segmentAccel))
    return statistics

def normalizeDateTime(date):
    month = date[:date.index('/')]
    date = date[date.index('/')+1:]
    day = date[:date.index('/')]
    date = date[date.index('/')+1:]
    year = '20' + date[:date.index(' ')]
    date = date[date.index(' ')+1:]
    hour = date[:date.index(':')]
    if len(hour) < 2:
        hour = '0' + hour
    minute = date[date.index(':')+1:]
    dateTime = year + '-' + month + '-' + day + ' ' + hour + ':' + minute
    return dateTime

################################################################################
# Accelerometer/ gyroscope functions
################################################################################
def getAverageXYZ(timeX, timeY, timeZ):
    # Function is given three arrays of x, y, z values
    # returns the averages, as a list
    try:
        x = sum(timeX) / len(timeX)
    except:
        x = 0
    try:
        y = sum(timeY) / len(timeY)
    except:
        y = 0
    try:
        z = sum(timeZ) / len(timeZ)
    except:
        z = 0
    return [x, y, z]

def consolidate(fil):
    final = []
    headers = ['id', 'timestamp', 'device_id', 'double_value_0', 
                'double_value_1', 'double_value_2', 'accuracy', 'date_time']
    contents = readFile(fil)
    contentsLines = contents.splitlines()
    contentsLines.pop(0)
    time = contentsLines[0].split(',')[8]
    time = time[1:len(time)-1]
    timeX = []
    timeY = []
    timeZ = []
    for line in contentsLines:
        lineList = line.split(',')
        if len(lineList) < 8:
            break
        curTime = lineList[8][1:len(lineList[8])-1]
        if curTime != time:
            avgXYZ = getAverageXYZ(timeX, timeY, timeZ)
            timeX = []
            timeY = []
            timeZ = []
            newLine = prevLine[:3] + avgXYZ + prevLine[3:] + [time]
            final.append(newLine)
            #print 'Finished consolidating for time: ', time
            time = curTime
        else:
            timeX.append(float(lineList[3][1:len(lineList[3])-1]))
            timeY.append(float(lineList[4][1:len(lineList[4])-1]))
            timeZ.append(float(lineList[5][1:len(lineList[5])-1]))
            prevLine = []
            prevLine.append(lineList[0][1:len(lineList[0])-1])
            prevLine.append(lineList[1][1:len(lineList[1])-1])
            prevLine.append(lineList[2][1:len(lineList[2])-1])
            prevLine.append(lineList[6][1:len(lineList[6])-1])
            prevLine.append(lineList[7][1:len(lineList[7])-1])
    # Returns one instance per second array of list data
    return final

def aggegateData(consolData, segment, window, fileName):
    newContents = []
    contentsLines = consolData
    idLine = ['id', 'timestamp', 'device_id', 'date_time'] 
    magStats = ['mag-min', 'mag-max', 'mag-stDev', 'mag-median', 'mag-q1', 
                'mag-q3', 'mag-variance']
    deltaStats = ['delta-min', 'delta-max', 'delta-stDev', 'delta-median', 
                'delta-q1', 'delta-q3', 'delta-variance']
    headerLine = idLine + magStats + deltaStats
    for seg in xrange(0, len(contentsLines), segment):
        segmentLine = contentsLines[seg]
        magnitudes = []
        deltas = []
        xSeg = []
        ySeg = []
        zSeg = []
        if seg+segment > len(contentsLines):
            end = len(contentsLines)-1
        else:
            end = seg+segment
        endLine = contentsLines[end]
        win0 = seg
        try:
            x0 = float(segmentLine[3])
        except:
            x0 = float(segmentLine[3][1:len(segmentLine[3])-1])
        try:
            y0 = float(segmentLine[4])
        except:
            y0 = float(segmentLine[4][1:len(segmentLine[4])-1])
        try:
            z0 = float(segmentLine[5])
        except:
            z0 = float(segmentLine[5][1:len(segmentLine[5])-1])
        for win in xrange(seg, end):
            windowLine = contentsLines[win]
            try:
                x1 = float(windowLine[3])
            except:
                x1 = float(windowLine[3][1:len(windowLine[3])-1])
            try:
                y1 = float(windowLine[4])
            except:
                y1 = float(windowLine[4][1:len(windowLine[4])-1])
            try:
                z1 = float(windowLine[5])
            except:
                z1 = float(windowLine[5][1:len(windowLine[5])-1])
            if win == win0 + window:
                # then calculate the delta
                delta = calculateDelta([x0, y0, z0], [x1, y1, z1])
                deltas.append(delta)
                win0 = win
                x0 = x1
                y0 = y1
                z0 = z1
            magnitudes.append(magnitude(x1, y1, z1))
            xSeg.append(x1)
            ySeg.append(y1)
            zSeg.append(z1)
        newLine = []
        newLine.append(segmentLine[0])
        newLine.append(segmentLine[1])
        newLine.append(segmentLine[2])
        timeInterval = segmentLine[8][:16]
        newLine.append(timeInterval)
        newLine.extend(calcStats(magnitudes))
        newLine.extend(calcStats(xSeg))
        newLine.extend(calcStats(ySeg))
        newLine.extend(calcStats(zSeg))
        newLine.extend(calcStats(deltas))
        newContents.append(newLine)
    writeFile(fileName, [], newContents)
    return newContents

################################################################################
# Blood alcohol functions
################################################################################
def normalizeEBAC(fil):
    contents = readFile(fil)
    contentsLines = contents.splitlines()
    contentsLines.pop(0)
    newContents = dict()
    dates = []
    for line in contentsLines:
        lineList = line.split(',')
        date = lineList[1]
        if lineList[2] != '':
            eBACLevel = lineList[2]
        else:
            eBACLevel = lineList[3]
        month = date[:date.index('/')]
        date = date[date.index('/')+1:]
        day = date[:date.index('/')]
        date = date[date.index('/')+1:]
        year = '20' + date[:date.index(' ')]
        date = date[date.index(' ')+1:]
        hour = date[:date.index(':')]
        if len(hour) < 2:
            hour = '0' + hour
        minute = date[date.index(':')+1:]
        dateTime = year + '-' + month + '-' + day + ' ' + hour + ':' + minute
        if dateTime not in newContents:
            newContents[dateTime] = float(eBACLevel)
            dates.append(dateTime)
        else:
            if newContents[dateTime] == '' or newContents[dateTime] == 0:
                newContents[dateTime] = float(eBACLevel)
    finalContents = []
    for date in dates:
        finalContents.append([date, newContents[date]])
    # returns one instance per second array of list data
    return finalContents

################################################################################
# Weather functions
################################################################################
def normalizeWeather(fileName):
    contents = readFile(fileName)
    contentsLines = contents.splitlines()
    newContents = []
    curDate = contentsLines[0].split(',')[15][1:17]
    # temperature, humidity, pressure, windSpeed, windDegrees, cloud 
    weatherInfo = []
    for line in contentsLines:
        lineList = line.split(',')
        if len(lineList) < 16:
            break
        date = lineList[15][1:17]
        if curDate != date: 
            for i in xrange(len(weatherInfo[0])):
                avg = 0
                for j in xrange(len(weatherInfo)):
                    avg += float(weatherInfo[j][i][1:-1])
                avg /= len(weatherInfo)
                prevLine.append(avg)
            prevLine.append(curDate)
            curDate = date
            newContents.append(prevLine)
            weatherInfo = []    
        prevLine = [lineList[0][1:-1]]
        prevLine.append(lineList[1][1:-1])
        prevLine.append(lineList[2][1:-1])
        prevLine.append(lineList[3][1:-1])
        prevLine.append(float(lineList[5][1:-1]))
        prevLine.append(float(lineList[6][1:-1]))
        prevLine.append(lineList[13][1:-1])
        prevLine.append(lineList[14][1:-1])
        weather = [lineList[4]]
        weather.append(lineList[8])
        weather.append(lineList[9])
        weather.append(lineList[10])
        weather.append(lineList[11])
        weather.append(lineList[12])
        weatherInfo.append(weather)
    return newContents

def aggregateWeather(weatherInfo, weatherStart):
    # Takes in a 2d list of ALL data from a line... (AAC, Gyro, etc)
    # Filter out all un needed data
    # Calculate statistics
    # will be an array of size 36
    weatherStats = []
    weatherData = []
    for line in weatherInfo:
        weatherData.append(line[weatherStart:]) # TURNIP
    for i in xrange(len(weatherData[0])):
        weathers = []
        for j in xrange(len(weatherData)):
            weathers.append(weatherData[j][i])
        weatherStats.extend(calcStats(weathers)) 
    return weatherStats

################################################################################
# Google activity functions
################################################################################
def normalizeActivity(fileName):
    contents = readFile(fileName)
    contentsLines = contents.splitlines()
    newContents = dict()
    for line in contentsLines:
        lineList = line.split(',')
        date = lineList[len(lineList)-1][:-4].strip('"')
        activity = lineList[3].strip('"')
        if date in newContents:
            newContents[date].append(activity)
        else:
            newContents[date] = [activity]
    return newContents

################################################################################
# Screen status functions
################################################################################
def normalizeScreen(fileName):
    contents = readFile(fileName)
    contentsLines = contents.splitlines()
    newContents = dict()
    prevDate = ''
    screenUse = []
    screenChanges = 0
    for line in contentsLines:
        lineList = line.split(',')
        date = lineList[4]
        if date != prevDate:
            if screenUse != []:
                lastScreen = screenUse[-1]
                normDate = normalizeDateTime(prevDate)
                newLine = [int(lastScreen)]
                newLine.append(screenChanges)
                screenOff = screenUse.count('0') + screenUse.count('2')
                screenOn = len(screenUse) - screenOff
                newLine.extend([screenOff, screenOn])
                if screenOn >= screenOff:
                    newLine.extend([0, 1])
                else:
                    newLine.extend([1, 0])
                newContents[normDate] = newLine
            prevDate = date
            screenUse = [lineList[3]]
            screenChanges = 0
        else:
            screen = lineList[3]
            if screenUse != [] and screen != screenUse[-1]:
                screenChanges += 1
            screenUse.append(screen)
    return newContents

################################################################################
# Location functions
################################################################################
def lazyHash(key, mode):
    if mode == 'hash':
        return repr(key)
    else:
        lat = float(key[1:key.index(',')])
        lon = float(key[key.index(' ')+1:len(key)-1])
        return (lat, lon)

def calcMidpoint(prevLoc, curLoc):
    lat0, lon0 = prevLoc[0]*math.pi/180.0, prevLoc[1]*math.pi/180.0
    lat1, lon1 = curLoc[0]*math.pi/180.0, curLoc[1]*math.pi/180.0
    totweight = 2191.5
    x0 = math.cos(lat0) * math.cos(lon0)
    y0 = math.cos(lat0) * math.sin(lon0)
    z0 = math.sin(lat0)
    x1 = math.cos(lat1) * math.cos(lon1)
    y1 = math.cos(lat1) * math.sin(lon1)
    z1 = math.sin(lat1)
    x = (x0 + x1) / totweight
    y = (y0 + y1) / totweight
    z = (z0 + z1) / totweight
    lon = math.atan2(y, x) * 180.0 / math.pi
    hyp = math.sqrt(x**2 + y**2)
    lat = math.atan2(z, hyp) * 180.0 / math.pi
    return [lat, lon]

def calcMidpoints(locationStart, locationEnd, locsToCalc):
    half = locsToCalc / 2
    midpoint = calcMidpoint(locationStart, locationEnd)
    if locsToCalc < 1 :
        return [midpoint]
    return calcMidpoints(locationStart, midpoint, half) + [midpoint] + calcMidpoints(midpoint, locationEnd, half)

def haversine(lat1, lon1, lat2, lon2, r = 637100):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    # Radius of earth in meters. Use 3956 for miles
    return c * r

def detectHome(locationContents):
    days = dict()
    dates = []
    for line in locationContents:
        dateTime = line[0]
        date = dateTime[dateTime.index('-')+1:dateTime.index(' ')]
        time = dateTime[dateTime.index(' ')+1:]
        # Check if in night time
        if int(time[:2]) <= 5:
            if date in days:
                days[date].append(line[1:])
            else:
                dates.append(date)
                days[date] = [line[1:]]
    # get clusters for the first day, use as reference for rest of days
    day1 = days[dates[0]]
    lat0, lon0 = day1[0][0], day1[0][1]
    posHomes = findClusters(lat0, lon0, day1)
    # compare rest of days to clusters found in first day
    for date in xrange(1, len(dates)):
        locationList = days[dates[date]]
        lat0, lon0 = locationList[0][0], locationList[0][1]
        clusters = findClusters(lat0, lon0, locationList)
        for cluster in clusters:
            location0 = lazyHash(cluster, 'unhash')
            for home in posHomes:
                location1 = lazyHash(home, 'unhash')
                dist = haversine(location0[0], location0[1], location1[0], location1[1])
                try:
                    if dist < closestDist:
                        closestDist = dist
                        closestLoc = location1
                except:
                    closestDist = dist
                    closestLoc = location1
            posHomes[lazyHash(closestLoc, 'hash')] += 1
            closestDist = None
    maxLocs = 0
    home = ''
    for loc in posHomes:
        if posHomes[loc] > maxLocs:
            home = loc
    return lazyHash(home, 'unhash')

def detectSigLocations(fileName):
    # for each latitude and longitude, loop through the entire file to find 
    # lats and lons which are 20 meters or less away, add them to the list
    # take the mean of that group and use that as a key
    contents = readFile(fileName)
    contentsLines = contents.splitlines()
    contentsLines.pop(0)
    clusters = dict() # the keys and lists of lats and lons
    addresses = dict()
    while len(contentsLines) > 0:
        nonClusters = [] # if a lat and lon more than 20 meters away, then store the line here
        lats, lons = [], [] # for the lats and lons in the clusters
        startLine = contentsLines[0].split(',')
        startLat = float(startLine[3])
        startLon = float(startLine[4])
        for line in contentsLines:
            lineList = line.split(',')
            lat = float(lineList[3])
            lon = float(lineList[4])
            dist = haversine(startLat, startLon, lat, lon)
            if dist < 200:
                lats.append(lat)
                lons.append(lon)
            else:
                nonClusters.append(line)
            startLat = sum(lats) / len(lats)
            startLon = sum(lons) / len(lons)
        key = lazyHash((startLat, startLon), 'hash')
        clusters[key] = (lats, lons)
        contentsLines = nonClusters
    newContents = dict()
    clusterKeys, clusterSizes = [], []
    for key in clusters:
        clusterKeys.append(key)
        clusterSizes.append(len(clusters[key][0]))
    name = 97
    fil = []
    while len(newContents) < 5:
        index = clusterSizes.index(max(clusterSizes))
        key = clusterKeys[index]
        newContents[key] = chr(name)
        location = lazyHash(key, 'unhash')
        name += 1
        clusterKeys = clusterKeys[:index] + clusterKeys[index+1:]
        clusterSizes = clusterSizes[:index] + clusterSizes[index+1:]
    return newContents

def findClusters(lat0, lon0, locationList): 
    clusters = dict()
    lats, lons = [], []
    for index in xrange(1, len(locationList)):
        lat1, lon1 = locationList[index][0], locationList[index][1]
        dist = haversine(lat0, lon0, lat1, lon1)
        if dist > 200:
            if len(lats) > 1:
                key = lazyHash((sum(lats)/len(lats), sum(lons)/len(lons)), 'hash')
                if key in clusters:
                    clusters[key] += 1
                else:
                    clusters[key] = 1
            lats, lons = [], []
        lats.append(lat1)
        lons.append(lon1)
        lat0, lon0 = lat1, lon1
    key = lazyHash((sum(lats)/len(lats), sum(lons)/len(lons)), 'hash')
    if key in clusters:
        clusters[key] += 1
    else:
        clusters[key] = 1
    return clusters

def normalizeLocations(fileName):
    contents = readFile(fileName)
    contentsLines = contents.splitlines()
    contentsLines.pop(0)
    newContents = []
    curDate = normalizeDateTime(contentsLines[0].split(',')[11])
    lats, lons = [], []
    for line in contentsLines:
        lineList = line.split(',')
        lat, lon = float(lineList[3]), float(lineList[4])
        date = normalizeDateTime(lineList[11])
        if date != curDate:
            newLine = []
            newLine.append(curDate)
            newLine.append(sum(lats)/len(lats))
            newLine.append(sum(lons)/len(lons))
            newContents.append(newLine)
            lats, lons = [], []
            curDate = date
        lats.append(lat)
        lons.append(lon)
    return newContents

################################################################################
# Call functions
################################################################################
def aggregateCalls(fileName, timeSegment):
    contents = readFile(fileName)
    contentsLines = contents.splitlines()
    newContents = []
    contentsLists = []
    contacts = dict()
    totTalkTime = 0 
    for line in contentsLines:
        lineList = line.split(',')
        contentsLists.append(lineList)
        contact = lineList[5].strip('"')
        if contact in contacts:
            contacts[contact] += int(lineList[4].strip('"'))
        else:
            contacts[contact] = int(lineList[4].strip('"'))
        totTalkTime += int(lineList[4].strip('"'))
    topTen = []
    aliases = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    while len(topTen) < 10:
        maxCall, maxContact = 0, ''
        for contact in contacts:
            if contacts[contact] > maxCall and contact not in topTen:
                maxCall = contacts[contact]
                maxContact = contact
        topTen.append(maxContact)
    for call in contentsLists:
        if call[5].strip('"') in topTen:
            newLine = [call[6].strip('"')] # date time
            newLine.append(int(call[3].strip('"'))) # type of call
            newLine.append(int(call[4].strip('"'))/timeSegment + 1) # duration in multiples of time segment
            newLine.append(aliases[topTen.index(call[5].strip('"'))]) # alias id
            newContents.append(newLine)
    finalContents = []
    previous = newContents[0]
    newLine = copy.copy(newContents[0])
    for index in xrange(1, len(newContents)):
        current = newContents[index]
        if previous[0] == current[0]:
            if current[3] not in newLine[3]:
                newLine[1] = 4
            if newLine[2] > current[2]:
                newLine[3] += current[3]
            else:
                newLine[3] = current[3] + newLine[3]
        else:
            finalContents.append(newLine)
            previous = current
            newLine = copy.copy(newContents[index])
    return finalContents

################################################################################
# Message functions
################################################################################
def aggregateMessages(fileName, timeSegment):
    contents = readFile(fileName)
    contentsLines = contents.splitlines()
    newContents = []
    contentsLists = []
    contacts = dict()
    for line in contentsLines:
        lineList = line.split(',')
        contentsLists.append(lineList)
        contact = lineList[4][1:-1]
        if contact in contacts:
            contacts[contact] += 1
        else:
            contacts[contact] = 1
    topTen = []
    aliases = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    while len(topTen) < 10:
        maxMessages, maxContact = 0, ''
        for contact in contacts:
            if contact not in topTen and contacts[contact] > maxMessages:
                maxMessages = contacts[contact]
                maxContact = contact
        topTen.append(maxContact)
    for line in contentsLists:
        contact = line[4][1:-1]
        if contact in topTen:
            newLine = [line[5][1:-4]] # date
            newLine.append(int(line[3][1:-1])) # type of message
            newLine.append(aliases[topTen.index(contact)]) # alias id
            newContents.append(newLine)
    # combine duplicates
    finalContents = []
    previous = newContents[0]
    newLine = copy.copy(newContents[0])
    count = 1
    for index in xrange(1, len(newContents)):
        current = newContents[index]
        if previous[0] == current[0]:
            if previous[1] != current[1]:
                newLine[1] = 3
            if current[2] not in newLine[2]:
                newLine[2] += current[2]
            count += 1
        else:
            newLine.append(count)
            finalContents.append(newLine)
            count = 1
            previous = current
            newLine = copy.copy(newContents[index])
    return finalContents

################################################################################
# Aggregating all features function
################################################################################
def aggregateAll(folderName, aggFolder, segment, window, weatherSegment):
    # the parameter folderName must have the raw data for AAC, Gyro, EBAC
    newContents = []
    global headerLine 
    headerLine = []
    allFiles = listFiles(folderName)
    files = onlyCSV(allFiles)
    locationStart = 0
    weatherStart = 0
    acc = False
    gyro = False
    weather = False
    location = False
    call = False
    message = False
    activity = False
    screen = False
    rawEBAC = False
    for i in xrange(len(files)):
        if 'ACC' in files[i]:
            rawAAC = files[i]
        elif 'gyro' in files[i]:
            rawGyro = files[i]
        elif 'weather' in files[i]:
            weather = True
            #weather data
            print 'Normalizing weather...'
            weatherContents = normalizeWeather(files[i])
            weatherDict = dict()
            weatherIndex = 0
            for i in xrange(len(weatherContents)):
                weatherDict[weatherContents[i][14]] = i
        elif 'location' in files[i]:
            location = True
            # Location data
            print 'Normalizing location...'
            locationContents = normalizeLocations(files[i])
            home = detectHome(locationContents)
            sigLocs = detectSigLocations(files[i])
        elif 'call' in files[i]:
            call = True
             # Call Data
            print 'Aggregating calls...'
            callContents = aggregateCalls(files[i], segment)
        elif 'message' in files[i]:
            message = True
            # Message Data
            print 'Aggregating messages...'
            messageContents = aggregateMessages(files[i], segment)
        elif 'activity' in files[i]:
            activity = True
            # Google activity data
            print 'Normalizing activity...'
        elif 'screen' in files[i]:
            screen = True
            # Screen usage data
            print 'Normalizing screen...'
            screenContents = normalizeScreen(files[i])
        else:
            rawEBAC = True
            # EBAC data
            abDict = dict()
            print 'Normalizing EBAC...'
            abContents = normalizeEBAC(files[i])
            abContents.pop(0)
            for line in abContents:
                lineList = line
                abDict[lineList[0]] = lineList[1]
    if aggFolder != None:
        aggFiles = onlyCSV(listFiles(aggFolder))
        # AACData
        acc = True
        contents = readFile(aggFiles[0])
        consol = contents.splitlines()
        consol.pop(0)
        consolAAC = []
        print 'Aggregating AAC...'
        for line in consol:
            consolAAC.append(line.split(','))
        AACContents = aggegateData(consolAAC, segment, window, 'aac.csv')
        # GyroData
        gyro = True
        rawGyro = rawGyro 
        contents = readFile(aggFiles[1])
        consol = contents.splitlines()
        consol.pop(0)
        consolGyro = []
        print 'Aggregating Gyro...'
        for line in consol:
            consolGyro.append(line.split(','))
        GyroContents = aggegateData(consolGyro, segment, window, 'gyro.csv')   
    else:
        acc = True
        print 'Consolidating AAC...'
        consolAAC = consolidate(rawAAC)
        print 'Aggregating AAC...'
        AACContents = aggegateData(consolAAC, segment, window)
        gyro = True
        print 'Consolidating Gyro...'
        consolGyro = consolidate(rawGyro)
        print 'Aggregating Gyro...'
        GyroContents = aggegateData(consolGyro, segment, window)    
    weekdays = ['M', 'T', 'W', 'H', 'F', 'S', 'U'] 
    dates = []
    newContent = []
    if acc == True:
        headerLine.extend(accHeaderLine)
        locationStart += len(accHeaderLine)
        weatherStart += len(accHeaderLine)
    if gyro == True:
        headerLine.extend(gyroHeaderLine)
        locationStart += len(gyroHeaderLine)
        weatherStart += len(gyroHeaderLine)
    if rawEBAC == True:
        headerLine.extend(ebacHeaderLine)
        locationStart += len(ebacHeaderLine)
        weatherStart += len(ebacHeaderLine)
    if call == True:
        headerLine.extend(callHeaderLine)
        locationStart += len(callHeaderLine)
        weatherStart += len(callHeaderLine)
    if message == True:
        headerLine.extend(messageHeaderLine)
        locationStart += len(messageHeaderLine)
        weatherStart += len(messageHeaderLine)
    if weather == True:
        headerLine.extend(weatherHeaderLine)
        locationStart += len(weatherHeaderLine)
        weatherStart += 7
    if activity == True:
        headerLine.extend(actHeaderLine)
        activityContents = normalizeActivity(files[i])
        locationStart += len(actHeaderLine)
    if screen == True:
        headerLine.extend(screenHeaderLine)
        locationStart += len(screenHeaderLine)
    if location == True:
        headerLine.extend(locHeaderLine)
    print 'Processing data...'
    for line in xrange(len(AACContents)):
        time = AACContents[line][3][11:]
        hour = int(time[:2])
        if hour <= 5:
            timeOfDay = 'N'
        elif hour <= 11:
            timeOfDay = 'M'
        elif hour <= 18:
            timeOfDay = 'A'
        else:
            timeOfDay = 'E'
        year = int(AACContents[line][3][:4])
        month = int(AACContents[line][3][5:7])
        day = int(AACContents[line][3][8:10])
        dayIndex = datetime.date(year, month, day).weekday()
        dayOfWeek = weekdays[dayIndex] 
        newLine = []
        newLine.extend([dayOfWeek, timeOfDay])
        date = AACContents[line][3][5:10]
        newLine.extend(AACContents[line])
        # Gyro Data
        gyroExtend = ['no_val'] * 39
        for i in xrange(len(GyroContents)):
            if GyroContents[i][3] == AACContents[line][3]:
                gyroExtend = (GyroContents[i])
        newLine.extend(gyroExtend)
        # EBAC data
        if rawEBAC == True:
            if AACContents[line][3] in abDict:
                ab = abDict[AACContents[line][3]]
                if ab == None or ab == '' or ab == -1.0:
                    code = 'N/A'
                elif float(ab) >= 0.08:
                    code = 'BD'
                elif float(ab) >= 0.008:
                    code = 'D'
                else:
                    code = 'N'
                newLine.extend([ab, code])
            else:
                newLine.extend([0, 'N'])
        # Call Data
        if call == True:
            if callContents != []:
                if callContents[0][0] == AACContents[line][3]:
                    newLine.extend([callContents[0][1], callContents[0][3]])
                    callContents[0][2] = -callContents[0][2]
                    callContents[0][2] += 1
                    if len(callContents[0][3]) > 1:
                        callContents[0][3] = callContents[0][3][0]
                elif callContents[0][2] < 0:
                    newLine.extend([callContents[0][1], callContents[0][3]])
                    callContents[0][2] += 1
                else:
                    newLine.extend([0, 0])
                if callContents[0][2] == 0:
                    callContents.pop(0)
            else:
                newLine.extend([0, '0'])
        # Message Data
        if message == True:
            if messageContents != []:
                if messageContents[0][0] == AACContents[line][3]:
                    newLine.extend(messageContents[0][1:])
                    messageContents.pop(0)
                else:
                    newLine.extend([0, '0', 0])
            else:
                newLine.extend([0, '0', 0])
        # Weather data
        if weather == True:
            if AACContents[line][3] in weatherDict:
                newLine.extend(weatherContents[weatherDict[AACContents[line][3]]][1:-1])
                weatherIndex = weatherDict[AACContents[line][3]]
            else:
                newLine.extend(weatherContents[weatherIndex][1:-1])       
            if date not in dates:
                dates.append(date)
        newContents.append(newLine)
    # Because of the way that the weather data was gathered
    # we will find the stats at the end
    if weather == True:
        print 'Aggregating weather...'
        for start in xrange(0, len(newContents), weatherSegment):
            if start + weatherSegment >= len(newContents):
                end = len(newContents)
            else:
                end = start + weatherSegment
            weatherInfo = newContents[start:end]
            weatherStats = aggregateWeather(weatherInfo, weatherStart)
            for seg in xrange(start, end):
                newContents[seg].extend(weatherStats)
    # Activity data
    if activity == True:
        print 'Aggregating activity...'
        for line in newContents:
            if line[5] in activityContents:
                if len(activityContents[line[5]]) > 1:
                    activities = []
                    count = []
                    for item in activityContents[line[5]]:
                        if item in activities:
                            count[activities.index(item)] += 1
                        else:
                            activities.append(item)
                            count.append(1)
                    activity = activities[count.index(max(count))]
                else:
                    activity = activityContents[line[5]][0]
                line.append(activity)
            else:
                line.append('unknown')
    # Screen Data
    if screen == True:
        print 'Aggregating screen...'
        prevScreen = None
        while prevScreen == None:
            for line in newContents:
                if line[5] in screenContents:
                    prevScreen = screenContents[line[5]]
        for line in newContents:
            if line[5] in screenContents:
                line.extend(screenContents[line[5]])
                prevScreen = screenContents[line[5]]
            else:
                lastScreen = prevScreen[0]
                if lastScreen == 0 or lastScreen == 2:
                    line.extend([0, 0, 1, 0, 1, 0])
                else:
                    line.extend([1, 0, 0, 1, 0, 1])
    # Location data
    if location == True:
        print 'Aggregating location...'
        startIndex = 0
        startLoc = (locationContents[0][1], locationContents[0][2])
        for index in xrange(len(newContents)):
            for jIndex in xrange(len(locationContents)):
                if newContents[index][5] == locationContents[jIndex][0]:
                    lat, lon = locationContents[jIndex][1], locationContents[jIndex][2]
                    distanceFromHome = haversine(lat, lon, home[0], home[1])
                    newContents[index].extend([lat, lon, distanceFromHome])
                    if index - startIndex > 1:
                        midpoints = calcMidpoints(startLoc, (lat, lon), index-startIndex-1)
                        for i in xrange(startIndex+1, index):
                            distFromHome = haversine(midpoints[0][0], midpoints[0][1], home[0], home[1])
                            newContents[i].extend([midpoints[0][0], midpoints[0][1], distFromHome])
                            midpoints.pop(0)
                    startLoc = (lat, lon)
                    startIndex = index
        for seg in xrange(0, len(newContents), segment):
            if seg + segment >= len(newContents):
                end = len(newContents) - 1
            else:
                end = seg + segment
            win0 = seg
            distsFromHome = []
            deltas = []
            for win in xrange(seg, end, window):
                if len(newContents[win]) >= locationStart+3 and len(newContents[win0]) >= locationStart+3:
                    distsFromHome.append(newContents[win][151])
                    if win0 + window == win:
                        lat0, lon0 = newContents[win0][locationStart], newContents[win0][locationStart +1]
                        lat1, lon1 = newContents[win][locationStart], newContents[win][locationStart+1]
                        deltaDist = haversine(lat0, lon0, lat1, lon1)
                        deltas.append(deltaDist)
                        newContents[win].append(deltaDist)
                        win0 = win
                    else:
                        newContents[win].append(0)
                else:
                    newContents[win].append(0)
            homeStats = calcStats(distsFromHome)
            deltaDistStats = calcStats(deltas)
            for entry in xrange(seg, end):
                newContents[entry].extend(homeStats)
                newContents[entry].extend(deltaDistStats)
                lat0, lon0 = float(newContents[entry][locationStart]), float(newContents[entry][locationStart+1])
                sigs = ''
                for sig in sigLocs:
                    location1 = lazyHash(sig, 'unhash')
                    lat1, lon1 = location1[0], location1[1]
                    dist = haversine(lat0, lon0, lat1, lon1)
                    if dist < 200:
                        sigs += sigLocs[sig]
                if sigs != '':
                    newContents[entry].append(sigs)
                else:
                    newContents[entry].append(0)
    return newContents, dates

################################################################################
# Writing files functions
################################################################################
def deleteVals(newContents):
    # Deleting no vals
    print 'Cleaning data...'
    files = dict()
    deletedVals = []
    for line in newContents:
        if 'no_val' in line or 'N/A' in line:
            deletedVals.append(line)
        else:
            date = line[5][line[5].index('-')+1:line[5].index(' ')]
            if date in files:
                files[date].append(line)
            else:
                files[date] = [line]
    return files, deletedVals

def oneMin(newContents):
    files, deletedVals = deleteVals(newContents)
    for fil in files:
        writeFile(fil+'.csv', headerLine, files[fil])
    writeFile('deletedVals.csv', headerLine, deletedVals)
    print 'Process complete.'

def largeTimeSeg(largeTime, newContents, dates):
    # if segment is larger than 60 then we have to aggregate further
    # Pick one and pick dominant for string things
    headers = headerLine[:142] + ['mostComTime', 'numAct', 'deltaActivity'] + headerLine[142:]
    headers = headers[0:2] + headers[6:41] + headers[45:80] + headers[82:87] + headers[90:-1] + [headers[81]]
    files, deletedVals = deleteVals(newContents)
    for date in files:
        newContents = []
        if date != '12-22':
            contents = files[date]
            start = 0
            startHours = int(contents[start][5][contents[start][5].index(' ')+1:contents[start][5].index(':')])
            startMin = int(contents[start][5][contents[start][5].index(':')+1:])
            startMins = startHours*60 + startMin
            endMin = (startMins + largeTime)
            instanceCount = 0
            segmentLines = []
            for index in xrange(len(contents)):
                line = contents[index]
                curHour = int(line[5][line[5].index(' ')+1:line[5].index(':')])
                curMin = int(line[5][line[5].index(':')+1:])
                curMins = curHour*60 + curMin
                if curMins >= endMin:
                    newLine = []
                    if index > 0 and segmentLines == []:
                        newContents.append(['no_val']*len(headerLine))
                    else:
                        for i in xrange(len(contents[start])):
                            if i == 5 or i == 44:
                                item0 = contents[start][5][:contents[start][5].index(' ')] + ' '
                                if startHours < 10:
                                    item0 += '0'
                                item0 += str(startHours) + ':' 
                                if startMins%60 < 10:
                                    item0 += '0'
                                item0 += str(startMins%60) + '-'
                                if (endMin-1)%60 < 10:
                                    item0 += '0'
                                item0 += str((endMin-1)%60)
                            elif i == 142:
                                activities, activitiesCount = [], []
                                deltaActivity = 0
                                for j in xrange(start, index):
                                    activity = contents[j][i]
                                    if activity in activities:
                                        activitiesCount[activities.index(activity)] += 1
                                    else:
                                        activities.append(activity)
                                        activitiesCount.append(1)
                                    try:
                                        if prevActivity != activity:
                                            deltaActivity += 1
                                            prevActivity = activity
                                    except:
                                        prevActivity = activity
                                mostComTime = max(activitiesCount)
                                mostComAct = activities[activitiesCount.index(mostComTime)]
                                numAct = len(activities)
                                actStats = [mostComTime, numAct, deltaActivity]
                                newLine.extend(actStats)
                                item0 = mostComAct
                            elif i == 143:
                                screenStatus, screenCount = [], []
                                for j in xrange(start, index):
                                    if contents[j][i] not in screenStatus:
                                        screenStatus.append(contents[j][i])
                                        screenCount.append(1)
                                    else:
                                        screenCount[screenStatus.index(contents[j][i])] += 1
                                item0 = screenStatus[screenCount.index(max(screenCount))]
                            elif i >= 144 and i <= 148:
                                item0 = 0
                                for j in xrange(start, index):
                                    item0 += contents[j][i]
                            elif i == 167:
                                item0 = 0
                                for j in xrange(start, index):
                                    if type(contents[j][i]) == str:
                                        item0 = contents[j][i]
                            elif type(contents[start][i]) == str:
                                items = []
                                count = []
                                for j in xrange(start, index):
                                    if contents[j][i] in items:
                                        count[items.index(contents[j][i])] += 1
                                    else:
                                        items.append(contents[j][i])
                                        count.append(1)
                                item0 = items[count.index(max(count))]
                            else:
                                info = []
                                for j in xrange(start, index):
                                    item = (contents[j][i])
                                    info.append(item)
                                item0 = sum(info) / len(info)
                                if i == 80:
                                    if almostEqual(item0, 0):
                                        item0 = 0
                            newLine.append(item0)
                        newLine.append(instanceCount)
                        newContents.append(newLine)
                        segmentLines = [line]
                        start = index
                        startMins = endMin
                        startHours = startMins / 60
                        startMin = startMins % 60
                        endMin = (startMins + largeTime)
                        instanceCount = 1
                else:
                    segmentLines.append(line)
                    instanceCount += 1                   
            files[date] = newContents
    for key in files:
        newContents = []
        contents = files[key]
        for line in contents:
            if 'no_val' in line:
                deletedValues.append(line)
            else:
                # newLine = line[0:2] + line[6:41] + line[45:80] + line[82:87] + line[90:] + [line[81]]
                newContents.append(line)
        files[key] = newContents
    # print 'Writing files...'
    # for key in files:
    #     writeFile(key+str(largeTime)+'.csv', headers, files[key])
    writeFile(str(largeTime)+'deletedVals.csv', headerLine, deletedVals)
    return files
    print 'Process complete.'

def oneHourOut(newContents, dates):
    files, deletedVals = deleteVals(newContents)
    for i in xrange(24):
        testContents, trainContents = [], []
        for date in dates:
            if date != '12-17' and date != '12-22':
                if i < 10:
                    hour = '0' + str(i)
                else:
                    hour = str(i)
                for line in files[date]:
                    if hour in line[5][line[5].index(' '):line[5].index(':')]:
                        testContents.append(line)
                    else:
                        trainContents.append(line)
        print 'Writing files...'
        writeFile('Test'+hour+'.csv', headerLine, testContents)
        writeFile('Train'+hour+'.csv', headerLine, trainContents)
    writeFile('deletedVals.csv', headerLine, deletedVals)
    print 'Process complete.'

def twoHours(newContents, dates):
    files, deletedVals = deleteVals(newContents)
    # FOR HOUR TIME SEGMENTS
    twoHours = dict()
    twoHours['00-01'] = []
    twoHours['02-03'] = []
    twoHours['04-05'] = []
    twoHours['06-07'] = []
    twoHours['08-09'] = []
    twoHours['10-11'] = []
    twoHours['12-13'] = []
    twoHours['14-15'] = []
    twoHours['16-17'] = []
    twoHours['18-19'] = []
    twoHours['20-21'] = []
    twoHours['22-23'] = []
    for i in xrange(1, len(dates)-1):
        contents = files[dates[i]]
        for line in contents:
            dateTime = line[5]
            time = dateTime[dateTime.index(' ')+1:]
            hour = int(dateTime[dateTime.index(' ')+1:dateTime.index(':')])
            if hour <= 1:
                twoHours['00-01'].append(line)
            elif hour <= 3:
                twoHours['02-03'].append(line)
            elif hour <= 5:
                twoHours['04-05'].append(line)
            elif hour <= 7:
                twoHours['06-07'].append(line)
            elif hour <= 9:
                twoHours['08-09'].append(line)
            elif hour <= 11:
                twoHours['10-11'].append(line)
            elif hour <= 13:
                twoHours['12-13'].append(line)
            elif hour <= 15:
                twoHours['14-15'].append(line)
            elif hour <= 17:
                twoHours['16-17'].append(line)
            elif hour <= 19:
                twoHours['18-19'].append(line)
            elif hour <= 21:
                twoHours['20-21'].append(line)
            else:
                twoHours['22-23'].append(line)
    for hour in twoHours:
        writeFile(hour+'.csv', headerLine, twoHours[hour])
    writeFile('deletedVals.csv', headerLine, deletedVals)
    print 'Process complete.'

def halfHourOut(newContents, dates):
    headers = headerLine[0:2] + headerLine[6:41] + headerLine[45:80] + headerLine[82:87] + headerLine[90:-1] + [headerLine[81]]
    files, deletedVals = deleteVals(newContents)
    for i in xrange(24):
        for j in xrange(2):
            halfHour = [j*30, (j+1)*30-1]
            testContents, trainContents = [], []
            for date in dates:
                if date != '12-17' and date != '12-22':
                    if i < 10:
                        hour = '0' + str(i)
                    else:
                        hour = str(i)
                    for line in files[date]:
                        newLine = line[0:2] + line[6:41] + line[45:80] + line[82:87] + line[90:] + [line[81]]
                        minute = int(line[5][line[5].index(':')+1:])
                        if (hour in line[5][line[5].index(' '):line[5].index(':')] and
                            minute >= halfHour[0] and minute <= halfHour[1]):
                            testContents.append(newLine)
                        else:
                            trainContents.append(newLine)
            print 'Writing files for ', i, halfHour[1], '...'
            writeFile('Test'+hour+str(halfHour[1])+'.csv', headers, testContents)
            writeFile('Train'+hour+str(halfHour[1])+'.csv', headers, trainContents)
    writeFile('deletedVals.csv', headerLine, deletedVals)
    print 'Process complete.'

def slidingWindow(segmentSize, windowSize, newContents, dates):
    headers = headerLine[:142] + ['mostComTime', 'numAct', 'deltaActivity'] + headerLine[142:]
    headers = headers[0:2] + headers[6:41] + headers[45:80] + headers[82:87] + headers[90:] + [headers[81]]
    files, deletedVals = deleteVals(newContents)
    for date in files:
        newContents = []
        if date != '12-22':
            contents = files[date]
            start, index = 0, 0
            startHours = int(contents[start][5][contents[start][5].index(' ')+1:contents[start][5].index(':')])
            startMin = int(contents[start][5][contents[start][5].index(':')+1:])
            startMins = startHours*60 + startMin
            endMin = (startMins + segmentSize)
            instanceCount = 0
            segmentLines = []
            while index < len(contents):
                line = contents[index]
                curHour = int(line[5][line[5].index(' ')+1:line[5].index(':')])
                curMin = int(line[5][line[5].index(':')+1:])
                curMins = curHour*60 + curMin
                if curMins >= endMin:
                    newLine = []
                    if index > 0 and segmentLines == []:
                        newContents.append(['no_val']*len(headerLine))
                    else:
                        for i in xrange(len(contents[start])):
                            if i == 5 or i == 44:
                                item0 = contents[start][5][:contents[start][5].index(' ')] + ' '
                                if startHours < 10:
                                    item0 += '0'
                                item0 += str(startHours) + ':' 
                                if startMins%60 < 10:
                                    item0 += '0'
                                item0 += str(startMins%60) + '-'
                                if (endMin-1)%60 < 10:
                                    item0 += '0'
                                item0 += str((endMin-1)%60)
                            elif i == 142:
                                activities, activitiesCount = [], []
                                deltaActivity = 0
                                for j in xrange(start, index):
                                    activity = contents[j][i]
                                    if activity in activities:
                                        activitiesCount[activities.index(activity)] += 1
                                    else:
                                        activities.append(activity)
                                        activitiesCount.append(1)
                                    try:
                                        if prevActivity != activity:
                                            deltaActivity += 1
                                            prevActivity = activity
                                    except:
                                        prevActivity = activity
                                mostComTime = max(activitiesCount)
                                mostComAct = activities[activitiesCount.index(mostComTime)]
                                numAct = len(activities)
                                actStats = [mostComTime, numAct, deltaActivity]
                                newLine.extend(actStats)
                                item0 = mostComAct
                            elif i == 143:
                                screenStatus, screenCount = [], []
                                for j in xrange(start, index):
                                    if contents[j][i] not in screenStatus:
                                        screenStatus.append(contents[j][i])
                                        screenCount.append(1)
                                    else:
                                        screenCount[screenStatus.index(contents[j][i])] += 1
                                item0 = screenStatus[screenCount.index(max(screenCount))]
                            elif i >= 144 and i <= 148:
                                item0 = 0
                                for j in xrange(start, index):
                                    item0 += contents[j][i]
                            elif i == 167:
                                item0 = 0
                                for j in xrange(start, index):
                                    if type(contents[j][i]) == str:
                                        item0 = contents[j][i]
                            elif type(contents[start][i]) == str:
                                items = []
                                count = []
                                for j in xrange(start, index):
                                    if contents[j][i] in items:
                                        count[items.index(contents[j][i])] += 1
                                    else:
                                        items.append(contents[j][i])
                                        count.append(1)
                                item0 = items[count.index(max(count))]
                            else:
                                info = []
                                for j in xrange(start, index):
                                    item = (contents[j][i])
                                    info.append(item)
                                item0 = sum(info) / len(info)
                                if i == 80:
                                    if almostEqual(item0, 0):
                                        item0 = 0
                            newLine.append(item0)
                        newLine.append(instanceCount)
                        newContents.append(newLine)
                    segmentLines = [line]
                    start += windowSize
                    index = start + 1
                    startMins += windowSize
                    startHours = startMins / 60
                    startMin = startMins % 60
                    endMin = (startMins + segmentSize)
                    instanceCount = 1
                else:
                    segmentLines.append(line)
                    instanceCount += 1  
                    index += 1                 
            files[date] = newContents
    for key in files:
        newContents = []
        contents = files[key]
        for line in contents:
            if 'no_val' in line:
                deletedValues.append(line)
            else:
                newLine = line[0:2] + line[6:41] + line[45:80] + line[82:87] + line[90:] + [line[81]]
                newContents.append(newLine)
        files[key] = newContents
    print 'Writing files...'
    for key in files:
        writeFile(key+'-'+str(segmentSize)+'-'+str(windowSize)+'.csv', headers, files[key])
    writeFile('deletedVals.csv', headerLine, deletedVals)
    print 'Process complete.'

def segHourOut(segment, newContents, dates):
    files = largeTimeSeg(segment, newContents, dates)
    newContents = []
    for date in dates:
        contents = files[date]
        for line in contents:
            newLine = line[0:2] + line[6:41] + line[45:80] + line[82:87] + line[90:] + [line[81]]
            newContents.append(newLine)
    headers = headerLine[:142] + ['mostComTime', 'numAct', 'deltaActivity'] + headerLine[142:]
    headers = headers[0:2] + headers[6:41] + headers[45:80] + headers[82:87] + headers[90:] + [headers[81]]
    writeFile(str(segment)+'allData.csv', headers, newContents)
    for i in xrange(24):
        testContents, trainContents = [], []
        for date in dates:
            if date != '12-17' and date != '12-22':
                if i < 10:
                    hour = '0' + str(i)
                else:
                    hour = str(i)
                for line in files[date]:
                    newLine = line[0:2] + line[6:41] + line[45:80] + line[82:87] + line[90:] + [line[81]]
                    if hour in line[5][line[5].index(' '):line[5].index(':')]:
                        testContents.append(newLine)
                    else:
                        trainContents.append(newLine)
        print 'Writing files...'
        writeFile(str(segment)+'Test'+hour+'.csv', headers, testContents)
        writeFile(str(segment)+'Train'+hour+'.csv', headers, trainContents)
    print 'Process complete.'

def trainTestFeat(newContents, dates):
    headers = headerLine[0:2] + headerLine[6:41] + headerLine[45:80] + headerLine[82:87] + headerLine[90:] + [headerLine[81]]
    files, deletedVals = deleteVals(newContents)
    newFiles = dict()
    newDates = []
    for date in dates:
        if date != '12-17' and date != '12-22':
            newDates.append(date)
            newContents = []
            for line in files[date]:
                newLine = line[0:2] + line[6:41] + line[45:80] + line[82:87] + line[90:] + [line[81]]
                newContents.append(newLine)
            newFiles[date] = newContents
    ext = [None]*3
    allTrainTest = []
    for h in xrange(len(newFiles)-1):
        ext[h] = 'te'
        for l in xrange(h):
            ext[l] = 'T'
        for l in xrange(h+1, 3):
            ext[l] = 'T'
        for i in xrange(len(newFiles)):
            trainTest = ext[:i] + ['f'] + ext[i:]
            allTrainTest.append(trainTest)
    for code in allTrainTest:
        extension = ''
        for item in code:
            extension += item
        trainContents, trainDates = [], ''
        print 'Writing files for ', extension
        for index in xrange(len(code)):
            if code[index] == 'T':
                trainContents.extend(newFiles[newDates[index]])
                trainDates += newDates[index]
            else:
                name = code[index]+newDates[index] + '.csv'
                filename = 'trainTestFeat/' + extension + '/' + name
                if not os.path.exists(os.path.dirname(filename)):
                    os.makedirs(os.path.dirname(filename))
                writeFile(filename, headers, newFiles[newDates[index]])
        writeFile('trainTestFeat/' + extension + '/T' + trainDates + '.csv', headers, trainContents)
    filename = 'trainTestFeat/deletedVals.csv'
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
    writeFile(filename, headers, deletedVals)
################################################################################
# Tkinter user interface
################################################################################
def choose(message, title, options):
    msg = message + "\n" + "Choose one:"
    for i in xrange(len(options)):
        msg += "\n" + str(i+1) + ": " + options[i]
    response = tkSimpleDialog.askstring(title, msg)
    return options[int(response)-1]

def init(root, canvas):
    canvas.data["message"] = "none"
    buttonFrame = Frame(root)
    b1 = Button(buttonFrame, text="Start", command=bingeDrinking)
    b1.grid(row=0,column=0)
    buttonFrame.pack(side=TOP)
    canvas.pack()
    redrawAll(canvas)

def redrawAll(canvas, message=None):
    canvas.delete(ALL)
    # background (fill canvas)
    # canvas.create_rectangle(0,0,300,300)
    if message != None:
        canvas.create_text(150,170,text=message)

def bingeDrinking():
    root = Tk()
    global canvas
    message = "Enter name of folder containing raw data: "
    title = "Folder name"
    folderName = tkSimpleDialog.askstring(title, message)
    if folderName == None:
        sys.exit()
    newContents, dates = aggregateAll(folderName, 60, 1, 60)
    msg = 'Aggregation complete.'
    canvas.create_text(150,170,text=msg)
    message = "Pick a file format option: "
    title = "File format options"
    options = [ "One Minute Instances", "X Minute Instances", "Two Hour Blocks", 
                "One hour Out", "Half Hour Out", "Sliding Window"]
    fileFormat = choose(message, title, options)
    if fileFormat == "One Minute Instances":        
        oneMin(newContents)
    elif fileFormat == "X Minute Instances":
        message = "Enter segment size in minutes: "
        title = "Segment size"
        segment = int(tkSimpleDialog.askstring(title, message))
        if segment == None:
            sys.exit()
        largeTimeSeg(largeTime, newContents, dates)
    elif fileFormat == "Two Hour Blocks":
        twoHours(newContents, dates)
    elif fileFormat == "One Hour Out":
        oneHourOut(newContents, dates)
    elif fileFormat == "Half Hour Out":
        halfHourOut(newContents, dates)
    elif fileFormat == "Sliding Window":
        message = "Enter segment size in minutes: "
        title = "Segment size"
        segment = int(tkSimpleDialog.askstring(title, message))
        if segment == None:
            sys.exit()
        message = "Enter window size in minutes: "
        title = "Window size"
        window = int(tkSimpleDialog.askstring(title, message))
        if segment == None:
            sys.exit()
        slidingWindow(segment, window, newContents, dates)
    else:
        message = "Invalid response"
        title = "Error box"
        tkMessageBox.showerror(title, message)

def run():
    # Dialog code from CMU course 15-112
    root = Tk()
    global canvas
    canvas = Canvas(root, width=300, height=300)
    root.canvas = canvas.canvas = canvas
    canvas.data = { }
    init(root, canvas)
    root.mainloop()

# run()

################################################################################
# Command line user interface
################################################################################
def commandLineUI():
    folderName = raw_input('Enter folder name with raw data: ')
    aggDataFold = raw_input('Enter folder name with aggregated accelerometer and gyroscope data (Leave blank if none): ')
    newContents, dates = aggregateAll(folderName, aggDataFold, 60, 1, 60)
    options = [ "One Minute Instances", "X Minute Instances", "Two Hour Blocks", 
                "One hour Out", "Half Hour Out", "Sliding Window"]
    for opt in options:
        print options.index(opt) + 1,
        print '.' + opt
    fileFormat = raw_input('Pick a file format (number): ')
    if fileFormat == 1:
        oneMin(newContents)
    elif fileFormat == 2:
        timeSeg = raw_input('Enter segment size in minutes: ')
        largeTimeSeg(largeTime, newContents, dates)
    elif fileFormat == 3:
        twoHours(newContents, dates)
    elif fileFormat == 4:
        oneHourOut(newContents, dates)
    elif fileFormat == 5:
        halfHourOut(newContents, dates)
    elif fileFormat == 6:
        timeSeg = raw_input('Enter segment size in minutes: ')
        winSize = raw_input('Enter sliding window size in minutes: ')
        slidingWindow(segment, window, newContents, dates)
    else:
        print 'Invalid entry.'

# commandLineUI()
newContents, dates = aggregateAll('rawData', 'Agg', 60, 1, 60)
trainTestFeat(newContents, dates)
# segHourOut(5, newContents, dates)
# segHourOut(10, newContents, dates)
# segHourOut(15, newContents, dates)
# segHourOut(30, newContents, dates)
# ext = [None]*3
# allTrainTest = []
# newFiles = [None]*4
# newDates = ['12-18', '12-19', '12-20', '12-21']
# for h in xrange(len(newFiles)-1):
#     ext[h] = 'te'
#     for l in xrange(h):
#         ext[l] = 'T'
#     for l in xrange(h+1, 3):
#         ext[l] = 'T'
#     for i in xrange(len(newFiles)):
#         trainTest = ext[:i] + ['f'] + ext[i:]
#         allTrainTest.append(trainTest)
# for code in allTrainTest:
#     extension = ''
#     for item in code:
#         extension += item
#     for index in xrange(len(code)):
#         name = code[index]+newDates[index] + '.csv'
#         fileName = extension + '/' + name
#         print fileName
