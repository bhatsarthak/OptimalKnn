from scipy import spatial
import numpy as np
import datetime
import random
"""
class GeographicalInfo :
    # Simple class that stores geographic data of a given place ID
    def __init__(self, lattitude, longitude, category, storeName) :
        self.lat = lattitude
        self.long = longitude
        self.category = category
        self.storeName = storeName
"""
        
def main():
    # parse command line options
    file = '/home/sarthakbhat/workspace/OptimalRetailStorePlacement/input/newyork_locdate.txt'
    data = []
    geoDict = {}
    quadrantNoVsPlacesCount = {}
    start = datetime.datetime.now()
    minLat = 10000
    minLong = minLat
    maxLat =-10000
    maxLong = maxLat
    sum = 0
    with open(file) as f:
        for line in f:
            geoDataArray = line.rstrip('\n').split(';')
            placeID = geoDataArray[0].rstrip('*')
            locData = geoDataArray[1].lstrip('*(').rstrip(')*').split(',')
            locTuple = (float(locData[0].strip()), float(locData[1].strip()), locData)
                # Open the text file and load the latitude and longitude of place into data
            if(locTuple[0]<minLat) :
                minLat = locTuple[0]
            if(locTuple[0]>maxLat) :
                maxLat= locTuple[0]
            if(locTuple[1]<minLong):
                minLong= locTuple[1]
            if(locTuple[1]> maxLong):
                maxLong = locTuple[1]          
            data.append(locTuple)
            # Load a dictionary with key as place ID and location info as value    
            geoDict[placeID] = locTuple
    #Create the KD Tree

    print "MINLAT:"+str(minLat) 
    print "MINLONG:"+str(minLong)
    print "MAXLAT:"+str(maxLat) 
    print "MAXLONG"+str(maxLong)
    noOfXDivisions = 100;
    noOfYDivisions = 100;
    deltaX = abs(minLat-maxLat)/noOfXDivisions;
    deltaY = abs(minLong-maxLong)/noOfYDivisions;
    #Initialize count of places(value for quadrantNoVsPlacesCount)  for each quadrant to zero
    for i in range(1,noOfXDivisions*noOfYDivisions + noOfXDivisions):
        quadrantNoVsPlacesCount[i]=0;
    for place in geoDict:
        x= (geoDict[place][0]-minLat)//deltaX
        y= (geoDict[place][1]-minLong)//deltaY
        quandrantNo = int(100 * y + x);
        quadrantNoVsPlacesCount[quandrantNo] += 1
    #scale the number of places to 10 million
    placeID=1;
    f = open('/home/sarthakbhat/workspace/OptimalRetailStorePlacement/input/500k.txt', 'r+')
    for qudrant in quadrantNoVsPlacesCount:
        noOfDeltaY = qudrant//100;
        noofDeltaX = qudrant%100;
        xStartRange = minLat + (noofDeltaX -1)*deltaX
        xEndRange = minLat + (noofDeltaX)*deltaX
        
        yStartRange = minLong + (noOfDeltaY -1)*deltaY
        yEndRange = minLong + (noOfDeltaY)*deltaY
        totalPlacesInQudrant = quadrantNoVsPlacesCount[qudrant]
        for i in range(0,totalPlacesInQudrant*5):
            xGenerated= random.uniform(xStartRange,xEndRange)
            yGenerated= random.uniform(yStartRange,yEndRange)
            entry = str(placeID) + '*;*('+str(xGenerated)+','+str(yGenerated)+ ',,,\n' 
            placeID += 1
            f.write(entry)
        #42889*;*(40.760265, -73.989105, 'Italian', '217', '291', 'Ristorante Da Rosina')
    f.close()
    print 'success'    
if __name__ == "__main__":
    main()
