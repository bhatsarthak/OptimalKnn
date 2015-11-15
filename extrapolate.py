from scipy import spatial
import numpy as np
import datetime
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
    start = datetime.datetime.now()
    minLat = 10000
    minLong = minLat
    maxLat =-10000
    maxLong = maxLat
    with open(file) as f:
        for line in f:
            geoDataArray = line.rstrip('\n').split(';')
            placeID = geoDataArray[0].rstrip('*')
            locData = geoDataArray[1].lstrip('*(').rstrip(')*').split(',')
            locTuple = (float(locData[0].strip()), float(locData[1].strip()))
                # Open the text file and load the latitude and longitude of place into data
            if(locTuple[0]<minLat) :
                minLat = locTuple[0]
            if(locTuple[0]>maxLat) :
                maxLat= locTuple[0]
            if(abs(locTuple[1])<abs(minLong)):
                minLong= locTuple[1]
            if(abs(locTuple[1])> abs(maxLong)):
                maxLong = locTuple[1]          
            data.append(locTuple)
            # Load a dictionary with key as place ID and location info as value    
            geoDict[placeID] = locData
    #Create the KD Tree

    print minLat + minLong + maxLat + maxLong    
if __name__ == "__main__":
    main()
