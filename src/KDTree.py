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
    fileName = '/home/sarthakbhat/workspace/OptimalRetailStorePlacement/input/newyork_locdate.txt'
    data = []
    geoDict = {}
    start = datetime.datetime.now()
    with open(fileName) as f:
        for line in f:
            geoDataArray = line.rstrip('\n').split(';')
            placeID = geoDataArray[0].rstrip('*')
            locData = geoDataArray[1].lstrip('*(').rstrip(')*').split(',')
            locTuple = (float(locData[0].strip()), float(locData[1].strip()))
                # Open the text file and load the latitude and longitude of place into data
            data.append(locTuple)
            # Load a dictionary with key as place ID and location info as value    
            geoDict[placeID] = locData
    #Create the KD Tree
    tree = spatial.KDTree(data)
    print 'Writing the output'
    f = open('/home/sarthakbhat/workspace/OptimalRetailStorePlacement/serialOutput', 'r+')
    print len(geoDict)
    tt=0
    for item in geoDict:
        print tt
        sourceLatitude = float(geoDict[item][0].strip())
        sourceLongitude = float(geoDict[item][1].strip())
        f.write('Source:'+ str(sourceLatitude) + str(sourceLongitude) +'\n')
        response = tree.query_ball_point([sourceLatitude, sourceLongitude], 0.01)
        result=""
        
   #     for i in range(0,5 if len(response)>5 else len(response)):
    #        result+= str(data[response[i]])
        print( str([data[i] for i in tree.query_ball_point([sourceLatitude, sourceLongitude], 0.01)]))
       # f.write(result)
        #f.write('/n')
        print result
        tt+=1
    f.close()
    end = datetime.datetime.now()
    print '\n'
    print start
    print end
    print end-start
        
if __name__ == "__main__":
    main()
