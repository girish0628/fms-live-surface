import datetime
from math import ceil, floor
from optparse import OptionParser
import struct
from time import time
import sys
from timeit import timeit
import traceback
import arcpy, numpy, os

import pandas
from scipy import stats


parser = OptionParser()
parser.add_option("-S", "-- SourceFolder", action="store", dest="sourcefolder", type="string", help="Modular CSV Path")
parser.add_option("-A", "-- zAdjustment", action="store", dest="zadjustment", type="float", help="Z Adjustment value", default=None)
parser.add_option("-D", "-- DecimalDigits", action="store", dest="decimaldigits", type="int", help="Decimal Digits", default=2)
parser.add_option("-G", "-- GridSize", action="store", dest="gridsize", type="int", help="Grid Size", default=2)
parser.add_option("-M", "-- MaxZ", action="store", dest="maxz", type="float", help="Maximum Elevation Z value", default=4000.0)
parser.add_option("-F", "-- FileFilter", action="store", dest="filefilter", type="string", help="Prefixes to validate for incoming CSV files", default="*")
parser.add_option("-R", "-- InputSpatialReference", action="store", dest="inputspatialreference", type="string", help="Input Spatial reference")
parser.add_option("-O", "-- OutputSpatialReference", action="store", dest="outputspatialreference", type="string", help="Output Raster Spatial reference")
parser.add_option("-C", "-- OutputCSVFile", action="store", dest="outputcsvfile", type="string", help="Output CSV File")
parser.add_option("-I", "-- AOI", action="store", dest="aoi", type="string", help="AOI Feature class", default = "")
parser.add_option("-W", "-- AOIWhere", action="store", dest="aoiwhere", type="string", help="AOI Feature class where clause", default="")
parser.add_option("-P", "-- Despike", action="store_true", dest="despike", help="Despike", default=False)
parser.add_option("-N", "-- MinNeighbours", action="store", dest="minneighbours", type="int", help="Minimum amount of neighbouring points", default=3)

global options
(options, args) = parser.parse_args()

#
# Minestar numeric scale factor
cScaleFactor = 0.01
# All points above this elevation will be disregarded
cMaxElevation = options.maxz
# Number of digits to round coordinates to
cDecimalDigits = options.decimaldigits
# Minestar Grid size (Used for de-spiking algorithm to consider neighbouring points)
cGridSize = options.gridsize
# Mine site Grid Z adjustment value (ADPH for ER, WB)
zAdjustment = options.zadjustment
# Dictionary of dictionaries with point data for output

# Kev - XY
Points = {}
# These will be removed from the points list before we write the file
InadequateNeighboursList = []
# Filters the incoming points
AOIGeometry = None

#---------------------------------------------------------------------------------------------------------------------------------------
# Read the AOI geometry if specified.

if options.aoi:
    for row in arcpy.da.SearchCursor(options.aoi, ['SHAPE@'], options.aoiwhere):
        if AOIGeometry:
            AOIGeometry = AOIGeometry.union(row[0])
        else:
            AOIGeometry = row[0]

#---------------------------------------------------------------------------------------------------------------------------------------
def Adjustz(Z):
    if zAdjustment:
        return Z + zAdjustment
    else:
        return Z

#---------------------------------------------------------------------------------------------------------------------------------------
def float_round(num, places = 0, direction = floor):
    return direction(num * (10**places)) / float(10**places)

#---------------------------------------------------------------------------------------------------------------------------------------

def ParseItem(Item, Filename):
    PointList = []
    def AddItem(Item, Items):
        def ItemExists(Item, Items):
            for p in Items:
                if p['Z'] == Item['Z'] and p['Timestamp'] == Item['Timestamp']:
                    return True            
            return False
            
        if Item['Z'] < cMaxElevation and Item['Timestamp'] != 0 and Item['Z'] != 0:
            if not ItemExists(Item, Items):
                Items.append(Item)

    X = float_round(Item[0]* cScaleFactor, cDecimalDigits, ceil)
    Y = float_round(Item[1] * cScaleFactor, cDecimalDigits, ceil)
    if (X <= 0) or (Y <= 0):
        return None

    Z = float_round(Item[2]* cScaleFactor, cDecimalDigits, ceil)
    Time1= Item[3]
    Z2 = float_round(Item[4]* cScaleFactor, cDecimalDigits, ceil)
    Time2= Item[5]
    Z3 = float_round(Item[6]* cScaleFactor, cDecimalDigits, ceil)
    Time3 = Item[7]
    Z4 = float_round(Item[8]*cScaleFactor, cDecimalDigits, ceil)
    Time4 = Item[9]
    AddItem({'X': X,'Y': Y, 'Z': Z, 'Timestamp': Time1, 'Filename': Filename}, PointList)
    AddItem({'X': X,'Y': Y, 'Z': Z2, 'Timestamp': Time2, 'Filename': Filename}, PointList)
    AddItem({'X': X, 'Y': Y, 'Z': Z3, 'Timestamp': Time3, 'Filename': Filename}, PointList)
    AddItem({'X': X,'Y': Y, 'Z': Z4, 'Timestamp': Time4, 'Filename': Filename}, PointList)

    return PointList

#---------------------------------------------------------------------------------------------------------------------------------------

def RemoveSpikes():
    global InadequateNeighboursList
    InadequateNeighboursList = []
    # Since the data is in a grid, we can predict the neighbour coordinates and use that to index the Z values
    # in our global 'points' dictionary

    def GetNeighbourCoordinatePairs(X,Y):
        Result = []
        Result.append((X+ cGridSize, Y -cGridSize))
        Result.append((X+ cGridSize, Y))
        Result.append((X+ cGridSize, Y + cGridSize))
        Result.append((X - cGridSize, Y- cGridSize))
        Result.append((X - cGridSize, Y))
        Result.append((X - cGridSize, Y + cGridSize))
        Result.append((X,Y+cGridSize))
        Result.append((X, Y -cGridSize))
        return Result

    # Returns the Z values of all neighbouring points.
    def GetZValuesWithinRange(X, Y, Z):
        ResultList = []
        NeighbourCoordinates = GetNeighbourCoordinatePairs(X, Y)
        # Look up the point, if it exists in the Points list, add the Z value to a list which we'll
        # use to estimate a better Z value
        for item in NeighbourCoordinates:
            Key = "{0}_{1}".format(item[0], item[1])
            if Key in Points:
                ResultList.append(Points[Key]['Z'])
        return ResultList

    def EstimateZValue(X, Y, Z):
        zList = GetZValuesWithinRange(X, Y, Z)
        # A point must have at least 3 neighbours, otherwise, remove
        if len(zList) < options.minneighbours:
            # Remove point
            InadequateNeighboursList.append("{0}_{1}".format(str(X), str(Y)))
        if len(zList) > 0:
            npList = numpy.array(zList)
            PercentileValue = numpy.percentile(npList, 50)
            # Take the median of the all the neighbouring points
            # Get the difference between Z and median and if greater than standard deviation, set Z to median
            if abs(PercentileValue - Z) > numpy.std(npList, axis=0):
                return PercentileValue
            else:
             return Z

    for item in Points.keys():
        Points[item]['Z']= EstimateZValue(Points[item]['X'], Points[item]['Y'], Points[item]['Z'])

#---------------------------------------------------------------------------------------------------------------------------------------

def ValidateSnippetfile(buffer):
    if len(buffer) > 36:
        return struct.unpack_from('L', buffer, 0)[0] == 201339251 and ((struct.unpack_from('B', buffer, 31)[0] == 11) or (struct.unpack_from('B', buffer, 36)[0] == 11))
    else:
        return False
    
#---------------------------------------------------------------------------------------------------------------------------------------

def ProcessCSVFile(Filename):
    def open_with_pandas_read_csv(Filename):
        df = pandas.read_csv(filename, sep=',')
        data = df.values
        return data
    
    def CSVDateTimeToInt(datestring):
        t = datetime.datetime.strptime(datestring, '%Y-%m-%d %H:%M:%S.%f')
        return int((t - datetime.datetime(1970, 1, 1)).total_seconds())
    
    if os.path.getsize(Filename) < 36:
        return False
    
    BaseFilename = os.path.basename(Filename)
    fileCount = open_with_pandas_read_csv(Filename)
    for item in fileCount:
        ParseItem = { 'Timestamp': CSVDateTimeToInt(item[3]), 'X': item[1], 'Y': item[2], 'Z': item[3]}
        ItemKey = "{0}_{1}".format(str(ParseItem['X']), str(ParseItem['Y']))
        if ItemKey in Points:
            if ParseItem['Timestamp'] > Points[ItemKey]['Timestamp']:
                ParseItem[3] = numpy.average([Points[ItemKey]['Z'], ParseItem['Z']])
                Points[ItemKey] = ParseItem
        else:
            Points[ItemKey] = ParseItem
    
#---------------------------------------------------------------------------------------------------------------------------------------       
def getSpatialReferenceFromFile(filename):
    sr = None
    if os.path.isfile(filename):
        sr = arcpy.SpatialReference()
        sr.createFromFile(filename)
    else:
        raise Exception("Spatial reference file not found : "+filename)
    return sr
#---------------------------------------------------------------------------------------------------------------------------------------  

def PreChecks():
    if not os.path.isdir(options.sourcefolder):
        raise Exception("Input folder not found : "+options.inputfolder)
    if options.aoi:
        if not arcpy.Exists(options.aoi):
            raise Exception("AOI Feature class not found : " + options.aoi)
    if not os.path.isfile(options.inputspatialreference):
        raise Exception("Input Spatial Reference File not found : " + options.inputspatialreference)
    if not os.path.isfile(options.outputspatialreference):
        raise Exception("Output Spatial Reference File not found : " + options.outputspatialreference)
    
#---------------------------------------------------------------------------------------------------------------------------------------  

def SavePointsToCSV(Points, InputSpatialReference, OutputSpatialReference, CSVFilename):
    stats = {'Total': len(Points), 'Outside AOI': 0, 'Projection Failed': 0, 'Valid': 0}
    Filter = AOIGeometry
    if not os.path.isdir(os.path.dirname(CSVFilename)):
        os.makedirs(os.path.dirname(CSVFilename))
    
    PointGeometry = arcpy.Point()
    with open(CSVFilename, 'w') as csvfile:
        for PointKey in Points.keys():
            Point = Points[PointKey]
            try:
                PointGeometry.X = Point['X']
                PointGeometry.Y = Point['Y']
                ProjectedPoint = arcpy.PointGeometry(arcpy.PointGeometry, InputSpatialReference).projectAs(OutputSpatialReference)
            except:
                stats['Projection Failed'] = stats['Projection Failed'] + 1
                continue

            if ProjectedPoint:
                if not ProjectedPoint.firstPoint:
                    continue
                    stats['Projection Failed'] = stats['Projection Failed'] + 1
            else:
                stats['Projection Failed'] = stats['Projection Failed'] + 1
                continue

            ValidPoint = True

            if Filter:
                ValidPoint = ProjectedPoint.within(AOIGeometry)

            if ValidPoint:
                stats['Valid'] = stats['Valid'] + 1

                csvfile.write(
                "{0},{1},{2)\n".format(str(float_round(ProjectedPoint.firstPoint.X, cDecimalDigits, ceil)),
                                        str(float_round(ProjectedPoint.firstPoint.Y, cDecimalDigits, ceil)),
                                        str(float_round(Adjustz(Point['Z']), cDecimalDigits, ceil))))

            else:
                stats['Outside AOI'] = stats['Outside AOI'] + 1

    for key in stats.keys():
        print("{0} : {1}".format(key, stats[key]))
    
#---------------------------------------------------------------------------------------------------------------------------------------  

def RemovePointsWithInadequateNeighbourCount():
    for key in InadequateNeighboursList:
        Points.pop(key)

Errors = False

try:
    PreChecks()
    
    if os.path.isfile(options.outputcsvfile):
        print("CSV Output file already exists, aborting ... ")
    else:
        start_time = timeit.default_timer()
        print("Getting input file list ... ")        
        InputFiles = get_file_list(options.sourcefolder, options.filefilter)

        if len(InputFiles) == 0:
            raise Exception("No files to process.")
        
        InputSpatialReference = getSpatialReferenceFromFile(options.inputspatialreference)
        OutputSpatialReference = getSpatialReferenceFromFile(options.outputspatialreference)

        print("Processing Files ... ")
        for filename in sorted(InputFiles):
            if os.path.isfile(filename):
                try:
                    ProcessCSVFile(filename)
                except:
                    print(traceback.format_exc())
                    print("Error processing : "+os.path.basename(filename))
                    # Purposely swallow the error but log. We can't have this process fail if a few snippet files
                    # are invalid. Further investigation required to determine why the snippet file parser struggles
                    # with some snippet files.
        
        print("Duration: "+ str((timeit.default_timer() - start_time))+" seconds")
        print("Total Files: "+ str(len(InputFiles)))
        print("Total Points :"+ str(len(Points)))

        if options.despike:
            print("Removing spikes")
            # Each time we reduce isolated points and smoothing becomes more efficient
            RemoveSpikes()
            RemovePointsWithInadequateNeighbourCount()

            # RemoveSpikes()
            # RemovePointsWithInadequateNeighbourCount()

            # RemoveSpikes()
            # RemovePointsWithInadequateNeighbourCount()

            print("Spikes removed.")

            if len(Points) >0:
                save_start_time = timeit.default_timer()
                print("Reprojecting points to CSV file ... ")
                SavePointsToCSV(Points, InputSpatialReference, OutputSpatialReference, options.outputcsvfile)
                print("Save/Project Duration : " + str(timeit.default_timer() - save_start_time))
            else:
                print("Error : No points to process.")
                Errors = True
            print ("Total Duration : "+str(timeit.default_timer() - start_time))
except:
    print(traceback.format_exc())
    Errors = True

if Errors:
    print("Errors were encountered during processing. Please review the log messages.")
    sys.exit(1)
