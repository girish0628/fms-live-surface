

from ast import Name
from datetime import datetime
from fileinput import filename
from fnmatch import fnmatch
import os
import sys
import uuid

import arcpy
from pytest import Item


def Log(item):
    print(item)
    sys.stdout.flush()

def getUniqueID():
    return "UUID_"+uuid.uuid4().hex

def getRowCount(table, whereclause):
    count = 0
    for row in arcpy.da.SearchCursor(table, ['*'], whereclause):
        count += 1
    return count

def Delete(item):
    try:
        if arcpy.Exists(item):
            arcpy.Delete_management(item)

    except:
        print("Warning : Could not delete item : "+item)

#

def get_file_list(folder, Filter=None):
    FilesList = [os.path.join(folder, name) for name in os.listdir(folder)]
    if Filter:
        FilesList = [f for f in FilesList if fnmatch.fnmatch(os.path.basename(f), Filter)]
    return FilesList

#
def getSpatialReferenceFromFile(filename):
    sr = None
    if os.path.isfile(filename):
        sr = arcpy.SpatialReference()
        sr.createFromFile(filename)
    else:
        raise Exception("Spatial reference file not found : "+filename)
    return sr


def GetDateFromName(Name):
    result = None
    if '_'in Name:
        items = os.path.basename(Name).split("_")
    else:
        items = [Name]

    for Item in items:
        if Item.isdigit():
            if len(Item) == 14:
                result = datetime.datetime.strptime(Item, '%Y%m%d%H%M%S')
                break
        elif len(Item) == 12:
            result = datetime.datetime.strptime(Item, '%Y%m%d%H%M')
            break
        elif len(Item) == 10:
            result = datetime.datetime.strptime(Item, '%Y%m%d%H')
        elif len(Item) == 8:
            result = datetime.datetime.strptime(Item, '%Y%m%d')
    return result


def GetRasterList(CSVFiles, RasterSource, RasterType):
    RasterList = []
    for csvfile in CSVFiles:
        if RasterType == "TIF":
            Raster = os.path.join(RasterSource, os.path.basename(csvfile).split('.')[0]+".tif")
            if os.path.isfile(Raster):
                RasterList.append(Raster)
            else:
                print("Warning : No corresponding raster found for CSV file :"+ os.path.basename(csvfile))
                print("Expected raster : "+Raster)

        else:
            Raster = os.path.join(RasterSource, os.path.basename(csvfile).split('.')[0])
            if arcpy.Exists(Raster):
                RasterList.append(Raster)
            else:
                print("Warning : No corresponding raster found for CSV file : "+ os.path.basename(csvfile))
                print("Expected raster : " + Raster)
    return RasterList

def UpdateFields(Rasters, mosaicDataset, DateFieldName, Fields):
    def get_field_dictionary():
        result = {}
        if Fields:
            for field in Fields:
                if '=' in field:
                    field_name = field.split('=')[0]
                    field_value = field.split('=')[1]
                    result[field_name] = field_value
        return result
    
    field_parameters = get_field_dictionary()
    field_list = ['Name', DateFieldName]
    raster_name_list = [os.path.basename(name).split('.')[0].upper()
                        if '.' in os.path.basename(name).upper()
                        else os.path.basename(name) for name in Rasters]
    
    field_list.extend(field_parameters.keys())
    
    with arcpy.da.UpdateCursor(mosaicDataset, field_list, "{0} is null".format(DateFieldName)) as UpdateCursor:
        for row in UpdateCursor:
            if row[0].upper() in raster_name_list:
                Log("Updating footprint '{0}' ... ".format(row[0]))
                date_time = GetDateFromName(row[0])
                if date_time:
                    row[1] = date_time
                index = 2
                for Field in field_parameters.iterkeys():
                    row[index] = field_parameters[Field]
                    index += 1