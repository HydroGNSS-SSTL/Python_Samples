
from datetime import datetime
import h5py
import math
import numpy as np
import os

DATA_ROOT_FOLDER = r"C:\SSTL\DownloadTest"
def getDateTimeFolder(startTime:datetime, endTime:datetime)->str:
    midPointTime = startTime + (endTime - startTime) / 2

    return midPointTime.strftime("%Y-%m/%d/H%H")
def  readAll6HourProducts(satellite, startTime, endTime):

    dateFolder = getDateTimeFolder(startTime, endTime)

    print(f"\nReading data for satellite {satellite} from {dateFolder}:\n")
    dateFolder = dateFolder.replace('/', '\\')
    timeDiff = endTime - startTime
    midPointDate = startTime + 0.5 * timeDiff
    dateStr = midPointDate.strftime("%Y%m%d")

    # Open all relevant L1 and L2 NetCDF files in this 6-hour period:
    dataRootFolder = DATA_ROOT_FOLDER + '\\' + satellite + '\\'
    filePath_L1Merged = dataRootFolder + '\\L1A_L1B\\' + dateFolder + '\\metadata_L1_merged.nc'

    filePath_L2OP_SI = dataRootFolder + '\\L2OP-SI\\' + dateFolder + '\\L2OP_SI.nc'
    filePath_L2OP_SSM = dataRootFolder + '\\L2OP-SSM\\' + dateFolder + '\\L2OP-SSM.nc'
    filePath_L2OP_FB = dataRootFolder + '\\L2OP-FB\\' + dateFolder + '\\L2OP-FB.nc'
    filePath_L2OP_FT = dataRootFolder + '\\L2OP-FT\\' + dateFolder + '\\L2OP-FT.nc'
    filePath_L2OP_SWS = dataRootFolder + '\\L2OP-SWS\\' + dateFolder + '\\L2OP_SWS.nc'

    try:
        # Open the NetCDF files for this 6-hour dataset

        # As PDGS is run periodically, all time segment files should be populated:
        l1MergedFile = None

        if os.path.exists(filePath_L1Merged):
            l1MergedFile = h5py.File(filePath_L1Merged, 'r')
        else:
            print(f"Missing L1 merged metadata_L1_merged.nc for {dataRootFolder}.")
            return

        siFile = None

        if os.path.exists(filePath_L2OP_SI):
            siFile = h5py.File(filePath_L2OP_SI, 'r')
        else:
            print("Missing SI file: " + filePath_L2OP_SI)

        smFile = None
        if os.path.exists(filePath_L2OP_SSM):
            smFile = h5py.File(filePath_L2OP_SSM, 'r')

        fbFile = None
        if os.path.exists(filePath_L2OP_FB):
            fbFile = h5py.File(filePath_L2OP_FB, 'r')

        ftFile = None

        if os.path.exists(filePath_L2OP_FT):
            ftFile = h5py.File(filePath_L2OP_FT, 'r')

        swsFile = None
        if os.path.exists(filePath_L2OP_SWS):
            swsFile = h5py.File(filePath_L2OP_SWS, 'r')

        currentFile = filePath_L1Merged
        TrackNames = []
        for name in l1MergedFile:
            if name[0].isdigit() == True:
                TrackNames.append(name)

        posFixValidArray = l1MergedFile['/PositionFixValidity'][:]
        numberOfValidPositionFixes = 0
        numberOfValidSMs = 0
        numberOfValidWPs = 0
        numberOfValidCoherentWPs = 0
        numberOfValidFBs = 0
        numberOfValidFTs = 0
        numberOfValidWSs = 0
        numberOfValidSeaIces = 0

        for value in posFixValidArray:
            if value == 3:
                numberOfValidPositionFixes += 1

        percentValidPosFixes = 100 * numberOfValidPositionFixes / len(posFixValidArray)

        print(f"percentValidPosFixes is {percentValidPosFixes}")

        # Loop over the tracks for this 6-hour period:
        for track in TrackNames:

            # Read L2 data:

            currentFile = filePath_L2OP_SSM

            if smFile is not None and not track in smFile:
                print(f"Missing track {track} in {filePath_L2OP_SSM}")
            elif smFile is not None and track in smFile:

                soilMoistureArray = smFile['/' + track + '/SoilMoisture'][:]
                qualityFlagArray = smFile['/' + track + '/QualityFlag'][:]

                usedSMs = []
                for (quality, soilMoisture) in zip(qualityFlagArray, soilMoistureArray):
                    if quality and not math.isnan(soilMoisture):
                        usedSMs.append(soilMoisture)
                        numberOfValidSMs = numberOfValidSMs + 1

            currentFile = filePath_L2OP_SI
            if siFile is not None and not track in siFile:
                print(f"Missing track {track} in {filePath_L2OP_SI}")
            elif siFile is not None and track in siFile:

                # Incoherent data:
                if track + '/Low_Resolution' not in siFile:
                    print(f"No Low_Resolution group for track {track} in {currentFile}")

                elif track + '/Low_Resolution/WaterProbabilityIncoherent'not in siFile or \
                    track + '/Low_Resolution/SIOverallQualityFlags' not in siFile:
                    print(f"No Low_Resolution group WaterProbabilityIncoherent or SIOverallQualityFlags for track {track} in {currentFile}")

                else:
                    waterProbabilityIncoherentArray = siFile['/' + track + '/Low_Resolution/WaterProbabilityIncoherent'][:]

                    siOverallQualityFlagsArray = siFile['/' + track + '/Low_Resolution/SIOverallQualityFlags'][:]

                    usedIncoherentWPs = []

                    for (waterProbability, siQuality) in zip(waterProbabilityIncoherentArray, siOverallQualityFlagsArray):
                        if siQuality == 1:
                            usedIncoherentWPs.append(waterProbability)
                            numberOfValidWPs = numberOfValidWPs + 1

                # Coherent data:
                # For TDS-1 the coherent SI params are scalars, i.e. effectively there is no data

                if track + '/High_Resolution' not in siFile:
                    print(f"No High_Resolution group for track {track} in {currentFile}")

                elif track + '/High_Resolution/WaterProbabilityCoherent' not in siFile or \
                        track + '/High_Resolution/WaterFlagCoherent' not in siFile:
                    print(f"No High_Resolution group WaterProbabilityCoherent or WaterFlagCoherent for track {track} in {currentFile}")
                else:
                    wp = siFile['/' + track + '/High_Resolution/WaterProbabilityCoherent']

                    if wp.size > 1:
                        waterProbabilityCoherentArray = siFile[
                                                          '/' + track + '/High_Resolution/WaterProbabilityCoherent'][:]
                        waterFlagCoherentArray = siFile['/' + track + '/High_Resolution/WaterFlagCoherent'][:]
                    else:
                        waterProbabilityCoherentArray = None
                        waterFlagCoherentArray = None

                    usedCoherentWPs = []

                    if waterProbabilityCoherentArray is not None:
                        for (waterProbability, waterFlag) in zip(waterProbabilityCoherentArray, waterFlagCoherentArray):
                            if waterProbability != 9999:
                                usedCoherentWPs.append(waterProbability)
                                numberOfValidCoherentWPs = numberOfValidCoherentWPs  + 1

            currentFile = filePath_L2OP_FB
            if fbFile is not None and not track in fbFile:
                print(f"Missing track {track} in {filePath_L2OP_FB}")
            elif fbFile is not None and track in fbFile:  # Last track was missing in one dataset

                agbArray = fbFile['/' + track + '/AGB'][:]

                qualityFlagArray = fbFile['/' + track + '/Quality_Flag'][:]
                usedFBs = []

                for (quality, agb) in zip(qualityFlagArray, agbArray):
                    if quality == 1 and not math.isnan(agb):
                        usedFBs.append(agb)
                        numberOfValidFBs = numberOfValidFBs + 1

            else:
                print(f"No track {track} in {filePath_L2OP_FB}")

            usedFTs = []

            currentFile = filePath_L2OP_FT
            if ftFile is not None and not track in ftFile:
                print(f"Missing track {track} in {filePath_L2OP_FT}")
            elif ftFile is not None  and track in ftFile:

                ftArray = ftFile['/' + track + '/FreezeThawState'][:]

                qualityFlagArray = ftFile['/' + track + '/LowOverallQuality'][:]
                for (quality, ft) in zip(qualityFlagArray, ftArray):
                    if quality and not math.isnan(ft):
                        usedFTs.append(ft)
                        numberOfValidFTs = numberOfValidFTs + 1

            currentFile = filePath_L2OP_SWS
            if swsFile is not None and not track in swsFile:
                print(f"Missing track {track} in {filePath_L2OP_SWS}")
            elif swsFile is not None and track in swsFile:

                # track0Groups = swsFile['/' + track].keys()
                if 'SI' in swsFile['/' + track]:
                    seaIceArray = swsFile['/' + track + '/SI'][:]
                    windSpeedArray = swsFile['/' + track + '/U10'][:]
                    usedWSs = []
                    usedSeaIces = []

                    for (seaIceFlag, surfaceWindSpeed) in zip(seaIceArray, windSpeedArray):
                        if seaIceFlag != -99:

                            usedSeaIces.append(seaIceFlag)
                            numberOfValidSeaIces = numberOfValidSeaIces + 1

                        if surfaceWindSpeed != -9999.0:
                            usedWSs.append(surfaceWindSpeed)
                            numberOfValidWSs = numberOfValidWSs + 1

                else:
                    print(f"No SI sea ice field in track {track} in {filePath_L2OP_SWS}.")

        print(f"numberOfValidSMs is {numberOfValidSMs}")
        print(f"numberOfValidWPs is {numberOfValidWPs}")
        print(f"numberOfValidCoherentWPs is {numberOfValidCoherentWPs}")
        print(f"numberOfValidFBs is {numberOfValidFBs}")
        print(f"numberOfValidFTs is {numberOfValidFTs}")
        print(f"numberOfValidSeaIces is {numberOfValidSeaIces}")
        print(f"numberOfValidWSs is {numberOfValidWSs}")

    except Exception as e:
        # List exception and the file that failed
        print(f"Error reading/accessing {currentFile} from {dateFolder}: " + str(e))

def main():

    # Download all 6-hour files between these dates
    startTime = datetime(2018, 2, 7, 21, 0, 0)
    endTime = datetime(2018, 2, 8, 3, 0, 0)

    satellites = ["HydroGNSS-1", "HydroGNSS-2"]

    for satellite in satellites:
        readAll6HourProducts(satellite, startTime, endTime)

if __name__ == '__main__':

    main()