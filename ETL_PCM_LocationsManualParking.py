"""
ETL_PCM_LocationsManualParking.py
ETL workflow pulling the Locations Manual/Parking information from the PCM Frontend Database and exporting (i.e. ETL) as
two Feature Layers to the AGOL/Portal.

This is an ETL from Access to GIS Feature Layers on Portal.

Created By:
Kirk Sherrill Data Scientist San Francisco Bay Area Network

Date Created:
March 2025
"""
# Import Required Dependices
import pandas as pd
import glob, os, sys
import traceback
import generalDM as dm
import ArcGIS_API as agl
from arcgis.features import GeoAccessor

import logging

# Set option in pandas to not allow chaining (views) of dataframes, instead force copy to be performed.
pd.options.mode.copy_on_write = True


class etl_PCMLocations:
    def __init__(self, QueryToSummarize, PortalTeam, Folder):

        """
        Define the QC Protocol instantiation attributes

        :param QueryToSummarize: Query in PCM Front End being ETL'd to NPS Portal
        :param PortalTeam: Name of Portal Team to add permissions to.
        :param Folder: Name of the folder to be imported to
        :return: zzzz
        """
        # Class Variables

        numETL_PCMLocations = 0

        # Define Instance Variables
        self.QueryToSummarize = QueryToSummarize
        self.PortalTeam = PortalTeam
        self.Folder = Folder
        numETL_PCMLocations += 1

    def process_PCMLocManual(etlInstance, dmInstance, generalArcGIS):

        """
        ETL workflow for the Annual PCM Locations Manual and Parking Feature layers to NPS Portal

        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance
        :param generalArcGIS: General ArcGIS Instnace

        :return:outETL: String denoting 'Success' or 'Error' on ETL Processing
        """

        try:
            # Create the ETL PMCLocations Instance
            etlPCMInstance = etl_PCMLocations('qrpt_PlotLocationsManual',
                                              'Plant Community Monitoring SFAN',
                                              'PCM')

            # Import the 'qrpt_PlotLocationsManual' summary query routine to DataFrame
            inQuery = f'SELECT * FROM {etlPCMInstance.QueryToSummarize};'
            # PUll Summary
            outDFPCM = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBFE)

            # Create the LocationManual and LocationPark Data Frame Subsets
            locFieldList = ['LocationID', 'LocName', 'UnitCode', 'VegCode', 'VegDescription',
                            'Latitude', 'Longitude', 'Aspect', 'A_Aspect', 'B_Aspect', 'C_Aspect', 'CountOfEvents',
                            'FirstEventDate', 'LastEventDate', 'LastPlotNotes', 'LastDisturbancesNotes', 'HikeDistance',
                            'HikeTime', 'Directions', 'SpecialAccess', 'Hazards', 'RequiredPPE',
                            'TransectPhotoNPGallery', 'NumberRebarMissing', 'ManualInfoUpdatedUser',
                            'ManualInfodateLastUpdated']

            # Location Manual Info DF
            DFLocManual = outDFPCM[locFieldList]

            # Drop records where Lat is null
            DFLocManual = DFLocManual.dropna(subset=['Latitude'])

            # Convert dataframe to a spatial geodataframe
            DFLocManual_SDF = GeoAccessor.from_xy(DFLocManual, "Longitude", "Latitude")

            # Parking Info DF
            parkFieldList = ['LocationID', 'LocName', 'UnitCode', 'VegCode', 'ParkingLatitude', 'ParkingLongitude',
                             'OffRoadDriving', 'DriveTime', 'DriveDistance', 'DriveDirections', 'ParkingUpdatedUser',
                             'ParkingDateLastUpdated']
            # Location Manual Info DF
            DFParking = outDFPCM[parkFieldList]

            # Drop records where Lat is null
            DFParking = DFParking.dropna(subset=['ParkingLatitude'])

            # Convert dataframe to a spatial geodataframe
            DFParking_SDF = GeoAccessor.from_xy(DFParking, "ParkingLongitude", "ParkingLatitude")

            ###############################
            # Push Dataframes to NPS Portal
            ###############################

            yearValue = etlInstance.yearLU

            inDic = {"title": f"SFAN_PCM_Plot_Locations_Manual_{yearValue}",
                     "tags": "San Francisco Bay Area Network, Plant Communities Monitoring, Plot Manuals",
                     "type": "Feature Service",
                     "description": f"PCM Plot Locations Manual Info Feature Layer to be used for PCM Field Maps"
                                    f" Navigation {yearValue}",
                     "snippet": f"Feature Layer with PCM Plot Locations Manual Info to be used for PCM Field Maps"
                                    f" Navigation {yearValue}",
                     "licenseInfo": "This dataset is for internal use only and should not be distributed without "
                                    "permission."}

            # Connect to the Cloud via passed credentials workflow
            if generalArcGIS.credentials.lower() == 'oauth':
                outGIS = agl.connectAGOL_clientID(generalArcGIS=generalArcGIS, dmInstance=dmInstance)
            else:  # Connect via ArcGISPro Environment
                outGIS = agl.connectAGOL_ArcGIS(generalArcGIS=generalArcGIS, dmInstance=dmInstance)

            # Push Loc Manual
            outFun = agl.loadDataFrameToFeatureLayer(DFLocManual_SDF, inDic, outGIS, etlPCMInstance)
            logging.info(outFun)

            # Push Parking DataFrame
            inDic = {"title": f"SFAN_PCM_Plot_Locations_Parking_{yearValue}",
                     "tags": "San Francisco Bay Area Network, Plant Communities Monitoring, Plot Parking",
                     "type": "Feature Service",
                     "description": f"PCM Plot Locations Parking Info Feature Layer to be used for PCM Field Maps"
                                    f" Navigation {yearValue}",
                     "snippet": f"Feature Layer with PCM Plot Parking Info to be used for PCM Field Maps"
                                f" Navigation {yearValue}",
                     "licenseInfo": "This dataset is for internal use only and should not be distributed without "
                                    "permission."}

            outFun = agl.loadDataFrameToFeatureLayer(DFParking_SDF, inDic, outGIS, etlPCMInstance)
            logging.info(outFun)

            outETL = "Successfully finished process_PCMLocManual"
            return outETL

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_PCMLocationsManualParking.py - process_PCMLocManual: {e}'
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)