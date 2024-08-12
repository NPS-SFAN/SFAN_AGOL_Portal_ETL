"""
ArcGIS_API.py
Methods for working within AGOL/Portal and the ArcGIS API.
"""
#Import Required Dependices
import os, sys, traceback
import generalDM as dm
import logging
import arcgis
from arcgis.gis import GIS

logger = logging.getLogger(__name__)


class generalArcGIS:

    numArcGISInstances = 0

    def __init__(self, agolEnv, layerID, cloudPath, credentials, pythonApp_ID):

        """
        Define the instantiated generalArcGIS instantiation attributes
    
        :param agolEnv: Full path to the ArcGISPro desktop environment
        :param layerID: AGOL/Portal ID reference value
        :param cloudPath: URL to the NPS or Portal 
        :param credentials: Defines if using OAuth 2.0 or ArcGISPro credentials
        :param pythonApp_ID: If using OAuth 2.0 will need to pass the client Id value
                 
         
        :return: generalArcGIS instance with passed variable definitions and defined methods
        """

        self.agolEnv = agolEnv
        self.layerID = layerID
        self.cloudPath = cloudPath
        self.credentials = credentials
        self.pythonApp_ID = pythonApp_ID

        generalArcGIS.numArcGISInstances += 1

    def processFeatureLayer(generalArcGIS, etlInstance, dmInstance):
        """
        Workflow for processing of the passed AGOL/Portal ID

        :param generalArcGIS: ArcGIS instance
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance

        :return: outDFDic - Dictionary with all imported dataframes from the imported feature layer
        """

        try:

            #STOPPED HERE 8/12/2024 - connectAGOL_clientID not working is seeing an extra argument.
            # Connect to the Cloud via pass credentials workflow
            if generalArcGIS.credentials.lower() == 'oauth':
                cloudPath_LU = str(generalArcGIS.cloudPath)
                pythonApp_LU = str(generalArcGIS.pythonApp_ID)
                outGIS = connectAGOL_clientID(generalArcGIS.cloudPath, generalArcGIS.pythonApp_ID,
                                                            dmInstance)

            else: #Connect via ArcGISPro Environment
                outGIS = generalArcGIS.connectAGOL_ArcGIS(cloudPath_LU, pythonApp_LU)

            #Import the feature layer
            outFeatureLayer = generalArcGIS.importFeatureLayer(outGIS, generalArcGIS, etlInstance, dmInstance)
            outzipPath = outFeatureLayer[0]
            outName = outFeatureLayer[1]

            # Extract Exported zip file and import .csv files to DBF files
            dm.generalDMClass.unZipZip(zipPath=outzipPath, outName=outName,outDir=etlInstance.outDir)

            fullPathZipped = f"{outzipPath}\\{outName}"
            # Import Extracted Files to Dataframes
            outDFDic = dm.generalDMClass.importFilesToDF(inDir=fullPathZipped)

            return outDFDic

        except Exception as e:

            logMsg = f'ERROR - processFeatureLayer - {etlInstance.agolEnv} - {etlInstance.layerID}: {e}'
            print (logMsg)
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def importFeatureLayer(outGIS, generalArcGIS, etlInstance):
        """
        Workflow for processing of the passed AGOL/Portal ID

        :param outGIS: GIS Connection
        :param generalArcGIS: ArcGIS instance
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance

        :return: outZipFull: full path to zip file of imported layer for AGOL/Portal
        dataTitle: tile of the feature layer that was imported
        """

        try:
            layerIDLU = generalArcGIS.layerID

            #Pull the desired AGOL content via the AGOL ID
            item = outGIS.content.get(layerIDLU)

            # Define DateTile for Feature Layer being exported
            dataTitle = item.title

            outZipFull = f'{etlInstance.outDir}\workspace\outLayer_{dataTitle}.zip'
            result = item.export(dataTile, '.csv', wait=True)
            outWorkDir = f'{etlInstance.outDir}\\workspace'
            result.download(outWorkDir)

            logMsg = f'Successfully Downloaded from - {etlInsance.agolEnv} - {dataTitle}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.log(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

            return outZipFull, dataTitle

        except Exception as e:

            logMsg = f'ERROR - Downloading from - {etlInsance.agolEnv} - {etlInstance.layerID}: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def connectAGOL_clientID(inPath, pythonApp_ID, dmInstance):

        """
        Connect to defined AGOL or Portal Path via a passed Python Application OAuth 2.0 Credential

        :param cloudPath: Path to ESRI service being ETL processed
        :param pythonApp_ID: Python Application OAuth 2.0 credential

        :return: Return GIS Connection to AGOL or Portal for the current user
        """

        try:
            gis = GIS(inPath, client_id=pythonApp_ID)
            print(f"Successfully connected to - {cloudPath}")

            return gis
        except Exception as e:

            logMsg = f'ERROR - "Exiting Error connectAGOl_clientID - ArcGIS_API.py: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def connectAGOL_ArcGIS(cloudPath, pythonEnvGISPro, dmInstance):
        """
        Connect to defined AGOL or Portal Path via the ArcGISPro Environment which will use the NPS AD credentials.
        This is an alternative to processing an Python Application ID in meethod 'connecdtAGOL_clientID'.

        :param cloudPath: Path to ESRI service being proceeded
        :param pythonEnvGISPro:  Full Path to the ArcGISPro Python Environment .exe on your local computer
                                Often will be C:/Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3/python.exe

        :return: Return GIS Connection
        """

        try:
            gis = GIS(cloudPath, client_id=pythonApp_ID)
            print(f"Successfully connected to - {cloudPath}")

            return GIS

        except Exception as e:

            logMsg = f'ERROR - "Exiting Error connectAGOl_ArcGIS - ArcGIS_API.py: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
