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
import log_config

logger = logging.getLogger(__name__)


class generalArcGIS:

    numArcGISInstances = 0

    def __init__(self, layerID, cloudPath, credentials, pythonApp_ID):

        """
        Define the instantiated generalArcGIS instantiation attributes
    
        :param layerID: AGOL/Portal ID reference value
        :param cloudPath: URL to the NPS or Portal 
        :param credentials: Defines if using OAuth 2.0 or ArcGISPro credentials
        :param pythonApp_ID: If using OAuth 2.0 will need to pass the client Id value
                 
         
        :return: generalArcGIS instance with passed variable definitions and defined methods
        """

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
            # ####Temp Turning Off While AGOL is having performance issues 8/14/2024
            # # Connect to the Cloud via pass credentials workflow
            # if generalArcGIS.credentials.lower() == 'oauth':
            #     outGIS = connectAGOL_clientID(generalArcGIS=generalArcGIS, dmInstance=dmInstance)
            # else: #Connect via ArcGISPro Environment
            #     outGIS = connectAGOL_ArcGIS(generalArcGIS=generalArcGIS, dmInstance=dmInstance)
            #
            # # Import the feature layer
            # outFeatureLayer = importFeatureLayer(outGIS, generalArcGIS, etlInstance, dmInstance)
            # outzipPath = outFeatureLayer[0]
            # outName = outFeatureLayer[1]

            outzipPath = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\Salmonids\Natural\_Fish\Salmonids\Data\ETL\2024\SFAN_Salmonids_EFish_2024v1.2_20240911-133512.zip'
            outName = 'SFAN_Salmonids_EFish_2024v1.2_20240911-133512'
            # Extract Exported zip file and import .csv files to DBF files
            dm.generalDMClass.unZipZip(zipPath=outzipPath, outName=outName,outDir=etlInstance.outDir)
            # Path to Unzipped files
            fullPathZipped = f"{etlInstance.outDir}\\{outName}"
            # Import Extracted Files to Dataframes
            outDFDic = dm.generalDMClass.importFilesToDF(inDir=fullPathZipped)

            return outDFDic

        except Exception as e:

            logMsg = f'ERROR - processFeatureLayer - {generalArcGIS.cloudPath} - {etlInstance.flID}: {e}'
            print(logMsg)
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg)
            traceback.print_exc(file=sys.stdout)

def importFeatureLayer(outGIS, generalArcGIS, etlInstance, dmInstance):
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

        # Define Unique Name for item being exported - this will be exported to your AGOL Content home page thus
        # a unique name is desirable.
        from datetime import datetime
        dateTimeNow = datetime.now().strftime('%Y%m%d-%H%M%S')
        dataTileUnique = f'{item.title}_{dateTimeNow}'

        outZipFull = f'{etlInstance.outDir}\\{dataTileUnique}.zip'
        result = item.export(dataTileUnique, export_format='CSV', wait=True)

        outWorkDir = f'{etlInstance.outDir}'
        #Delete outZipFull if exists
        if os.path.exists(outZipFull):
            os.remove(outZipFull)
            logMsg = f'Deleted existing zip - {outZipFull}'
            print(logMsg)
            logging.info(logMsg)


        #Export Result to the zip file
        result.download(outWorkDir)


        #Add Log Messages
        logMsg = f'Successfully Downloaded from - {generalArcGIS.cloudPath} - {dataTileUnique}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.info(logMsg)
        traceback.print_exc(file=sys.stdout)

        return outZipFull, dataTileUnique

    except Exception as e:

        logMsg = f'ERROR - Downloading from - {generalArcGIS.cloudPath} - {etlInstance.flID}: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg)
        traceback.print_exc(file=sys.stdout)



def connectAGOL_ArcGIS(generalArcGIS, dmInstance):
    """
    Connect to defined AGOL or Portal Path via the ArcGISPro Environment which will use the NPS AD credentials.
    This is an alternative to processing an Python Application ID in meethod 'connecdtAGOL_clientID'.

    Full Path to the ArcGISPro Python Environment .exe on your local computer often will be
     C:/Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3/python.exe.  If you are behind the VPN you can simply
     connect to the 'Pro' environment.

    :param generalArcGIS: generalArcGIS instance
    :param dmInstance: dmInstance instance

    :return: Return GIS Connection
    """
    pathToAGOL = generalArcGIS.cloudPath

    try:

        # Not Sure how to get a defined path for 'AGOL' or 'Portal' and include the '
        #gis = GIS(pathToAGOL, None, None, None, None, False, True,
        #          None, None, 'Pro')
        #gis = GIS(url=pathToAGOL, username='Pro')
        gis = GIS('Pro')

        print(f"Successfully connected to - {pathToAGOL}")

        return gis

    except Exception as e:

        logMsg = f'ERROR - "Exiting Error connectAGOl_ArcGIS - ArcGIS_API.py: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)


def connectAGOL_clientID(generalArcGIS, dmInstance):

    """
    Connect to defined AGOL or Portal Path via a passed Python Application OAuth 2.0 Credential

    :param generalArcGIS: generalArcGIS instance
    :param dmInstance: dmInstance instance

    :return: Return GIS Connection to AGOL or Portal for the current user
    """

    pathToAGOL = generalArcGIS.cloudPath
    pythonID = generalArcGIS.pythonApp_ID
    try:
        gis = GIS(pathToAGOL, client_id=pythonID)
        print(f"Successfully connected to - {pathToAGOL}")

        return gis
    except Exception as e:

        logMsg = f'ERROR - "Exiting Error connectAGOl_clientID - ArcGIS_API.py: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)
