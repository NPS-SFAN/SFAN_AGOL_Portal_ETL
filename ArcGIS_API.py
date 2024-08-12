"""
ArcGIS_API.py
Methods for working within AGOL/Portal and the ArcGIS API.
"""
#Import Required Dependices
import os, sys, traceback
import generalDM as dm
import logging

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
        """`

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

        :return: outDFListList - List of dataframes imported
        """

        #Connect to the Cloud via pass credentialling workflow
        if generalArcGIS.credentials.lower() = 'oauth':
            outGIS = generalArcGIS.connectAGOL_clientID(generalArcGIS)
        else: #Connect via ArcGISPro Environment
            outGIS = generalArcGIS.connectAGOL_ArcGIS(generalArcGIS)

        #Import the feature layer
        outDFListList = generalArcGIS.importFeatureLayer(outGIS, generalArcGIS, etlInstance, dmInstance)

    def importFeatureLayer(generalArcGIS, etlInstance, dmInstance):
        """
        Workflow for processing of the passed AGOL/Portal ID

        :param outGIS: GIS Connection
        :param generalArcGIS: ArcGIS instance
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance

        :return: outDFListList - List of dataframes imported
        """

        #Connect to the Cloud via pass credentialling workflow
        if generalArcGIS.credentials.lower() = 'oauth':
            outGIS = generalArcGIS.connectAGOL_clientID(generalArcGIS)
        else: #Connect via ArcGISPro Environment
            outGIS = generalArcGIS.connectAGOL_ArcGIS(generalArcGIS)

        #Import the feature layer
        outDFListList = generalArcGIS.importFeature(outGIS, etlInstance, dmInstance)


    def connectAGOL_clientID(cloudPath, pythonApp_ID):
        """
        Connect to defined AGOL or Portal Path via a passed Python Application OAuth 2.0 Credential

        :param cloudPath: Path to ESRI service being proceeded
        :param pythonApp_ID: Python Application OAuth 2.0 credential

        :return: Return GIS Connection to AGOL or Portal for the current user
        """

        try:
            gis = GIS(cloudPath, client_id=pythonApp_ID)
            print (f"Successfully connected to - {cloudPath}")

            return GIS
        except Exception as e:

            logMsg = f'ERROR - "Exiting Error connectAGOl_clientID - ArcGIS_API.py: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def connectAGOL_ArcGIS(cloudPath, pythonEnvGISPro):
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
