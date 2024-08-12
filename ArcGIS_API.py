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

    dateNow = datetime.now().strftime('%Y%m%d')

    def __init__(self):
    """
    Define the instantiated generalArcGIS instantiation attributes

    :param logFile: File path and name of .txt logFile
    :return: zzzz
    """

    def importFeatureLayer(flID, etlInstance, dmInstance):
        """
        Import the passed AGOL/Portal ID

        :param flID: Feature Layer ID
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance

        :return: Dataframe with the imported feature layer
        """


    def connectAGOL_clientID(cloudPath, pythonApp_ID):
        """
        Connect to defined AGOL or Portal Path via a passed Python Application OAuth 2.0 Credential

        :param cloudPath: Path to ESRI service being proceeded
        :param pythonApp_ID:

        :return: Return GIS Connection
        """

        try:
            gis = GIS(cloudPath, client_id=pythonApp_ID)




    def connectAGOL_ArcGIS(cloudPath, pythonEnvGISPro):
        """
        Connect to defined AGOL or Portal Path via the ArcGISPro Environment which will use the NPS AD credentials.
        This is an alternative to processing an Python Application ID in meethod 'connecdtAGOL_clientID'.

        :param cloudPath: Path to ESRI service being proceeded
        :param pythonEnvGISPro:  Full Path to the ArcGISPro Python Environment .exe on your local computer
                                Often will be C:/Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3/python.exe

        :return: Return GIS Connection
        """