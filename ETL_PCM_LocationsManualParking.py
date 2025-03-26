"""
ETL_PCM_LocationsManualParking.py
ETL workflow pulling the Locations Manual/Parking information from the PCM Frontend Database and exporting (i.e. ETL) as
two Feature Layers on NPS Portal.

This is an ETL from Access to GIS Feature Layers on Portal.
"""
# Import Required Dependices
import pandas as pd
import numpy as np
import glob, os, sys
import traceback
import ETL as ETL
import generalDM as dm

import logging


# Set option in pandas to not allow chaining (views) of dataframes, instead force copy to be performed.
pd.options.mode.copy_on_write = True


class etl_PCMLocations:
    def __init__(self):

        """
        Define the QC Protocol instantiation attributes

        :param QueryToSummarize: Query in PCM Front End being ETL'd to NPS Portal
        :return: zzzz
        """
        # Class Variables

        numETL_PCMLocations = 0

        # Define Instance Variables
        self.QueryToSummarize = 'qrpt_PlotLocationsManual'

        numETL_PCMLocations += 1

    def process_PCMLocManual(etlInstance, dmInstance):

        """
        ETL workflow for the Annual PCM Locations Manual and Parking Feature layers to NPS Portal

        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outETL: String denoting 'Success' or 'Error' on ETL Processing
        """

        try:

            # Create the ETL PMCLocations Instance
            etlPCMInstance = etl_PCMLocations()

            # Import the 'qrpt_PlotLocationsManual' summary query routine to DataFrame
            inQuery = f'SELECT {etlPCMInstance.QueryToSummarize}.* FROM {etlPCMInstance.QueryToSummarize};'

            outDFPCm = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            logMsg = f"Success process_PCMLocManual.py - process_ETLSNPLPORE."
            logging.info(logMsg)

            outETL = "Success process_PCMLocManual"
            return outETL

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_PCMLocaitonsManualParking.py - process_PCMLocManual: {e}'
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
