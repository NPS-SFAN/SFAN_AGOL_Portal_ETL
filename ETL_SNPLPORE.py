"""
ETL_SNPLPORE.py
Methods/Functions to be used for Snowy Plover PORE ETL workflow.

"""

#Import Required Libraries
import pandas as pd
import glob, os, sys
import traceback
import ETL as ETL
import generalDM as dm
import logging
import log_config

logger = logging.getLogger(__name__)

class etl_SNPLPORE:
    def __init__(self):
        """
        Define the instantiated QC Protocol instantiation attributes

        :param TBD
        :return: zzzz
        """
        # Class Variables

        numETL_SNPLPORE = 0

        # Define Instance Variables
        #self.filterRecQuery = 'qsel_QA_Control'

        numETL_SNPLPORE += 1

    def process_ETLSNPLPORE(outDFDic, etlInstance, dmInstance):
        """
        Import files in passed folder to dataframe(s). Uses GLOB to get all files in the directory.
        Currently defined to import .csv, and .xlsx files

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outETL: String denoting 'Success' or 'Error' on ETL Processing
        """

        try:

            

            # Pass Variable Success
            outETL = 'Success'

            logMsg = f"Success ETL_SNPLPORE.py - process_ETLSNPLPORE."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            return outETL

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_ETLSNPLPORE: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)