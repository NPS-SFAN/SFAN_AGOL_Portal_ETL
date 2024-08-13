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

    def proces_Survey(outDFDic, etlInstance, dmInstance):

            """
            ETL routine for the parent survey form SFAN_SNPLPORE_Survey{YearVersion}- table.
            The majority of this information on this form will be pushed to the following tables:
            tblEvents
            xxxx

            :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
            :param etlInstance: ETL processing instance
            :param dmInstance: Data Management instance:

            :return:outDFSurvey: Data Frame of the exported form will be used in subsequent table ETL.
            """

        try:

            # Pass Output Data Frame
            outDFSurvey = []

            logMsg = f"Success ETL_SNPLPORE.py - proces_Survey."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            return outDFSurvey

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - proces_Survey: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

        def proces_Observations(outDFDic, etlInstance, dmInstance, outDFSurvey):
            try:
                """
                ETL routine for the 'SNPL Observation' form - table 'SNPL_Observations'.
                The major of information on this form is pushed to the following tables:
                tbl_SNPL_Observations
                xxxx

                :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
                :param etlInstance: ETL processing instance
                :param dmInstance: Data Management instance
                :param outDFSurvey: Data frame output from the proces_Survey method.  Used is workflow processing.

                :return:outDFObs: Data Frame with the exported/imported Observation data, to be used as needed.  
                """

                # Pass Output Data Frame
                outDFSurvey = []

                logMsg = f"Success ETL_SNPLPORE.py - process_Observations."
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.info(logMsg)

                return outDFObs

            except Exception as e:

                logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_Observations: {e}'
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.critical(logMsg, exc_info=True)
                traceback.print_exc(file=sys.stdout)

        def proces_Bands(outDFDic, etlInstance, dmInstance, outDFSurvey, outDFObs):

            """
            ETL routine for the 'SNPL Bands Sub Form' form - table 'tbl_SNPL_Banded'.
            The major of information on this form is pushed to the following tables:
            tbl_SNPL_Banded


            :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
            :param etlInstance: ETL processing instance
            :param dmInstance: Data Management instance
            :param outDFSurvey: Data frame output from the proces_Survey method.
            :param outDFObs: Data frame output from the process_Observations method.

            :return:outDFBands: Data Frame with the exported/imported data, to be used as needed.
            """

            try:
                # Pass Output Data Frame
                outDFSurvey = []

                logMsg = f"Success ETL_SNPLPORE.py - process_Bands."
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.info(logMsg)

                return outDFBands

            except Exception as e:

                logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_Bands: {e}'
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.critical(logMsg, exc_info=True)
                traceback.print_exc(file=sys.stdout)

    def proces_Predator(outDFDic, etlInstance, dmInstance, outDFSurvey):

        """
        ETL routine for the 'Predator Observations' form - table 'PredatorObservations_2'.
        The major of information on this form is pushed to the following tables:
        tbl_Predator_Survey


        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance
        :param outDFSurvey: Data frame output from the proces_Survey method.

        :return:outDFPredator: Data Frame with the exported/imported data
        """

        try:
            # Pass Output Data Frame
            outDFSurvey = []

            logMsg = f"Success ETL_SNPLPORE.py - process_Predators."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            return outDFPredator

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_Predators: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)