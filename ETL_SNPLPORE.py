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
        Define the QC Protocol instantiation attributes

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


            ######
            # Process Survey Metadata Form
            ######
            outDFSurvey = etl_SNPLPORE.proces_Survey(outDFDic, etlInstance, dmInstance)

            ######
            # Process SNPL Observations Form
            ######
            outDFObs = etl_SNPLPORE.proces_Observations(outDFDic, etlInstance, dmInstance, outDFSurvey)

            ######
            # Process Bands Sub Form
            ######
            outDFBands = etl_SNPLPORE.proces_Bands(outDFDic, etlInstance, dmInstance, outDFSurvey, outDFObs)

            ######
            # Process Predator
            ######
            outDFPredator = etl_SNPLPORE.proces_Predator(outDFDic, etlInstance, dmInstance, outDFSurvey)



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

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outDFSurvey: Data Frame of the exported form will be used in subsequent table ETL.
        """

        try:
            #Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None
            for key, df in outDFDic.items():
                if 'Survey' in key:
                    inDF = df
                    break

            # Create initial dataframe subset
            outDFSubset = inDF[['GlobalID', 'Survey Location', "SurveyDate", "Time Start Survey", "Time End Survey",
                            "CreationDate", "Creator"]].rename(
                columns={'GlobalID': 'Event_ID',
                         'SurveyDate': 'Start_Date',
                         'Time Start Survey': 'Start_Time',
                         'Time End Survey': 'End_Time',
                         'CreationDate': 'Created_Date',
                         'Creator': 'Created_By'})

            ##############################
            # Numerous Field CleanUp Steps
            ##############################
            #To DateTime Field
            outDFSubset['Start_Date'] = pd.to_datetime(outDFSubset['Start_Date'])
            # Format to m/d/yyy
            outDFSubset['Start_Date'] = outDFSubset['Start_Date'].dt.strftime('%m/%d/%Y')

            # Insert 'Location_ID' field
            outDFSubset.insert(1, "Location_ID", None)

            # Insert 'Protocol_Name' field
            outDFSubset.insert(2, "Protocol_Name", "PORE SNPL")

            fieldLen = outDFSubset.shape[1]
            # Insert 'DataProcesingLevelID' = 1
            outDFSubset.insert(fieldLen, "DataProcessingLevelID", 1)

            # Insert 'dataProcesingLevelDate
            from datetime import datetime
            dateNow = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
            outDFSubset.insert(fieldLen + 1, "DataProcessingLevelDate", dateNow)

            # Insert 'dataProcesingLevelUser
            outDFSubset.insert(fieldLen + 2, "DataProcessingLevelUser", etlInstance.inUser)


            #####################################
            # Define Location_Id via lookup table
            #####################################

            # Read in the Lookup Table
            inQuery = f"Select * FROM tbl_Locations';"

            outDFLookup = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)
            # Perform the lookup to field 'Location_ID'
            outDF_Step2 = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFLookup,
                                                                 "Loc_Name", "Location_ID",
                                                                 outDFSubset, "Survey Location",
                                                                 "Location_ID")

            # Drop field "Survey Location
            outDF_Step2 = outDF_Step2.drop(columns=['Survey Location'])


            ############################
            # Define desired field types
            ############################


            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ["Event_ID", "Location_ID", "Protocol_Name", "Start_Date", "Start_Time", "End_Time",
                                    "Created_Date", "Created_By", "DataProcessingLevelID", "DataProcessingLevelDate",
                                    "DataProcessingLevelUser"],
                             'Type': ["object", "object", "object", "datetime64", "datetime64", "datetime64",
                                    "datetime64", "object", "int64", "DataProcessingLevelDate",
                                    "object"],
                            'DateTimeFormat': ["na", "na", "na", "%m/%d/%Y", "%H:%M", "%H:%M",
                                    "%m/%d/%Y %H:%M", "na", "na", "na",
                                    "na"]}

            outDFSurvey = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDF_Step2)

            # Append outDFSurvey to 'tbl_Events'
            # Pass final Query to be appended
            insertQuery = (f'INSERT INTO tbl_Events (Event_ID, Location_ID, Protocol_Name, Start_Date, Start_Time, '
                           f'End_Time, Created_Date, Created_By, DataProcessingLevelID, DataProcessingLevelDate, '
                           f'DataProcessingLevelUser) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFSurvey, "tblEvents", insertQuery,
                                                        dmInstance)

            # STOPPED HERE 8/14/2024 KRS

            ##################
            # Define Observers -  table xref_EventContacts
            ##################






            ##################
            # Create tbl_Events_Details record - push field 'Survey Note' to 'Event_Notes', LE Violation,
            # LE Violation Notes, Predator Stop Time (min), DeviceName
            ###################




            # X and Y Fields are being ignored, These coordiantes are being pushed to the SNPL Observations table
















            #Define final query
            insertQuery = f""


            logMsg = f"Success ETL_SNPLPORE.py - process_Survey"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            #Might Return a DF with a query of tblEvents aftern appending - TBD

            return outDFSurvey

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - proces_Survey: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def proces_Observations(outDFDic, etlInstance, dmInstance, outDFSurvey):

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

        try:

            # Define final query
            insertQuery = f""



            logMsg = f"Success ETL_SNPLPORE.py - process_Observations."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            # Might Return a DF with a query of tblEvents aftern appending - TBD

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
            outDFBands = []

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