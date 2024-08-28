"""
ETL_Salmonids_Electro.py
Methods/Functions to be used for Salmonids Electrofishing ETL workflow.
"""

#Import Required Libraries
import pandas as pd
import numpy as np
import glob, os, sys
import traceback
import ETL as ETL
import generalDM as dm
import logging
import log_config

logger = logging.getLogger(__name__)

class etl_SalmonidsElectro:
    def __init__(self):

        """
        Define the QC Protocol instantiation attributes

        :param TBD
        :return: zzzz
        """
        # Class Variables
        numETL_SalmonidsElectro = 0

        # Define Instance Variables
        numETL_SalmonidsElectro += 1

    def process_ETLElectro(outDFDic, etlInstance, dmInstance):

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
            # Process Event/Metadata from
            ######
            outDFEvent = etl_SalmonidsElectro.process_Event(outDFDic, etlInstance, dmInstance)

            logMsg = f"Success ETL_Salmonids_Electro.py - process_ETLElectro."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            outETL = "Success ETL Salmonids Electrofishing"
            return outETL

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_ETLSNPLPORE: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_Event(outDFDic, etlInstance, dmInstance):

        """
        ETL routine for the parent Event Form - TO BE DEVELOPED 8/28/2024

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
                                    "%m/%d/%Y %I:%M:%S %p", "na", "na", "na",
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

            ##################
            # Define Observers -  table xref_EventContacts
            # Harvest Mutli-select field Define Observers, if other, also harvest 'Specify Other.
            # Lookup table for contacts is tlu_Contacts - Contact_ID being pushed to table xref_EventContacts
            ##################

            outContactsDF = processSNPLContacts(inDF, etlInstance, dmInstance)
            #Retain only the Fields of interest
            outContactsDFAppend = outContactsDF[['Event_ID', 'Contact_ID']]

            insertQuery = (f'INSERT INTO xref_Event_Contacts (Event_ID, Contact_ID) VALUES (?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            #Append the Contacts to the xref_EventContacts table
            dm.generalDMClass.appendDataSet(cnxn, outContactsDFAppend, "xref_Event_Contacts", insertQuery,
                                            dmInstance)

            ##################
            # Create tbl_Events_Details record - push field 'Survey Note' to 'Event_Notes', LE Violation,
            # LE Violation Notes, Predator Stop Time (min), DeviceName
            ###################

            outDFEventDetails = inDF[['GlobalID', 'Survey Note', "LE Violation", "LE Violation Notes",
                                "Predator Stop Time (min)", "Collection Device"]].rename(
                columns={'GlobalID': 'Event_ID',
                         'Survey Note': 'Event_Notes',
                         'LE Violation': 'LE_Violation',
                         'LE Violation Notes': 'Violation_Notes',
                         'Predator Stop Time (min)': 'PredatorStop',
                         'Collection Device': 'DeviceName'})

            # Clean Up Notes Fields
            outDFEventDetails['Event_Notes'] = outDFEventDetails['Event_Notes'].str.replace(',', '')
            outDFEventDetails['Violation_Notes'] = outDFEventDetails['Violation_Notes'].str.replace(',', '')

            fieldTypeDic2 = {
                'Field': ["Event_ID", "Event_Notes", "LE_Violation", "Violation_Notes", "PredatorStop", "DeviceName"],
                'Type': ["object", "object", "object", "object", "int32", "object"],
                'DateTimeFormat': ["na", "na", "na", "na", "na", "na"]}

            # Check and Update Field types as needed
            outDFEventDetails2 = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic2,
                                                               inDF=outDFEventDetails)

            # Convert Nans in Object/String and defined Numeric fields to None, NaN will not import to text
            # fields in access.  Numeric fields when 'None' is added will turn to 'Object' fields but will import to the
            # numeric (e.g. Int or Double) fields still when an Object type with numeric only values and the added
            # none values. A real PITA None and Numeric is.
            cols_to_update = ["Event_Notes", "LE_Violation", "Violation_Notes", "DeviceName", "PredatorStop"]
            for col in cols_to_update:
                outDFEventDetails2[col] = dm.generalDMClass.nan_to_none(outDFEventDetails2[col])

            ## Predator Stop Time set Pandas NaN values to 0 so plays nice with Access - Not Using - 8/15/2024
            ##outDFEventDetails2['PredatorStop']=outDFEventDetails2['PredatorStop'].fillna(0)

            # Change Check Box Field If Yes to True and No to False
            outDFEventDetails2['LE_Violation'] = outDFEventDetails2['LE_Violation'].apply(
                lambda x: True if x == 'Yes' else False)

            #Define final query
            insertQuery = (f'INSERT INTO tbl_Event_Details (Event_ID, Event_Notes, LE_Violation, Violation_Notes'
                           f', PredatorStop, DeviceName) VALUES (?, ?, ?, ?, ?, ?)')

            # Connect to DB
            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            # Append misc Event Fields to the tbl_Event_Details table
            dm.generalDMClass.appendDataSet(cnxn, outDFEventDetails2, "tbl_Event_Details", insertQuery, dmInstance)

            logMsg = f"Success ETL Survey/Event Form ETL_SNPLPORE.py - process_Survey"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            # Returning the Dataframe survey which was pushed to 'tbl_Events, will be used in subsequent ETL.
            return outDFSurvey

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - proces_Survey: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

