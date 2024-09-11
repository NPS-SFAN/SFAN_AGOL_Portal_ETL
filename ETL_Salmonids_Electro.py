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

            ######
            # Process  Pass/Water Quality - TO BE DEVELOPED
            ######
            outDFPassWQ = etl_SalmonidsElectro.process_Event(outDFEvent, etlInstance, dmInstance)

            ######
            # Process  Measurements - TO BE DEVELOPED
            ######
            #outDFMeasurements = etl_SalmonidsElectro.process_Event(outDFEvent, outDFPassWQ, etlInstance, dmInstance)








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
        ETL routine for the parent Event Form

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outDFSurvey: Data Frame of the exported form will be used in subsequent table ETL.
        """

        try:
            #Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None
            for key, df in outDFDic.items():
                if 'EFish' in key:
                    inDF = df
                    break

            # Create initial dataframe subset
            outDFSubset = inDF[['GlobalID', 'StreamID', "Device", "other_Device", "StartDate", "FieldSeason",
                                "Define Observers(s)", "other_Observer", "ProjectCode", "ProjectDescription",
                                "LocationID", "IndexReach", "IndexUnit", "BasinWideUnit", "BasinWideUnitCode",
                                "UnitType", "UnitTypeSecondary", "CalibrationPool", "Temp_C", "DO_percent",
                                "DO_mg_per_L", "Conductivity_uS_per_cm", "SpecificConductance_uS_per_cm",
                                "NumberOfPasses", "CreationDate"]].rename(
                columns={'Device': 'FieldDevice',
                         'Define Observers(s)': 'Observers',
                         'Temp_C': 'Temp',
                         'DO_percent': 'DO',
                         'DO_mg_per_L': 'DO mg/l',
                         'Conductivity_uS_per_cm': 'Conductivity',
                         'SpecificConductance_uS_per_cm': 'Specific Conductance',
                         'CreationDate': 'CreatedDate',
                         'Creator': 'CreatedBy'})

            ##############################
            # Numerous Field CleanUp Steps
            ##############################
            #To DateTime Field
            outDFSubset['StartDate'] = pd.to_datetime(outDFSubset['StartDate'])
            # Format to m/d/yyy
            outDFSubset['StartDate'] = outDFSubset['StartDate'].dt.strftime('%m/%d/%Y')

            # Change 'CreatedDate' to Date Time Format
            outDFSubset['CreatedDate'] = pd.to_datetime(outDFSubset['CreatedDate'])

            # Insert 'EventID' field - will populated via join on the 'GlobalID' field post join of records to tblEvents
            outDFSubset.insert(1, "EventID", np.nan)

            # Insert 'ProtocolID' field - setting default value to 2 - 'SFAN_IMD_Salmonids_1' see tluProtocolVersion
            outDFSubset.insert(2, "ProtocolID", 2)

            # Insert 'CreatedBy' field - define to inUser
            outDFSubset.insert(3, "CreatedBy", etlInstance.inUser)

            fieldLen = outDFSubset.shape[1]
            # Insert 'DataProcesingLevelID' = 1
            outDFSubset.insert(fieldLen, "DataProcessingLevelID", 1)

            # Insert 'dataProcesingLevelDate
            from datetime import datetime
            dateNow = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
            outDFSubset.insert(fieldLen + 1, "DataProcessingLevelDate", dateNow)

            # Insert 'dataProcesingLevelUser
            outDFSubset.insert(fieldLen + 2, "DataProcessingLevelUser", etlInstance.inUser)

            # Insert 'SurveyType'
            outDFSubset.insert(fieldLen + 3, "SurveyType", "EFISH")

            # Change 'CreatedDate' to Date Time Format
            outDFSubset['CreatedDate'] = pd.to_datetime(outDFSubset['CreatedDate'])

            ############################
            # Define desired field types
            ############################

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ['StreamID', 'FieldDevice', 'other_Device', 'StartDate', 'FieldSeason',
                                      'Observers', 'other_Observer', 'ProjectCode', 'ProjectDescription',
                                      'LocationID', 'IndexReach', 'IndexUnit', 'BasinWideUnit', 'BasinWideUnitCode',
                                      'UnitType', 'UnitTypeSecondary', 'CalibrationPool', 'Temp', 'DO',
                                      'DO mg/l', 'Conductivity', 'Specific Conductance',
                                      'NumberOfPasses', 'CreatedDate', 'DataProcessingLevelID', 'DataProcessingLevelDate',
                                      'DataProcessingLevelUser', 'SurveyType'],
                            'Type': ["int32", "object", "object", "datetime64", "object",
                                     "object", "object", "object", "object",
                                     "int32", "object", "object", "int32", "object",
                                     "object", "object", "object", "float32", "float32",
                                     "float32", "float32", "float32",
                                     "int32", "datetime64", "int32", "datetime64",
                                     "object", "object"],
                            'DateTimeFormat': ["na", "na", "na", "%m/%d/%Y", "na",
                                               "na", "na", "na", "na",
                                               "na", "na", "na", "na", "na",
                                               "na", "na", "na", "na", "na",
                                               "na", "na", "na",
                                               "na", "%m/%d/%Y %I:%M:%S %p", "na", "%m/%d/%Y %I:%M:%S %p",
                                               "na", "na"]}

            outDFSubset2 = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDFSubset)

            # Convert Nans in Object/String and defined Numeric fields to None, NaN will not import to text
            # fields in access.  Numeric fields when 'None' is added will turn to 'Object' fields but will import to the
            # numeric (e.g. Int or Double) fields still when an Object type with numeric only values and the added
            # none values. A real PITA None and Numeric is.
            cols_to_update = ['ProtocolID', 'StreamID', 'ProjectCode', 'SurveyType', 'ProjectDescription',
                              'DataProcessingLevelID', 'DataProcessingLevelUser', 'IndexReach']
            for col in cols_to_update:
                outDFSubset2[col] = dm.generalDMClass.nan_to_none(outDFSubset2[col])

            # Define the Mask - only work where "other_Device' is not na - If not NA not relevant to process
            mask = outDFSubset2['other_Device'].notna()

            # Replace 'other' in 'Device' field with the value from 'other_Device' where applicable
            outDFSubset2.loc[mask, 'FieldDevice'] = outDFSubset2.loc[mask].apply(
                lambda row: row['FieldDevice'].replace('other', row['other_Device']) if 'other' in row[
                    'FieldDevice'] else row['FieldDevice'], axis=1)

            outDFEvent = outDFSubset2[['GlobalID', 'ProtocolID', 'StreamID', 'ProjectCode', 'SurveyType',
                                      'ProjectDescription', 'FieldSeason', 'StartDate', 'FieldDevice', 'CreatedDate',
                                      'CreatedBy', 'DataProcessingLevelID', 'DataProcessingLevelDate',
                                      'DataProcessingLevelUser']]

            # Append outDFSurvey to 'tbl_Events'
            # Pass final Query to be appended
            insertQuery = (f'INSERT INTO tblEvents (GlobalID, ProtocolID, StreamID, ProjectCode, SurveyType, '
                           f'ProjectDescription, FieldSeason, StartDate, FieldDevice, CreatedDate, '
                           f'CreatedBy, DataProcessingLevelID, DataProcessingLevelDate,DataProcessingLevelUser) '
                           f'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFEvent, "tblEvents", insertQuery,
                                                        dmInstance)

            ################
            # Define the EventID Field via lookup

            inQuery = (f"SELECT tblEvents.EventID, tblEvents.GlobalID FROM tblEvents"
                       f" WHERE ((Not (tblEvents.GlobalID) Is Null));")

            outDFEventIDGlobalID = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Define the EventID via a join Global ID via lookup approach
            outDFSubset2wEventID = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFEventIDGlobalID,
                                                                         "GlobalID", "EventID",
                                                                         outDFSubset2, "GlobalID",
                                                                         "EventID")

            # Insert 'EventID' field - will populated via join on the 'GlobalID' field post join of records to tblEvents
            outDFEvent.insert(1, "EventID", np.nan)

            # Define the EventID via a join Global ID via lookup approach
            outDFEvent2 = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFEventIDGlobalID,
                                                                         "GlobalID", "EventID",
                                                                         outDFEvent, "GlobalID",
                                                                         "EventID")

            ####################################
            # Append outDFESurvey to 'tblEfishSurveys'
            ####################################

            # Water Quality variables set values of 0 to null - In Survey 123 forms year 1 there were required fields
            # and 0 where entered to allow for continued data entry
            fieldList = ['Temp', 'DO', 'DO mg/l', 'Conductivity', 'Specific Conductance']
            for field in fieldList:
                outDFSubset2wEventID[field] = outDFSubset2wEventID[field].replace(0, None)

            # Calibration pool is a Yes/No Boleen field, setting to 'True'/ 'False' (i.e. Boolen)
            outDFSubset2wEventID['CalibrationPool'] = outDFSubset2wEventID['CalibrationPool'].apply(
                lambda x: True if x == 'Yes' else False)


            # Get Fields IndexReach, IndexUnit and UnitType to None so Nan doesn't import



            # Define Survey DataFrame - with Subset
            outDFSurvey = outDFSubset2wEventID[['EventID', 'LocationID', 'IndexReach', 'IndexUnit', 'BasinWideUnit',
                                                'BasinWideUnitCode', 'UnitType', 'UnitTypeSecondary', 'CalibrationPool',
                                                'Temp', 'DO', 'DO mg/l', 'Conductivity', 'Specific Conductance',
                                                'NumberOfPasses']]

            # Pass final Query to be appended to tblEFishSurveys - Must have brackets around fields with spaces.
            insertQuery = (f'INSERT INTO tblEfishSurveys (EventID, LocationID, IndexReach, IndexUnit, BasinWideUnit, '
                           f'BasinWideUnitCode, UnitType, UnitTypeSecondary, CalibrationPool, Temp, DO, [DO mg/l], '
                           f'Conductivity, [Specific Conductance], NumberOfPasses) '
                           f'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')



            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFSurvey, "tblEfishSurveys", insertQuery,
                                            dmInstance)


            # STOPPED HER 9/11/2024 - KRS

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
            return outDFEvent2

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - proces_Survey: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

