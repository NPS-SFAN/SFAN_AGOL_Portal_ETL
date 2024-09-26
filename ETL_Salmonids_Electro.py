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
            # ETL Event/Survey/Observers
            ######
            outDFEventSurvey = etl_SalmonidsElectro.process_Event_Electrofishing(outDFDic, etlInstance, dmInstance)
            outDFEvent= outDFEventSurvey[0]

            ######
            # ETL Passes
            ######
            outDFPassWQ = etl_SalmonidsElectro.process_Pass_Electrofishing(outDFDic, outDFEvent, etlInstance,
                                                                           dmInstance)
            ######
            # ETL Process Measurements
            ######
            outDFMeasurements = etl_SalmonidsElectro.process_Measurements_Electrofishing(outDFDic, outDFEvent,
                                                                                        etlInstance, dmInstance)
            ######
            # ETL Process Counts (combined Measurements and Tallied)
            ######
            outDFCounts = etl_SalmonidsElectro.process_Counts_Electrofishing(outDFDic, outDFEvent, outDFMeasurements,
                                                                             outDFPassWQ, etlInstance, dmInstance)

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

    def process_Event_Electrofishing(outDFDic, etlInstance, dmInstance):

        """
        ETL routine for the parent Event Form for Electrofishing. Processes the main parent form {SFAN_Salmonids_EFish_}
        Data is ETL'd to tblEvents, tblEfishSurveys, tblEventObservers.

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outDFEvent2 - Dataframe of the import records to tblEvents.
                outDFSurvey - Dataframe of the imported records to tblEfishSurveys
                outContactsDF - Dataframe with the imported Observer/Contacts per event to tblEventObservers
        """

        try:
            #Export the Parent Event/Survey Dataframe from Dictionary List - Wild Card in Key is *EFish*
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
                                "NumberOfPasses", "CreationDate", "Creator"]]

            # Rename might be best to not include in the subset operation
            outDFSubset.rename(columns={'Device': 'FieldDevice',
                               'Define Observers(s)': 'Observers',
                               'Temp_C': 'Temp',
                               'DO_percent': 'DO',
                               'DO_mg_per_L': 'DO mg/l',
                               'Conductivity_uS_per_cm': 'Conductivity',
                               'SpecificConductance_uS_per_cm': 'Specific Conductance',
                               'CreationDate': 'CreatedDate',
                               'Creator': 'CreatedBy'}, inplace=True)

            # Address NAN values pushed to None - Do this prior to data type conversion
            cols_to_update = ['IndexReach', 'IndexUnit',
                              'UnitTypeSecondary', 'ProjectDescription']
            for col in cols_to_update:
                outDFSubset[col] = dm.generalDMClass.nan_to_none(outDFSubset[col])

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

            # # Insert 'CreatedBy' field - define to inUser - Use variable passed in survey
            # outDFSubset.insert(3, "CreatedBy", etlInstance.inUser)

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

            # Calibration pool is a Yes/No Boleen field, setting to 'True'/ 'False' (i.e. Boolean)
            outDFSubset2wEventID['CalibrationPool'] = outDFSubset2wEventID['CalibrationPool'].apply(
                lambda x: True if x == 'Yes' else False)

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

            ##################
            # Define tblEventObservers
            # Harvest Mutli-select field Define Observers, if other, also harvest 'Specify Other.
            # Lookup table for contacts is tluObserver
            ##################

            outContactsDF = process_SalmonidsContacts(outDFSubset2wEventID, etlInstance, dmInstance)

            # Retain Needed fields
            outContactsDF.drop(columns={'Observers'}, inplace=True)

            # Insert the 'CreateDate' field
            from datetime import datetime
            dateNow = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
            outContactsDF.insert(2, 'CreatedDate', dateNow)

            insertQuery = (f'INSERT INTO tblEventObservers (EventID, OBSCODE, CreatedDate) VALUES (?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            #Append the Contacts to the xref_EventContacts table
            dm.generalDMClass.appendDataSet(cnxn, outContactsDF, "tblEventObservers", insertQuery,
                                            dmInstance)

            logMsg = f"Success ETL Event/Survey Form ERL_Salmonids_Electro.py - process_Event"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            # Returning the Dataframe survey which was pushed to 'tbl_Events, will be used in subsequent ETL.
            return outDFEvent2, outDFSurvey, outContactsDF

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - proces_Survey: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_Pass_Electrofishing(outDFDic, outDFEvent, etlInstance, dmInstance):

        """
        ETL routine for the Pass form of the Electrofishing Survey 123 form.
        Data is processed to

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param outDFEvent - Dataframe with the processed Event/Survey info
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:
        """

        try:
            #Export the Parent Event/Survey Dataframe from Dictionary List - Wild Card in Key is *EFish*
            inDF = None
            for key, df in outDFDic.items():
                if 'Passes' in key:
                    inDF = df
                    break

            # Create initial dataframe subset
            outDFSubset = inDF[['GlobalID', 'Pass', 'PassType', 'Time_s', 'Volts', 'Setting', 'Comments', 'QCFlag',
                                'QCNotes', 'ParentGlobalID', 'CreationDate', 'Creator']]

            # Rename fields
            outDFSubset.rename(columns={'Time_s': 'Time',
                               'CreationDate': 'CreatedDate',
                               }, inplace=True)

            # Address NAN values pushed to None - Do this prior to data type conversion
            cols_to_update = ['Volts', 'Setting', 'Comments', 'QCFlag', 'QCNotes', 'PassType', 'Time']
            for col in cols_to_update:
                outDFSubset[col] = dm.generalDMClass.nan_to_none(outDFSubset[col])

            # Join on GlobalID to get the EventID
            outDFPass = pd.merge(outDFSubset, outDFEvent[['GlobalID', 'EventID']], left_on='ParentGlobalID', right_on='GlobalID',
                                 how='inner', suffixes=('', '_y'))

            # Drop fields
            outDFPass.drop(columns={'GlobalID_y', 'GlobalID', 'Creator', 'ParentGlobalID'}, inplace=True)


            # Define CreatedDate to DateTime
            outDFPass['CreatedDate'] = pd.to_datetime(outDFPass['CreatedDate'], format='%m/%d/%Y %I:%M:%S %p',
                                                      errors='coerce')

            # Set Time to lowest interger
            outDFPass['Time'] = pd.to_numeric(outDFPass['Time'], errors='coerce', downcast='integer')

            # Set Time as Object
            outDFPass['Time'] = outDFPass['Time'].astype(object)

            outDFPass2 = outDFPass[['EventID', 'QCFlag', 'QCNotes', 'Pass', 'PassType', 'Volts', 'Time',
                                    'Setting', 'Comments', 'CreatedDate']]
            # Time IS a Reserved Word must be enclose in []
            insertQuery = (f'INSERT INTO tblSummerPasses (EventID, QCFlag, QCNotes, Pass, PassType, Volts, [Time], '
                           f'Setting, Comments, CreatedDate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            # Append the Contacts to the xref_EventContacts table
            dm.generalDMClass.appendDataSet(cnxn, outDFPass2, "tblSummerPasses", insertQuery,
                                            dmInstance)

            logMsg = f"Success ETL EFishing Pass ETL_Salmonids_Electro.py - process_Pass_Electrofishing"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            # Returning the Dataframe survey which was pushed to 'tblSummerPasses
            return outDFPass

        except Exception as e:

            logMsg = f'WARNING ERROR  ETL EFishing Pass ETL_Salmonids_Electro.py - process_Pass_Electrofishing: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_Measurements_Electrofishing(outDFDic, outDFEvent, etlInstance, dmInstance):

        """
        ETL routine for the Measurements form data in the Electrofishing Survey 123 form.
        Data is processed to table 'tblSummerMeasurements'.  This is the data on which measurements were made.
        By Event and Pass first ten 'COHO' occurrences are given a 'RandomSample' value of 'True', all other
        records by Event, Pass are given a 'RandomSample' value of 'False'

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param outDFEvent - Dataframe with the processed Event/Survey info
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outDFMeasurements - Measurements dataframe appended to table 'tblSummerMeasurements
        """

        try:
            #Export the Measurements dataframe Dictionary List - Wild Card in Key is *Measurements*
            inDF = None
            for key, df in outDFDic.items():
                if 'Measurements' in key:
                    inDF = df
                    break

            # Create initial dataframe subset
            outDFSubset = inDF[['Pass', 'SpeciesCode', 'LifeStage', 'Tally', 'NumberOfFish',
                                'ForkLength_mm', 'LengthCategoryID', 'TotalWeight_g', 'BagWeight_g', 'FishWeight_g',
                                'RandomSample', 'Injured', 'Dead', 'Scales', 'Tissue', 'EnvelopeID', 'PriorSeason',
                                'PITTag', 'Comments', 'QCFlag', 'QCNotes', 'ParentGlobalID', 'CreationDate']]

            # Rename fields
            outDFSubset.rename(columns={'ForkLength_mm': 'ForkLength',
                                        'TotalWeight_g': 'TotalWeight',
                                        'BagWeight_g': 'BagWeight',
                                        'FishWeight_g': 'FishWeight',
                                        'CreationDate': 'CreatedDate'}, inplace=True)

            # Subset to only the 'Tally' = 'No' fields, that is records with measurements
            outDFSubset = outDFSubset[outDFSubset['Tally'] == 'No']

            # Fields Tissue and Scales and Text fields but are treated like Boolean.  If Null value set to 'No'
            outDFSubset['Tissue'] = outDFSubset['Tissue'].fillna('No')
            outDFSubset['Scales'] = outDFSubset['Scales'].fillna('No')

            # Address NAN values pushed to None - Do this prior to data type conversion
            cols_to_update = ['NumberOfFish', 'ForkLength', 'LengthCategoryID', 'TotalWeight', 'BagWeight',
                              'FishWeight', 'Scales', 'Tissue', 'EnvelopeID', 'PITTag', 'Comments', 'QCFlag',
                              'QCNotes', 'CreatedDate']

            for col in cols_to_update:
                outDFSubset[col] = dm.generalDMClass.nan_to_none(outDFSubset[col])

            # LifeStage field has lookup code value of text 'NA' which is being imported as nan change these values
            # to a text 'NA'
            outDFSubset['LifeStage'] = outDFSubset['LifeStage'].fillna('NA')

            # Change Yes - No Fields to 'True'/ 'False' (i.e. Boolean)
            outDFSubset['Tally'] = outDFSubset['Tally'].apply(
                lambda x: True if x == 'Yes' else False)
            outDFSubset['Injured'] = outDFSubset['Injured'].apply(
                lambda x: True if x == 'Yes' else False)
            outDFSubset['Dead'] = outDFSubset['Dead'].apply(
                lambda x: True if x == 'Yes' else False)
            outDFSubset['PriorSeason'] = outDFSubset['PriorSeason'].apply(
                lambda x: True if x == 'Yes' else False)


            # Join on GlobalID to get the EventID
            outDFMeasurements = pd.merge(outDFSubset, outDFEvent[['GlobalID', 'EventID']], left_on='ParentGlobalID',
                                         right_on='GlobalID', how='inner', suffixes=('', '_y'))

            # Drop fields Not Being Appended
            outDFMeasurements.drop(columns={'GlobalID', 'ParentGlobalID'}, inplace=True)

            # Define CreatedDate to DateTime
            outDFMeasurements['CreatedDate'] = pd.to_datetime(outDFMeasurements['CreatedDate'], format='%m/%d/%Y %I:%M:%S %p',
                                                              errors='coerce')

            # Define NumberOfFish (i.e. Count) for measurements dataset where null should be all as 1.
            outDFMeasurements['NumberOfFish'] = outDFMeasurements['NumberOfFish'].fillna(1)

            ######################
            # Define Random Sample
            ######################

            # Set all RandomSample values to False - then will define the first ten Coho by Pass to 'True' (i.e. Yes Random)
            outDFMeasurements['RandomSample'] = False

            # Apply the 'assignRandomSampleFirst10' after grouping by 'Event' and 'Pass' to
            outDFMeasurementswRandom = outDFMeasurements.groupby(['EventID', 'Pass']).apply(assignRandomSampleFirst10)
            ##################################################

            # Define insert/append query
            insertQuery = (f'INSERT INTO tblSummerMeasurements (Pass, SpeciesCode, LifeStage, Tally, NumberOfFish, '
                           f'ForkLength, LengthCategoryID, TotalWeight, BagWeight, FishWeight, RandomSample, Injured, '
                           f'Dead, Scales, Tissue, EnvelopeID, PriorSeason, PITTag, Comments, QCFlag, QCNotes, '
                           f'CreatedDate, EventID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                           f'?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            # Append the Contacts to the xref_EventContacts table
            dm.generalDMClass.appendDataSet(cnxn, outDFMeasurementswRandom, "tblSummerMeasurements", insertQuery,
                                            dmInstance)

            logMsg = f"Success ETL EFishing Pass ETL_Salmonids_Electro.py - process_Measurements_Electrofishing"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            # Returning the Dataframe survey which was pushed to 'tblSummerPasses
            return outDFMeasurements

        except Exception as e:

            logMsg = f'WARNING ERROR  ETL EFishing Pass ETL_Salmonids_Electro.py - process_Measurements_Electrofishing: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)


    def process_Counts_Electrofishing(outDFDic, outDFEvent, outDFMeasurements, outDFPassWQ, etlInstance, dmInstance):

        """
        ETL routine for the Count data in the Electrofishing Survey 123 form. Routine will calculate the data going to
        the 'tblSummerCount' table. This data is the total across Measured and Tallied data. Fish counts for each pass
        during electrofishing and snorkeling surveys. Count values per records are the combined measured and
        unmeasured fish by Species and Lifeform.

        The processed measurement data in 'process_Measurements_Electrofishing' and dataframe 'outDFMeasurements' is
        used in addition to the 'Tallied' data when calculating the Total Count (measured and unmeasured) by Pass,
        Taxon, and Lifeform per survey.

        NOTE - Survey 123 Form in Field Season 2025 needs to be modified to by adding a 'Mortality' field to the survey.
        This will also need to be added to this ETL workflow - KRS 20240913

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param outDFEvent - Dataframe with the processed Event/Survey info
        :param outDFPassWQ - Dataframe wit the Pass information - used to defined PassType field
        :param outDFMeasurements - Dataframe with the measurements data pushed to tblSummerMeasurements
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outDFCounts - Counts (measured and unmeasured) dataframe appended to table 'tblSummerCounts'
        """
        try:

            # Export the Measurements dataframe Dictionary List - Wild Card in Key is *Measurements*
            inDF = None
            for key, df in outDFDic.items():
                if 'Measurements' in key:
                    inDF = df
                    break

            # Create initial dataframe subset
            outDFSubset = inDF[['Pass', 'SpeciesCode', 'LifeStage', 'Tally', 'NumberOfFish', 'Comments',
                                'QCFlag', 'QCNotes', 'ParentGlobalID', 'Dead']]

            # Rename fields
            outDFSubset.rename(columns={'NumberOfFish': 'Count'}, inplace=True)

            # Subset to Tally = Yes (i.e. No Measurements data
            outDFSubset = outDFSubset[outDFSubset['Tally'] == 'Yes']

            # Address NAN values pushed to None - Do this prior to data type conversion
            cols_to_update = ['Comments', 'QCNotes', 'QCFlag']

            for col in cols_to_update:
                outDFSubset[col] = dm.generalDMClass.nan_to_none(outDFSubset[col])

            # LifeStage field has lookup code value of text 'NA' which is being imported as nan change these values
            # to a text 'NA'
            outDFSubset['LifeStage'] = outDFSubset['LifeStage'].fillna('NA')

            # Join ParentGlobalID on GlobalID to get the EventID in the Events Dataframe
            outDFCounts = pd.merge(outDFSubset, outDFEvent[['GlobalID', 'EventID']], left_on='ParentGlobalID',
                                   right_on='GlobalID', how='inner', suffixes=('', '_y'))

            # Define Mortality Field to the Count Value if 'Dead' = 'Yes' else set to zero.
            outDFCounts['Mortality'] = np.where(outDFCounts['Dead'] == 'Yes', outDFCounts['Count'], 0)

            # Drop fields Not Being Appended
            outDFCounts.drop(columns={'GlobalID', 'ParentGlobalID', 'Tally', 'Dead'}, inplace=True)

            # Add 'Source' field = Counts
            outDFCounts.insert(8, 'Source', 'Counts')

            # Prior to Composite can't have any None or nan values
            outDFCounts['QCFlag'] = outDFCounts['QCFlag'].fillna('NoValue')

            # Create Composite Index
            outDFCounts['Composite'] = (outDFCounts['EventID'].astype(str) + '-' + outDFCounts['Pass'].astype(str)
                                        + '-' + outDFCounts['SpeciesCode'] + '-' + outDFCounts['LifeStage'] + '-' +
                                        outDFCounts['QCFlag'])

            # Working to Identify Duplicate Tally Records
            # Create Series index with counts by 'Composite Index'
            counts = outDFCounts['Composite'].value_counts()

            # Add field with count of the 'Composite Index
            outDFCounts['ReCount'] = outDFCounts['Composite'].map(counts)

            # Adding Sort to easily see duplicates or more than one per composite
            outDFCounts = outDFCounts.sort_values(by='Composite')

            if outDFCounts['ReCount'].max() > 1:
                # Where Duplicates on the 'Composite' index fields - Merge records into one. Delete in the 'outDFCounts'
                # dataframe and append back in the merged records

                # Not Unique Dataframe
                outDFCountsNotUnique = outDFCounts[outDFCounts['ReCount'] > 1]

                outDFCountsNotUniqueSub1 = outDFCountsNotUnique[['EventID', 'Pass', 'SpeciesCode', 'LifeStage',
                                                                 'QCFlag', 'Composite']]

                # Define Unique Index on the ['EventID', 'Pass', 'SpeciesCode', 'LifeStage', 'QCFlag'] fields
                outDFCountsUnique = outDFCountsNotUniqueSub1.drop_duplicates(
                    subset=['EventID', 'Pass', 'SpeciesCode', 'LifeStage', 'QCFlag', 'Composite'])

                # Calculate the 'Sum' of the 'Count' field across the duplicates - GroupBy on the 'Composite' field
                outDFCountsNotUniqueToSum = outDFCountsNotUnique[['Composite', 'Count']]

                dupCountRecDF = outDFCountsNotUniqueToSum.groupby(['Composite']).sum()

                dupCountRecDF.reset_index(inplace=True)

                # Join the 'dupCountRecDF' with the 'Count' field summed across the duplciates to the df to be appended
                outDFCountsUniqueToAppend = pd.merge(outDFCountsUnique, dupCountRecDF[['Composite', 'Count']],
                                                     left_on='Composite', right_on='Composite', how='inner',
                                                     suffixes=('', '_y'))

                # Insert 'QCNotes', and 'Source' fields
                outDFCountsUniqueToAppend.insert(2, 'QCNotes', None)
                outDFCountsUniqueToAppend.insert(1, 'Source', 'Counts')

                # Drop Records in the 'outDFCounts' where 'RecCount > 1
                outDFCounts2 = outDFCounts[outDFCounts['ReCount'] <= 1]

                # Append Dups that have been Summed with tne non duplicates
                outDFCounts3 = pd.concat([outDFCounts2, outDFCountsUniqueToAppend], axis=0)

                outDFCounts = outDFCounts3[['Pass', 'SpeciesCode', 'LifeStage', 'Count', 'Comments',
                                                        'QCFlag', 'QCNotes', 'EventID', 'Mortality']]

                outDFCounts.insert(8, 'Source', 'Counts')

                del outDFCounts2, outDFCounts3
            else:
                # Drop Count and Composite fields
                outDFCounts.drop(columns={'RecCount', 'Composite'}, inplace=True)

            # Replace NaN values in 'Mortality' with 0
            outDFCounts['Mortality'] = outDFCounts['Mortality'].fillna(0)

            print(outDFCounts)

            #########################################
            # Start processing Measurements Dataframe
            # #######################################

            # Clean Up 'outDFMeasurements' to match the OutDFCounts field schema
            outDFMeasurementsMatch = outDFMeasurements[['Pass', 'SpeciesCode', 'LifeStage', 'NumberOfFish', 'Comments',
                                                        'QCFlag', 'QCNotes', 'EventID', 'Dead']]

            # Change field name NumberOfFish to Count
            outDFMeasurementsMatch.rename(columns={'NumberOfFish': 'Count'}, inplace=True)

            # Define Mortality Field to the Count Value if 'Dead' = True else set to zero.
            outDFMeasurementsMatch['Mortality'] = np.where(outDFMeasurementsMatch['Dead'] == True,
                                                           outDFMeasurementsMatch['Count'], 0)

            # Drop Dead Field
            outDFMeasurementsMatch.drop(columns=['Dead'], inplace=True)

            # Add 'Source' field = Measurements
            outDFMeasurementsMatch.insert(8, 'Source', 'Measurements')

            # Set QC Value None to 'NoValue'
            outDFMeasurementsMatch['QCFlag'] = outDFMeasurementsMatch['QCFlag'].fillna('NoValue')

            # Append the 'outDFCounts' and 'outDFMeasurements' dataframes
            outDFCountswMeasure= pd.concat([outDFCounts, outDFMeasurementsMatch], axis=0)

            # Create a dataframe with only the subset of fields with the unique index to be summed, this will be the
            # value pushed to the 'tblSummerCounts' with accomanying metadata (e.g. PassType, and Tally Comments)
            outDFToSummary = outDFCountswMeasure[['EventID', 'Pass', 'SpeciesCode', 'LifeStage', 'QCFlag', 'Count']]

            # Group By Summary doesn't handle nan or none values so setting 'QCFlag' null values to NoFlag
            outDFToSummary['QCFlag'] = outDFToSummary['QCFlag'].fillna('NoFlag')

            # Summarize Counts by EventID, Pass, SpeciesCode, LifeStage, 'QCFlag'
            outSummary = outDFToSummary.groupby(['EventID', 'Pass', 'SpeciesCode', 'LifeStage', 'QCFlag']).sum()

            # Remove Index on Group By
            outSummaryNoIndex = outSummary.reset_index()

            # Set the QCFlag = 'NoFlag' back to None
            outSummaryNoIndex['QCFlag'] = outSummaryNoIndex['QCFlag'].replace('NoFlag', None)

            # Define the Pass Type via the PassWQ dataframe
            outDFCountswPass = pd.merge(outSummaryNoIndex, outDFPassWQ[['EventID', 'Pass', 'PassType']],
                                        left_on=['EventID', 'Pass'], right_on=['EventID', 'Pass'],
                                        how='left', suffixes=('', '_y'))

            ###############################################
            # Summarize the Total Mortalities by index 'EventID', 'Pass', 'SpeciesCode', 'LifeStage', this will be
            # the 'Mortality' field value by the above index.
            outDFCountswMeasureSub = outDFCountswMeasure[['EventID', 'Pass', 'SpeciesCode', 'LifeStage', 'Mortality',
                                                          'QCFlag']]

            outDFMortalityByIndex = outDFCountswMeasureSub.groupby(['EventID', 'Pass', 'SpeciesCode',
                                                                    'LifeStage', 'QCFlag']).sum()

            # Remove the index
            outDFMortalityByIndex.reset_index(inplace=True)

            # Define the Mortality Field on the index 'EventID', 'Pass', 'SpeciesCode', 'LifeStage', 'QCFlag'
            outDFCountswPass_wMort = pd.merge(outDFCountswPass, outDFMortalityByIndex[['EventID', 'Pass', 'SpeciesCode',
                                                                                       'LifeStage', 'QCFlag',
                                                                                       'Mortality']],
                                              on=['EventID', 'Pass', 'SpeciesCode', 'LifeStage', 'QCFlag'],
                                              how='left', suffixes=('', '_y'))

            ################################
            # Check is the PassType is nan, if yes then there is an undefined pass that should be addressed post ETL
            # Routine.  Should still be able to Append but will want to define an accompanying Pass and Pass Type in the
            # tblSummerPasses tables
            outDFCountswPassNull = outDFCountswPass_wMort[outDFCountswPass['PassType'].isna()]
            numRec = outDFCountswPassNull.shape[0]
            if numRec >= 1:

                # Flag records with no PassType as PND = Pass Not Defined during data processing, extract, transform
                # and load
                outDFCountswPass_wMort.loc[outDFCountswPass['PassType'].isna(), 'QCFlag'] = 'PND'
                outDFCountswPassNull.loc[outDFCountswPassNull['PassType'].isna(), 'QCFlag'] = 'PND'
                logMsg = (f'WARNING there are {numRec} records without a defined PassType, adding QCFlag - PND.')

                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.warning(logMsg)

                outPath = f'{etlInstance.outDir}\RecordsSalmonids_Counts_Pass_NoDefinedPass.csv'
                if os.path.exists(outPath):
                    os.remove(outPath)

                outDFCountswPassNull.to_csv(outPath, index=True)

                logMsg = (f'WARNING Exporting Records without a defined Pass in Summer Counts see - {outPath} \n'
                          f'Flagged with QC Flag PND - Address these pass omissions post ETL routine.')
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.warning(logMsg)

                # Set nan PassType value to None so will append without issue
                outDFCountswPass_wMort['PassType'] = dm.generalDMClass.nan_to_none(outDFCountswPass_wMort['PassType'])

            # Add back the Comments, QCNotes and Mortality fields for the Tally records (i.e. dataframe outDFCounts) by EventID,
            # Pass, Species, LifeStage from the 'outDFCounts' DF.

            # Define Composite Index values on the 'outDFCount' (i.e. Tally Records) and the 'outDFCountsWPass' df so
            # a join can be done to populate the 'QCNotes' and 'Comments' fields in the 'Tally' Records.  Only QCNotes
            # and Comments are being retained for the 'Tally' reocrds, these attributes are being retain for the m
            # measurement records in the 'tblSummerMeasurments' table

            # Prior to Composite can't have any None or nan values
            outDFCounts['QCFlag'] = outDFCounts['QCFlag'].fillna('NoValue')

            # Create Composite Index
            outDFCounts['Composite'] = (outDFCounts['EventID'].astype(str) + '-' + outDFCounts['Pass'].astype(str)
                                        + '-' + outDFCounts['SpeciesCode'] + '-' + outDFCounts['LifeStage'] + '-' +
                                        outDFCounts['QCFlag'])

            # Prior to Composite can't have any None or nan values
            outDFCountswPass_wMort['QCFlag'] = outDFCountswPass_wMort['QCFlag'].fillna('NoValue')

            outDFCountswPass_wMort['Composite'] = (outDFCountswPass_wMort['EventID'].astype(str) + '-' +
                                                   outDFCountswPass_wMort['Pass'].astype(str)
                                                   + '-' + outDFCountswPass_wMort['SpeciesCode'] + '-'
                                                   + outDFCountswPass_wMort['LifeStage'] + '-' +
                                                   outDFCountswPass_wMort['QCFlag'])

            # Join the DF wit the Counts and the dataframe with the Tally 'QCNotes' and 'Comments' fields so these
            # fields are not dropped during the Count Summary Routine
            outDFCountswPasswComments = pd.merge(outDFCountswPass_wMort, outDFCounts[['Composite', 'QCNotes',
                                                                                      'Comments']],
                                                 left_on='Composite',
                                                 right_on='Composite',
                                                 how='left', suffixes=('', '_y'))

            # Set the nan to None
            cols_to_update = ['Comments', 'QCNotes']

            for col in cols_to_update:
                outDFCountswPasswComments[col] = dm.generalDMClass.nan_to_none(outDFCountswPasswComments[col])

            # Set 'QCFlag' value 'NoValue' to None prior to appending so Access doesn't have issue
            outDFCountswPasswComments.loc[outDFCountswPasswComments['QCFlag'] == 'NoValue', 'QCFlag'] = None

            # Define Created Date
            from datetime import datetime
            dateNow = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
            outDFCountswPasswComments['CreatedDate'] = dateNow

            # Drop Composite
            outDFCountswPasswComments.drop(columns={'Composite'}, inplace=True)

            # Append final query to the count table
            insertQuery = (f'INSERT INTO tblSummerCounts (EventID, Pass, SpeciesCode, LifeStage, QCFlag, Count, '
                           f'PassType, Mortality, QCNotes, Comments, CreatedDate) '
                           f'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            # Append the Contacts to the xref_EventContacts table
            dm.generalDMClass.appendDataSet(cnxn, outDFCountswPasswComments, "tblSummerCounts", insertQuery,
                                            dmInstance)

            logMsg = f"Success ETL EFishing Counts ETL_Salmonids_Electro.py - process_Counts_Electrofishing"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            # Returning the Dataframe s
            return outDFCountswPasswComments

        except Exception as e:

            logMsg = f'WARNING ERROR  ETL EFishing Pass ETL_Salmonids_Electro.py - process_Measurements_Electrofishing: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)


def process_SalmonidsContacts(inDF, etlInstance, dmInstance):
    """
    Define Observers for Salmonids
    Harvest Multi-select field 'Define Observers', if other, also harvest 'Specify Other' field in Survey .csv
    Lookup table for contacts is tlu_Contacts - Contact_ID being pushed to table xref_EventContacts

    :param inDF: Data Frame being processed
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return:
    """

    try:

        inDFContacts = inDF[['EventID', 'Observers', 'other_Observer']]
        inDFContacts.rename(columns={'other_Observer': 'Other'}, inplace=True)

        #####################################
        # Parse the 'Observers' field on ','
        # First remove the records where Observers == 'other'
        inObsNotOther = inDFContacts[inDFContacts['Observers'] != 'other']
        inDFObserversParsed = inObsNotOther.assign(Observers=inObsNotOther['Observers'].str.split(',')).explode('Observers')
        # Drop any records with 'Other' some cases have defined people and then also other
        inDFObserversParsed2 = inDFObserversParsed[inDFObserversParsed['Observers'] != 'other']
        # Drop field 'other'
        inDFObserversParsed3 = inDFObserversParsed2.drop(['Other'], axis=1)
        # Reset Index
        inDFObserversParsed3.reset_index(drop=True, inplace=True)

        # Trim leading white spaces in the 'Observers' field
        inDFObserversParsed3['Observers'] = inDFObserversParsed3['Observers'].str.lstrip()

        # Define OBSCODE
        inDFObserversParsed3.insert(2, 'OBSCODE', None)

        # Import the tluObservers tables
        inQuery = f"SELECT * FROM tluObservers"
        outDFtluObservers = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

        # Define the OBSCODE via a join lookup approach
        inDFObserversDefined = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFtluObservers,
                                                             "OBSCODE", "OBSCODE",
                                                             inDFObserversParsed3, "Observers",
                                                             "OBSCODE")

        ##################################
        # Parse the 'Other' field on ','
        # Retain only the records where Observers contains 'other'
        inObsOther = inDFContacts[inDFContacts['Observers'].str.contains('other')]

        # Assign to String
        inObsOther['Other'] = inObsOther['Other'].astype(str)

        # Retain only records with Other to be defined
        inDFOthersSubset = inObsOther[inObsOther['Other'] != 'nan']

        numberRecords = inDFOthersSubset.shape[0]

        # Proceed on Processing
        if numberRecords > 0:
            inDFOthersParsed = inObsOther.assign(Observers=inObsOther['Other'].str.split(',')).explode('Observers')
            inDFOthersParsed2 = inDFOthersParsed.drop(['Other'], axis=1)

            # Reset Index
            inDFOthersParsed3 = inDFOthersParsed2.reset_index(drop=True)

            # Trim leading white spaces in the 'Observers' field
            inDFOthersParsed3['Observers'] = inDFOthersParsed3['Observers'].str.lstrip()

            # Define First and Last Name Fields
            inDFOthersParsed3.insert(2, "Last_Name", None)
            inDFOthersParsed3.insert(3, "First_Name", None)

            # Insert 'OBSCODE to be defined
            inDFOthersParsed3.insert(1, "OBSCODE", None)

            # Retain only records that aren't null
            inDFOthersParsed3_subset = inDFOthersParsed3[inDFOthersParsed3['Observers'].notna()]

            # Add Field checking if '_' is in field Observers
            inDFOthersParsed3_subset['Underscore'] = inDFOthersParsed3_subset['Observers'].apply(lambda x: 'Yes' if '_' in x else 'No')

            ###############################
            # Define the 'First_Name' field
            # Parse the name before the '_' into the 'First_Name' field if 'Underscore' equals 'Yes'
            inDFOthersParsed3_subset['First_Name'] = inDFOthersParsed3_subset.apply(
                lambda row: row['Observers'].split('_')[0] if row['Underscore'] == 'Yes' else row['First_Name'], axis=1)
            # Parse the name before the ' ' into the 'First_Name' field if 'Underscore' equals 'No'
            inDFOthersParsed3_subset['First_Name'] = inDFOthersParsed3_subset.apply(
                lambda row: row['Observers'].split(' ')[0] if row['Underscore'] == 'No' else row['First_Name'], axis=1)

            ###############################
            # Define the 'Last_Name' field
            # Parse the name after the '_' into the 'Last_Name' field if 'Underscore' equals 'Yes'
            inDFOthersParsed3_subset['Last_Name'] = inDFOthersParsed3_subset.apply(
                lambda row: row['Observers'].split('_')[1] if row['Underscore'] == 'Yes' else row['Last_Name'], axis=1)
            # Parse the name after the ' ' into the 'Last_Name' field if 'Underscore' equals 'No'
            inDFOthersParsed3_subset['Last_Name'] = inDFOthersParsed3_subset.apply(
                lambda row: row['Observers'].split(' ')[1] if row['Underscore'] == 'No' else row['Last_Name'], axis=1)

            # Define the OBSCODE via a join on the First and Last Name fields in dataframes 'inDFOthersParsed3_subset'
            # and outDFtluObservers

            mergedOtherDf = pd.merge(inDFOthersParsed3_subset, outDFtluObservers, left_on=['Last_Name', 'First_Name'],
                right_on=['LASTNAME', 'FIRSTNAME'], how='left', suffixes=('_x', ''))

            # Subset to the needed fields
            mergedOtherDf2 = mergedOtherDf[['EventID', 'Observers', 'OBSCODE']]

            # Append 'mergedOtherDf2 with the  'inDFObserversDefined' dataframe
            inDFObserverDefinedAll = pd.concat([inDFObserversDefined, mergedOtherDf2],  axis=0)

            inDFObserverDefinedAll.reset_index()
        # Not Processing Other field
        else:
            inDFObserverDefinedAll = inDFObserversDefined

        ###############################
        # Check for Lookups not defined
        ###############################

        dfObserversNull = inDFObserverDefinedAll[inDFObserverDefinedAll['OBSCODE'].isna()]
        numRec = dfObserversNull.shape[0]
        if numRec >= 1:
            # Sort on the Observers field
            dfObserversNull_sorted = dfObserversNull.sort_values(by='Observers')
            logMsg = f'WARNING there are {numRec} records without a defined records in the tluObservers lookup table.'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.warning(logMsg)

            outPath = f'{etlInstance.outDir}\RecordsSalmonids_Electro_NoDefinedContact.csv'
            if os.path.exists(outPath):
                os.remove(outPath)

            dfObserversNull_sorted.to_csv(outPath, index=True)

            logMsg = (f'Exporting Records without a defined lookup see - {outPath} \n'
                      f'Exiting ETL_Salmonids_Electro.py - process_SalmonidsContacts with out full completion.')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.warning(logMsg)
            exit()

        logMsg = f"Success ETL_Salmonids_Electro.py - processSalmonidsContacts."
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.info(logMsg)

        return inDFObserverDefinedAll

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_Salmonids_Electro.py - processSalmonidsContacts: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)


# Function to assign RandomSample to the first 'CO' in the first 10 rows
def assignRandomSampleFirst10(group):
    """
    Assigned a True value to the First 10 occurrences of 'CO'  by passed Group By on 'Event' and 'Pass'

    :param group: Group By value

    :return:
    """

    # Filter for rows where 'SpeciesCode' is 'CO'
    co_mask = group['SpeciesCode'] == 'CO'
    co_subset = group[co_mask]

    # Set 'RandomSample' to True for the first 10 'CO' records
    if len(co_subset) > 0:
        group.loc[co_subset.head(10).index, 'RandomSample'] = True

    return group