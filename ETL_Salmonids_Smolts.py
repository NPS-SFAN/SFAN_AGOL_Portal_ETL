"""
ETL_Salmonids_Smolts.py
Methods/Functions to be used for Salmonids Smolts ETL workflow.
Created By: Kirk Sherrill Data Manager San Francisco Bay Area Network Inventory and Monitoring National Park Service
Created Date: 6/3/2025
"""

#Import Required Libraries
import pandas as pd
import numpy as np
import glob, os, sys
import traceback
import generalDM as dm
import logging

class etl_SalmonidsSmolts:
    def __init__(self):

        """
        Define the QC Protocol instantiation attributes

        :param TBD
        :return: zzzz
        """

        # Class Variables
        numETL_SalmonidsSmolts = 0

        # Define Instance Variables
        numETL_SalmonidsSmolts += 1

    def process_ETLSmolts(outDFDic, etlInstance, dmInstance):

        """
        Workflow parent script for ETL workflow

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outETL: String denoting 'Success' or 'Error' on ETL Processing
        """

        try:

            ######
            # ETL Event
            ######
            outMethod = etl_SalmonidsSmolts.process_Event_Smolts(outDFDic, etlInstance, dmInstance)
            outDFEvent = outMethod[0]

            ######
            # ETL Measurements
            ######
            outMethod = etl_SalmonidsSmolts.process_repeat_Smolts(outDFDic, outDFEvent, etlInstance,
                                                                              dmInstance)
            if outMethod == "Success":
                logMsg = f'Successfully finished processing - ETL_Salmonids_Smolts.py - process_ETLSmolts'
                logging.info(logMsg, exc_info=True)

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_Salmonids_Smolts.py - process_ETLSmolts: {e}'
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_Event_Smolts(outDFDic, etlInstance, dmInstance):

        """
        ETL routine for the parent Event Form for Smolts. Processes the main parent form {SFAN_Salmonids_Smolts_}
        Data is ETL'd to tblEvents, tblSmoltSurveys, tblEventObservers.

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outDFEvent - Dataframe of the import records to tblEvents.
                outDFSurvey - Dataframe of the imported records to tblSmoltSurveys
                outContactsDF - Dataframe with the imported Observer/Contacts per event to tblEventObservers
        """

        try:
            #Export the Parent Event/Survey Dataframe from Dictionary List - Wild Card in Key is *EFish*
            inDF = None
            for key, df in outDFDic.items():
                if 'Salmonids_Smolts' in key:
                    inDF = df
                    break

            # Create initial dataframe subset
            outDFSubset = inDF[['GlobalID', 'Device', 'other_Device', 'StartDate', 'Start Time', 'End Time',
                                'FieldSeason', 'Define Observers(s)', 'other_Observer', 'ProjectCode',
                                'ProjectDescription', 'StreamID', 'other_Stream', 'LocationID', 'other_Location',
                                'Weather', 'StageHeight', 'WaterTemp', 'SurveyComments', 'MarkType1', 'CreationDate',
                                'Creator', 'Trap Status', 'FieldVerified']]

            # Rename might be best to not include in the subset operation
            outDFSubset.rename(columns={'SurveyComments': 'Comments',
                                        'Define Observers(s)': 'Observers',
                                        'CreationDate': 'CreatedDate',
                                        'Creator': 'CreatedBy',
                                        'Trap Status': 'TrapStatus',
                                        'Device': 'FieldDevice',
                                        'Start Time': 'StartTime',
                                        'End Time': 'EndTime',
                                        'FieldVerified': 'Verified'}, inplace=True)

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            outDFSubset = outDFSubset.replace([np.nan, 'nan'], None)

            ##############################
            # Numerous Field CleanUp Steps
            ##############################

            # Change 'CreatedDate' to Date Time Format
            outDFSubset['CreatedDate'] = pd.to_datetime(outDFSubset['CreatedDate'])
            outDFSubset['StartDate'] = pd.to_datetime(outDFSubset['StartDate'])

            # Update Yes No fields to Boolean - True, False
            outDFSubset['Verified'] = outDFSubset['Verified'] == 'Yes'

            # Insert 'ProtocolID' field - setting default value to 2 - 'SFAN_IMD_Salmonids_1' see tluProtocolVersion
            outDFSubset.insert(1, "ProtocolID", 2)

            fieldLen = outDFSubset.shape[1]
            # Insert 'DataProcesingLevelID' = 1
            outDFSubset.insert(fieldLen, "DataProcessingLevelID", 1)

            # Insert 'dataProcesingLevelDate
            from datetime import datetime
            dateNow = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
            outDFSubset.insert(fieldLen + 1, "DataProcessingLevelDate", dateNow)

            # Insert 'dataProcesingLevelUser
            outDFSubset.insert(fieldLen + 2, "DataProcessingLevelUser", etlInstance.inUser)

            # Change to Date Time Format
            outDFSubset['DataProcessingLevelDate'] = pd.to_datetime(outDFSubset['DataProcessingLevelDate'])

            outDFSubset.insert(fieldLen + 3, "SurveyType", "SMOLT")

            ############################
            # Define desired field types
            ############################

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ['GlobalID', 'FieldDevice', 'other_Device', 'StartDate', 'Start Time', 'End Time',
                                      'FieldSeason', 'Observers', 'other_Observer', 'ProjectCode', 'ProjectDescription',
                                      'StreamID', 'other_Stream', 'LocationID', 'other_Location', 'Weather',
                                      'StageHeight', 'WaterTemp', 'SurveyComments', 'MarkType1', 'CreatedDate',
                                      'CreatedBy', 'TrapStatus'],
                            'Type': ['object', 'object', 'object', 'datetime64', 'datetime64', 'datetime64',
                                     'int64', 'object', 'object', 'object', 'object',
                                     'int64', 'object', 'int64', 'object', 'object',
                                     'float32', 'float32', 'object', 'object', 'datetime64',
                                     'object', 'object'],
                            'DateTimeFormat': ['na', 'na', 'na', '%m/%d/%Y', '%H:%M', '%H:%M',
                                               'na', 'na', 'na', 'na', 'na',
                                               'na', 'na', 'na', 'na', 'na',
                                               'na', 'na', 'na', 'na', '%m/%d/%Y %I:%M:%S %p',
                                               'na', 'na']}

            outDFSubset2 = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDFSubset)

            # If there are 'Other' values in the 'LocationID', 'StreamID',and or other_Device
            # fields will need to define these in the lookup table before proceeded - will exit processing

            outOtherStatus = process_OtherValues(outDFSubset2, ['other_Location', 'other_Stream',
                                                                'other_Device'], etlInstance,
                                                            'other_Loc_Stream_Device')

            # Retain only the fields going to tlbEvents
            outDFEventsOnly = outDFSubset2[['GlobalID', 'FieldDevice', 'StartDate', 'StartTime', 'EndTime',
                                            'FieldSeason', 'ProjectCode', 'ProjectDescription', 'StreamID',
                                            'CreatedDate', 'CreatedBy', 'DataProcessingLevelID',
                                            'DataProcessingLevelDate', 'DataProcessingLevelUser', 'SurveyType',
                                            'Verified']]


            # Append outDFEventsOnly to 'tbl_Events'
            # Pass final Query to be appended
            insertQuery = (f'INSERT INTO tblEvents (GlobalID, FieldDevice, StartDate, StartTime, EndTime, FieldSeason,'
                           f'ProjectCode, ProjectDescription, StreamID, CreatedDate, CreatedBy, DataProcessingLevelID,'
                           f'DataProcessingLevelDate, DataProcessingLevelUser, SurveyType, Verified) '
                           f'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFEventsOnly, "tblEvents", insertQuery, dmInstance)

            ##################
            # Add the EventID via lookup field to the outDFSubset2
            ################

            # Read in the tluDevices lookup table
            # Import Event Table to define the EventID via the GlobalID
            inQuery = f"SELECT tblEvents.* FROM tblEvents;"

            # Import Event Table with defined EventID
            outDFwEVentID = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Merge on the Event Data Frame to get the EventID via the ParentGlobalID - GlobalID fields
            outDFSubSet2wEventID = pd.merge(outDFSubset2, outDFwEVentID[['GlobalID', 'EventID']], how='left',
                                             left_on="GlobalID", right_on="GlobalID", suffixes=("_src", "_lk"))

            ##################
            # Define tblEventObservers
            # Harvest Mutli-select field Define Observers, if other, also harvest 'Specify Other.
            # Lookup table for contacts is tluObserver
            ##################

            outContactsDF = process_SalmonidsContacts(outDFSubSet2wEventID, etlInstance, dmInstance)

            #########################
            # Process tblSmoltSurveys
            #########################

            outSurveyDF = process_SalmonidsSurvey(outDFSubSet2wEventID, etlInstance, dmInstance)

            logMsg = f"Success ETL Event/Survey Form ERL_Salmonids_Electro.py - process_Event_Smolts"
            logging.info(logMsg)

            # Returning the Dataframe survey which was pushed to 'tbl_Events, will be used in subsequent ETL.
            return outDFSubSet2wEventID, outSurveyDF, outContactsDF


        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - proces_Survey: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_repeat_Smolts(outDFDic, outDFEvent, etlInstance, dmInstance):

        """
        ETL routine for the Measurements form data in the Smolt Survey 123 form.
        Data is processed to table 'tblSmoltMeasurements - If measurement and or PITTag data', else going to the
        tblSmoltCounts table.  TB further defined - 5/29/2025

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param outDFEvent - Dataframe with the processed Event/Survey info
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outDFMeasurements - Measurements dataframe appended to table 'tblSummerMeasurements

        Updates:
        20241011 - Removed criteria only Measurements records are being processed.  No both
        """

        try:
            #Export the Measurements dataframe Dictionary List - Wild Card in Key is *Measurements*
            inDF = None
            for key, df in outDFDic.items():
                if 'Measurements' in key:
                    inDF = df
                    break

            # Create initial dataframe subset
            outDFSubset = inDF[['SpeciesCode', 'LifeStage', 'Tally', 'ForkLength_mm', 'LengthCategoryID',
                                'TotalWeight_g', 'BagWeight_g', 'FishWeight_g', 'NewRecaptureCode', 'PITTag',
                                'MarkColorMeasurements', 'PriorSeason', 'Injured', 'Dead', 'Scales',
                                'Tissue', 'EnvelopeID', 'CommentsMeasurements', 'QCFlag',
                                'Other QC Flag - please specify', 'QCNotes', 'ParentGlobalID', 'CreationDate']]

            # Rename fields
            outDFSubset.rename(columns={'Tally': 'FishTally',
                                        'ForkLength_mm': 'ForkLength',
                                        'TotalWeight_g': 'TotalWeight',
                                        'BagWeight_g': 'BagWeight',
                                        'FishWeight_g': 'FishWeight',
                                        'MarkColorMeasurements': 'MarkColor',
                                        'CommentsMeasurements': 'Comments',
                                        'Other QC Flag - please specify': 'OtherQCFlag',
                                        'CreationDate': 'CreatedDate'}, inplace=True)

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            outDFSubset = outDFSubset.replace([np.nan, 'nan'], None)

            ##############################
            # Numerous Field CleanUp Steps
            ##############################

            # Change 'CreatedDate' to Date Time Format
            outDFSubset['CreatedDate'] = pd.to_datetime(outDFSubset['CreatedDate'])

            fieldList = ['PriorSeason', 'Injured', 'Dead', 'Scales', 'Tissue']

            # Update Yes No fields to Boolean - True, False
            for field in fieldList:
                outDFSubset[field] = outDFSubset[field].map({'Yes': True, 'No': False})

            ############################
            # Define desired field types
            ############################

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ['SpeciesCode', 'LifeStage', 'Tally', 'ForkLength', 'LengthCategoryID',
                                      'TotalWeight', 'BagWeight', 'FishWeight', 'NewRecaptureCode', 'PITTag',
                                      'MarkColorMeasurements', 'PriorSeason', 'Injured', 'Dead',
                                      'Scales', 'Tissue', 'EnvelopeID', 'Comments', 'QCFlag', 'OtherQCFlag',
                                      'QCNotes', 'ParentGlobalID', 'CreatedDate'],


                            'Type': ['object', 'object', 'int64', 'int64', 'object', 'float32', 'float32', 'float32',
                                     'object', 'int64', 'object', 'object', 'object', 'object', 'object', 'object',
                                     'object', 'object', 'Object', 'object', 'object', 'Object', 'datetime64'],

                            'DateTimeFormat': ['na', 'na', 'na', 'na', 'na',
                                               'na', 'na', 'na', 'na', 'na',
                                               'na', 'na', 'na', 'na',
                                               'na', 'na', 'na', 'na', 'na', 'na',
                                               'na', 'na', '%m/%d/%Y %I:%M:%S %p']}

            outDFSubset2 = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDFSubset)

            # If there are 'Other' values in the 'LocationID', 'StreamID',and or other_Device
            # fields will need to define these in the lookup table before proceeded - will exit processing

            outOtherStatus = process_OtherValues(outDFSubset2, ['OtherQCFlag'], etlInstance, "OtherQCFlag")

            #
            # Define the EventID via lookup on the ParentGlobalID field and the GlobalID field in outDFEvent
            #

            # Merge on the Event Data Frame to get the EventID via the ParentGlobalID - GlobalID fields
            outDFSubSet2wEventID = pd.merge(outDFSubset2, outDFEvent[['GlobalID', 'EventID']], how='left',
                                            left_on="ParentGlobalID", right_on="GlobalID", suffixes=("_src", "_lk"))

            # Subset to Records going to tblSmoltMeasurements - Either has measurements in Measurements Field List or
            # has a Pittag value
            # Measurements fields to check for a value are - 'ForkLength', 'LengthCategoryID', 'TotalWeight',
            # 'BagWeight', 'FishWeight', 'NewRecaptureCode', 'PITTag', 'MarkColorMeasurements'

            measurementFields = ['ForkLength', 'LengthCategoryID', 'TotalWeight', 'BagWeight', 'FishWeight',
                                 'NewRecaptureCode', 'PITTag', 'MarkColor', 'EnvelopeID']

            subsetDFMeasurements = outDFSubSet2wEventID[outDFSubSet2wEventID[measurementFields].notnull().any(axis=1)]

            # Use the Inverse Logic to define the Records to go to tblSmoltCounts
            subsetDFCounts = outDFSubSet2wEventID[outDFSubSet2wEventID[measurementFields].isnull().all(axis=1)]

            # QC Check - Sum of 'subsetDFMeasurements' and 'subsetDFCounts' dataframe records equal the number of
            # records in dataframe 'outDFSubset2'
            total_records = len(outDFSubSet2wEventID)
            sum_subset_records = len(subsetDFMeasurements) + len(subsetDFCounts)

            if total_records == sum_subset_records:
                logMsg = f"QC Check Passed: Subsets account for all records Measurement and Counts dataframes."
                logging.info(logMsg)
            else:
                logMsg = (f"WARNING QC Check Failed: Subsets total {sum_subset_records} - is not equal to the Repeat "
                          f"Record Numbers {total_records} - Subsetting logic is not accurate. \r Exiting Script")
                logging.critical(logMsg)
                exit()

            ######
            # Process the Smolt Measurements
            #####

            outDFMeasurements = process_Measurements(subsetDFMeasurements, etlInstance, dmInstance)

            ######
            # Process the Smolt Counts
            #####

            outDFCounts = process_Counts(subsetDFCounts, etlInstance, dmInstance)

            logMsg = f"Success ETL Salmonids_Smolts.py - process_repeat_Smolts"
            logging.info(logMsg)

            # Returning the Dataframe survey which was pushed to 'tblSummerPasses
            return "Success"

        except Exception as e:

            logMsg = f'WARNING ERROR  ETL Salmonids_Smolts.py - process_repeat_Smolts: {e}'
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

        inDFContacts = inDF[['GlobalID', 'EventID', 'Observers', 'other_Observer']]
        inDFContacts.rename(columns={'other_Observer': 'Other'}, inplace=True)

        #####################################
        # Parse the 'Observers' field on ','.

        # First remove the records where Observers == 'other'
        inObsNotOther = inDFContacts[inDFContacts['Observers'] != 'other']
        inDFObserversParsed = (inObsNotOther.assign(Observers=inObsNotOther['Observers'].str.split(','))
                               .explode('Observers'))
        # Drop any records with 'Other' some cases have defined people and then also other
        inDFObserversParsed2 = inDFObserversParsed[inDFObserversParsed['Observers'] != 'other']
        # Drop field 'other'
        inDFObserversParsed3 = inDFObserversParsed2.drop(['Other'], axis=1)
        # Reset Index
        inDFObserversParsed3.reset_index(drop=True, inplace=True)

        # Trim leading white spaces in the 'Observers' field
        inDFObserversParsed3['Observers'] = inDFObserversParsed3['Observers'].str.lstrip()

        # Define OBSCODE which is already defined in the Obserer Field
        inDFObserversParsed3['OBSCODE']= inDFObserversParsed3['Observers']

        # Import the tluObservers tables
        inQuery = f"SELECT * FROM tluObservers"
        outDFtluObservers = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

        # Define the OBSCODE via a join lookup approach
        inDFObserversDefined = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFtluObservers,
                                                                      "OBSCODE", "OBSCODE",
                                                                      inDFObserversParsed3, "Observers",
                                                                      "OBSCODE")

        inDFObserversDefined = inDFObserversDefined[['GlobalID', 'EventID', 'Observers', 'OBSCODE']]

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

            inDFOthersParsed3 = inDFOthersParsed3.replace([np.nan, 'nan', 'None'], None)

            # Retain only records that aren't null
            inDFOthersParsed3_subset = inDFOthersParsed3[inDFOthersParsed3['Observers'].notnull()]

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
                lambda row: row['Observers'].split(' ')[0] if row['Underscore'] == 'No' else row['Last_Name'], axis=1)

            # Define the OBSCODE via a join on the First and Last Name fields in dataframes 'inDFOthersParsed3_subset'
            # and outDFtluObservers

            mergedOtherDf = pd.merge(inDFOthersParsed3_subset, outDFtluObservers, left_on=['Last_Name', 'First_Name'],
                right_on=['LASTNAME', 'FIRSTNAME'], how='left', suffixes=('_x', ''))

            # Subset to the needed fields
            mergedOtherDf2 = mergedOtherDf[['GlobalID', 'EventID', 'Observers', 'OBSCODE']]

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

            logging.warning(logMsg)
            exit()

        # Retain Needed fields
        inDFObserverDefinedAll.drop(columns={'GlobalID', 'Observers'}, inplace=True)

        # Insert the 'CreateDate' field
        from datetime import datetime
        dateNow = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        inDFObserverDefinedAll.insert(2, 'CreatedDate', dateNow)

        insertQuery = f'INSERT INTO tblEventObservers (EventID, OBSCODE, CreatedDate) VALUES (?, ?, ?)'

        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        # Append the Contacts to the xref_EventContacts table
        dm.generalDMClass.appendDataSet(cnxn, inDFObserverDefinedAll, "tblEventObservers", insertQuery,
                                        dmInstance)

        logMsg = f"Success ETL_Salmonids_Smolts.py - processSalmonidsContacts."
        logging.info(logMsg)

        return inDFObserverDefinedAll

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_Salmonids_Smolts.py - processSalmonidsContacts: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)

def process_OtherValues(inDF, fieldList, etlInstance, outSuffix):
    """
    Checking for values (i.e. not None, Nan) in the other fields for Location, StreamID, and or Devices that are in need of
    definition in the associated lookup tables in the Salmonids database.  Will export records with other values in need
    of definition to a .csv file and will exit the script.


    :param inDF: Data Frame being processed
    :param fieldList: List with the other fields to be checked
    :param etlInstance: Data Management instance
    :param outSuffix: Suffix to be applied to the output .csv file name.

    :return: String "Success" or exit script
    """

    try:

        # Subset rows where any field in fieldList is not null (None or NaN)
        subsetDF = inDF[inDF[fieldList].notnull().any(axis=1)]

        # Export Records in need of Definition in the LocationID or StreamID fields
        if subsetDF.shape[0] > 0:

            outPath = f'{etlInstance.outDir}\RecordsOther_{outSuffix}.csv'
            if os.path.exists(outPath):
                os.remove(outPath)

            # Export dataframe with other value to subset and exit scrit
            subsetDF.to_csv(outPath, index=True)

            recCount = subsetDF.shape[0]
            logMsg = (f'WARNING there are {recCount} records with other field'
                      f' values in need of definition.\rThese other values must be defined in the associated lookup'
                      f'table before processing can continue.\r Exported records in need of definition see -'
                      f' {outPath}.\rExiting Script')

            print(logMsg)
            logging.critical(logMsg, exc_info=True)
            sys.exit()

        logMsg = f"Success ETL_Salmonids_Smolts.py - process_OtherValues."
        print(logMsg)
        logging.info(logMsg)

        return "Success"

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_Salmonids_Smolts.py - process_OtherValues: {e}'
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)

def process_SalmonidsSurvey(inDF, etlInstance, dmInstance):
    """
    ETL routine to process from the main parent form {SFAN_Salmonids_Smolts_} to the tblSmoltSurveys table
    Data is ETL'd to tblSmoltSurveys

    :param inDF - Dataframe with the Subset of fields from the SFAN_Slamonids_Smolts for to be process, will be further
    subet for the tblSmoltSurvey table.
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance:

    :return:inDFAppend - dataframe that was appended to the 'tblSmoltSurvey' table.
    """

    try:

        inDFAppend = inDF[['EventID', 'LocationID', 'Weather', 'StageHeight', 'WaterTemp', 'Comments', 'MarkType1',
                           'TrapStatus', 'CreatedDate']]

        # Update any 'nan' string or np.nan values to None to consistently handle null values. Having to do a second
        # time, not sure why the initial time didn't accomplish this conversion to None.  Might not stick post defining
        # of field types in the dm.generalDMClass.defineFieldTypesDF upstream workflow routine.

        inDFAppend2 = inDFAppend.replace([np.nan, 'nan'], None)

        insertQuery = (f'INSERT INTO tblSmoltSurveys (EventID, LocationID, Weather, StageHeight, WaterTemp, Comments,'
                       f' MarkType1, TrapStatus, CreatedDate) VALUES'
                       f' (?, ?, ?, ?, ?, ?, ?, ?, ?)')

        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        # Append the Contacts to the xref_EventContacts table
        dm.generalDMClass.appendDataSet(cnxn, inDFAppend2, "tblSmoltSurveys", insertQuery,
                                        dmInstance)

        logMsg = f"Success ETL_Salmonids_Smolts.py - process_SalmonidsSurvey."
        logging.info(logMsg)

        return inDFAppend2

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_Salmonids_Smolts.py - process_SalmonidsSurvey: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)

def process_Measurements(inDF, etlInstance, dmInstance):
    """
        ETL routine to Append the Measurements form data with Measurement and or Pittag information to the
        tblSmoltMeasurements table

        :param inDF - Dataframe with the Subset of records to be appeneded, will be further
        subet for the tblSmoltSurvey table.
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:inDFAppend - dataframe that was appended to the 'tblSmoltMeasurements' table.
        """

    try:

        inDFAppend = inDF[['EventID', 'SpeciesCode', 'LifeStage', 'FishTally', 'ForkLength', 'LengthCategoryID',
                           'TotalWeight', 'BagWeight', 'FishWeight', 'NewRecaptureCode', 'PITTag', 'MarkColor',
                           'PriorSeason', 'Injured', 'Dead', 'Scales', 'Tissue', 'EnvelopeID', 'Comments', 'QCFlag',
                           'QCNotes', 'CreatedDate']]

        # Round to two significant digits (ie. hundredths and then turncate the Weight fields
        roundTruncateList = ['TotalWeight', 'BagWeight', 'FishWeight']
        inDFAppend[roundTruncateList] = inDFAppend[roundTruncateList].applymap(truncate_to_2_decimal)

        # Update any 'nan' string or np.nan values to None to consistently handle null values. Having to do a second
        # time, not sure why the initial time didn't accomplish this conversion to None.  Might not stick post defining
        # of field types in the dm.generalDMClass.defineFieldTypesDF upstream workflow routine.

        inDFAppend2 = inDFAppend.replace([np.nan, 'nan'], None)

        insertQuery = (f'INSERT INTO tblSmoltMeasurements (EventID, SpeciesCode, LifeStage, FishTally, ForkLength, '
                       f'LengthCategoryID, TotalWeight, BagWeight, FishWeight, NewRecaptureCode, PITTag, MarkColor, '
                       f'PriorSeason, Injured, Dead, Scales, Tissue, EnvelopeID, Comments, QCFlag, QCNotes, CreatedDate)'
                       f' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')

        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        # Append the Contacts to the xref_EventContacts table
        dm.generalDMClass.appendDataSet(cnxn, inDFAppend2, "tblSmoltMeasurements", insertQuery,
                                        dmInstance)

        logMsg = f"Success ETL_Salmonids_Smolts.py - process_Measurements."
        logging.info(logMsg)

        return inDFAppend2

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_Salmonids_Smolts.py - process_Measurements: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)


def process_Counts(inDF, etlInstance, dmInstance):
    """
        ETL routine to Append the Measurements form data without Measurement and or Pittag information to the
        tblSmoltCount table

        :param inDF - Dataframe with the Subset of records to be appeneded, will be further
        subet for the tblSmoltSurvey table.
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:inDFAppend - dataframe that was appended to the 'tblSmoltCount' table.
        """

    try:

        # Calculate the UnmeasuredLive and Unmeasured Dead fields
        inDF['UnmeasuredLive'] = np.where(inDF['Dead'] == False, inDF['FishTally'], 0)
        inDF['UnmeasuredDead'] = np.where(inDF['Dead'] == True, inDF['FishTally'], 0)

        inDFAppend = inDF[['EventID', 'SpeciesCode', 'LifeStage', 'Comments', 'QCFlag', 'QCNotes',
                           'CreatedDate', 'UnmeasuredLive', 'UnmeasuredDead']]

        nullRecordQCFields = ['SpeciesCode', 'LifeStage', 'Comments', 'QCFlag', 'QCNotes']
        # Delete records if null in fields
        inDFFiltered = inDFAppend[~inDFAppend[nullRecordQCFields].isnull().all(axis=1)]

        insertQuery = (f'INSERT INTO tblSmoltCounts (EventID, SpeciesCode, LifeStage, Comments, QCFlag, QCNotes,'
                       f'CreatedDate, UnmeasuredLive, UnmeasuredDead) VALUES'
                       f' (?, ?, ?, ?, ?, ?, ?, ?, ?)')

        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        # Append the Contacts to the xref_EventContacts table
        dm.generalDMClass.appendDataSet(cnxn, inDFFiltered, "tblSmoltCounts", insertQuery,
                                        dmInstance)

        logMsg = f"Success ETL_Salmonids_Smolts.py - process_Counts."
        logging.info(logMsg)

        return inDFFiltered

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_Salmonids_Smolts.py - process_Countss: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)

def truncate_to_2_decimal(x):
    return np.floor(x * 100) / 100 if pd.notnull(x) else x