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
            outDFEvent = etl_SalmonidsElectro.process_Event_Electrofishing(outDFDic, etlInstance, dmInstance)

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
                              'UnitTypeSecondary']
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

            # Calibration pool is a Yes/No Boleen field, setting to 'True'/ 'False' (i.e. Boolen)
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

            # Define the OBSCODE via a join on the First and Last Name fields in dataframes 'inDFOthersParsed3_subset' and
            # outDFtluObservers

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