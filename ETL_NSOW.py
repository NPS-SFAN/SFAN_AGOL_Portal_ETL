"""
ETL_NSOW.py
Methods/Functions to be used for Northern Spotted Owl ETL workflow.
"""

#Import Required Libraries

import glob, os, sys
import traceback
import generalDM as dm
import logging
import inspect
import pandas as pd
import numpy as np
from datetime import datetime
import re
import geopandas as gpd

class etl_NSOW:
    def __init__(self):

        """
        Define the instantiation attributes

        :param TBD
        :return: zzzz
        """
        # Class Variables

        numETL_NSWO = 0

        # Define Instance Variables


        numETL_NSWO += 1

    def process_ETLNSOW(outDFDic, etlInstance, dmInstance, generalArcGIS):

        """
        Import files in passed folder to dataframe(s). Uses GLOB to get all files in the directory.
        Currently defined to import .csv, and .xlsx files

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance
        :param generalArcGIS: ArcGIS instance

        :return:outETL: String denoting 'Success' or 'Error' on ETL Processing
        """

        try:

            ######
            # Process Monitoring Survey - in the SFAN_NSOW_AGOL_{YearVersion}- table - DONE 8/6/2026
            ######

            #etl_NSOW.process_MonitoringSurvey(outDFDic, etlInstance, dmInstance)

            ####
            # Process tblMouseOffer table - Survey 123 table - mouseofferingrepeat_4 - DONE 8/6/2026
            ####

            #etl_NSOW.processMouseOffer(outDFDic, etlInstance, dmInstance)

            ####
            # Process the Observers Repeat table - Survey 123 table - observersrepeat_1 - Done 8/11/2026
            # Check for output table - RecordsNSOSurveys_OtherObserverDefinitionNeeded_MonitoringSurvey_{DateHour}.csv
            # with Other Observers that need to be added to the tblEventPersonnel table post ETL processing.
            ####

            #etl_NSOW.processObservers(outDFDic, etlInstance, dmInstance, surveyType="MonitoringSurvey")


            ####
            # Process Inventory Call Response table - Survey 123 table - inventorycallrepeat_5
            # Use ParentGlobalID - to join on the GlobalID in the tblEventSurvey to get the EventSurveyID in tblCallPointResponse
            ####

            #etl_NSOW.processInventoryCall(outDFDic, etlInstance, dmInstance)

            ######
            # Process New Tree Nest  - in the SFAN_NSOW_AGOL_{YearVersion}- table - these should be done prior to the
            # Nest Tree Survey so the new tree is in the database when Nest Surveys are performed
            ######

            #etl_NSOW.process_NewTreeNest(outDFDic, etlInstance, dmInstance)

            ######
            # Process Nest Survey - in the SFAN_NSOW_AGOL_{YearVersion}- table - In Process - STOPPED HERE 2026/8/13
            ######

            etl_NSOW.process_NestSurveys(outDFDic, etlInstance, dmInstance)



            # Process Nest Survey Observations in the 'observersrepeatnestsurvey' table - starting in 2026v1.3  - To Do
            etl_NSOW.processObservers(outDFDic, etlInstance, dmInstance, surveyType="NestSurvey")



            ############################
            # Process Species Detections - speciesdetectionrepeat_2.csv - TO DO
            ############################

            ############################
            # Process Other Species  - otherrspecies_3.csv - To DO
            ############################





            func_name = inspect.currentframe().f_code.co_name
            logMsg = f"Success ETL_SNPLPORE.py - {func_name}"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            outETL = "Success ETL SNPLPORE"
            return outETL

        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETl_NSOW.py - {func_name}: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_MonitoringSurvey(outDFDic, etlInstance, dmInstance):

        """
        ETL routine for the parent survey form SFAN_NSOW_AGOL_{YearVersion}- table.
        The majority of this information on this form will be pushed to the following tables:
        tblEventSurvey, tblMonitoringOwlCall, tblWeather, tblEvidence, tblStatusIndicators.

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance

        :return
        """

        try:
            # Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None
            for key, df in outDFDic.items():
                if 'SFAN_NSOW' in key:
                    inDF = df
                    break


            # Subset to Only the 'Monitoring Survey' events -
            outDFSubsetInitial = inDF[inDF['Event Type'] == 'MonitoringSurvey']


            # Create initial dataframe subset
            outDFSubset = outDFSubsetInitial[['GlobalID', 'EventPurposeID', 'ProtocolConfigurationID', 'EventDate',
                                       'EventStartTime', 'EventEndTime', 'IsOwlCallSimulated', 'CallStartTime', 'CallMethodID',
                                              'SiteID', 'WindTypeID', 'PercipitationTypeID', 'LightTypeID',
                                              'Temperature_F', 'CloudsPercentage',
                                              'Narrative', 'IsEffortToSeeBands', 'IsWereOwlsBanded',
                                              'IsMousingPerformed', 'MousePurposeID', 'IsNestViewAdequate', 'EvidenceID',
                                              'NonNestingIndicatorID', 'NestingIndicatorID', 'ReproductionID',
                                              'CreationDate', 'Creator', 'OrganizationID'
                                              ]].rename(
                columns={'SiteID': 'SiteName',
                    'CreationDate': 'CreatedDate',
                         'Creator': 'CreatedBy'})

            ##############################
            # Numerous Field CleanUp Steps
            ##############################
            # To DateTime Field
            outDFSubset['EventDate'] = pd.to_datetime(outDFSubset['EventDate'])
            # Format to m/d/yyy
            outDFSubset['EventDate'] = outDFSubset['EventDate'].dt.strftime('%m/%d/%Y')

            fieldLen = outDFSubset.shape[1]

            # Insert 'DataProcesingLevelID' = 1
            outDFSubset.insert(fieldLen, "DataProcessingLevelID", 1)


            # Owl Call Simulated if yes set to 1 else 0.
            outDFSubset['IsOwlCallSimulated'] = (
                    outDFSubset['IsOwlCallSimulated'].str.strip().str.lower() == 'yes'
            ).astype(int)


            # Insert 'dataProcesingLevelDate
            from datetime import datetime
            dateNow = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
            outDFSubset.insert(fieldLen + 1, "DataProcessingLevelDate", dateNow)

            # Insert 'dataProcesingLevelUser
            outDFSubset.insert(fieldLen + 2, "DataProcessingLevelUserID", etlInstance.inUser)

            # Define SiteID
            # Import the refSite lookup
            inQuery = f"SELECT refSite.ID, refSite.SiteName FROM refSite;"

            outDFrefSite = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Define the SiteID via lookup in refSite table - SiteName to ID
            site_lookup = outDFrefSite.set_index('SiteName')['ID']
            outDFSubset['SiteID'] = outDFSubset['SiteName'].map(site_lookup)

            # Drop SiteName post definition of SiteID
            outDFSubset =outDFSubset.drop(columns=['SiteName'])


            ### MousePurposeID - If 'IsMousingPerformed' is no (i.e. 2) set 'MousePurposeID' to 4 - No Mousing
            outDFSubset.loc[outDFSubset['IsMousingPerformed'] == 2, 'MousePurposeID'] = 4

            ## Set float fields to Integer
            fieldListToInt = ['IsEffortToSeeBands', 'IsWereOwlsBanded', 'IsMousingPerformed', 'MousePurposeID', 'IsNestViewAdequate',
                              'OrganizationID', 'EventPurposeID', 'ProtocolConfigurationID']

            cols = [c for c in fieldListToInt if c in outDFSubset.columns]
            outDFSubset[cols] = df[cols].astype('Int64')

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            pd.set_option('mode.copy_on_write', False)
            outDFSubset = outDFSubset.replace([np.nan, 'nan'], None)

            # If field IsNestViewAdquqate is null set to 5 (i.e Not Recorded - NR)
            outDFSubset['IsNestViewAdequate'] = outDFSubset['IsNestViewAdequate'].fillna(5).astype('Int64')

            ########
            # Append to tbl_EventSurvey
            ########
            ## Remove Fields that aren't in event survey table

            fieldListDrop = ['CallStartTime', 'CallMethodID', 'WindTypeID', 'PercipitationTypeID',
                             'Temperature_F', 'CloudsPercentage', 'LightTypeID', 'EvidenceID', 'NonNestingIndicatorID',
                             'NestingIndicatorID', 'ReproductionID']

            outDFSubset2 = outDFSubset.drop(columns=fieldListDrop, errors='raise')

            # Add 'MergedDate' field with date/time now
            now = datetime.now()
            iso_date = now.strftime("%Y-%m-%d")
            outDFSubset2['MergedDate'] = iso_date


            ###Check for Duplicates prior to appending Unique on fields:
            uniqueFieldsList = ['EventDate', 'SiteID', 'OrganizationID', 'EventStartTime']

            duplicatesDF = outDFSubset2[outDFSubset2.duplicated(subset=uniqueFieldsList, keep=False)]

            if duplicatesDF.shape[0] > 0:

                outPath = f'{etlInstance.outDir}\Duplicates_MonitoringSurveys.csv'
                if os.path.exists(outPath):
                    os.remove(outPath)

                duplicatesDF.to_csv(outPath, index=True)

                msgLog = f'WARNING Duplicate Monitoring Survey Records - see export - {outPath} - Exiting Script'
                logging.critical(msgLog, exc_info=True)
                print (msgLog)

                sys.exit(1)


            # Grab all column names from the dataframe
            cols = outDFSubset2.columns.tolist()

            # Build the SQL query dynamically
            insertQuery = (
                f"INSERT INTO tblEventSurvey ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFSubset2, "tblEvents", insertQuery, dmInstance)

            ##########
            # Function to Populate the tblMonitoringOwlCal
            ##########

            fieldListOwlCall = ['GlobalID', 'CallStartTime', 'CallMethodID', 'MergedDate', 'IsOwlCallSimulated']

            etl_NSOW.processMonitoringOwlCall(fieldListOwlCall, outDFSubset, etlInstance, dmInstance)

            ##########
            # Function to Populate the tblWeather table
            ##########

            # List of Fields to retain tblWeather - Drop from Event Dataframe
            fieldListWeather = ['GlobalID', 'WindTypeID', 'PercipitationTypeID', 'Temperature_F', 'CloudsPercentage',
                                'LightTypeID']

            etl_NSOW.processWeather(fieldListWeather, outDFSubset, etlInstance, dmInstance)

            ##########
            # Function to Populate the tblEvidence table
            ##########

            # List of Fields to retain
            fieldList = ['GlobalID', 'EvidenceID']

            etl_NSOW.processEvidence(fieldList, outDFSubset, etlInstance, dmInstance)

            ##########
            # Function to Populate the tblStatusIndicators table
            ##########

            # List of Fields to retain
            fieldList = ['GlobalID', 'NonNestingIndicatorID', 'NestingIndicatorID', 'ReproductionID']

            etl_NSOW.processStatusIndicators(fieldList, outDFSubset, etlInstance, dmInstance)

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f"Success ETL Survey/Event Form ETL_NSOW.py - {func_name}"
            logging.info(logMsg)
            print(logMsg)

            return

        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETL_NSOW.py - {func_name}: {e}'
            logging.critical(logMsg, exc_info=True)


    def processMouseOffer(outDFDic, etlInstance, dmInstance):

        """
        ETL routine for the mouse offering repeat (i.e. mouseofferingrepeat table).
        The majority of this information on this form will be pushed to the following tables:
        tblMousingOffer.

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return
        """

        try:
            # Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None
            for key, df in outDFDic.items():
                if 'mouseoffering' in key:
                    inDF = df
                    break


            inDF2 = inDF.rename(columns={'OwlSexID.1': 'OwlAgeID'}) #OwlSexID.1 was inadvertently defined as 'OwlSexID' in the Survey 'bind::esri::fieldAlias' field hence the two 'OwlSexID' fields.

            # Create initial dataframe subset
            outDFSubset = inDF2[['GlobalID', 'TimeOut', 'TimeTaken', 'MouseFateID', 'FateTime', 'OwlSexID',
                                              'OwlAgeID', 'BehaviorNotes', 'ParentGlobalID']]

            ##############################
            # Numerous Field CleanUp Steps
            ##############################

            # Add 'MergedDate' field with date/time now
            now = datetime.now()
            iso_date = now.strftime("%Y-%m-%d")
            outDFSubset['MergedDate'] = iso_date

            # Define the EventID via the ParentGlobalID field
            # Read in the tblEventSurvey table
            inQuery = f"SELECT tblEventSurvey.* FROM tblEventSurvey;"
            dfEventSurvey = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Define the EvenetSurveyID via join on the 'GlobalID' and 'ParentGlobalID' fields
            inDFAppend = outDFSubset.merge(
                dfEventSurvey[['GlobalID', 'ID']],
                left_on = 'ParentGlobalID',
                right_on= 'GlobalID',
                how='left')

            # Rename ID field to 'EventSurveyID' and drop unneeded fields
            inDFAppendFinal = inDFAppend.drop(columns=['GlobalID_x', 'GlobalID_y', 'ParentGlobalID']).rename(
                columns={'ID': 'EventSurveyID'})

            # Check for Orphaned Records (i.e. no match in EventSurvey) - Shouldn't happen but doesn't hurt to have the check
            unmatched = inDFAppendFinal['EventSurveyID'].isna().sum()
            if unmatched:

                msgLog = f'{unmatched} child rows had no matching EventSurvey parent - exiting script'
                logging.critical(msgLog, exc_info=True)
                print(msgLog)

                sys.exit(1)

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            pd.set_option('mode.copy_on_write', False)
            inDFAppendFinalClean = inDFAppendFinal.replace([np.nan, 'nan'], None)

            ########
            # Append to tblMousingOffer
            ########

            # Grab all column names from the dataframe
            cols = inDFAppendFinalClean.columns.tolist()

            # Build the SQL query dynamically
            insertQuery = (
                f"INSERT INTO tblMousingOffer ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, inDFAppendFinalClean, "tblMousingOffer", insertQuery, dmInstance)

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f"Success ETL Survey/Event Form ETL_NSOW.py - {func_name}"
            logging.info(logMsg)
            print(logMsg)

            return

        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETL_NSOW.py - {func_name}: {e}'
            logging.critical(logMsg, exc_info=True)


    def processObservers(outDFDic, etlInstance, dmInstance, surveyType):

        """
        ETL routine for the Observers offering repeat (i.e. observersrepeat_1 table). These are the observers for all
        but the Nest Tree Surveys.
        Information on this form will be pushed to the following tables:
        tblEventPersonnel.

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance
        :param surveyType: Variable defines if processing is for MonitoringSurvey or NestSurvey (

        :return
        """

        try:
            # Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None

            # If Monitoring Survey Process the Observers Repeat
            if surveyType == 'MonitoringSurvey':
                for key, df in outDFDic.items():
                    if 'observersrepeat' in key:
                        inDF = df
                        break

            # If Monitoring Survey Process the Nest Observers Repeat
            if surveyType == 'NestSurvey':
                for key, df in outDFDic.items():
                    if 'observersrepeatnestsurvey' in key:
                        inDF = df
                        break

            # Create initial dataframe subset
            outDFSubset = inDF[['PersonnelID', 'PersonnelRoleID', 'OtherObserver', 'OtherObserverRole',
                                 'ParentGlobalID']]

            ##############################
            # Numerous Field CleanUp Steps
            ##############################

            # Add 'MergedDate' field with date/time now
            from datetime import datetime
            now = datetime.now()
            iso_date = now.strftime("%Y-%m-%d")
            outDFSubset['MergedDate'] = iso_date

            # Define the EventID via the ParentGlobalID fields
            # Read in the tblEventSurvey table
            inQuery = f"SELECT tblEventSurvey.* FROM tblEventSurvey;"
            dfEventSurvey = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Define the EvenetSurveyID via join on the 'GlobalID' and 'ParentGlobalID' fields
            inDFAppend = outDFSubset.merge(
                dfEventSurvey[['GlobalID', 'ID', 'EventDate']],
                left_on = 'ParentGlobalID',
                right_on= 'GlobalID',
                how='left')



            # Rename ID field to 'EventSurveyID' and drop unneeded fields
            inDFAppend = inDFAppend.drop(columns=['ParentGlobalID']).rename(
                columns={'ID': 'EventSurveyID'})


            # Drop records without an EventID
            inDFAppendwEventID = inDFAppend[inDFAppend['EventSurveyID'].notna()]


            # Check for Orphaned Records (i.e. no match in EventSurvey) - Shouldn't happen after implementation of the Nest Survey Observer Repeat
            # Turn this back on after importing the 2026v1.2 feature layer
            # unmatched = inDFAppend['EventSurveyID'].isna().sum()
            # if unmatched:
            #
            #     msgLog = f'{unmatched} child rows had no matching EventSurvey parent - exiting script'
            #     logging.critical(msgLog, exc_info=True)
            #     print(msgLog)
            #
            #     sys.exit(1)

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            pd.set_option('mode.copy_on_write', False)
            inDFAppendwEventID = inDFAppendwEventID.replace([np.nan, 'nan'], None)

            #####################################
            #####################################
            # Identify Others - Export to .csv so they can be defined in the backend
            # Alternatively could push concatenated Other Observers and Records to the Event Narrative - this has not
            # been developed - 8/6/2026 KRS.

            # Subset to only records with other
            inDFOthers = inDFAppendwEventID[inDFAppendwEventID['OtherObserver'].notna()].copy()
            inDFOthersSubset = inDFOthers[['OtherObserver', 'OtherObserverRole',
                                 'EventSurveyID', 'EventDate', 'GlobalID']]
            numberRecords = inDFOthersSubset.shape[0]

            # Proceed on Processing
            if numberRecords > 0:

                    inDFOthersSubset = inDFOthersSubset.sort_values(by='EventSurveyID')
                    logMsg = (f'WARNING there are {numberRecords} records with Other Observers defined - add these observers to the Access Database table - .\n'
                    f'tblEventPersonnel - after ETL Processing.  It will be necessary to define the Observer if not already defined in the refPersonnel table.\n'
                    f'Post processing add these Other Observers.')
                    dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                    logging.warning(logMsg)

                    from datetime import datetime
                    dateHour = datetime.now().strftime("%Y-%m-%d_%H%M%S")
                    outPath = f'{etlInstance.outDir}\RecordsNSOSurveys_OtherObserverDefinitionNeeded_{surveyType}_{dateHour}.csv'
                    if os.path.exists(outPath):
                        os.remove(outPath)

                    inDFOthersSubset.to_csv(outPath, index=True)

                    logMsg = f'Exporting - {surveyType} - Observer Records in need of Observer Definition in the Backend Database {etlInstance.inDBBE} see - {outPath}'

                    dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                    logging.warning(logMsg)


            ########
            # Once Other Observer Records have been take care of - Append to tblEventPersonnel
            ########

            # Drop the EventDate field
            inDFAppendwEventIDCleaned = inDFAppendwEventID.drop(columns=['EventDate', 'OtherObserver', 'OtherObserverRole', 'GlobalID'])

            # Grab all column names from the dataframe
            cols = inDFAppendwEventIDCleaned.columns.tolist()

            # Build the SQL query dynamically
            insertQuery = (
                f"INSERT INTO tblEventPersonnel ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, inDFAppendwEventIDCleaned, "tblEventPersonnel", insertQuery, dmInstance)

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f"Success ETL Survey/Event Form ETL_NSOW.py - {func_name} - for - {surveyType}"
            logging.info(logMsg)
            print(logMsg)

            return

        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETL_NSOW.py - {func_name} - for - {surveyType}: {e}'
            logging.critical(logMsg, exc_info=True)


    def processMonitoringOwlCall(fieldList, inDF, etlInstance, dmInstance):
        """
        ETL to process the tblMonitoringOwl tables attributes

        :param fieldList - 'List of fields to be processed in the 'inDF' dataframe
        :param inDF - data frame being processed
        :param etlInstance - etl instance
        :param dmInstance: Data Management instance

        :return
        """

        try:

            # Read in the tblEventSurvey table
            inQuery = f"SELECT tblEventSurvey.* FROM tblEventSurvey;"
            dfEventSurvey = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            #Subset to the fieldList
            inDFSubset = inDF[[col for col in fieldList if col in inDF.columns]]

            # Subset to records where owl call was simulated
            inDFSubsetwOwl = inDFSubset[inDFSubset['IsOwlCallSimulated'] == 1].copy()

            # Define the EvenetSurveyID via join on the 'GlobalID' field
            inDFAppend = inDFSubsetwOwl.merge(
                dfEventSurvey[['GlobalID', 'ID']],
                on='GlobalID',
                how='left')

            # Rename ID field to 'EventSurveyID' and drop unneeded fields
            inDFAppendFinal = inDFAppend.drop(columns=['IsOwlCallSimulated', 'GlobalID']).rename(columns={'ID': 'EventSurveyID'})

            # Add 'MergedDate' field with date/time now
            now = datetime.now()
            iso_date = now.strftime("%Y-%m-%d")
            inDFAppendFinal['MergedDate'] = iso_date

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            pd.set_option('mode.copy_on_write', False)
            inDFAppendFinal = inDFAppendFinal.replace([np.nan, 'nan'], None)


            # Grab all column names from the dataframe
            cols = inDFAppendFinal.columns.tolist()

            # Build the SQL query dynamically
            insertQuery = (
                f"INSERT INTO tblMonitoringOwlCall ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, inDFAppendFinal, "tblMonitoringOwlCall", insertQuery, dmInstance)

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'Success Method - {func_name}'
            logging.info(logMsg, exc_info=True)
            print(logMsg)


        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETL_NSOW.py - {func_name}: {e}'
            logging.critical(logMsg, exc_info=True)


    def processWeather(fieldList, inDF, etlInstance, dmInstance):
        """
        ETL to process the tblWeather table attributes

        :param fieldList - 'List of fields to be processed in the 'inDF' dataframe
        :param inDF - data frame being processed
        :param dmInstance: Data Management instance:

        :return
        """

        try:

            # Read in the tblEventSurvey table
            inQuery = f"SELECT tblEventSurvey.* FROM tblEventSurvey;"
            dfEventSurvey = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            #Subset to the fieldList
            inDFSubset = inDF[[col for col in fieldList if col in inDF.columns]]

            # Define the EvenetSurveyID via join on the 'GlobalID' field
            inDFAppend = inDFSubset.merge(
                dfEventSurvey[['GlobalID', 'ID']],
                on='GlobalID',
                how='left')

            # Add 'MergedDate' field with date/time now
            now = datetime.now()
            iso_date = now.strftime("%Y-%m-%d")
            inDFAppend['MergedDate'] = iso_date

            # Rename ID field to 'EventSurveyID' and drop unneeded fields
            inDFAppendFinal = inDFAppend.drop(columns=['GlobalID']).rename(
                columns={'ID': 'EventSurveyID'})

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            pd.set_option('mode.copy_on_write', False)
            inDFAppendFinal = inDFAppendFinal.replace([np.nan, 'nan', ''], None)

            # Subset to only events with weather information - if all null don't append
            cols_to_check = [c for c in inDFAppendFinal.columns if c != 'EventSurveyID']
            inDFAppendFinalwData = inDFAppendFinal.dropna(subset=cols_to_check, how='all')

            # Grab all column names from the dataframe
            cols = inDFAppendFinalwData.columns.tolist()

            # Build the SQL query dynamically
            insertQuery = (
                f"INSERT INTO tblWeather ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, inDFAppendFinalwData, "tblEvents", insertQuery, dmInstance)

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'Success Method - {func_name}'
            logging.info(logMsg, exc_info=True)
            print(logMsg)


        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETL_NSOW.py - {func_name}: {e}'
            logging.critical(logMsg, exc_info=True)

    def processEvidence(fieldList, inDF, etlInstance, dmInstance):
        """
        ETL to process the tblEvidence table attributes. Exploding the multi-select comma delimited field into a stacked
        format.

        :param fieldList - 'List of fields to be processed in the 'inDF' dataframe
        :param inDF - data frame being processed
        :param etlInstance - etl instance
        :param dmInstance: Data Management instance

        :return
        """

        try:

            # Read in the tblEventSurvey table
            inQuery = f"SELECT tblEventSurvey.* FROM tblEventSurvey;"
            dfEventSurvey = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            #Subset to the fieldList
            inDFSubset = inDF[[col for col in fieldList if col in inDF.columns]]

            # Define the EvenetSurveyID via join on the 'GlobalID' field
            inDFAppend = inDFSubset.merge(
                dfEventSurvey[['GlobalID', 'ID']],
                on='GlobalID',
                how='left')

            # Rename ID field to 'EventSurveyID' and drop unneeded fields
            inDFAppend = inDFAppend.drop(columns=['GlobalID']).rename(
                columns={'ID': 'EventSurveyID'})

            #Sub to records with Evidence
            inDFAppendSubset = inDFAppend[inDFAppend['EvidenceID'].notna()].copy()

            # Explode to stacked format
            inDFEvidence = (
                inDFAppendSubset
                .assign(EvidenceID=inDFAppendSubset['EvidenceID'].fillna('').str.split(r'\s*,\s*'))
                .explode('EvidenceID', ignore_index=True)
            )

            #Add 'MergedDate' field with date/time now
            now = datetime.now()
            iso_date = now.strftime("%Y-%m-%d")
            inDFEvidence['MergedDate'] = iso_date

            # Grab all column names from the dataframe
            cols = inDFEvidence.columns.tolist()

            # Build the SQL query dynamically
            insertQuery = (
                f"INSERT INTO tblEvidence ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, inDFEvidence, "tblEvidence", insertQuery, dmInstance)

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'Success Method - {func_name}'
            logging.info(logMsg, exc_info=True)
            print(logMsg)

        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETL_NSOW.py - {func_name}: {e}'
            logging.critical(logMsg, exc_info=True)


    def processInventoryCall(outDFDic, etlInstance, dmInstance):
        """
        ETL to process the the inventorycallrepeat_5.csv table.  Data is processed to the
        tblCallPointResponse table.

        :param etlInstance - etl instance
        :param dmInstance: Data Management instance

        :return
        """

        try:

            # Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None

            # Import the Inventory Call table
            for key, df in outDFDic.items():
                if 'inventorycallrepeat' in key:
                    inDF = df
                    break

            # Subset to the Needed Fields
            outDFSubset = inDF[['GlobalID', 'CallPointID', 'Call Point Number', 'TimeStart', 'TimeEnd', 'MinutesTotal', 'IsResponse',
                                'ParentGlobalID']]

            # Define the EventID via the ParentGlobalID field
            # Read in the tblEventSurvey table
            inQuery = f"SELECT tblEventSurvey.* FROM tblEventSurvey;"
            dfEventSurvey = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Define the EventSurveyID via join on the 'GlobalID' and 'ParentGlobalID' fields
            inDFAppend = outDFSubset.merge(
                dfEventSurvey[['GlobalID', 'ID']],
                left_on='ParentGlobalID',
                right_on='GlobalID',
                how='left')

            # Rename ID field to 'EventSurveyID' and drop unneeded fields
            inDFAppendFinal = inDFAppend.drop(columns=['GlobalID_x', 'GlobalID_y', 'ParentGlobalID', 'Call Point Number']).rename(
                columns={'ID': 'EventSurveyID'})



            #Sub to records with data aonly values
            cols_to_check = [c for c in inDFAppendFinal.columns if c != 'EventSurveyID']
            inDFAppendFinalwData = inDFAppendFinal.dropna(subset=cols_to_check, how='all')

            #Add 'MergedDate' field with date/time now
            now = datetime.now()
            iso_date = now.strftime("%Y-%m-%d")
            inDFAppendFinalwData['MergedDate'] = iso_date

            # Grab all column names from the dataframe
            cols = inDFAppendFinalwData.columns.tolist()

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            pd.set_option('mode.copy_on_write', False)
            inDFAppendFinalwData = inDFAppendFinalwData.replace([np.nan, 'nan', ''], None)

            # Build the SQL query dynamically
            insertQuery = (
                f"INSERT INTO tblStatusIndicators ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, inDFAppendFinalwData, "tblCallPointResponse",
                                            insertQuery, dmInstance)

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'Success Method - {func_name}'
            logging.info(logMsg, exc_info=True)
            print(logMsg)

        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETL_NSOW.py - {func_name}: {e}'
            logging.critical(logMsg, exc_info=True)

    def process_NewTreeNest(outDFDic, etlInstance, dmInstance):

        """
        ETL to process the New Nest Trees.  This data is in the SFAN_NSOW_AGOL_{YearVersion}.csv table.
        Event Type = 'Nest Survey' and 'newtreeneeded' = 'Yes'.

        New  Nest Tree Survey data is pushed to the 'refNestTree' and 'refNestTreeDetails'

        :param etlInstance - etl instance
        :param dmInstance: Data Management instance

        :return
        """

        try:
            # Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None
            for key, df in outDFDic.items():
                if 'SFAN_NSOW' in key:
                    inDF = df
                    break

            # Subset to Only the 'New Nest Tree' Records  -
            outDFSubsetInitial = inDF[(inDF['Event Type'] == 'NestSurvey') & (inDF['newtreeneeded'] == 'yes')]

            # Create initial dataframe subset
            outDFSubset = outDFSubsetInitial[['GlobalID', 'SiteIDNewTree', 'SurveyYearNewTree',
                                              'TaxonID', 'LongitudeNewTree', 'LatitudeNewTree','UTM_EastingNewTree',
                                              'UTM_NorthingNewTree', 'UTM_ZoneNewTree',
                                              'CoordinateSystemNewTree', 'CoordinateMethodNewTree',
                                              'AccuracyNewTree', 'NestTreeDirections', 'IsActive',
                                              'AspectDegrees', 'BearingTypeID', 'SlopePercent', 'SlopePositionID',
                                              'CreationDate', 'GPSUnitID']].rename(
                columns={'SiteIDNewTree': 'SiteName',
                         'CreationDate': 'CreatedDate',
                         'SurveyYearNewTree': 'FirstYearUsed',
                         'TaxonID': 'NestTreeSpeciesID',
                         'LongitudeNewTree': 'Longitude',
                         'LatitudeNewTree': 'Latitude',
                         'UTM_EastingNewTree': 'UTME',
                         'UTM_NorthingNewTree': 'UTMN',
                         'UTM_ZoneNewTree': 'UTMZone',
                         'CoordinateSystemNewTree': 'CoordinateSystemID',
                         'AccuracyNewTree':'AccuracyMeters',
                         'CoordinateMethodNewTree': 'CoordinateMethodID'})

            # List of Fields to be pushed to the refNestTreeDetails - GlobalID will be used to get the ID field in the
            # parent refNestTree table.
            refNestTreeDetailsList = ['AspectDegrees', 'BearingTypeID', 'SlopePercent', 'SlopePositionID']

            ##############################
            # Numerous Field CleanUp Steps
            ##############################

            # If CoordinateMethodID is null set to 1
            outDFSubset['CoordinateMethodID'] = outDFSubset['CoordinateMethodID'].fillna(1)


            fieldLen = outDFSubset.shape[1]

            # Insert 'LastModifiedBy' = 1
            outDFSubset.insert(fieldLen, "LastModifiedBy", etlInstance.inUser)

            # Insert 'ProtectedStatusID' = 1 - defaulting all to protectec
            outDFSubset.insert(fieldLen - 1, "ProtectedStatusID", 1)

            ########
            # Define the SiteID via lookup on the RefSite table
            ########

            # Define the EventID via the ParentGlobalID field
            # Read in the tblEventSurvey table
            inQuery = f"SELECT refSite.* FROM refSite;"
            dfRefSite = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Define the SiteID via join on the 'SiteName'fields
            outDFSubsetwSiteID = outDFSubset.merge(
                dfRefSite[['SiteName', 'ID']],
                left_on='SiteName',
                right_on='SiteName',
                how='left')

            # Rename ID field to 'SiteID' and drop unneeded fields
            outDFSubsetwSiteID = outDFSubsetwSiteID.drop(
                columns=['SiteName', 'SiteName']).rename(
                columns={'ID': 'SiteID'})

            # Add 'MergedDate' field with date/time now
            now = datetime.now()
            iso_date = now.strftime("%Y-%m-%d")
            outDFSubsetwSiteID['MergedDate'] = iso_date
            iso_datetime = now.strftime("%Y-%m-%d %H:%M:%S")
            outDFSubsetwSiteID['LastModifiedDate'] = iso_datetime

            #######
            # Define the Lat/Lon or UTM values as needed via GeoPandas
            ########

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            pd.set_option('mode.copy_on_write', False)
            outDFSubsetwSiteIDCleaned = outDFSubsetwSiteID.replace([np.nan, 'nan', ''], None)

            ##########
            # Define missing Lat/Lon or UTMS values using the coordinates that were defined in the survey
            ##########

            outDFSubsetwCoords = processGeospatialPoints(outDFSubsetwSiteIDCleaned, etlInstance, dmInstance)

            ########
            # Append the New Nest Tree Records
            ########

            # Remove the NestTreeDetails Fields
            outDFNestTreeToAppend = outDFSubsetwCoords.drop(columns=refNestTreeDetailsList)

            # Convert the UTM to Integer - round prior to conversion
            for c in ['UTME', 'UTMN']:
                outDFNestTreeToAppend[c] = (
                    pd.to_numeric(outDFNestTreeToAppend[c], errors='coerce')
                    .round()
                    .astype('Int64'))

            # Convert Float fields to Integer
            for c in ['FirstYearUsed', 'NestTreeSpeciesID', 'CoordinateSystemID', 'CoordinateMethodID', 'GPSUnitID']:
                outDFNestTreeToAppend[c] = pd.to_numeric(outDFNestTreeToAppend[c], errors='coerce').astype('Int64')
            # IsActive is default definng to 1 in the db table.
            outDFNestTreeToAppend = outDFNestTreeToAppend.drop(columns={'IsActive'})


            # Grab all column names from the dataframe
            cols = outDFNestTreeToAppend.columns.tolist()

            # Build the SQL query dynamically
            insertQuery = (
                f"INSERT INTO refNestTree ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFNestTreeToAppend, "refNestTree",
                                            insertQuery, dmInstance)

            ##########
            # Process the refNestTreeDetails attributes
            ##########

            # Add Merged Date to the list of field
            refNestTreeDetailsList.append('MergedDate')
            refNestTreeDetailsList.append('GlobalID')
            inDFNestTreeDetails = outDFSubsetwSiteIDCleaned[refNestTreeDetailsList]

            etl_NSOW.process_NestTreeDetails(inDFNestTreeDetails, etlInstance, dmInstance)

            logMsg = f'Completed New Nest Tree - and Nest Tree Details processing'
            print(logMsg)
            logging.info(logMsg, exc_info=True)

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'Success Method - {func_name}'
            logging.info(logMsg, exc_info=True)
            print(logMsg)


        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETL_NSOW.py - {func_name}: {e}'
            logging.critical(logMsg, exc_info=True)


    def process_NestTreeDetails(inDF, etlInstance, dmInstance):
        """
        Routine to for ETL for refNestTreeDetails - these are applicable when it is a New Nest Tree

        :param inDF - Dataframe Nest Tree Details to be processed
        :param etlInstance - etl instance
        :param dmInstance: Data Management instance

        :return
        """
        try:

            # Define the 'NestTreeID' value in 'refNesTreeDetails' via join on refNestTree - GlobalID - the ID field in
            # refNestTree is the 'NestTreeID' in refNestTreeDetails.

            ########
            # Define the NestTreeID via lookup on the refNestTree table
            ########

            # Define the EventID via the ParentGlobalID field
            # Read in the tblEventSurvey table
            inQuery = f"SELECT refNestTree.* FROM refNestTree;"
            dfrefNestTree = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Define the SiteID via join on the 'SiteName'fields
            outDFwNestTreeID = inDF.merge(
                dfrefNestTree[['ID', 'GlobalID']],
                left_on='GlobalID',
                right_on='GlobalID',
                how='left')

            # Rename ID field to 'SiteID' and drop unneeded fields
            outDFwNestTreeID = outDFwNestTreeID.drop(
                columns=['GlobalID']).rename(
                columns={'ID': 'NestTreeID'})

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            pd.set_option('mode.copy_on_write', False)
            outDFwNestTreeIDCleaned = outDFwNestTreeID.replace([np.nan, 'nan', ''], None)

            # Grab all column names from the dataframe
            cols = outDFwNestTreeIDCleaned.columns.tolist()

            # Build the SQL query dynamically
            insertQuery = (
                f"INSERT INTO refNestTreeDetails ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFwNestTreeIDCleaned, "refNestTreeDetails",
                                            insertQuery, dmInstance)

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'Success Method - {func_name}'
            logging.info(logMsg, exc_info=True)
            print(logMsg)


        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETL_NSOW.py - {func_name}: {e}'
            logging.critical(logMsg, exc_info=True)


    def process_NestSurveys(outDFDic, etlInstance, dmInstance):
        """
        Routine to for ETL processing of nest tree surveys (not new). Data being processed resides in
        form SFAN_NSOW_AGOL_{YearVersion}.csv.

        Processed records are pushed tables: NestTreeSurvey, tblHabitatFeatures, tblOverstoryVegetation, and
        tblNestTreeFeatures .

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance

        :return
        """
        try:

            # Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None
            for key, df in outDFDic.items():
                if 'SFAN_NSOW' in key:
                    inDF = df
                    break

            # Subset to Only the 'Monitoring Survey' events -
            outDFSubsetInitial = inDF[inDF['Event Type'] == 'NestSurvey']

            # Create initial dataframe subset  - NOTE WaterTypeID was entered twice in version 1 of survey
            # Changed value to 'ForestOpeningID' starting in version 1.2
            outDFSubset = outDFSubsetInitial[['GlobalID', 'Nest Tree (when New)', 'SurveyYear', 'MeasuredDateHabitat',
                                              'DistanceToWater_Meters', 'WaterTypeID', 'DistanceToForestOpening_Meters',
                                              'ForestOpeningID', 'DistanceToForestEdge_Meters', 'ForestEdgeID',
                                              'OverstoryID', 'UnderstoryID', 'MeasuredDateTreeNestFeatures',
                                              'IsTreeAlive', 'IsTreeTagged', 'TreeTagNumber', 'TreeHeight_Meters',
                                              'DiameterBreastHeight_cm', 'NestTypeID', 'NestHeight_Meters',
                                              'NestDescription', 'Creator', 'CreationDate']].rename(
                columns={'Nest Tree (when New)': 'NestTreeID',
                         'Creator': 'CreatedBy',
                         'CreationDate': 'CreatedDate'})

            ##############################
            # Numerous Field CleanUp Steps
            ##############################
            # To DateTime Field
            outDFSubset['CreatedDate'] = pd.to_datetime(outDFSubset['CreatedDate'])
            # Format to m/d/yyy
            outDFSubset['CreatedDate'] = outDFSubset['CreatedDate'].dt.strftime('%m/%d/%Y')
            ########################
            # STOPPED HERE 8/13/2026





            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            pd.set_option('mode.copy_on_write', False)
            outDFwNestTreeIDCleaned = outDFwNestTreeID.replace([np.nan, 'nan', ''], None)

            # Grab all column names from the dataframe
            cols = outDFwNestTreeIDCleaned.columns.tolist()

            # Build the SQL query dynamically
            insertQuery = (
                f"INSERT INTO refNestTreeDetails ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFwNestTreeIDCleaned, "refNestTreeDetails",
                                            insertQuery, dmInstance)

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'Success Method - {func_name}'
            logging.info(logMsg, exc_info=True)
            print(logMsg)


        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETL_NSOW.py - {func_name}: {e}'
            logging.critical(logMsg, exc_info=True)


def processGeospatialPoints(inDF, etlInstance, dmInstance):
    """
    Routine to use Geopandas to define the Lat/Lon and or UTM fields that are null

    :param inDF - Dataframe with the New Nest Records and Coordinates to be defined
    :param etlInstance - etl instance
    :param dmInstance: Data Management instance

    :return outDFwCoordinates - Dataframe with processed Lat/Lon or UTM fields
    """

    try:

        # If Lat/Lon Defined - set Coordinate System to  4 - WGS84/Lat/Lon else will be manually defined during the workflow
        # via the lookup table
        mask = inDF['Longitude'].notna() & inDF['Latitude'].notna()
        # Define Coordinate System WGS84/Lat/Lon - see tluCoordinateSystem
        inDF.loc[mask, 'CoordinateSystemID'] = 4
        # Define Coordinate Method WGS84/Lat/Lon - see tluCoordinateMethod
        inDF.loc[mask, 'CoordinateSystemID'] = 1

        coordinateSystemDic = {'id':['1', '2', '3', '4'],
                               'System':['NAD83', 'WGS84', 'NAD83','WGS84']}

        #Function to define the Lat/Lon or UTM depending upon what fields are defined and null
        outDFwCoordinates = fill_coordinates(coordinateSystemDic, inDF)

        func_name = inspect.currentframe().f_code.co_name
        logMsg = f'Success Function - {func_name}'
        logging.info(logMsg, exc_info=True)
        print(logMsg)

        return outDFwCoordinates

    except Exception as e:

        func_name = inspect.currentframe().f_code.co_name
        logMsg = f'WARNING ERROR  - ETL_NSOW.py - {func_name}: {e}'
        logging.critical(logMsg, exc_info=True)

def _zone_number(zone_val, lon=None):
    """Zone from the UTMZone field (strips any hemisphere letter), or derived from longitude."""
    if pd.notna(zone_val):
        m = re.search(r'\d+', str(zone_val))
        if m:
            return int(m.group())
    if lon is not None and pd.notna(lon):
        return int((float(lon) + 180) // 6) + 1
    return None

def _hemisphere(zone_val, lat=None):
    """Hemisphere from a trailing N/S in UTMZone, else from latitude sign (defaults to N)."""
    if isinstance(zone_val, str):
        z = zone_val.strip().upper()
        if z.endswith('S'):
            return 'S'
        if z.endswith('N'):
            return 'N'
    if lat is not None and pd.notna(lat):
        return 'N' if float(lat) >= 0 else 'S'
    return 'N'

def _datum_from_id(v, datum_lookup, default='WGS84'):
    if pd.isna(v):
        return default
    return datum_lookup.get(str(int(float(v))), default)

def _utm_epsg(datum, zone, hemi, UTM_BASE):
    return UTM_BASE[datum][hemi] + int(zone)


def fill_coordinates(coordinateSystemDic, inDF):
    # Define the datum lookup from the above Coordinate System Dictionary
    datum_lookup = dict(zip(coordinateSystemDic['id'], coordinateSystemDic['System']))

    # Define the EPSG code for each (datum, hemisphere); add the zone number to get the CRS.
    # WGS84 N: 326xx / S: 327xx   |   NAD83 N: 269xx (NAD83 is North-America-centric)
    UTM_BASE = {'WGS84': {'N': 32600, 'S': 32700},
                'NAD83': {'N': 26900, 'S': 26900}}

    df = inDF.copy()

    # Make sure all columns exist and numeric ones are numeric.
    for c in ['Longitude', 'Latitude', 'UTME', 'UTMN', 'UTMZone', 'CoordinateSystemID']:
        if c not in df.columns:
            df[c] = pd.NA
    for c in ['Longitude', 'Latitude', 'UTME', 'UTMN']:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    has_latlon = df['Latitude'].notna() & df['Longitude'].notna()
    has_utm = df['UTME'].notna() & df['UTMN'].notna() & df['UTMZone'].notna()

    # ------------------------------------------------------------------
    # UTM -> Lat/Lon  (UTM defined, Lat/Lon missing)
    # ------------------------------------------------------------------
    need_latlon = has_utm & ~has_latlon
    if need_latlon.any():
        sub = df.loc[need_latlon].copy()
        sub['_datum'] = sub['CoordinateSystemID'].apply(lambda v: _datum_from_id(v, datum_lookup))
        sub['_zone'] = sub['UTMZone'].apply(lambda v: _zone_number(v))
        sub['_hemi'] = sub.apply(lambda r: _hemisphere(r['UTMZone'], r['Latitude']), axis=1)
        sub['_epsg'] = sub.apply(lambda r: _utm_epsg(r['_datum'], r['_zone'], r['_hemi'], UTM_BASE), axis=1)

        for epsg, grp in sub.groupby('_epsg'):
            g = gpd.GeoDataFrame(
                grp,
                geometry=gpd.points_from_xy(grp['UTME'], grp['UTMN']),
                crs=int(epsg),
            ).to_crs(4326)
            df.loc[g.index, 'Longitude'] = g.geometry.x.values
            df.loc[g.index, 'Latitude'] = g.geometry.y.values

    # ------------------------------------------------------------------
    # Lat/Lon -> UTM  (Lat/Lon defined, UTM missing)
    # ------------------------------------------------------------------
    need_utm = has_latlon & ~has_utm
    if need_utm.any():
        sub = df.loc[need_utm].copy()
        sub['_datum'] = sub['CoordinateSystemID'].apply(lambda v: _datum_from_id(v, datum_lookup))
        # Use the stated zone if present, otherwise derive it from longitude.
        sub['_zone'] = sub.apply(lambda r: _zone_number(r['UTMZone'], r['Longitude']), axis=1)
        sub['_hemi'] = sub.apply(lambda r: _hemisphere(r['UTMZone'], r['Latitude']), axis=1)
        sub['_epsg'] = sub.apply(lambda r: _utm_epsg(r['_datum'], r['_zone'], r['_hemi'], UTM_BASE), axis=1)

        for epsg, grp in sub.groupby('_epsg'):
            g = gpd.GeoDataFrame(
                grp,
                geometry=gpd.points_from_xy(grp['Longitude'], grp['Latitude']),
                crs=4326,
            ).to_crs(int(epsg))
            df.loc[g.index, 'UTME'] = g.geometry.x.values
            df.loc[g.index, 'UTMN'] = g.geometry.y.values
            # Only fill UTMZone where it was missing; don't clobber existing values.
            zone_fill = pd.Series(grp['_zone'].values, index=g.index)
            df.loc[g.index, 'UTMZone'] = df.loc[g.index, 'UTMZone'].fillna(zone_fill)

    # Posts Hoc update:
    df.loc[df['UTMZone'] == 10, 'UTMZone'] = '10N'

    return df
