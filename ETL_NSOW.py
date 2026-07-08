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


class etl_NSOW:
    def __init__(self):

        """
        Define the QC Protocol instantiation attributes

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
            # Process Monitoring Survey - in the SFAN_NSOW_AGOL_{YearVersion}- table - IN PROCESS 7/7/2026
            ######

            outDFEventSurvey = etl_NSOW.process_MonitoringSurvey(outDFDic, etlInstance, dmInstance)

            ####
            # Process tblMouseOffer table - Survey 123 table - mouseofferingrepeat_4  - TO DO
            ####

            etl_NSOW.processMouseOffer(outDFEventSurvey)

            ####
            # Process Inventory Call Response table - Survey 123 table - inventorycallrepeat_5 - TO DO
            # Use ParentGlobalID - to join on the GlobalID in the tblEventSurvey to get the EventSurveyID in tblCallPointResponse
            ####

            etl_NSOW.processInventoryCall(outDFEventSurvey)

            ######
            # Process New Tree Nest  - in the SFAN_NSOW_AGOL_{YearVersion}- table - these should be done prior to the
            # Nest Tree Survey - To Be Developed
            ######

            outDFNewTreeNest = etl_NSOW.process_NewTreeNest(outDFDic, etlInstance, dmInstance)

            ######
            # Process Nest Survey - in the SFAN_NSOW_AGOL_{YearVersion}- table - To Be Developed
            ######

            outDFNestSurvey = etl_NSOW.process_NestSurvey(outDFDic, etlInstance, dmInstance)









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
        tblEventSurvey, xxxx, yyyy, zzzz



        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outDFSurvey: Data Frame of the exported form will be used in subsequent table ETL.
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
                                              'SiteID', 'WindTypeID', 'PercipitationTypeID', 'Temperature_F', 'CloudsPercentage',
                                              'LightTypeID', 'Narrative', 'IsEffortToSeeBands', 'IsWereOwlsBanded',
                                              'IsMousingPerformed', 'MousePurposeID', 'IsNestViewAdequate', 'EvidenceID',
                                              'NonNestingIndicatorID', 'NestingIndicatorID', 'ReproductionID',
                                              'CreationDate', 'Creator', 'OrganizationID']].rename(
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
            outDFSubset.insert(fieldLen + 2, "DataProcessingLevelUser", etlInstance.inUser)

            # Define SiteID

            # Import the refSite lookup
            inQuery = f"SELECT refSite.ID, refSite.SiteName FROM refSite;"

            outDFrefSite = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Define the SiteID via lookup in refSite table - SiteName to ID
            site_lookup = outDFrefSite.set_index('SiteName')['ID']
            outDFSubset['SiteID'] = outDFSubset['SiteName'].map(site_lookup)

            ### MousePurposeID - If 'IsMousingPerformed' is no (i.e. 2) set 'MousePurposeID' to 4 - No Mousing
            outDFSubset.loc[outDFSubset['IsMousingPerformed'] == 2, 'MousePurposeID'] = 4

            ########
            # Append to tbl_EventSurvey
            ########

            # Pass final Query to be appended

            # Grab all column names from the dataframe
            cols = outDFSubset.columns.tolist()

            # Build the SQL query dynamically
            insertQuery = (
                f"INSERT INTO tbl_EventSurvey ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFSubset, "tblEvents", insertQuery, dmInstance)

            ####
            # Function to Populate the tblMonitoringOwlCall - TO DO
            ####

            # List of Fields to retain
            fieldList = ['GlobalID', 'CallStartTime', 'CallMethodID']

            etl_NSOW.processMonitoringOwlCall(fieldList, outDFSubset)

            ####
            # Function to Populate the tblWeather table - TO DO
            ####

            # List of Fields to retain
            fieldList = ['GlobalID', 'WindyTypeID', 'PercipitationTypeID', 'Temperature_F', 'CloudsPercentage',
                         'LightTypeID']

            etl_NSOW.processWeather(fieldList, outDFSubset)

            ####
            # Function to Populate the tblEvidence table - TO DO
            ####

            # List of Fields to retain
            fieldList = ['GlobalID', 'EvidenceID']

            etl_NSOW.processEvidence(fieldList, outDFSubset)

            ####
            # Function to Populate the tblStatusIndicators table - TO DO
            ####

            # List of Fields to retain
            fieldList = ['NonNestingIndicatorID', 'NestingIndicatorID', 'ReproductionID']

            etl_NSOW.processStatusIndicators(fieldList, outDFSubset)




            func_name = inspect.currentframe().f_code.co_name
            logMsg = f"Success ETL Survey/Event Form ETL_NSOW.py - {func_name}"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            # Returning the Dataframe survey which was pushed to 'tbl_Events, will be used in subsequent ETL.
            return outDFSurvey

        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - {func_name}: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)

