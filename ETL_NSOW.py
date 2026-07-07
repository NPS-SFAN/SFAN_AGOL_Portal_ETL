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








            ######
            # Process Nest Survey - in the SFAN_NSOW_AGOL_{YearVersion}- table - To Be Developed
            ######

            outDFEventSurvey = etl_NSOW.process_NestSurvey(outDFDic, etlInstance, dmInstance)









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


            # STOPPED HERE 7/7/2026

            # Subset to Only the 'Monitoring Survey' events -
            outDFSubset = inDF[inDF['Event Type'] == 'MonitoringSurvey']


            # Create initial dataframe subset
            outDFSubset = outDFSubset[['GlobalID', 'EventPurposeID',
                                'CreationDate', 'Creator']].rename(
                columns={'CreationDate': 'CreatedDate',
                         'Creator': 'CreatedBy'})

            ##############################
            # Numerous Field CleanUp Steps
            ##############################
            # To DateTime Field
            outDFSubset['Start_Date'] = pd.to_datetime(outDFSubset['Start_Date'])
            # Format to m/d/yyy
            outDFSubset['Start_Date'] = outDFSubset['Start_Date'].dt.strftime('%m/%d/%Y')

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

            outDF_Step2 = pd.merge(outDFSubset, outDFLookup[['Loc_Name', 'Location_ID']], how='left',
                                   left_on="Survey Location", right_on="Loc_Name", suffixes=("_src", "_lk"))

            # Drop field "Survey Location
            outDF_Step2 = outDF_Step2.drop(columns=['Survey Location', 'Loc_Name'])

            ############################
            # Define desired field types
            ############################

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {
                'Field': ["Event_ID", "Location_ID", "Protocol_Name", "Start_Date", "Start_Time", "End_Time",
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

            # Grab all column names from the dataframe
            cols = outDFSurvey.columns.tolist()

            # Build the SQL query dynamically
            insertQuery = (
                f"INSERT INTO tbl_Events ({', '.join(cols)}) "
                f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFSurvey, "tblEvents", insertQuery, dmInstance)









            func_name = inspect.currentframe().f_code.co_name
            logMsg = f"Success ETL Survey/Event Form ETL_SNPLPORE.py - {func_name}"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            # Returning the Dataframe survey which was pushed to 'tbl_Events, will be used in subsequent ETL.
            return outDFSurvey

        except Exception as e:

            func_name = inspect.currentframe().f_code.co_name
            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - {func_name}: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)

