"""
ETL_PINN_Elephant.py
Methods/Functions to be used for Pinnipeds Elephant Seal ETL workflow.

Created: 5/7/2025
Created By: Kirk Sherrill SFAN Data Scientist
Updates:
"""

#Import Required Libraries
import pandas as pd
import numpy as np
import os, sys
import traceback
import generalDM as dm
import logging


class etl_PINNElephant:
    def __init__(self):

        """
        Define the QC Protocol instantiation attributes

        :param TBD
        :return: zzzz
        """
        # Class Variables

        numETL_PINNElephant = 0

        # Define Instance Variables
        # self.filterRecQuery = 'qsel_QA_Control'

        # Define Instance Variables
        numETL_PINNElephant += 1

    def process_PINNElephant(outDFDic, etlInstance, dmInstance):

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
            outDFSurvey = etl_PINNElephant.process_SurveyMetadata(outDFDic, etlInstance, dmInstance)

            ######
            # Process Observations Form
            ######
            # outDFObs = etl_SNPLPORE.process_Observations(outDFDic, etlInstance, dmInstance, outDFSurvey)


            logMsg = f"Success ETL_PINN_Elephant.py - process_PINNElephant."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            outETL = "Success ETL PINN Elephant"
            return outETL

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_ETLSNPLPORE: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_SurveyMetadata(outDFDic, etlInstance, dmInstance):

        """
        ETL routine for the parent survey form SFAN_ElephantSeal_{YearVersion}- table.
        The majority of this information on this form will be pushed to the following tables:
        tblEvents.

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outDFSurveyMetadata: Data Frame of the exported form will be used in subsequent ETL Routines.
        """

        try:
            #Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None
            for key, df in outDFDic.items():
                if 'ElephantSeal' in key:
                    inDF = df
                    break


            outDFSubset = inDF[['GlobalID', 'Survey Name', "ProjectType", "Park Code", "Season", "Survey Date",
                                "Start Time Survey", "End Time Survey", "Define Observer(s)", "Specify other.",
                                "Visibility", "Survey Type", "subsite_notsurveyed_sm", "Regional Survey",
                                "Regional Survey Code", "Event Comment", "Collection Device", "CreationDate",
                                "Creator", "subsite_notsurveyed_sm"]].rename(
                columns={'Project_Type': 'ProjectCode',
                         'Park Code': 'ParkCode',
                         'Project Type': 'ProjectCode',
                         'Survey Date': 'StartDate',
                         'Start Time Survey': 'StartTime',
                         'End Time Survey': 'EndTime',
                         'Define Observer(s)': 'Observers',
                         'Specify other.': 'ObserversOther',
                         'Survey Type': 'SurveyType',
                         'subsite_notsurveyed_sm': 'SubSitesNotSurveyed',
                         'Regional Survey': 'RegionalSurvey',
                         'Regional Survey Code': 'RegionalSurveyCode',
                         'Event Comment': 'Comments',
                         'Collection Device': 'CollectionDeviceID',
                         'Specify other..1': 'DefineOthersCollection',
                         'CreationDate': 'CreatedDate',
                         'Creator': 'CreatedBy'})

            ##############################
            # Numerous Field CleanUp Steps
            ##############################
            # To DateTime Field
            outDFSubset['StartDate'] = pd.to_datetime(outDFSubset['StartDate'])
            # Convert to date only
            outDFSubset['Start_Date'].dt.strftime('%m/%d/%Y')

            # Data Processing Level Fields for Event Table
            # Get Dataframe Length
            fieldLen = outDFSubset.shape[1]

            # Insert 'DataProcesingLevelID' = 1
            outDFSubset.insert(fieldLen, "DataProcessingLevelID", 1)

            # Insert 'dataProcesingLevelDate
            from datetime import datetime
            dateNow = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
            outDFSubset.insert(fieldLen + 1, "DataProcessingLevelDate", dateNow)

            # Insert 'dataProcesingLevelUser
            outDFSubset.insert(fieldLen + 2, "DataProcessingLevelUser", etlInstance.inUser)

            ############################
            # Subset to the Event Fields
            ############################

            outDFEvent = outDFSubset[['GlobalID', "ProjectCode", "StartDate", "StartTime", "EndTime", "CreatedDate",
                                      "CreatedBy", "DataProcessingLevelID", "DataProcessingLevelDate",
                                      "DataProcessingLevelUser"]]

            # Define desired field types

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ["GlobalID", "ProjectCode", "StartDate", "StartTime", "EndTime", "CreatedDate",
                                "CreatedBy", "DataProcessingLevelID", "DataProcessingLevelDate", "DataProcessingLevelUser"],
                             'Type': ["object", "object", "datetime64", "datetime64", "datetime64", "datetime64",
                                      "object", "object", "datetime64", "object"],
                            'DateTimeFormat': ["na", "na", "%m/%d/%Y", "%H:%M", "%H:%M", "%m/%d/%Y %I:%M:%S %p", "na",
                                               "na", "%m/%d/%Y %H:%M:%S", "na"]}

            outDFSurvey = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDF_Step2)

            ###########################
            # STOPPED HERE 5/9/2025 KRS
















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
            outContactsDFAppend = outContactsDF[['Event_ID', 'Contact_ID', 'Contact_Role']]

            insertQuery = (f'INSERT INTO xref_Event_Contacts (Event_ID, Contact_ID, Contact_Role) VALUES (?, ?, ?)')

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


