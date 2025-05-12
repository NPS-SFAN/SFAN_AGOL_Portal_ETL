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
            # Process Survey Metadata Form - tblEvents
            ######
            outDFEvents = etl_PINNElephant.process_SurveyMetadata(outDFDic, etlInstance, dmInstance)







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


            outDFSubset = inDF[['GlobalID', 'Survey Name', "Project Type", "Park Code", "Season", "Survey Date",
                                "Start Time Survey", "End Time Survey", "Define Observer(s)", "Specify other.",
                                "Visibility", "Survey Type", "Sub Sites Not Surveyed", "Regional Survey",
                                "Regional Survey Code", "Event Comment", "Collection Device", "CreationDate",
                                "Creator"]].rename(
                columns={'Project_Type': 'ProjectCode',
                         'Park Code': 'ParkCode',
                         'Project Type': 'ProjectCode',
                         'Survey Date': 'StartDate',
                         'Start Time Survey': 'StartTime',
                         'End Time Survey': 'EndTime',
                         'Define Observer(s)': 'Observers',
                         'Specify other.': 'ObserversOther',
                         'Survey Type': 'SurveyType',
                         '"Sub Sites Not Surveyed': 'SubSitesNotSurveyed',
                         'Regional Survey': 'RegionalSurvey',
                         'Regional Survey Code': 'RegionalCountCode',
                         'Event Comment': 'Comments',
                         'Collection Device': 'CollectionDeviceID',
                         'Specify other..1': 'DefineOthersCollection',
                         'CreationDate': 'CreatedDate',
                         'Creator': 'CreatedBy'})

            ##############################
            # Numerous Field CleanUp Steps
            ##############################

            # Convert to date only
            outDFSubset['StartDate'] = pd.to_datetime(outDFSubset['StartDate']).dt.normalize()

            # Copy StartDate to EndDate
            outDFSubset['EndDate'] = outDFSubset['StartDate']

            # Reorder columns to place EndDate right after StartDate
            cols = list(outDFSubset.columns)
            start_idx = cols.index('StartDate')
            # Move 'EndDate' to right after 'StartDate'
            cols.insert(start_idx + 1, cols.pop(cols.index('EndDate')))
            outDFSubset = outDFSubset[cols]

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

            # Insert 'Project' field
            outDFSubset.insert(fieldLen + 3, "Project", "Pinniped")

            # Insert 'ProtcolID' field
            outDFSubset.insert(fieldLen + 4, "ProtocolID", "4")

            ############################
            # Subset to the Event Fields
            ############################

            outDFEvent = outDFSubset[['GlobalID', "ProjectCode", "StartDate", "EndDate", "StartTime", "EndTime",
                                      "CreatedDate", "CreatedBy", "DataProcessingLevelID", "DataProcessingLevelDate",
                                      "DataProcessingLevelUser", "Project", "ProtocolID"]]

            # Define desired field types

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ["GlobalID", "ProjectCode", "StartDate", "EndDate", "StartTime", "EndTime",
                                      "CreatedDate", "CreatedBy", "DataProcessingLevelID", "DataProcessingLevelDate",
                                      "DataProcessingLevelUser", "Project", "ProtocolID"],
                             'Type': ["object", "object", "datetime64", "datetime64", "datetime64", "datetime64",
                                      "datetime64", "object", "object", "datetime64", "object", "object", "object"],
                            'DateTimeFormat': ["na", "na", "%m/%d/%Y", "%m/%d/%Y", "%H:%M", "%H:%M",
                                               "%m/%d/%Y %I:%M:%S %p", "na", "na", "%m/%d/%Y %H:%M:%S", "na", "na", "na"
                                               ]}

            outDFSurvey = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDFEvent)

            # Append outDFSurvey to 'tbl_Events'
            # Pass final Query to be appended
            insertQuery = (f'INSERT INTO tblEvents (GlobalID, ProjectCode, StartDate, EndDate, StartTime, EndTime, '
                           f'CreatedDate, CreatedBy, DataProcessingLevelID, DataProcessingLevelDate, '
                           f'DataProcessingLevelUser, Project, ProtocolID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                           f'?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFSurvey, "tblEvents", insertQuery, dmInstance)

            ##################
            # Define Observers -  table tblEventObservers
            # Harvest Mutli-select field Define Observers, if other, also harvest 'Specify Other.
            # Lookup table for contacts is tlu_Contacts - Contact_ID being pushed to table xref_EventContacts
            ##################

            outContactsDF = processElephantContacts(outDFSubset, etlInstance, dmInstance)

            # Lookup the EventID field via the GlobalID field
            outContactsDF.insert(0, "EventID", None)

            # Import Event Table to define the EventID via the GlobalID
            inQuery = (f"SELECT tblEvents.EventID, tblEvents.GlobalID FROM tblEvents WHERE ((Not (tblEvents.GlobalID)"
                       f" Is Null));")

            # Import Events
            outDFEventsLU = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Lookup the EventID via the Global ID field
            dfObsEvents_wEventID = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFEventsLU,
                                                                         "GlobalID", "EventID",
                                                                         outContactsDF, "GlobalID",
                                                                         "EventID")

            # Retain only the Fields of interest
            dfObsEvents_wEventID = dfObsEvents_wEventID[['EventID', 'ObserverID', 'CreatedDate']]

            insertQuery = (f'INSERT INTO tblEventObservers (EventID, ObserverID, CreatedDate) VALUES (?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            #Append the Contacts to the tblEventObserers table
            dm.generalDMClass.appendDataSet(cnxn, dfObsEvents_wEventID, "tblEventObservers", insertQuery,
                                            dmInstance)

            ##################
            # Process ElephantEvents table
            ##################

            outDFElephantEvents = outDFSubset[["GlobalID", "ParkCode", "Season", "Visibility", "SurveyType",
                                                  "RegionalSurvey", "RegionalCountCode", "Comments",
                                                  "CollectionDeviceID", "CreatedDate"]]

            # Define desired field types

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ["GlobalID", "ParkCode", "Season", "Visibility", "SurveyType",
                                                  "RegionalSurvey", "RegionalCountCode", "Comments",
                                                  "CollectionDeviceID", "CreatedDate"],
                             'Type': ["object", "object", "object", "int64", "object", "object",
                                      "object", "object", "object", "datetime64"],
                            'DateTimeFormat': ["na", "na", "na", "na", "na", "na",
                                               "na", "na", "na", "%m/%d/%Y %I:%M:%S %p"]}

            outDFElephantEvents = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDFElephantEvents)

            # Lookup the EventID field via the GlobalID field
            outDFElephantEvents.insert(0, "EventID", None)

            # Lookup the EventID via the Global ID field
            dfElephantEvents_wEventID = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFEventsLU,
                                                                          "GlobalID", "EventID",
                                                                          outDFElephantEvents, "GlobalID",
                                                                          "EventID")

            # Drop the GlobalID field not in the 'tblElephantsEvents' table
            dfElephantEvents_append = dfElephantEvents_wEventID.drop(columns='GlobalID')

            # Lookup the CollectionDeviceID field
            dfElephantEvents_append = dfElephantEvents_append.rename(columns={'CollectionDeviceID':'CollectionDeviceFull'})

            # Add the CollectionDeviceID field
            dfElephantEvents_append.insert(0, "ID", None)

            # Read in the tluDevices lookup table
            # Import Event Table to define the EventID via the GlobalID
            inQuery = f"SELECT tluDevices.* FROM tluDevices;"

            # Import Devices Table
            outDFDevices = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Lookup the CollectionDeviceID via the Global ID field
            dfElephantEvents_append = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFDevices,
                                                                               "DeviceCode", "ID",
                                                                               dfElephantEvents_append,
                                                                             "CollectionDeviceFull",
                                                                               "ID")

            # Drop CollectionDeviceID field
            dfElephantEvents_append = dfElephantEvents_append.drop(columns='CollectionDeviceFull')
            # Rename ID to CollectionDeviceID
            dfElephantEvents_append = dfElephantEvents_append.rename(
                columns={'ID': 'CollectionDeviceID'})


            # Set RegionalSurvey field to True if 'Yes' else False
            dfElephantEvents_append['RegionalSurvey'] = dfElephantEvents_append['RegionalSurvey'] == 'Yes'

            # Confirm the tluESealSeasons has been defiend for the Realized Seasons

            inQuery = f"SELECT tluESealSeasons.* FROM tluESealSeasons"

            #
            # Check Season Table - Import Seasons Table
            #

            uniqueSeasonsDF = pd.DataFrame(dfElephantEvents_append['Season'].unique(), columns=['Season'])
            uniqueSeasonsDF.insert(0, "SeasonDefined", None)


            outDFSeasons = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Lookup the CollectionDeviceID via the Global ID field
            dfSeasonsDefined = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFSeasons,
                                                                             "Season", "Season",
                                                                             uniqueSeasonsDF,
                                                                             "Season",
                                                                             "SeasonDefined")

            # Confirm the Season has been defined - if not exist
            # Check for Lookups not defined via an outer join.
            # If is null then these are undefined contacts
            dfSeasonsDefined_Null = dfSeasonsDefined[dfSeasonsDefined['SeasonDefined'].isna()]

            numRec = dfSeasonsDefined_Null.shape[0]
            if numRec >= 1:
                logMsg = f'WARNING there are {numRec} records without a defined tluESealSeasons.'
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.warning(logMsg)

                outPath = f'{etlInstance.outDir}\ESealSeasonNotDefined.csv'
                if os.path.exists(outPath):
                    os.remove(outPath)

                dfSeasonsDefined_Null.to_csv(outPath, index=True)

                logMsg = (f'Exporting Records without a defined lookup see - {outPath} \n'
                          f'Exiting ETL_PINN_Elephant.py - process_SurveyMetadata with out full completion.')
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.warning(logMsg)
                exit()

            logMsg = f"Success ETL_PINN_ELephant.py - processElephantContacts."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)


            # Append the Elephant Event Records
            insertQuery = (f'INSERT INTO tblElephantEvents (CollectionDeviceID, EventID, ParkCode, Season, Visibility, '
                           f'SurveyType, RegionalSurvey, RegionalCountCode, Comments, CreatedDate) '
                           f'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            # Append the Contacts to the tblEventObserers table
            dm.generalDMClass.appendDataSet(cnxn, dfElephantEvents_append, "tblElephantEvents", insertQuery,
                                            dmInstance)


            #################################
            # Define Table SubSiteNotDefined  - STOPPED HERE 5/12/2025
            # Table SubSiteNotDefined
            #################################


            # Return Survey with the Regional and EventID Defined

            logMsg = f"Success ETL Survey/Event Form ETL_SNPLPORE.py - process_Survey"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            # Returning the Dataframe survey which was pushed to 'tbl_Events, will be used in subsequent ETL.
            return outDFEvent

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - proces_Survey: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)


def processElephantContacts(inDF, etlInstance, dmInstance):
    """
    Define Observers in Pinnipeds table tblEventObservers
    Harvest Multi-select field 'Define Observers', if other, also harvest 'Specify Other' field in Survey .csv
    Lookup table for contacts is tlu_Contacts - Contact_ID being pushed to table xref_EventContacts

    :param inDF: Data Frame being processed
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return:
    """

    try:

        inDFContacts = inDF[['GlobalID', "Observers", "ObserversOther", "CreatedDate"]]

        #####################################
        # Parse the 'Observers' field on ','
        # First remove the records where Observers == 389
        inObsNotOther = inDFContacts[inDFContacts['Observers'] != '389']
        inDFObserversParsed = inObsNotOther.assign(Observers=inObsNotOther['Observers'].str.split(',')).explode('Observers')

        # Trim white space in observers field
        inDFObserversParsed['Observers'] = inDFObserversParsed['Observers'].str.lstrip()

        # Drop any records with 'Other' some cases have defined people and then also other
        inDFObserversParsed2 = inDFObserversParsed[inDFObserversParsed['Observers'] != '389']
        # Drop field 'other'
        inDFObserversParsed3 = inDFObserversParsed2.drop(['ObserversOther'], axis=1)
        # Reset Index
        inDFObserversParsed3.reset_index(drop=True)

        ##################################
        # Parse the 'Other' field on ','
        # Retain only the records where Observers contains 'other'
        inObsOther = inDFContacts[inDFContacts['Observers'].str.contains('389')]
        inDFOthersParsed = inObsOther.assign(Observers=inObsOther['Observers'].str.split(',')).explode('Observers')

        # Trim white space in observers field
        inDFOthersParsed['Observers'] = inDFOthersParsed['Observers'].str.lstrip()

        # Remove Records that are not 389 - other
        inDFOthersParsed2 = inDFOthersParsed[inDFOthersParsed['Observers'] == '389']

        # Reset Index
        inDFOthersParsed3 = inDFOthersParsed2.reset_index(drop=True)

        ##################################
        # Combine both parsed dataframes for fields Observers and Others
        dfObserversOther = pd.concat([inDFObserversParsed3, inDFOthersParsed3], ignore_index=True)

        # Set Observers field to 'Integer'
        dfObserversOther['Observers'] = dfObserversOther['Observers'].astype(int)

        # Define First and Last Name Fields
        dfObserversOther.insert(2, "Last_Name", None)
        dfObserversOther.insert(3, "First_Name", None)
        dfObserversOther.insert(4, "ObserverID", None)
        #######################################
        # Read in 'Lookup Table - tlu Contacts'
        inQuery = f"SELECT tluObservers.ObserverID, [FirstName] & '_' & [LastName] AS First_Last FROM tluObservers;"

        outDFContactsLU = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

        # Apply the Lookup Code on the Two Data Frames to get the Obse
        dfObserversOtherwLK = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFContactsLU,
                                                             "ObserverID", "ObserverID",
                                                             dfObserversOther, "Observers",
                                                             "ObserverID")

        # Check for Lookups not defined via an outer join.
        # If is null then these are undefined contacts
        dfObserversNull = dfObserversOtherwLK[
            dfObserversOtherwLK['ObserverID'].isna() | (dfObserversOtherwLK['ObserverID'] == 389)
            ]

        numRec = dfObserversNull.shape[0]
        if numRec >= 1:
            logMsg = f'WARNING there are {numRec} records without a defined tluObservers ObserverID value.'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.warning(logMsg)

            outPath = f'{etlInstance.outDir}\RecordsNoDefinedContact.csv'
            if os.path.exists(outPath):
                os.remove(outPath)

            dfObserversNull.to_csv(outPath, index=True)

            logMsg = (f'Exporting Records without a defined lookup see - {outPath} \n'
                      f'Exiting ETL_PINN_Elephant.py - processSNPLContacts with out full completion.')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.warning(logMsg)
            exit()

        logMsg = f"Success ETL_PINN_ELephant.py - processElephantContacts."
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.info(logMsg)

        return dfObserversOtherwLK

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - processElephantContacts: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)