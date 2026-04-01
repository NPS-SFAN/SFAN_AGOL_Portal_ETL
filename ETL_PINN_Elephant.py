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
import ArcGIS_API as agl


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

    def process_PINNElephant(outDFDic, etlInstance, dmInstance, generalArcGIS):

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
            # Subset AGOL Dictionary/DataFrames to the elephantSeason defined.  Breeding Season if first then Molt
            # Season.
            #########

            outFCDicSub = subsetToSeason(outDFDic, etlInstance, dmInstance)

            ######
            # Process Survey Metadata Form - tblEvents
            ######
            outFun = etl_PINNElephant.process_SurveyMetadata(outFCDicSub, etlInstance, dmInstance)

            outDFEvents = outFun[0]
            outDFElephantEvents = outFun[1]

            ######
            # Process Counts Form - tblSealCount and tblPhocaSealCount-(RedFur and Shark Bite)
            ######
            outDFCounts = etl_PINNElephant.process_Counts(outFCDicSub, outDFEvents, etlInstance, dmInstance)

            ######
            # Process Resights Form - Create Resight Events and Resight Records
            ######
            outFun = etl_PINNElephant.process_Resights(outFCDicSub, outDFEvents, etlInstance, dmInstance)

            outDFResightEvents = outFun[0]
            outDFResightRec = outFun[1]

            ######
            # Process Observations Form
            ######
            outDFDisturbance = etl_PINNElephant.process_Disturbance(outFCDicSub, outDFEvents, etlInstance, dmInstance)

            ######
            # Consolidate Events collected on multiple tablets
            ######
            outDFEventsConsolidated = etl_PINNElephant.process_MultipleTabletEvents(outDFEvents, outDFElephantEvents,
                                                                                    outDFResightEvents, etlInstance,
                                                                                    dmInstance)

            ########
            # Process the Images in the Resight Form - requires direct hit of the ArcGIS API
            ########
            outFun = etl_PINNElephant.process_ResightPhotos(outDFEvents, outDFResightRec, etlInstance,
                                                            dmInstance, generalArcGIS)



            logMsg = f"Success ETL_PINN_Elephant.py - process_PINNElephant."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            outETL = "Success ETL PINN Elephant"
            return outETL

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_PINN_Elephant.py - process_PINNElephant: {e}'
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
            # Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None
            for key, df in outDFDic.items():
                if 'ElephantSeal' in key:
                    inDF = df
                    break

            outDFSubset = inDF[['GlobalID', 'Survey Name', "Project Type", "Park Code", "Season", "Survey Date",
                                "Start Time Survey", "End Time Survey", "Define Observer(s)", "Specify other.",
                                "Visibility", "Survey Type", "Sub Sites Not Surveyed", "Regional Survey",
                                "Regional Survey Code", "Event Comment", "Collection Device", "CreationDate",
                                "Creator", "Resight Data"]].rename(
                columns={'Project_Type': 'ProjectCode',
                         'Park Code': 'ParkCode',
                         'Project Type': 'ProjectCode',
                         'Survey Date': 'StartDate',
                         'Start Time Survey': 'StartTime',
                         'End Time Survey': 'EndTime',
                         'Define Observer(s)': 'Observers',
                         'Specify other.': 'ObserversOther',
                         'Survey Type': 'SurveyType',
                         'Sub Sites Not Surveyed': 'SubSitesNotSurveyed',
                         'Regional Survey': 'RegionalSurvey',
                         'Regional Survey Code': 'RegionalCountCode',
                         'Event Comment': 'Comments',
                         'Collection Device': 'CollectionDeviceID',
                         'Specify other..1': 'DefineOthersCollection',
                         'CreationDate': 'CreatedDate',
                         'Creator': 'CreatedBy',
                         'Resight Data':'ResightData'})

            ##############################
            # Numerous Field CleanUp Steps
            ##############################

            # Update 'E_SEAL' to 'E_Seal' for consistency
            outDFSubset['ProjectCode'] = outDFSubset['ProjectCode'].str.replace(r'^E_SEAL$', 'E_Seal', regex=True)

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
                                      "DataProcessingLevelUser", "Project", "ProtocolID", "ResightData"]]

            # Define desired field types

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ["GlobalID", "ProjectCode", "StartDate", "EndDate", "StartTime", "EndTime",
                                      "CreatedDate", "CreatedBy", "DataProcessingLevelID", "DataProcessingLevelDate",
                                      "DataProcessingLevelUser", "Project", "ProtocolID", "ResightData"],
                             'Type': ["object", "object", "datetime64", "datetime64", "datetime64", "datetime64",
                                      "datetime64", "object", "object", "datetime64", "object", "object", "object",
                                      "object"],
                            'DateTimeFormat': ["na", "na", "%m/%d/%Y", "%m/%d/%Y", "%H:%M", "%H:%M",
                                               "%m/%d/%Y %I:%M:%S %p", "na", "na", "%m/%d/%Y %H:%M:%S", "na", "na",
                                               "na", "na"]}

            outDFSurvey = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDFEvent)

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            outDFSurvey = outDFSurvey.replace([np.nan, 'nan'], None)

            # Drop field ResightData
            outDFSurveyAppend = outDFSurvey.drop(columns=['ResightData'])

            # Append outDFSurvey to 'tbl_Events' All Events (E_Seal and Resight)
            # Pass final Query to be appended
            insertQuery = (f'INSERT INTO tblEvents (GlobalID, ProjectCode, StartDate, EndDate, StartTime, EndTime, '
                           f'CreatedDate, CreatedBy, DataProcessingLevelID, DataProcessingLevelDate, '
                           f'DataProcessingLevelUser, Project, ProtocolID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                           f'?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFSurveyAppend, "tblEvents", insertQuery, dmInstance)

            print("Successfully imported initial Events to tblEvents")

            ######################
            # Create second Event where both 'ESeal' and 'Resight' data collection.  This will be 'ResightData' = 'Yes'
            # and Project Type != 'Seal Resight'  On the append above the initial ESeal is created but not the Resight
            # Event.  These will be Seal_Resight events
            #####################

            outDFSurveyResight2nd = outDFSurvey[(outDFSurvey['ProjectCode'] != 'Seal_Resight') &
                                                (outDFSurvey['ResightData'] == 'Yes')]

            # Drop field ResightData
            outDFSurveyResight2ndAppend = outDFSurveyResight2nd.drop(columns=['ResightData'])

            # Set ProjectCode = 'Seal_Resight'
            outDFSurveyResight2ndAppend['ProjectCode'] = "Seal_Resight"

            # Append outDFSurvey to 'tbl_Events' All Events (E_Seal and Resight)
            # Pass final Query to be appended
            insertQuery = (f'INSERT INTO tblEvents (GlobalID, ProjectCode, StartDate, EndDate, StartTime, EndTime, '
                           f'CreatedDate, CreatedBy, DataProcessingLevelID, DataProcessingLevelDate, '
                           f'DataProcessingLevelUser, Project, ProtocolID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                           f'?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFSurveyResight2ndAppend, "tblEvents", insertQuery,
                                            dmInstance)

            print("Successfully imported 2nd set of events for concurrent 'E_Seal' and 'Resight_Events', these are "
                  "Seal_Resight events.")

            ##################
            # Define Observers -  table tblEventObservers
            # Harvest Mutli-select field Define Observers, if other, also harvest 'Specify Other.
            # Lookup table for contacts is tlu_Contacts - Contact_ID being pushed to table xref_EventContacts
            ##################

            outContactsDF = processElephantContacts(outDFSubset, etlInstance, dmInstance)

            # Lookup the EventID field via the GlobalID field
            # outContactsDF.insert(0, "EventID", None)

            # Import Event Table to define the EventID via the GlobalID
            inQuery = (f"SELECT tblEvents.EventID, tblEvents.GlobalID, tblEvents.ProjectCode FROM tblEvents WHERE"
                       f" ((Not (tblEvents.GlobalID)"
                       f" Is Null));")

            # Import Events
            outDFEventsLU = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Lookup the EventID via the Global ID field
            dfObsEvents_wEventID = pd.merge(
                outContactsDF,
                outDFEventsLU[['EventID', 'GlobalID']],  # only keep these
                left_on='GlobalID',
                right_on='GlobalID',
                how='left'
            )

            # Retain only the Fields of interest
            dfObsEvents_wEventID = dfObsEvents_wEventID[['EventID', 'ObserverID', 'CreatedDate']]

            insertQuery = f'INSERT INTO tblEventObservers (EventID, ObserverID, CreatedDate) VALUES (?, ?, ?)'

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            # Append the Contacts to the tblEventObserers table
            dm.generalDMClass.appendDataSet(cnxn, dfObsEvents_wEventID, "tblEventObservers", insertQuery,
                                            dmInstance)

            print("Successfully imported Observer records to tblEventObservers'")
            logMsg = f"Success ETL_PINN_ELephant.py - processElephantContacts."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            ##################
            # Process ElephantEvents table - Don't create events if Only Resight, only applicable if Count data
            # collected (i.e. E_Seal only).
            ##################

            outDFElephantEvents = outDFSubset[["GlobalID", "ParkCode", "Season", "Visibility", "SurveyType",
                                                  "RegionalSurvey", "RegionalCountCode", "Comments",
                                                  "CollectionDeviceID", "CreatedDate", "ResightData", "ProjectCode"]]

            # Define desired field types

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ["GlobalID", "ParkCode", "Season", "Visibility", "SurveyType",
                                                  "RegionalSurvey", "RegionalCountCode", "Comments",
                                                  "CollectionDeviceID", "CreatedDate", "ProjectCode"],
                             'Type': ["object", "object", "object", "int64", "object", "object",
                                      "object", "object", "object", "datetime64", "object"],
                            'DateTimeFormat': ["na", "na", "na", "na", "na", "na",
                                               "na", "na", "na", "%m/%d/%Y %I:%M:%S %p", "na"]}

            outDFElephantEvents = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDFElephantEvents)

            # Drop the Resight_Only records - no Elephant Seal Record created - i.e. Project Type = 'Seal_Resight'
            outDFElephantEvents2nd = outDFElephantEvents[outDFElephantEvents['ProjectCode'] != 'Seal_Resight']

            # Drop field ResightData
            outDFElephantEvents2nd = outDFElephantEvents2nd.drop(columns=['ResightData'])

            # Lookup the EventID via the Global ID  and ProjectCode = SurveyTypefield
            dfElephantEvents_wEventID = pd.merge(
                outDFElephantEvents2nd,
                outDFEventsLU[['EventID', 'GlobalID', 'ProjectCode']],  # only keep these
                left_on=['GlobalID', 'ProjectCode'],
                right_on=['GlobalID', 'ProjectCode'],
                how='left'
            )

            # Drop the GlobalID field not in the 'tblElephantsEvents' table
            dfElephantEvents_append = dfElephantEvents_wEventID.drop(columns=['GlobalID', 'ProjectCode'])

            # Lookup the CollectionDeviceID field
            dfElephantEvents_append = dfElephantEvents_append.rename(columns={'CollectionDeviceID':'CollectionDeviceFull'})

            # Add the CollectionDeviceID field
            # dfElephantEvents_append.insert(0, "ID", None)

            # Read in the tluDevices lookup table
            # Import Event Table to define the EventID via the GlobalID
            inQuery = f"SELECT tluDevices.* FROM tluDevices;"

            # Import Devices Table
            outDFDevices = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Lookup the CollectionDeviceID via the Global ID field
            dfElephantEvents_append2 = pd.merge(
                dfElephantEvents_append,
                outDFDevices[['ID', 'DeviceCode']],  # only keep these
                left_on='CollectionDeviceFull',
                right_on='DeviceCode',
                how='left')

            # Drop CollectionDeviceID field
            dfElephantEvents_append2 = dfElephantEvents_append2.drop(columns='CollectionDeviceFull')
            # Rename ID to CollectionDeviceID
            dfElephantEvents_append2 = dfElephantEvents_append2.rename(
                columns={'ID': 'CollectionDeviceID'})

            # Set RegionalSurvey field to True if 'Yes' else False
            dfElephantEvents_append2['RegionalSurvey'] = dfElephantEvents_append2['RegionalSurvey'] == 'Yes'

            # Confirm the tluESealSeasons has been defined for the Realized Seasons

            inQuery = f"SELECT tluESealSeasons.* FROM tluESealSeasons"

            #
            # Check Season Table - Import Seasons Table
            #

            uniqueSeasonsDF = pd.DataFrame(dfElephantEvents_append['Season'].unique(), columns=['Season'])
            uniqueSeasonsDF.insert(0, "SeasonToDefine", None)

            outDFSeasons = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Lookup the Season
            dfSeasonsDefined = pd.merge(
                uniqueSeasonsDF,
                outDFSeasons,
                left_on='Season',
                right_on='Season',
                how='left')

            # Confirm the Season has been defined - if not exist
            # Check for Lookups not defined via an outer join.
            # If is null then these are undefined contacts
            dfSeasonsDefined_Null = dfSeasonsDefined[dfSeasonsDefined['Active'].isna()]

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

            # Remove/Clean Up Fields - EventID_lk, ID_lk, DeviceCode, DeviceName, Notes
            dfElephantEvents_append3 = dfElephantEvents_append2.drop(
                columns=['DeviceCode']
            )

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            dfElephantEvents_append3 = dfElephantEvents_append3.replace([np.nan, 'nan'], None)

            # Append the Elephant Event Records
            insertQuery = (f'INSERT INTO tblElephantEvents (ParkCode, Season, Visibility, SurveyType, RegionalSurvey, '
                           f'RegionalCountCode, Comments, CreatedDate, EventID, CollectionDeviceID) '
                           f'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            # Append the Contacts to the tblEventObserers table
            dm.generalDMClass.appendDataSet(cnxn, dfElephantEvents_append3, "tblElephantEvents", insertQuery,
                                            dmInstance)

            print("Successfully created ESeal/Count Events to tblElephantEvents'")

            #################################
            # Define Table SubSiteNotDefined
            # Table SubSiteNotDefined
            #################################

            tblSubSitesNotSurveyed(outDFSubset, outDFEventsLU, etlInstance, dmInstance)

            #####################################################
            # Return Survey with the Regional and EventID Defined
            # Use existing outDFEvent dataframe and the already imported outDFEventsLU

            # Define the EventID and Visibility fields via join on the ParentGlobalID - GlobalID join
            outDFEventwGlIDwEventID = pd.merge(outDFSurvey[["GlobalID", "StartDate", "StartTime", "EndTime"]],
                                               outDFEventsLU[["GlobalID", "ProjectCode", "EventID"]],
                                               how='outer', left_on="GlobalID",
                                               right_on="GlobalID")

            # Last Join to get the Visibility Field - Needed for Elephant and Resight Event Table - Added 8/18/2025
            outDFEventwGlIDwEventIDwVis = pd.merge(outDFEventwGlIDwEventID[["GlobalID", "StartDate", "StartTime",
                                                                            "EndTime", "EventID", "ProjectCode"]],
                                               inDF[["GlobalID", "Visibility", "Season", "Park Code", "Event Comment"]],
                                               left_on="GlobalID",
                                               right_on="GlobalID",
                                               how='inner')

            outDFEventwGlIDwEventIDFinal = outDFEventwGlIDwEventIDwVis.rename(columns={'Park Code': 'ParkCode',
                                                                                       'Event Comment': 'Comments'})

            logMsg = f"Success ETL Survey/Event Form ETL_SNPLPORE.py - process_Survey"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            # Returning the Dataframe with the Survey and Event information and the Elephant Seal Event Dataframe
            return outDFEventwGlIDwEventIDFinal, dfElephantEvents_append3

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - proces_SurveyMetadata: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_Counts(outDFDic, outDFEvents, etlInstance, dmInstance):

        """
        ETL routine for the Counts Repeat Form table.
        The majority of this information on this form will be pushed to the following tables:
        tblSealCount and Red Seal and Shark Bite - tblPhocaSealCount.

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param outDFEvents - Event Data Frame from the SurveyMetadata, with GlobalID and EventID definition
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outDFCounts: Dataframe of the exported Count table records will be used in subsequent ETL Routines.
        """

        try:
            # Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None
            for key, df in outDFDic.items():
                if 'countsrepeats' in key:
                    inDF = df
                    break

            outDFSubset = inDF[["Sub Site",	"Bull", "SA4","SA3", "SA2", "SA1", "Other SA", "Cow", "Pup", "Dead Pup",
                                "WNR", "IMM", "YRLNG", "PHOCA", "PHOCA Pup", "ZALOPHUS", "Other", "Define Other",
                                "Specify other.", "Red Seal", "Shark Bite", "ParentGlobalID", "CreationDate"]].rename(
                columns={"Sub Site": "LocationID",
                         "Other SA": "OtherSA",
                         "Pup": "EPUP",
                         "Dead Pup": "DEPUP",
                         "PHOCA": "ADULT",
                         "PHOCA Pup": "HPUP",
                         "ZALOPHUS": "ZAL",
                         "Define Other": "DefineOther",
                         "Specify other.": "SpecifyOther",
                         "Red Seal": "RedFurPhoca",
                         "Shark Bite": "SharkBitePhoca",
                         "CreationDate": "CreatedDate"})

            ##############################
            # Numerous Field CleanUp and Standardization of field format
            ##############################

            # Define field types

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ["LocationID",	"Bull", "SA4", "SA3", "SA2", "SA1", "OtherSA", "Cow", "EPUP", "DPUP",
                                "WNR", "IMM", "YRLNG", "ADULT", "HPUP", "ZAL", "Other", "DefineOther", "SpecifyOther",
                                      "RedFurPhoca", "SharkBitePhoca", "ParentGlobalID", "CreatedDate"],
                            'Type': ["int64", "int64", "int64", "int64", "int64", "int64", "int64", "int64", "int64",
                                     "int64", "int64", "int64", "int64", "int64", "int64", "int64", "int64", "object",
                                     "object", "int64", "int64", "object", "datetime64"],
                            'DateTimeFormat': ["na", "na", "na","na", "na", "na", "na", "na", "na", "na",
                                "na", "na", "na", "na", "na", "na", "na", "na", "na", "na", "na", "na",
                                               "%m/%d/%Y %I:%M:%S %p"]}

            outDFCounts = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDFSubset)

            # Subset to only 'E_Seal' events - Resight_Events are not applicable
            outDFEvents_ESeal = outDFEvents[outDFEvents['ProjectCode'] == 'E_Seal']

            # Merge on the Event Data Frame to get the EventID via the ParentGlobalID - GlobalID fields
            outDFCountswEventID = pd.merge(outDFCounts, outDFEvents_ESeal, how='left', left_on="ParentGlobalID",
                                           right_on="GlobalID", suffixes=("_src", "_lk"))

            #
            # Move field that will be in all stack records to front
            #

            cols_to_move = ['EventID', 'StartTime', 'CreatedDate']

            # All columns
            all_cols = list(outDFCountswEventID.columns)

            # Index of the 'LocationID' column
            loc_index = all_cols.index('LocationID')

            # Remove the columns to move from their current positions
            for col in cols_to_move:
                all_cols.remove(col)

            # Insert them before 'LocationID'
            for col in reversed(cols_to_move):  # reverse to maintain order when inserting
                all_cols.insert(loc_index, col)

            # Reorder the DataFrame
            outDFCountswEventID = outDFCountswEventID[all_cols]

            # Rename Start TIme to ObservationTime
            outDFCountswEventID = outDFCountswEventID.rename(columns={'StartTime':'ObservationTime'})

            #
            # Subset to the Counts Fields and Stack Records - Going to the tblSealCount table
            #
            outDFCountsStack1 = outDFCountswEventID.drop(['RedFurPhoca', 'SharkBitePhoca', 'ParentGlobalID',
                                                            'GlobalID', 'Other', 'DefineOther', 'SpecifyOther',
                                                          'StartDate', 'ProjectCode', 'Visibility', 'Season',
                                                          'ParkCode', 'Comments', 'EndTime'], axis=1)

            # Define fields identifying stack records
            id_vars = ['CreatedDate', 'EventID', 'ObservationTime', 'LocationID']

            # Stack Records
            outDFCountsStack1Melt = outDFCountsStack1.melt(id_vars=id_vars, var_name='MatureCode', value_name='Enumeration')

            # Drop records where Enumeration in Null
            outDFCountsStack1Melt = outDFCountsStack1Melt.dropna(subset=['Enumeration'])

            # Add QC Notes Field for pending append
            outDFCountsStack1Melt.insert(6, 'QCNotes', np.nan)

            ####################################
            # Harvest the Other, DefineOther, and SpecifyOther fields.  Other is the Enumerated/Count field, DefineOther
            # will be the DefineOther values - EUJU, ARTO, and CAUR values per defined value.
            # For the Specify Other if present add the ND Maturity Code and add the other value to the QCNotes field
            # This will be used
            ####################################
            outDFCountsStack2 = outDFCountswEventID[['CreatedDate', 'EventID', 'ObservationTime', 'LocationID', 'Other',
                                                  'DefineOther', 'SpecifyOther']]

            # Drop records where Other is Null
            outDFCountsStack2 = outDFCountsStack2.dropna(subset=['Other'])

            # 1- Process the Other where is a Defined Other (i.e. EUJU, ARTO or CAUR.
            outDFCountsStack3 = outDFCountsStack2[~outDFCountsStack2['DefineOther'].str.lower().eq('other')]

            # Drop the SpecifyOther field
            outDFCountsStack3 = outDFCountsStack3.drop(['SpecifyOther'], axis=1)

            # Rename 'Other' field to 'Enumeration' and 'DefineOther' to 'MatureCode
            outDFCountsStack3 = outDFCountsStack3.rename(columns={'Other': 'Enumeration', 'DefineOther': 'MatureCode'})

            # Move 'MatureCode' to before 'Enumeration' field
            # Get current list of columns
            cols = list(outDFCountsStack3.columns)

            # Remove 'MatureCode' from its current position
            cols.remove('MatureCode')

            # Find the index of 'Enumeration'
            enum_index = cols.index('Enumeration')

            # Insert 'MatureCode' before 'Enumeration'
            cols.insert(enum_index, 'MatureCode')

            # Reorder the DataFrame - This is ready to be added to the 'outDFCountsStack1Melt' dataframe
            outDFCountsStack3 = outDFCountsStack3[cols]

            # Insert QCNotes field for pending append
            outDFCountsStack3.insert(6, 'QCNotes', np.nan)

            # 2 - Add Other record with a ND value, count and name in the QC Notes field
            outDFCountsStack_NE = outDFCountsStack2[outDFCountsStack2['SpecifyOther'].notna()]

            # Rename 'Other' field to 'Enumeration' and 'DefineOther' to 'MatureCode
            outDFCountsStack_NE = outDFCountsStack_NE.rename(columns={'Other': 'Enumeration'})

            # Define a MatureCode value of 'ND' that is not Defined
            outDFCountsStack_NE.insert(4, "MatureCode", "ND")

            # Add QCNotes field with the Specify Other value
            outDFCountsStack_NE["QCNotes"] = "Taxon Not Defined: " + outDFCountsStack_NE['SpecifyOther']

            # Drop fields DefineOther and SpecifyOther
            outDFCountsStack_NE = outDFCountsStack_NE.drop(['DefineOther', 'SpecifyOther'], axis=1)

            # Combine/Append the Other records
            combinedOtherDF = pd.concat([outDFCountsStack3, outDFCountsStack_NE], ignore_index=True)

            # Combine/Append the initial Stacked Count records in data frame - outDFCountsStack1Melt
            combinedAllCountsDF = pd.concat([outDFCountsStack1Melt, combinedOtherDF], ignore_index=True)


            #######
            # New workflow March 2026 to handle newly added 'DeadPup' field not in the 2025 workflow
            #######

            # Add 'Qualifier' and 'QCFlag' fields
            combinedAllCountsDF[['Qualifier', 'QCFlag']] = pd.NA

            # Define 'Qualifier' and 'QCFlag' fields to 'DEAD' where 'DEPUP'
            combinedAllCountsDF.loc[combinedAllCountsDF['MatureCode'] == 'DEPUP', ['Qualifier', 'QCFlag']] = 'DEAD'

            # Change DEPUP MatureCode values to EPUP
            combinedAllCountsDF['MatureCode'] = combinedAllCountsDF['MatureCode'].replace('DEPUP', 'EPUP')

            # After Stacking all the records ready to append the records to 'tblSealCount' - Check for duplicates
            duplicatesDF = combinedAllCountsDF[combinedAllCountsDF.duplicated()]

            if duplicatesDF.shape[0] > 0:

                duplicates_all = combinedAllCountsDF[combinedAllCountsDF.duplicated(keep=False)]

                outPath = f'{etlInstance.outDir}\DuplicateCounts.csv'
                if os.path.exists(outPath):
                    os.remove(outPath)

                duplicates_all.to_csv(outPath, index=True)

                msgLog = f'Duplicate Records in the Counts Data Frame to be appended see export - {outPath}'
                logging.critical(msgLog, exc_info=True)

            # Update any 'nan' string or np.nan values to None to consistently handle null values.
            combinedAllCountsDF = combinedAllCountsDF.replace([np.nan, 'nan'], None)

            # Pass final Query to be appended
            insertQuery = (f'INSERT INTO tblSealCount (CreatedDate, EventID, ObservationTime, LocationID, '
                           f'MatureCode, Enumeration, QCNotes, Qualifier, QCFlag) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, combinedAllCountsDF, "tblSealCount", insertQuery, dmInstance)

            logMsg = f"Successfully imported Count Data to tblSealCount"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)
            print(logMsg)


            ######################################################################
            # Process RedFur and Shark Bite Records this goes to tblPhocaSealCount
            ######################################################################

            outRedFurShark = processRedFurShark(outDFCountswEventID, etlInstance, dmInstance)

            logMsg = f'Success process_Counts ETL Routines'
            logging.info(logMsg, exc_info=True)
            print(logMsg)
            return combinedAllCountsDF

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - proces_Counts: {e}'
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_Resights(outDFDic, outDFEvents, etlInstance, dmInstance):

        """
        ETL routine for the Resights Repeat Form table.
        The majority of this information on this form will be pushed to the following tables:
        tblResightEvents and tblResights

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param outDFEvents - Event Data Frame from the SurveyMetadata, with GlobalID and EventID definition
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outDFResightEvents - Dataframe appended to Resight Events and outDFResightRec - Dataframe appended to
             tblResights.
        """

        try:
            # Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None
            for key, df in outDFDic.items():
                if 'resightsrepeats' in key:
                    inDF = df
                    break

            outDFSubset = inDF[["Sub Site", "Maturity", "Sex", "ConditionCode", "Dye Number", "Dye Code", "Left Color",
                                "Left Tag #", "Left Position", "Left Tag Code", "Right Color", "Right Tag #",
                                "Right Position", "Right Tag Code", "Bull/Cow Status", "Pup Size", "Comments",
                                "photonameleft_name", "photonameright_name", "CreationDate", "ParentGlobalID", 'GlobalID']].rename(
                columns={"Sub Site": "LocationID",
                         "Maturity": "MatureCode",
                         "Dye Number": "Dye",
                         "Dye Code": "DyeCode",
                         "Left Color": "LtagColor",
                         "Left Tag #": "LtagNo",
                         "Left Position": "LtagPosn",
                         "Left Tag Code": "LtagCode",
                         "Right Color": "RtagColor",
                         "Right Tag #": "RtagNo",
                         "Right Position": "RtagPosn",
                         "Right Tag Code": "RtagCode",
                         "Bull/Cow Status": "ReproductiveStatusCode",
                         "Pup Size": "PupSize",
                         "photonameleft_name": "PhotoNameLeft",
                         "photonameright_name": "PhotoNameRight",
                         "CreationDate": "CreatedDate"})

            ##############################
            # Numerous Field CleanUp and Standardization of field format
            ##############################

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ["LocationID", "MaturityCode", "Sex", "ConditionCode", "Dye", "DyeCode",
                                      "LtagColor", "LtagNo", "LtagPosn",
                                      "LtagCode", "RtagColor", "RtagNo", "RtagPosn", "RtagCode",
                                      "ReproductiveStatusCode", "PupSize", "PhotoNameLeft", "PhotoNameRight",
                                      "Comments", "CreatedDate", "ParentGlobalID", "GlobalID"],
                'Type': ["int64", "object", "object", "object", "object", "object", "object", "object", "object", "int64",
                          "object", "object", "object", "int64", "object", "object",
                          "object", "object", "object", "object", "object", "object"],
                'DateTimeFormat': ["na", "na", "na", "na", "na", "na", "na", "na", "na", "na",
                          "na", "na", "na", "na", "na", "na", "na", "na", "na", "%m/%d/%Y %I:%M:%S %p", "na", "na"]}

            outDFResights = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDFSubset)

            # Subset to only 'Seal_Resight' events
            outDFEvents_Resight = outDFEvents[outDFEvents['ProjectCode'] == 'Seal_Resight']

            # Merge on the Event Data Frame to get the EventID via the ParentGlobalID - GlobalID fields
            outDFResightswEventID = pd.merge(outDFResights, outDFEvents_Resight[['GlobalID', 'EventID']], how='left',
                                             left_on="ParentGlobalID", right_on="GlobalID")

            outDFResightswEventID = outDFResightswEventID.drop(columns=['GlobalID_y']).rename(columns={'GlobalID_x': 'GlobalID'})


            ############
            # Create the Resight  Event Table Records - this will be an import of the 'outDFEvents_Resight'
            ############

            outDFResightEvents = processResightEvents(outDFEvents_Resight, etlInstance, dmInstance)

            ############
            # Create the Resight Records
            ############

            outDFResightRec = processResightRecords(outDFResightswEventID, etlInstance, dmInstance)

            logMsg = f'Success process_Resights ETL Routine'
            logging.info(logMsg, exc_info=True)

            return outDFResightEvents, outDFResightRec

        except Exception as e:
            logMsg = f'WARNING ERROR  - ETL_PINN_Elephant.py - process_Resights: {e}'
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_Disturbance(outDFDic, outDFEvents, etlInstance, dmInstance):
        """
        ETL routine for Disturbance Form
        This information on this form is pushed to the following tables:
        tblDisturbance and tblDisturbanceBehave

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param outDFEvents - Event Data Frame from the SurveyMetadata, with GlobalID and EventID definition
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:String denoting Success or Failed
        """

        try:
            # Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None
            for key, df in outDFDic.items():
                if 'disturbancerepeat' in key:
                    inDF = df
                    break

            outDFSubset = inDF[["Site", "Sub Site", "Start Time Disturbance", "Disturbance Source",
                                "Disturbance Specific Source", "Number of Disturbance Source", "Response",
                                "Count Before (Total Seals)", "# Remaing Adults", "# Remaining Pups",
                                "# Flushed (Total Seals)", "# Pups Flushed", "# Pups Left Alone", "Did seals return",
                                "Rehaul Time", "Where Rehaul", "Comments", "CreationDate", "ParentGlobalID"
                                ]].rename(
                columns={"Sub Site": "LocationID",
                         "Start Time Disturbance": "DisturbanceTime",
                         "Disturbance Source": "Source",
                         "Disturbance Specific Source": "SpecificSource",
                         "Number of Disturbance Source": "DisturbanceNumber",
                         "Count Before (Total Seals)": "CountBefore",
                         "# Remaing Adults": "RemainOnSite",
                         "# Remaining Pups": "PupsRemain",
                         "# Flushed (Total Seals)": "Flush",
                         "# Pups Flushed": "PupsFlush",
                         "# Pups Left Alone": "PupsAlone",
                         "Did seals return": "Return",
                         "Rehaul Time": "RehaulTime",
                         "Where Rehaul": "WhereRehaul",
                         "CreationDate": "CreatedDate"})

            # Disturbance might be null if null exist
            recNum = outDFSubset.shape[0]
            if recNum == 0:
                logMsg = (f'WARNING INFO No Disturbance Records - existing ETL_PINN_Elephant.py - process_Disturbance'
                          f'without Appending (i.e. No Records to Append')
                logging.info(logMsg, exc_info=True)
                print(logMsg)
                return "Success - No Append"

            ##############################
            # Numerous Field CleanUp and Standardization of field format
            ##############################

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ["Site", "LocationID", "DisturbanceTime", "Source", "SpecificSource", "DisturbanceNumber",
                                      "Response", "CountBefore", "RemainOnSite", "PupsRemain", "Flush", "PupsFlush",
                                      "PupsAlone", "Return", "RehaulTime", "WhereRehaul", "Comments", "CreatedDate",
                                      "ParentGlobalID"],

                            'Type': ["object", "int64", "datetime64", "object", "object", "int64",
                                      "object", "int64", "int64", "int64", "int64", "int64",
                                      "int64", "object", "datetime64", "object", "object", "datetime64", "object"],

                            'DateTimeFormat': ["na", "na", "na", "na", "na", "na",
                                      "na", "na", "na", "na", "na", "na",
                                      "na", "na", "%H:%M", "na", "na", "%m/%d/%Y %I:%M:%S %p", "na"]}

            outDFDist = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic,
                                                                 inDF=outDFSubset)


            # Disturbance events will be for 'E_Seal' events only.
            # Subset to only 'Seal_Resight' events
            outDFEvents_ESEAL = outDFEvents[outDFEvents['ProjectCode'] == 'E_Seal']

            # Merge on the Event Data Frame to get the EventID via the ParentGlobalID - GlobalID fields
            outDFDistwEventID = pd.merge(outDFDist, outDFEvents_ESEAL[['GlobalID', 'EventID']], how='left',
                                             left_on="ParentGlobalID", right_on="GlobalID",
                                             suffixes=("_src", "_lk"))

            ############
            # Create the DisturbanceTable Records
            ############
            outDFDistRec = processDistRec(outDFDistwEventID, etlInstance, dmInstance)

            ############
            # Create the tblDisturbanceBehav Records
            ############
            outDFDistBehave = processDistBehavior(outDFDistwEventID, etlInstance, dmInstance)

            logMsg = f'Success process_Disturbance ETL Routine'
            logging.info(logMsg, exc_info=True)

            return "Success"

        except Exception as e:
            logMsg = f'WARNING ERROR  - ETL_PINN_Elephant.py - process_Disturbance: {e}'
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_MultipleTabletEvents(outDFEvents, outDFElephantEvents, outDFResightEvents, etlInstance, dmInstance):
        """
        ETL routine for Events that where collected on multiple events/tablets.  Using multiple tablets is common
        data collection practice for Elephant seal monitoring.

        After initial processing of all events this workflow  identifies when multiple events/tablets on the same
        day and retains one event and migrated all other associated events to the select event.

        Workflow consolidates all information across all related events (e.g. Min and Max Start Times, all observers,
        etc.,) and updates related monitoring components/tables to the master event.  After successful migration of
        all events to the master event these migrated events (i.e. second event other tablet)
        are deleted in the event table.

        :param outDFEvents - Event Data Frame from the SurveyMetadata, with GlobalID and EventID definition
        :param outDFElephantEvents - Elephant Seal Events
        :param outDFResightEvents - Resight Events
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:String denoting Success or Failed
        """

        try:

            # Identify where multiple events and define the master events when multiple
            outUniqueEventsDF = uniqueEvents(outDFEvents, etlInstance, dmInstance)


            # Consolidate the Events with Multiple/Split Events - will push an Update back to the Events Table
            outFun = consolidateSplitEvents(outUniqueEventsDF, outDFElephantEvents, outDFResightEvents,
                                                       etlInstance, dmInstance)


            # Define the CrossWalk to the Master Event when Multiple/Split Events -
            notMasterEventsFinal = defineXwalkToMaster(outUniqueEventsDF, dmInstance)

            # Process the tblEventObservers Not Master - duplicates must be deleted prior to update to Master EventID
            # in the updateToMasterEventID routine below.
            outFun = removeEventObserersDuplicates(notMasterEventsFinal, etlInstance, dmInstance)

            # Update Downstream Tables with Multiple/Split Events to the Master Event - t
            outFun = updateToMasterEventID(notMasterEventsFinal, etlInstance, dmInstance)

            # Delete the Not Master Events - these have been migrated
            outFun = deleteNotMasterEvents(notMasterEventsFinal, etlInstance, dmInstance)

            logMsg = f'Success process_MultipleTabletEvents routine'
            logging.info(logMsg, exc_info=True)

            return "Success"


        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_PINN_Elephant.py - process_MultipleTabletEvents: {e}'

            logging.critical(logMsg, exc_info=True)

            traceback.print_exc(file=sys.stdout)


    def process_ResightPhotos(outDFResightRec, etlInstance, dmInstance, generalArcGIS):

        """
        Process the Nest Photos in the Nest Repeat. Workflow also pushes photo informatio to the tbl_Nest_Photos table.

        Photos are exported to the directory etlInstanace.photDir location - subsequently push mannual to the
        PORE/Azure server (e.g. \\INPPORE07\Resources\Science\ESeal\Eseal2026\Images\TagResight).

        The path in the tblResightPhotos will default be defined as:
        \\INPPORE07\Resources\Science\ESeal\Eseal{Year}\Images\TagResight

        :param outDFEvents - Imported Dataframe event
        :param outDFResightRec - Resight Photos Dataframe
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance
        :param generalArcGIS: General ArcGIS instance

        :return:String - denoting success for failure.
        """

        try:

            # Resight Records that where appended - use to get the ResightID in tblResights
            outDFSubset = outDFResightRec[['GlobalID']]

            # Import the Resight Table
            inQuery = (f"SELECT tblResightEvents.Season, tblResights.ResightID, tblResights.GlobalID FROM tblResights INNER JOIN"
                       f" tblResightEvents ON tblResights.EventID = tblResightEvents.EventID;")

            # Import Resights
            resightsDF = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # This will get the Resight Records processed with the ResightID
            resightDF2 = pd.merge(
                outDFSubset,
                resightsDF,
                left_on = 'GlobalID',  # column in dfObserversOther
                right_on = 'GlobalID',  # column in outDFContactsLU
                how = 'inner')

            # Convert all nan to None
            resightDF2None = resightDF2.where(pd.notna(resightDF2), None)

            # Subset to only the fields needed
            resightDF2None = resightDF2None[['GlobalID', 'ResightID', 'Season']]

            ##############################################
            # Process Records - Photos import via API REST
            ##############################################

            # Connect to AGOL - via 'oauth'
            if generalArcGIS.credentials.lower() == 'oauth':
                outGIS = agl.connectAGOL_clientID(generalArcGIS=generalArcGIS, dmInstance=dmInstance)
            # Connect via ArcGISPro Environment
            else:
                outGIS = agl.connectAGOL_ArcGIS(generalArcGIS=generalArcGIS, dmInstance=dmInstance)

            # Process the Photos in the Resight Repeat Table
            outPhotosDF =  agl.generalArcGIS.download_attachments_from_flc(outGIS, etlInstance.flID,
                                                                           etlInstance.photoDir, 'resightsrepeats',
                                                                           where="1=1", is_table=True)

            # Temp Delete Post Successfully Processing
            outFullName = f'{etlInstance.outDir}\\outPhotosDF_backup.csv'
            outPhotosDF.to_csv(outFullName, index=True)

            # Create records in the 'tblResightPhotos' directory
            outPhotosDFwAtt = pd.merge(resightDF2None, outPhotosDF[['ParentGlobalID', 'ID', 'PhotoName']],
                                       how='inner', left_on="GlobalID", right_on="ParentGlobalID",
                                       suffixes=("_src", "_lk"))


            # Define the 'Server' Location field - defaulting to the SNPL SFAN Azure location by year
            dynamicDir = f'ESeal{etlInstance.yearLU}'
            serverLoc = fr'\\INPPORE07\Resources\Science\ESeal\{dynamicDir}\Images\TagResight'
            outPhotosDFwAtt['ServerLocation'] = serverLoc

            # Fields to drop
            fieldListDrop = ['ParentGlobalID', 'ID', 'GlobalID']
            outPhotosDFwAtt.drop(fieldListDrop, axis=1, inplace=True)

            # Append the records that had photo attachments
            # Grab all column names from the dataframe
            cols = outPhotosDFwAtt.columns.tolist()

            recCount = outPhotosDFwAtt.shape[0]

            # Append to table
            # Build the SQL query dynamically
            insertQuery = (
               f"INSERT INTO tblResightPhotos ({', '.join(cols)}) "
               f"VALUES ({', '.join(['?'] * len(cols))})")

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outPhotosDFwAtt, "tblResightPhotos", insertQuery,
                                           dmInstance)

            logMsg = f"Success process_ResightPhotos - appended - {recCount} - records to the tblResightPhotos table."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            return outDFSubset

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_PINN_Elephant.py.py - process_ResightPhotos: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)







def processRedFurShark(inDF, etlInstance, dmInstance):
    """
    Process Red Fur and or Shark Bite Records to tblPhocaSealCount table

    :param inDF: Data Frame being processed, with defined EventID - this will be the preprocessed Counts Dataframe
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return: outRedSealDF - dataframe with the records pushed to the tblPhocaSealCount table
    """

    try:

        dfRedFurShark = inDF[['EventID', 'LocationID', 'ObservationTime', 'RedFurPhoca', 'SharkBitePhoca',
                              'CreatedDate']]

        # Drop all records where
        dfRedFurSharkNA = dfRedFurShark.dropna(subset=['RedFurPhoca', 'SharkBitePhoca'], how='all')

        # Set all red/shark value where null to 0
        dfRedFurSharkNA[['RedFurPhoca', 'SharkBitePhoca']] = dfRedFurSharkNA[['RedFurPhoca', 'SharkBitePhoca']].fillna(0)

        # Set Field Type
        dfRedFurSharkNA[['RedFurPhoca', 'SharkBitePhoca']] = (dfRedFurSharkNA[['RedFurPhoca', 'SharkBitePhoca']].
                                                              astype('int64'))
        # Pass final Query to be appended
        insertQuery = (f'INSERT INTO tblPhocaSealCount (EventID, LocationID, ObservationTime, RedFurPhoca, '
                       f'SharkBitePhoca, CreatedDate) VALUES (?, ?, ?, ?, ?, ?)')

        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        dm.generalDMClass.appendDataSet(cnxn, dfRedFurSharkNA, "tblPhocaSealCount", insertQuery, dmInstance)

        logMsg = f'Success processRedFurShark ETL Routine'
        logging.info(logMsg, exc_info=True)
        print(logMsg)
        return dfRedFurSharkNA

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - procesRedFurShark: {e}'
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)


def processElephantContacts(inDF, etlInstance, dmInstance):
    """
    Define Observers in Pinnipeds table tblEventObservers
    Harvest Multi-select field 'Define Observers', if other, also harvest 'Specify Other' field in Survey .csv
    Lookup table for contacts is tlu_Contacts - Contact_ID being pushed to table xref_EventContacts. Need Observers for
    both E_Seal, and Resight_Events.

    :param inDF: Data Frame being processed
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return:
    """

    try:

        inDFContacts = inDF[['GlobalID', "Observers", "ObserversOther", "CreatedDate"]]

        # If single value will be integer - want as string so can work through parse logic
        inDFContacts['Observers'] = inDFContacts['Observers'].astype(str)

        ####################################
        # Parse the 'Observers' field on ','
        ####################################

        # First remove the records where Observers == 389
        inObsNotOther = inDFContacts[inDFContacts['Observers'] != '389']

        # Parse Observer Field
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

        if inObsOther.shape[0] >= 1:

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

        else:  # Not any Other Observers
            dfObserversOther = inDFObserversParsed3

        # Set Observers field to 'Integer'
        dfObserversOther['Observers'] = dfObserversOther['Observers'].astype(int)

        #######################################
        # Read in 'Lookup Table - tlu Contacts'
        inQuery = f"SELECT tluObservers.ObserverID, [FirstName] & '_' & [LastName] AS First_Last FROM tluObservers;"

        outDFContactsLU = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

        # Join Obersvers with tluObservers lookup table
        dfObserversOtherwLK = pd.merge(
            dfObserversOther,
            outDFContactsLU,
            left_on='Observers',  # column in dfObserversOther
            right_on='ObserverID',  # column in outDFContactsLU
            how='left'  # or 'inner', 'right', 'outer' depending on needs
        )

        # Check for Lookups not defined via an outer join.
        # If is null then these are undefined contacts
        dfObserversNull = dfObserversOtherwLK[
            dfObserversOtherwLK['ObserverID'].isna()]

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

def tblSubSitesNotSurveyed(inDF, inDFEvents, etlInstance, dmInstance):
    """
    Workflow to define the Sub Sites Not Defined table - tblSubSitesNotSurveyed
    Lookup the EventID, Explode the Multi Select SubSiteNotSurveyed field then look up the LocationID via the
    SubSiteCode in the tblLocations Table and append records to the tblSubSitesNotSurveyed table
    Undefined/Not Present subsites (i.e. 9999) are going to unknown subsite definition.

    NOTE - The field 'Sub Site Not Survey' in the Main Event Form (i.e. SFAN_ElephantSeal_2025v1_3_0.csv) is not being
    used. The field 'Sub Sites Not Surveyed' is the correct field to harvest.

    :param inDF: Data Frame being processed with Sub Sites Not Surveys, and Regional ID field
    :param inDFEvents: Data Frame with the EventID and GlabalID attributes used to define the EventID
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return:
    """

    try:
        # Subset to only need fields
        subSiteDF = inDF[['GlobalID', 'SubSitesNotSurveyed']]

        # Lookup the EventID value via RegionalID
        subSiteDFwEventID = pd.merge(
            subSiteDF,
            inDFEvents[['EventID', 'GlobalID', 'ProjectCode']],
            left_on=['GlobalID'],
            right_on=['GlobalID'],
            how='left')

        ###################################
        # Explode the 'SubSitesNotSurveyed'
        subSiteDFwEventIDParsed = (subSiteDFwEventID.assign(SubSitesNotSurveyed=subSiteDFwEventID['SubSitesNotSurveyed'].str.split(',')).
                            explode('SubSitesNotSurveyed'))

        # Trim white space in observers field
        subSiteDFwEventIDParsed['SubSitesNotSurveyed'] = subSiteDFwEventIDParsed['SubSitesNotSurveyed'].str.lstrip()

        ##################################
        # Lookup the LocationID via the SubSitesNotSurveyed

        # Read in the Locations lookup table
        inQuery = (f"SELECT tblLocations.LocationID, tblLocations.SubSiteCode, tblLocations.ESealLocation FROM "
                   f"tblLocations WHERE tblLocations.ESealLocation=True;")

        # Import Locations Table
        outDFLocations = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

        # Lookup the LocationID value via SubSiteCode
        subSiteDFwEventID = pd.merge(
            subSiteDFwEventIDParsed,
            outDFLocations[['LocationID', 'SubSiteCode']],
            left_on='SubSitesNotSurveyed',
            right_on='SubSiteCode',
            how='left')

        # Retain the Sub Sites Not Surveyed where LocationID is not Null
        subSiteDFwEventIDNotNull = subSiteDFwEventID.dropna(subset=['LocationID'])

        ###################################
        # Append the Sub Sites Not Surveyed Records

        subSiteDFAppend = subSiteDFwEventIDNotNull[['EventID', 'LocationID']]

        insertQuery = f'INSERT INTO tblSubSitesNotSurveyed (EventID, LocationID) VALUES (?, ?)'

        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        # Append the Contacts to the tblEventObserers table
        dm.generalDMClass.appendDataSet(cnxn, subSiteDFAppend, "tblSubSitesNotSurveyed", insertQuery,
                                        dmInstance)

        logMsg = f"Success ETL_PINN_ELephant.py - tblSubSitesNotSurveyed."
        logging.info(logMsg)
        print(logMsg)

        return subSiteDFAppend

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - tblSubSitesNotSurveyed: {e}'
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)



def processResightEvents(inDF, etlInstance, dmInstance):
    """
    Define the Resight Events Table (i.e. tblResightEvents) and Append.  Using the already appended Event/Survey
    dataframe to define.

    :param inDF: Data Frame being processed
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return:dfResightEvents Dataframe with records appended
    """

    try:

        # Read in the Event Table to get the CreatedDate, and Comments Fields
        inQuery = (f"SELECT tblEvents.EventID, tblEvents.CreatedDate"
                   f" FROM tblEvents;")

        # Import Events
        outDFEvents_misc = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

        # Merge on the Event Data Frame to get the CreatedDate
        inDFwVisSeason = pd.merge(inDF, outDFEvents_misc[['EventID', 'CreatedDate']]
                                  , how='left', left_on="EventID", right_on="EventID", suffixes=("_src", "_lk"))

        # Subset to field to append
        inDFwVisSeasonAppend = inDFwVisSeason[['EventID', 'Visibility', 'Season', 'ParkCode', 'CreatedDate',
                                               'Comments']]

        # Update any 'nan' string or np.nan values to None to consistently handle null values.
        inDFwVisSeasonAppend2 = inDFwVisSeasonAppend.replace([np.nan, 'nan'], None)

        # Append to the 'tblResightEvents' table
        insertQuery = (f'INSERT INTO tblResightEvents (EventID, Visibility, Season, ParkCode, CreatedDate, Comments)'
                       f' VALUES (?, ?, ?, ?, ?, ?)')

        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        dm.generalDMClass.appendDataSet(cnxn, inDFwVisSeasonAppend2, "tblResightEvents", insertQuery, dmInstance)

        logMsg = f"Successfully completed ETL_PINN_ELephant.py - processResightEvents."
        logging.info(logMsg)

        return inDFwVisSeasonAppend

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - processResightEvents: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)

def processResightRecords(inDF, etlInstance, dmInstance):
    """
    Define the Resight Record Table (i.e. tblResights) and Append

    :param inDF: Data Frame being processed
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return outDFResightRec: Data Frame with the append records

    Updates:

    4/1/2026 - Removed PhotoNames Left and Right from Processing, added GlobalID, photo names will be pushed to the new
    tblResightPhotos table.
    """

    try:

        # Subset to field to append
        inDFResightRec = inDF[["EventID", "LocationID", "MatureCode", "ConditionCode", "Sex", "Dye", "DyeCode",
                               "LtagColor", "LtagNo", "LtagPosn",
                               "LtagCode", "RtagColor", "RtagNo", "RtagPosn", "RtagCode",
                               "ReproductiveStatusCode", "PupSize", "Comments", "CreatedDate", "GlobalID"]]

        # Update any 'nan' string or np.nan values to None to consistently handle null values.
        inDFResightRec2 = inDFResightRec.replace([np.nan, 'nan'], None)

        # In LtagNo and RtagNo fields change any lower case text to upper case
        for field in ['LtagNo', 'RtagNo']:
            inDFResightRec2[field] = inDFResightRec2[field].apply(
                lambda x: x.upper() if isinstance(x, str) else x
            )

        # Trim all DyeCode String Decimal points to integer (e.g. 2.0 to 2, etc.) so lookup has match in tluDyeCode.
        # append query
        inDFResightRec2['DyeCode'] = (
            inDFResightRec2['DyeCode']
            .where(inDFResightRec2['DyeCode'].isna(), inDFResightRec2['DyeCode'].astype(str).str.split('.').str[0]))

        # Append to the 'tblResights' table
        insertQuery = (
            f'INSERT INTO tblResights (EventID, LocationID, MatureCode, ConditionCode, Sex, Dye, DyeCode, '
            f'LtagColor,'
            f' LtagNo, LtagPosn, LtagCode, RtagColor, RtagNo, RtagPosn, RtagCode, ReproductiveStatusCode, '
            f' PupSize, Comments, CreatedDate, GlobalID)'
            f' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')



        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        dm.generalDMClass.appendDataSet(cnxn, inDFResightRec2, "tblResights", insertQuery, dmInstance)

        logMsg = f"Successfully completed ETL_PINN_ELephant.py - processResightRecords."
        logging.info(logMsg)

        return inDFResightRec

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - processResightRecords: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)

def processDistRec(inDF, etlInstance, dmInstance):
    """
    Define the Parent Disturbance Records and append to tblDisturbances

    :param inDF: Data Frame being processed
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return outDFDistRec: Data Frame with the appended records
    """

    try:

        # Subset to field to append
        inDFDistRec= inDF[["EventID", "DisturbanceTime", "Source", "SpecificSource", "DisturbanceNumber",
                              "CreatedDate", "GlobalID"]]

        # Update any 'nan' string or np.nan values to None to consistently handle null values.
        inDFDistRec2 = inDFDistRec.replace([np.nan, 'nan'], None)

        # Append to the 'tblResights' table
        insertQuery = (f'INSERT INTO tblDisturbances (EventID, DisturbanceTime, Source, SpecificSource, '
                       f'DisturbanceNumber, CreatedDate, GlobalID)'
                       f' VALUES (?, ?, ?, ?, ?, ?, ?)')

        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        dm.generalDMClass.appendDataSet(cnxn, inDFDistRec2, "tblDisturbances", insertQuery, dmInstance)

        logMsg = f"Successfully completed ETL_PINN_ELephant.py - processDistRec."
        logging.info(logMsg)

        return inDFDistRec2

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - processDistRec: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)

def processDistBehavior(inDF, etlInstance, dmInstance):
    """
    Define the Disturbance Beharior Records and append to tblDisturbanceBehav

    :param inDF: Data Frame being processed
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return outDFDistRec: Data Frame with the appended records
    """

    try:

        # Subset
        inDFDistBehave = inDF[["LocationID", "Response", "CountBefore", "RemainOnSite", "PupsRemain", "Flush", "PupsFlush",
                               "PupsAlone", "Return", "RehaulTime", "WhereRehaul", "Comments", "CreatedDate",
                               "GlobalID"]]

        # Connect to the Database import the tblDisturbance - need to get the DisturbanceID foreign key for
        # records just pushed in 'processDistRec' function. Joining on "DisturbanceTime", "Source", "SpecificSource",
        #                                                   "DisturbanceNumber"
        inQuery = (f"SELECT tblDisturbances.DisturbanceID, tblDisturbances.EventID, tblDisturbances.DisturbanceTime,"
                   f"tblDisturbances.Source, tblDisturbances.SpecificSource, tblDisturbances.DisturbanceNumber, "
                   f"tblDisturbances.GlobalID FROM tblDisturbances;")

        outDFDisturbanceAll = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

        outDFDisturbanceAll2 = pd.merge(inDFDistBehave, outDFDisturbanceAll[["GlobalID", "DisturbanceID"]], how='left',
                                         left_on=["GlobalID"],
                                         right_on=["GlobalID"], suffixes=("_src", "_lk"))

        ######
        # Drop fields
        outDFDisturbanceAll2 = outDFDisturbanceAll2.drop(columns=["GlobalID"])

        # Update any 'nan' string or np.nan values to None to consistently handle null values.
        outDFDisturbanceAll2 = outDFDisturbanceAll2.replace([np.nan, 'nan'], None)

        # Append to the 'tblDisturbances' table
        insertQuery = (f'INSERT INTO tblDisturbanceBehav (LocationID, Response, CountBefore, RemainOnSite, PupsRemain,'
                       f' Flush, PupsFlush, PupsAlone, Return, '
                       f'RehaulTime, WhereRehaul, Comments, CreatedDate, DisturbanceID)'
                       f' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')

        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        dm.generalDMClass.appendDataSet(cnxn, outDFDisturbanceAll2, "tblDisturbanceBehav", insertQuery,
                                        dmInstance)

        logMsg = f"Successfully completed ETL_PINN_ELephant.py - processDistBeahvior"
        logging.info(logMsg)

        return outDFDisturbanceAll2

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - processDistRec: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)

def uniqueEvents(outDFEvents, etlInstance, dmInstance):

    """
    Identify where multiple events and define the master events when multiple.
    Multiple events will be determined using the 'ProjectCode' (i.e. E_Seal or 'Seal_Resight') and SurveyDate fields

    :param outDFEvents: Events Dataframe being processed
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return outUniqueEventsDF: DataFrame Identifying the Events with Multiple and which Event will be the Master Event.
    """

    try:

        outUniqueEventsDF = outDFEvents.copy()

        # Get Count of Event Records by the 'ProjectCode', 'SurveyDate' fields
        outUniqueEventsDF['RecordCount'] = (
            outUniqueEventsDF.groupby(['ProjectCode', 'StartDate'])['ProjectCode']
            .transform('size'))

        # Identify the Master Event - these will be retained.
        outUniqueEventsDF['MasterEvent'] = (
            outUniqueEventsDF.groupby(['ProjectCode', 'StartDate'])
            .cumcount()
            .eq(0)
            .map({True: 'Yes', False: 'No'})
        )

        #Sort By Start and ProjectCode
        outUniqueEventsDF = outUniqueEventsDF.sort_values(by=['StartDate', 'ProjectCode'])


        logMsg = f"Successfully completed ETL_PINN_ELephant.py - uniqueEvents"
        logging.info(logMsg)

        return outUniqueEventsDF

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - uniqueEvents: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)


def consolidateSplitEvents(outUniqueEventsDF, outDFElephantEvents, outDFResightEvents, etlInstance, dmInstance):

    """
    Parent Script for workflow to consolidate/merage the Event tables when multiple tablet data collection.
    Processing tblEvents, tblElephantEvents, and tblResightEvents

    Find Min and Max Start Times
    Collection Device - Compiled


    :param outUniqueEventsDF: Events Dataframe with the Multiple/Split Events
    :param outDFElephantEvents: Elephant Seals Event dataframe
    :param outDFResightEvents: Resight Event dataframe
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return String - denoting success or failure
    """

    try:

        # Subset to only the Records that are split events.
        eventsWithMultiple = outUniqueEventsDF[outUniqueEventsDF['RecordCount'] > 1]

        # Consolidate tblEvents
        outFun = consolidateTblEvents(eventsWithMultiple, etlInstance, dmInstance)

        # Consolidate tblElephantEvents
        outFun = consolidateTblElephantEvents(eventsWithMultiple, outDFElephantEvents, etlInstance, dmInstance)

        # Consolidate tblResightEvents
        outFun = consolidateTblResightEvents(eventsWithMultiple, outDFResightEvents, etlInstance, dmInstance)

        logMsg = f"Successfully completed ETL_PINN_ELephant.py - consolidateSplitEvents"
        logging.info(logMsg)

        return "Success"

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - consolidateSplitEvents: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)
        return "Fail"


def deleteNotMasterEvents(notMasterEventsFinal, etlInstance, dmInstance):
    """
    Delete the not Master Multiple/Split Events for the Event Tables. This is done post completion of the Migration to
    master routines in the 'updateToMasterEventID' function.

    :param notMasterEventsFinal - Dataframe with the Not Master Events and the corresponding Master Event ID
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return string: - Denoting Success or Failed
    should be de
    """

    try:

        # Temporary Table Created for Updated Query Processing
        tempTable = 'tmpTable_ETL'

        # Create the temp table
        dm.generalDMClass.createTableFromDF(notMasterEventsFinal, tempTable, etlInstance.inDBBE)

        # Event tables to be processed
        tableList = ['tblEvents', 'tblElephantEvents', 'tblResightEvents']

        for table in tableList:

            # Define the Delete Query
            update_sql = (f'DELETE {table}.* FROM {table} INNER JOIN tmpTable_ETL ON '
                          f'{table}.EventID = tmpTable_ETL.EventID;')

            # Apply the Delete Query to the Access DB using the passed temp table
            dm.generalDMClass.excuteQuery(update_sql, etlInstance.inDBBE)

            logMsg = f'Successfully Deleted Not Master Events from - {table}'
            print(logMsg)
            logging.info(logMsg)

        logMsg = f"Successfully completed ETL_PINN_ELephant.py - deleteNotMasterEvents"
        logging.info(logMsg)

        return "Success"

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - deleteNotMasterEvents: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)
        return "Failed"


def removeEventObserersDuplicates(outUniqueEventsDF, etlInstance, dmInstance):

    """
    For Events with Multiple/Split Events identify the duplicate observers and delete these to avoid duplicate index
    key errors when the Multiple/Split Events are updated to the Master Event.

    :param notMasterEventsFinal - Dataframe with the Not Master Events and the corresponding Master Event ID
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return notMasterEventsFinal - Dataframe with the Not Master Events and the corresponding Master Event ID that
    should be defined.
    """

    try:

        # Import the tblEventObservers
        # Import Event Table to define the EventID via the GlobalID
        inQuery = f"SELECT tblEventObservers.* FROM tblEventObservers;"

        # Import tblEventObservers
        eventObserversDF = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)


        # Get Observers by EventID in the Not Master Events
        observersNotMasterDF = pd.merge(
            outUniqueEventsDF,
            eventObserversDF,
            left_on='EventID',
            right_on='EventID',
            how='inner')

        # Get Observers by EventID in the Master Events
        observersMasterDF = pd.merge(
            outUniqueEventsDF,
            eventObserversDF,
            left_on='MasterEventID',
            right_on='EventID',
            how='inner')

        observersMasterDF = observersMasterDF.drop(columns=['EventID_y'])
        observersMasterDF = observersMasterDF.rename(columns={'EventID_x': 'EventID'})

        # Join to identify the duplicate Observers - these will be deleted
        duplicatesByEventIDNotMasterDF = pd.merge(
            observersNotMasterDF,
            observersMasterDF,
            left_on=['EventID', 'ObserverID'],
            right_on=['EventID', 'ObserverID'],
            how='inner')


        # Delete the duplicate observers
        # Temporary Table Created for Updated Query Processing
        tempTable = 'tmpTable_ETL'

        # Create the temp table
        dm.generalDMClass.createTableFromDF(duplicatesByEventIDNotMasterDF, tempTable, etlInstance.inDBBE)

        # Define the Delete Query
        update_sql = (f'DELETE tblEventObservers.* FROM tblEventObservers INNER JOIN tmpTable_ETL ON'
                      f' (tblEventObservers.ObserverID = tmpTable_ETL.ObserverID) AND'
                      f' (tblEventObservers.EventID = tmpTable_ETL.EventID);')

        # Apply the Delete Query to the Access DB using the passed temp table
        dm.generalDMClass.excuteQuery(update_sql, etlInstance.inDBBE)

        recCount = duplicatesByEventIDNotMasterDF.shape[0]

        logMsg = (f"Deleted - {recCount} - Duplicate Observer Records (post Split Event Harmonization) from"
                  f" 'tblEventObservers'")
        print(logMsg)
        logging.info(logMsg)

        logMsg = f"Successfully completed ETL_PINN_ELephant.py - removeEventObserersDuplicates"
        logging.info(logMsg)

        return "Success"

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - removeEventObserersDuplicates: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)
        return "Failed"

def defineXwalkToMaster(outUniqueEventsDF, dmInstance):

    """
    Create the Crosswalk to the Master Event EventID for the Multi/Split Event records.


    :param outUniqueEventsDF: Events Dataframe with the Multiple/Split Events
    :param dmInstance: Data Management instance

    :return notMasterEventsFinal - Dataframe with the Not Master Events and the corresponding Master Event ID that
    should be defined.
    """

    try:

        # For table Xwalk to the Master EventID
        # Step 1: Create a helper column with master EventID only where MasterEvent == "Yes"
        outUniqueEventsDF['MasterEventID'] = np.where(outUniqueEventsDF['MasterEvent'] == 'Yes',
                                                      outUniqueEventsDF['EventID'], np.nan)

        # Step 2: Forward/backward fill within each group to propagate the master ID
        outUniqueEventsDF['MasterEventID'] = (
            outUniqueEventsDF.groupby(['ProjectCode', 'StartDate'])['MasterEventID']
            .transform(lambda x: x.ffill().bfill())
        )

        # Only Process MasterEvent not - 'No'
        notMasterEventsDF = outUniqueEventsDF[outUniqueEventsDF['MasterEvent'] == 'No']

        # Subset to the 'EventID' and 'MasterEventID' fields
        cols_needed = ['EventID', 'MasterEventID']
        notMasterEventsFinal = notMasterEventsDF[cols_needed]


        logMsg = f"Successfully completed ETL_PINN_ELephant.py - defineXwalkToMaster"
        logging.info(logMsg)

        return notMasterEventsFinal

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - defineXwalkToMaster: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)


def updateToMasterEventID(notMasterEventsFinal, etlInstance, dmInstance):

    """
    Update the downstream tables EventID to the Master Event EventID for the Multi/Split Event records.


    :param notMasterEventsFinal - Dataframe with the Not Master Events and the corresponding Master Event ID
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return string: Denote Success for Failed
    """

    try:

        tableList = ['tblEventObservers', 'tblSealCount', 'tblPhocaSealCount', 'tblResights', 'tblDisturbances',
                    'tblSubSitesNotSurveyed']

        # Temporary Table Created for Updated Query Processing
        tempTable = 'tmpTable_ETL'

        # Create the temp table
        dm.generalDMClass.createTableFromDF(notMasterEventsFinal, tempTable, etlInstance.inDBBE)

        # Get Count of records being processed
        recCount = notMasterEventsFinal.shape[0]

        # Process the tables in need of update
        for table in tableList:

            # Create the update SQL Statement
            update_sql = dm.generalDMClass.build_access_update_sqlEventID(df=notMasterEventsFinal,
                                                                   target_table=table,
                                                                   source_table=tempTable,
                                                                   join_field="EventID")

            # Apply the Update Query to the Access DB using the passed temp table
            dm.generalDMClass.excuteQuery(update_sql, etlInstance.inDBBE)

            logMsg = f'Successfully Updated EventID to the MasterEventID in - {table}.'
            logging.info(logMsg)

        logMsg = f"Successfully completed ETL_PINN_ELephant.py - updateToMasterEventID"
        logging.info(logMsg)

        return "Success"

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - updateToMasterEventID: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)
        return "Failed"


def consolidateTblEvents(outUniqueEventsDF, etlInstance, dmInstance):

    """
    Consolidate tblEvents - Min StartTime, Max EndTime, GlobalID - Concatenated.
    Updates are pushed back to the tblEvents table

    :param outUniqueEventsDF: Events Dataframe being processed
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return String - denoting success or failure
    """

    try:

        # Convert to Date Time
        outUniqueEventsDF['StartTime'] = pd.to_datetime(outUniqueEventsDF['StartTime'], format='%H:%M:%S')
        outUniqueEventsDF['EndTime'] = pd.to_datetime(outUniqueEventsDF['EndTime'], format='%H:%M:%S')

        # Unique Fields
        grp_keys = ['ProjectCode', 'StartDate']

        # 1) Aggregated values per (ProjectCode, StartDate) ---
        agg = (outUniqueEventsDF.groupby(grp_keys)
               .agg(StartTime=('StartTime', 'min'),
                    EndTime=('EndTime', 'max'),
                    GlobalID=('GlobalID', dm.generalDMClass.concat_comments))
               .reset_index())

        #2) Keep exactly one master row per group ---
        masters = (outUniqueEventsDF[outUniqueEventsDF['MasterEvent'].str.upper().eq('YES')]
                   .sort_values(grp_keys + ['StartTime'])  # tie-breaker if needed
                   .drop_duplicates(grp_keys, keep='first'))

        #3) Merge aggregated fields onto masters ---
        eventsDFToUpdate = (masters
                     .drop(columns=['StartTime', 'EndTime', 'GlobalID'])  # will replace with agg values
                     .merge(agg, on=grp_keys, how='left')
                     .sort_values(grp_keys))

        #4) Convert the StartDate to ISO and StartTime and EndTime to ISO string objects
        eventsDFToUpdate['StartDate'] = pd.to_datetime(eventsDFToUpdate['StartDate']).dt.strftime('%Y-%m-%d')

        eventsDFToUpdate['StartTime'] = pd.to_datetime(eventsDFToUpdate['StartTime']).dt.strftime('%H:%M:%S')
        eventsDFToUpdate['EndTime'] = pd.to_datetime(eventsDFToUpdate['EndTime']).dt.strftime('%H:%M:%S')

        # Perform the Update Query
        # Temporary Table Created for Updated Query Processing
        tempTable = 'tmpTable_ETL'

        #Subset to Only the fields needing update:
        cols_needed = ['EventID', 'StartTime', 'EndTime', 'GlobalID']
        eventsDFToUpdateFinal = eventsDFToUpdate[cols_needed]


        # Create the update SQL Statement
        update_sql = dm.generalDMClass.build_access_update_sql(df=eventsDFToUpdateFinal, target_table="tblEvents",
                                                               source_table=tempTable,
                                                               join_field="EventID")

        # Create the temp table
        dm.generalDMClass.createTableFromDF(eventsDFToUpdateFinal, tempTable, etlInstance.inDBBE)

        # Apply the Update Query to the Access DB using the passed temp table
        dm.generalDMClass.excuteQuery(update_sql, etlInstance.inDBBE)

        # Get Count of records being processed
        recCount = eventsDFToUpdateFinal.shape[0]

        logMsg = f'Successfully Updated Multi-Table Events for - {recCount} - records in tblEvents'
        logging.info(logMsg)
        print(logMsg)
        logMsg = f"Successfully completed ETL_PINN_ELephant.py - consolidateTblEvents"
        logging.info(logMsg)

        return "Success"

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - consolidateTblEvents: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)

def consolidateTblElephantEvents(outUniqueEventsDF, outDFElephantEvents, etlInstance, dmInstance):

    """
    Consolidate tblElephantEvents -
    Updates are pushed back to the tblElephantEvents table

    Logic applied to multiple records:
    Comments=('Comments', dm.generalDMClass.concat_comments) - Concatenates
    DeviceListCompiled=('CollectionDeviceID', dm.generalDMClass.concat_comments - Concatenates
    Visibility=('Visibility', dm.generalDMClass.first_not_null - First Not Null
    SurveyType=('SurveyType', dm.generalDMClass.first_not_null - First Not Null
    RegionalSurvey=('RegionalSurvey', 'any') - Takes first Not Null RegionalSurvey if present
    RegionalCountCode=('RegionalCountCode', dm.generalDMClass.first_not_null - First Not Null

    :param outUniqueEventsDF: Events Dataframe being processed
    :param outDFElephantEvents: Output Elephant Events Dataframe
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return String - denoting success or failure
    """

    try:

        #Only Need the CollectiveDeviceID field
        outDFElephantEvents_subset = outDFElephantEvents[['EventID', 'CollectionDeviceID', 'SurveyType',
                                                          'RegionalSurvey', 'RegionalCountCode']]
        #Inner Join On the Elephant Seal Events, only these need updating
        elephantEventsMerge = pd.merge(
            outUniqueEventsDF,
            outDFElephantEvents_subset,
            left_on= 'EventID',
            right_on='EventID',
            how='inner')

        # Unique Fields
        grp_keys = ['ProjectCode', 'StartDate']

        # 1) Aggregated values per Unique Key
        agg = (elephantEventsMerge.groupby(grp_keys)
              .agg(
                Comments=('Comments', dm.generalDMClass.concat_comments),
                DeviceListCompiled=('CollectionDeviceID', dm.generalDMClass.concat_comments),
                Visibility=('Visibility', dm.generalDMClass.first_not_null),
                SurveyType=('SurveyType', dm.generalDMClass.first_not_null),
                RegionalSurvey=('RegionalSurvey', 'any'),
                RegionalCountCode=('RegionalCountCode', dm.generalDMClass.first_not_null)
                )
               .reset_index()
               )

        # 2) Keep exactly one master row per group ---
        masters = (elephantEventsMerge[elephantEventsMerge['MasterEvent'].str.upper().eq('YES')]
                   .sort_values(grp_keys + ['StartTime'])  # tie-breaker if needed
                   .drop_duplicates(grp_keys, keep='first'))

        # 3) Merge aggregated fields onto masters ---
        elephantEventsMerge2 = (masters
                            .drop(columns=['Comments', 'Visibility', 'SurveyType', 'RegionalSurvey', 'RegionalCountCode'])  # will replace with agg values
                            .merge(agg, on=grp_keys, how='left')
                            .sort_values(grp_keys))

        #4) Add the DeviceListCompiled to the Comments list field
        elephantEventsMerge2['Comments'] = (elephantEventsMerge2[
            ['Comments', 'DeviceListCompiled']].
                                        apply(lambda x: '|Devices: '.join([str(v) for v in x if pd.notna(v)
                                                                  and str(v) != '']), axis=1))

        #  Drop the 'DeviceListCompiledField
        elephantEventsMerge2 = elephantEventsMerge2.drop(columns=['DeviceListCompiled'])

        # Perform the Update Query
        # Temporary Table Created for Updated Query Processing
        tempTable = 'tmpTable_ETL'

        # Subset to Only the fields needing update:
        cols_needed = ['EventID', 'Comments', 'Visibility', 'SurveyType', 'RegionalSurvey', 'RegionalCountCode']
        eventsDFToUpdateFinal = elephantEventsMerge2[cols_needed]


        # Set Nan to None
        eventsDFToUpdateFinal = eventsDFToUpdateFinal.replace({np.nan: None})

        # Create the update SQL Statement
        update_sql = dm.generalDMClass.build_access_update_sql(df=eventsDFToUpdateFinal, target_table="tblElephantEvents",
                                                               source_table=tempTable,
                                                               join_field="EventID")

        # Create the temp table
        dm.generalDMClass.createTableFromDF(eventsDFToUpdateFinal, tempTable, etlInstance.inDBBE)

        # Apply the Update Query to the Access DB using the passed temp table
        dm.generalDMClass.excuteQuery(update_sql, etlInstance.inDBBE)

        # Get Count of records being processed
        recCount = eventsDFToUpdateFinal.shape[0]

        logMsg = f'Successfully Updated Multi-Table Events for - {recCount} - records in tblElephantEvents'
        logging.info(logMsg)

        logMsg = f"Successfully completed ETL_PINN_ELephant.py - consolidateElephantEvents"
        logging.info(logMsg)

        return "Success"

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - consolidateElephantEvents: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)


def consolidateTblResightEvents(outUniqueEventsDF, outDFResightEvents, etlInstance, dmInstance):

    """
    Consolidate tblResightEvents -
    Updates are pushed back to the tblElephantEvents table

    Logic applied to multiple records:
    Comments=('Comments', dm.generalDMClass.concat_comments) - Concatenates
    Visibility=('Visibility', dm.generalDMClass.first_not_null - First Not Null
    Park Code - First Not Null
    Season - First Not Null

    :param outUniqueEventsDF: Events Dataframe being processed
    :param outDFResightEvents: Output Resight Events Dataframe
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return String - denoting success or failure
    """

    try:

        #Only Need the EventID field the Comments, Visibility, Season, and ParkCode values are also in the Event DF
        outDFResightEvents_subset = outDFResightEvents[['EventID']]
        #Inner Join on the events needing update
        resightEventsMerge = pd.merge(
            outUniqueEventsDF,
            outDFResightEvents_subset,
            left_on= 'EventID',
            right_on='EventID',
            how='inner')

        # Unique Fields
        grp_keys = ['ProjectCode', 'StartDate']

        # 1) Aggregated values per Unique Key
        agg = (resightEventsMerge.groupby(grp_keys)
              .agg(
                Comments=('Comments', dm.generalDMClass.concat_comments),
                Visibility=('Visibility', dm.generalDMClass.first_not_null),
                Season=('Season', dm.generalDMClass.first_not_null),
                ParkCode=('ParkCode', dm.generalDMClass.first_not_null),
                )
               .reset_index()
               )

        # 2) Keep exactly one master row per group ---
        masters = (resightEventsMerge[resightEventsMerge['MasterEvent'].str.upper().eq('YES')]
                   .sort_values(grp_keys + ['StartTime'])  # tie-breaker if needed
                   .drop_duplicates(grp_keys, keep='first'))

        # 3) Merge aggregated fields onto masters ---
        resightEventsMerge2 = (masters
                            .drop(columns=['Comments', 'Visibility', 'Season', 'ParkCode'])  # will replace with agg values
                            .merge(agg, on=grp_keys, how='left')
                            .sort_values(grp_keys))

        # Perform the Update Query
        # Temporary Table Created for Updated Query Processing
        tempTable = 'tmpTable_ETL'

        # Subset to Only the fields needing update:
        cols_needed = ['EventID', 'Comments', 'Visibility', 'Season', 'ParkCode']
        eventsDFToUpdateFinal = resightEventsMerge2[cols_needed]

        # # Set Nan to None
        # eventsDFToUpdateFinal = eventsDFToUpdateFinal.replace({np.nan: None})

        # Comments to None including white space ''
        eventsDFToUpdateFinal['Comments'] = (
            eventsDFToUpdateFinal['Comments']
            .astype(object)
            .replace(r'^\s*$', None, regex=True)  # empty or whitespace → None
        )


        # Create the update SQL Statement
        update_sql = dm.generalDMClass.build_access_update_sql(df=eventsDFToUpdateFinal, target_table="tblResightEvents",
                                                               source_table=tempTable,
                                                               join_field="EventID")

        # Create the temp table
        dm.generalDMClass.createTableFromDF(eventsDFToUpdateFinal, tempTable, etlInstance.inDBBE)

        # Apply the Update Query to the Access DB using the passed temp table
        dm.generalDMClass.excuteQuery(update_sql, etlInstance.inDBBE)

        # Get Count of records being processed
        recCount = eventsDFToUpdateFinal.shape[0]

        logMsg = f'Successfully Updated Multi-Table Events for - {recCount} - records in tblResightEvents'
        logging.info(logMsg)

        logMsg = f"Successfully completed ETL_PINN_ELephant.py - consolidateTblResightEvents"
        logging.info(logMsg)

        return "Success"

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - consolidateTblResightEvents: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)

def subsetToSeason(outDFDic, etlInstance, dmInstance):
    """
    Subset the passed AGOL dataframe dictionaries to the defined elephanSeason.

    :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return outFCDicSub: Dictionary with all imported dataframes from the imported feature layer subset to the defined
        elephantSeason.
    """

    try:

        eSeasonLU = etlInstance.elephantSeason

        # Filter to the Season
        if "breeding" in eSeasonLU.lower():
            filtered = "Yes"

            # Read in the Survey Metadata Dataframe:
            inDFSurvey = None
            for key, df in outDFDic.items():
                if 'ElephantSeal' in key:
                    inDFSurvey = df
                    break
            # Apply the subset
            inDFSurveySub = inDFSurvey[inDFSurvey['Season'].str.contains('breeding', case=False, na=False)]

        elif "molt" in eSeasonLU.lower():
            filtered = "Yes"

            # Read in the Survey Metadata Dataframe:
            inDFSurvey = None
            for key, df in outDFDic.items():
                if 'ElephantSeal' in key:
                    inDFSurvey = df
                    break
            # Apply the subset
            inDFSurveySub = inDFSurvey[~inDFSurvey['Season'].str.contains('breeding', case=False, na=False)]

        else: #No Filter Applied
            filtered = "No"

            logMsg = f'No Filter/Subset applied to Feature Layer - elephantSeason - defined as: {eSeasonLU}.'
            logging.info(logMsg)
            print(logMsg)

        if filtered == "No":  #Return copy of the original dataframe
            outDFDicSub = {k: v.copy() for k, v in outDFDic.items()}

        else:

            # Read in Counts Repeat
            inDFCounts = None
            for key, df in outDFDic.items():
                if 'countsrepeats' in key:
                    inDFCounts = df
                    break
            # Resights
            inDFResights = None
            for key, df in outDFDic.items():
                if 'resightsrepeats' in key:
                    inDFResights = df
                    break

            # Disturbance
            inDFDisturbance = None
            for key, df in outDFDic.items():
                if 'disturbancerepeat' in key:
                    inDFDisturbance = df
                    break

            # Subset Counts, Resights, and Disturbance to the 'inDFSurveySub' dataframe subset
            inDFCountsSub = subset_by_survey(inDFCounts, inDFSurveySub, join_field='ParentGlobalID')
            inDFResightsSub = subset_by_survey(inDFResights, inDFSurveySub, join_field='ParentGlobalID')
            inDFDisturbanceSub = subset_by_survey(inDFDisturbance, inDFSurveySub, join_field='ParentGlobalID')

            # Combine all four dataframes into a dictionary (ie. outFCDicSub) one key per dataframe
            outFCDicSub = {
                'ElephantSealSub': inDFSurveySub,
                'countsrepeatsSub': inDFCountsSub,
                'resightsrepeatsSub': inDFResightsSub,
                'disturbancerepeatSub': inDFDisturbanceSub
            }

        logMsg = f"Successfully completed ETL_PINN_ELephant.py - subsetToSeason"
        logging.info(logMsg)

        return outFCDicSub

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - subsetToSeason: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)
        return "Failed"

def subset_by_survey(df_to_filter, survey_df, join_field='ParentGlobalID'):
    """
    Keeps only rows in df_to_filter where join_field matches 'GlobalID' in survey_df
    """
    filterDF = pd.merge(
        df_to_filter,
        survey_df[['GlobalID']],  # Only need GlobalID from survey
        left_on=join_field,
        right_on='GlobalID',
        how='inner'
    )  # Drop duplicate join field

    # Drop GlobalID_y and renamie GlobalID_x
    dfFinal = filterDF.drop(columns=['GlobalID_y']).rename(columns={'GlobalID_x': 'GlobalID'})

    return dfFinal