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
            # Process Counts Form - tblSealCount
            ######
            outDFCounts = etl_PINNElephant.process_Counts(outDFDic, outDFEvents, etlInstance, dmInstance)





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
                         'Sub Sites Not Surveyed': 'SubSitesNotSurveyed',
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

            # Confirm the tluESealSeasons has been defined for the Realized Seasons

            inQuery = f"SELECT tluESealSeasons.* FROM tluESealSeasons"

            #
            # Check Season Table - Import Seasons Table
            #

            uniqueSeasonsDF = pd.DataFrame(dfElephantEvents_append['Season'].unique(), columns=['Season'])
            uniqueSeasonsDF.insert(0, "SeasonToDefine", None)

            outDFSeasons = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Lookup the Season
            dfSeasonsDefined = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFSeasons,
                                                                             "Season", "Season",
                                                                             uniqueSeasonsDF,
                                                                             "Season",
                                                                             "SeasonToDefine")

            # Confirm the Season has been defined - if not exist
            # Check for Lookups not defined via an outer join.
            # If is null then these are undefined contacts
            dfSeasonsDefined_Null = dfSeasonsDefined[dfSeasonsDefined['SeasonToDefine'].isna()]

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

            # Remove/Clean Up Fields - EventID_lk, ID_lk, DeviceCode, DeviceName, Notes
            dfElephantEvents_append = dfElephantEvents_append.drop(
                columns=['EventID_lk', 'ID_lk', 'DeviceCode', 'DeviceName', 'Notes']
            )

            # Append the Elephant Event Records
            insertQuery = (f'INSERT INTO tblElephantEvents (CollectionDeviceID, EventID, ParkCode, Season, Visibility, '
                           f'SurveyType, RegionalSurvey, RegionalCountCode, Comments, CreatedDate) '
                           f'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            # Append the Contacts to the tblEventObserers table
            dm.generalDMClass.appendDataSet(cnxn, dfElephantEvents_append, "tblElephantEvents", insertQuery,
                                            dmInstance)

            #################################
            # Define Table SubSiteNotDefined
            # Table SubSiteNotDefined
            #################################

            outSubSitesNotSurveyDF = tblSubSitesNotSurveyed(outDFSubset, outDFEventsLU, etlInstance, dmInstance)

            #####################################################
            # Return Survey with the Regional and EventID Defined
            # Use existing outDFEvent dataframe and the already imported outDFEventsLU

            outDFEvent.insert(0, "EventID", None)

            # Lookup the EventID value via SubSiteCode
            outDFEventwGlIDwEventID = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFEventsLU,
                                                                       "GlobalID", "EventID",
                                                                       outDFSurvey, "GlobalID",
                                                                       "EventID")
            # Subset to only the Event Field to be retained for down stream processing
            outDFEventwGlIDwEventID2 = outDFEventwGlIDwEventID[['EventID', 'GlobalID', 'StartTime']]

            logMsg = f"Success ETL Survey/Event Form ETL_SNPLPORE.py - process_Survey"
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            # Returning the Dataframe with the Survey and Event information pushed
            return outDFEventwGlIDwEventID2

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
            fieldTypeDic = {'Field': ["LocationID",	"Bull", "SA4","SA3", "SA2", "SA1", "OtherSA", "Cow", "EPUP", "DPUP",
                                "WNR", "IMM", "YRLNG", "ADULT", "HPUP", "ZAL", "Other", "DefineOther", "SpecifyOther",
                                      "RedFurPhoca", "SharkBitePhoca", "ParentGlobalID", "CreatedDate"],
                            'Type': ["int64", "int64", "int64", "int64", "int64", "int64", "int64", "int64", "int64",
                                     "int64", "int64", "int64", "int64", "int64", "int64", "int64", "int64", "object",
                                     "object", "int64", "int64", "object", "datetime64"],
                            'DateTimeFormat': ["na", "na", "na","na", "na", "na", "na", "na", "na", "na",
                                "na", "na", "na", "na", "na", "na", "na", "na", "na", "na", "na", "na",
                                               "%m/%d/%Y %I:%M:%S %p"]}

            outDFCounts = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDFSubset)

            # Merge on the Event Data Frame to get the EventID via the ParentGlobalID - GlobalID fields
            outDFCountswEventID = pd.merge(outDFCounts, outDFEvents, how='left', left_on="ParentGlobalID",
                                           right_on="GlobalID", suffixes=("_src", "_lk"))

            #
            # Move field tha will be in all stack records to front
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
                                                            'GlobalID', 'Other', 'DefineOther', 'SpecifyOther'], axis=1)

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

            # After Stacking all the records ready to append the records to 'tblSealCount'

            # Pass final Query to be appended
            insertQuery = (f'INSERT INTO tblSealCount (CreatedDate, EventID, ObservationTime, LocationID, '
                           f'MatureCode, Enumeration, QCNotes) VALUES (?, ?, ?, ?, ?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, combinedAllCountsDF, "tblSealCount", insertQuery, dmInstance)

            ######################################################################
            # Process RedFur and Shark Bite Records this goes to tblPhocaSealCount
            ######################################################################

            outRedFurShark = processRedFurShark(outDFCountswEventID, etlInstance, dmInstance)

            return combinedAllCountsDF

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - proces_Counts: {e}'
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




        return combinedAllCountsDF

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - procesRedFurShark: {e}'
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

def tblSubSitesNotSurveyed(inDF, inDFEvents, etlInstance, dmInstance):
    """
    Workflow to define the Sub Sites Not Defined table - tblSubSitesNotSurveyed
    Lookup the EventID, Explode the Multi Select SubSiteNotSurveyed field then look up the LocationID via the
    SubSiteCode in the tblLocations Table and append records to the tblSubSitesNotSurveyed table
    Undefined/Not Present subsites (i.e. 9999) are going to unknown subsite definition.

    :param inDF: Data Frame being processed with Sub Sites Not Surveys, and Regional ID field
    :param inDFEvents: Data Frame with the EventID and GlabalID attributes used to define the EventID
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return:
    """

    try:
        # Subset to only need fields
        subSiteDF = inDF[['GlobalID', 'SubSitesNotSurveyed']]

        # Add EventID field
        subSiteDF.insert(0, "EventID", None)

        # Lookup the EventID value via RegionalID
        subSiteDFwEventID = dm.generalDMClass.applyLookupToDFField(dmInstance, inDFEvents,
                                                                           "GlobalID", "EventID",
                                                                           subSiteDF, "GlobalID",
                                                                           "EventID")
        ###################################
        # Explode the 'SubSitesNotSurveyed'
        subSiteDFwEventIDParsed = (subSiteDFwEventID.assign(SubSitesNotSurveyed=subSiteDFwEventID['SubSitesNotSurveyed'].str.split(',')).
                            explode('SubSitesNotSurveyed'))

        # Trim white space in observers field
        subSiteDFwEventIDParsed['SubSitesNotSurveyed'] = subSiteDFwEventIDParsed['SubSitesNotSurveyed'].str.lstrip()

        ##################################
        # Lookup the LocationID via the SubSitesNotSurveyed

        # Add LocationID field
        subSiteDFwEventIDParsed.insert(0, "LocationID", None)

        # Set LocationID field to Int64
        subSiteDFwEventIDParsed['LocationID'] = subSiteDFwEventIDParsed['LocationID'].astype('Int64')

        # Read in the tluDevices lookup table
        # Import Event Table to define the EventID via the GlobalID
        inQuery = (f"SELECT tblLocations.LocationID, tblLocations.SubSiteCode, tblLocations.ESealLocation FROM "
                   f"tblLocations WHERE tblLocations.ESealLocation=True;")

        # Import Locations Table
        outDFLocations = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

        # Lookup the LocationID value via SubSiteCode
        subSiteDFwEventID = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFLocations,
                                                                   "SubSiteCode", "LocationID",
                                                                   subSiteDFwEventIDParsed, "SubSitesNotSurveyed",
                                                                   "LocationID")

        ###################################
        # Append the Sub Sites Not Surveyed Records

        subSiteDFAppend = subSiteDFwEventID[['EventID', 'LocationID']]

        insertQuery = f'INSERT INTO tblSubSitesNotSurveyed (EventID, LocationID) VALUES (?, ?)'

        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        # Append the Contacts to the tblEventObserers table
        dm.generalDMClass.appendDataSet(cnxn, subSiteDFAppend, "tblSubSitesNotSurveyed", insertQuery,
                                        dmInstance)

        logMsg = f"Success ETL_PINN_ELephant.py - tblSubSitesNotSurveyed."
        logging.info(logMsg)

        return subSiteDFAppend

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_PINN_ELephant.py - tblSubSitesNotSurveyed: {e}'
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)


