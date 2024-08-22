"""
ETL_SNPLPORE.py
Methods/Functions to be used for Snowy Plover PORE ETL workflow.

"""

#Import Required Libraries
import pandas as pd
import glob, os, sys
import traceback
import ETL as ETL
import generalDM as dm
import logging
import log_config

logger = logging.getLogger(__name__)

class etl_SNPLPORE:
    def __init__(self):

        """
        Define the QC Protocol instantiation attributes

        :param TBD
        :return: zzzz
        """
        # Class Variables

        numETL_SNPLPORE = 0

        # Define Instance Variables
        #self.filterRecQuery = 'qsel_QA_Control'

        numETL_SNPLPORE += 1

    def process_ETLSNPLPORE(outDFDic, etlInstance, dmInstance):

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
            outDFSurvey = etl_SNPLPORE.process_Survey(outDFDic, etlInstance, dmInstance)

            ######
            # Process SNPL Observations Form
            ######
            outDFObs = etl_SNPLPORE.process_Observations(outDFDic, etlInstance, dmInstance, outDFSurvey)

            ######
            # Process Bands Sub Form
            ######
            outDFBands = etl_SNPLPORE.process_Bands(outDFDic, etlInstance, dmInstance, outDFSurvey, outDFObs)

            ######
            # Process Predator
            ######
            outDFPredator = etl_SNPLPORE.process_Predator(outDFDic, etlInstance, dmInstance, outDFSurvey)

            logMsg = f"Success ETL_SNPLPORE.py - process_ETLSNPLPORE."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            return outETL

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_ETLSNPLPORE: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_Survey(outDFDic, etlInstance, dmInstance):

        """
        ETL routine for the parent survey form SFAN_SNPLPORE_Survey{YearVersion}- table.
        The majority of this information on this form will be pushed to the following tables:
        tblEvents.

        Note - X and Y Fields are being ignored, These coordiantes are being pushed to the SNPL Observations table

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance:

        :return:outDFSurvey: Data Frame of the exported form will be used in subsequent table ETL.
        """

        try:
            #Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Survey*
            inDF = None
            for key, df in outDFDic.items():
                if 'Survey' in key:
                    inDF = df
                    break

            # Create initial dataframe subset
            outDFSubset = inDF[['GlobalID', 'Survey Location', "SurveyDate", "Time Start Survey", "Time End Survey",
                            "CreationDate", "Creator"]].rename(
                columns={'GlobalID': 'Event_ID',
                         'SurveyDate': 'Start_Date',
                         'Time Start Survey': 'Start_Time',
                         'Time End Survey': 'End_Time',
                         'CreationDate': 'Created_Date',
                         'Creator': 'Created_By'})

            ##############################
            # Numerous Field CleanUp Steps
            ##############################
            #To DateTime Field
            outDFSubset['Start_Date'] = pd.to_datetime(outDFSubset['Start_Date'])
            # Format to m/d/yyy
            outDFSubset['Start_Date'] = outDFSubset['Start_Date'].dt.strftime('%m/%d/%Y')

            # Insert 'Location_ID' field
            outDFSubset.insert(1, "Location_ID", None)

            # Insert 'Protocol_Name' field
            outDFSubset.insert(2, "Protocol_Name", "PORE SNPL")

            fieldLen = outDFSubset.shape[1]
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
            outDF_Step2 = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFLookup,
                                                                 "Loc_Name", "Location_ID",
                                                                 outDFSubset, "Survey Location",
                                                                 "Location_ID")

            # Drop field "Survey Location
            outDF_Step2 = outDF_Step2.drop(columns=['Survey Location'])


            ############################
            # Define desired field types
            ############################


            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ["Event_ID", "Location_ID", "Protocol_Name", "Start_Date", "Start_Time", "End_Time",
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
            outContactsDFAppend = outContactsDF[['Event_ID', 'Contact_ID']]

            insertQuery = (f'INSERT INTO xref_Event_Contacts (Event_ID, Contact_ID) VALUES (?, ?)')

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

    def process_Observations(outDFDic, etlInstance, dmInstance, outDFSurvey):

        """
        ETL routine for the 'SNPL Observation' form data is pushed to the tbl_SNPL_Observations, tbl_Nest_Master, and
        tbl_SNPL_Behaviors.  A minimal record is added to the 'tbl_Nest_Master' table which is necessary due to the
        dependice of the 'Nest_ID' field having a defined record in the ''tbl_Nest_Master' table.  Starting in field
        season 2024 specific SNPL Behaviors are being collected (previously went to a notes field) to the
        'tbl_SNPL_Behaviors' table.

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance
        :param outDFSurvey: Data frame output from the proces_Survey method.  Used is workflow processing.

        :return:outDFObs: Data Frame with the exported/imported Observation data, to be used as needed.
        """

        try:

            # Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Observations*
            inDF = None
            for key, df in outDFDic.items():
                if 'SNPLObservations' in key:
                    inDF = df
                    break

            # Create initial dataframe subset
            outDFSubset = inDF[['ParentGlobalID', 'GlobalID', 'Time', "Males", "Female", "Unknown", "Hatchling",
                                "Fledgling", "Eggs", "Nest ID", "Territory Behavior", "Nest Behavior", "Chicks Behavior",
                                "Other Behavior", "Specify other.", "SNPL Notes", "Long", "Lat"]].rename(
                columns={'ParentGlobalID': 'Event_ID',
                         'GlobalID': 'SNPL_Data_ID',
                         'Time': 'SNPL_Time',
                         'Males': 'SNPL_Male',
                         'Female': 'SNPL_Female',
                         'Unknown': 'SNPL_Unk',
                         'Hatchling': 'SNPL_Hatchlings',
                         'Fledgling': 'SNPL_Fledglings',
                         'Eggs': 'Number_Eggs',
                         'Nest ID': 'Nest_ID',
                         'SNPL Notes': 'SNPL_Notes',
                         'Long': 'X_Coord',
                         'Lat': 'Y_Coord'})

            ####################
            # Create a Master Nest ID if it doesn't already Exist - Nest_ID is a related table that must first be
            # created before creating the Observation
            ####################

            outDFNewNests = process_NestMasterInitial(etlInstance, dmInstance, outDFSurvey, outDFSubset)

            ##############################
            # CleanUp Wrangle Steps Observations Table
            ##############################


            # Convert Nans in Object/String and defined Numeric fields to None, NaN will not import to text
            # fields in access.  Numeric fields when 'None' is added will turn to 'Object' fields but will import to the
            # numeric (e.g. Int or Double) fields still when an Object type with numeric only values and the added
            # none values. A real PITA None and Numeric is.
            cols_to_update = ["Nest_ID", "Territory Behavior", "Nest Behavior", "Chicks Behavior", "Other Behavior",
                              "Specify other.", "Number_Eggs"]
            for col in cols_to_update:
                outDFSubset[col] = dm.generalDMClass.nan_to_none(outDFSubset[col])


            ############################
            # Define desired field types
            ############################

            # Dictionary with the list of fields in the dataframe and desired pandas dataframe field type
            # Note if the Seconds are not in the import then omit in the 'DateTimeFormat' definitions
            fieldTypeDic = {'Field': ['ParentGlobalID', 'GlobalID', 'Time', "Males", "Female", "Unknown", "Hatchling",
                                "Fledgling", "Eggs", "Nest ID", "Territory Behavior", "Nest Behavior", "Chicks Behavior",
                                "Other Behavior", "Specify other.", "SNPL Notes", "Long", "Lat"],
                'Type': ["object", "object", "datetime64", "int32", "int32", "int32",
                         "int32", "int32", "int32", "object", "object", "object", "object",
                         "object", "object", "object", "float32", "float32"],
                'DateTimeFormat': ["na", "na", "%H:%M", "na", "na", "na", "na",
                                "na", "na", "na", "na", "na", "na",
                                "na", "na", "na", "na", "na"]}

            outDFObs = dm.generalDMClass.defineFieldTypesDF(dmInstance, fieldTypeDic=fieldTypeDic, inDF=outDFSubset)

            # Number_Eggs field - variable was added mid-season, setting all 'None' values converts above to 0.
            outDFObs['Number_Eggs'] = outDFObs['Number_Eggs'].fillna(0)

            # Drop the Behavior fields
            outDFObsOnly = outDFObs.drop(columns=["Territory Behavior", "Nest Behavior", "Chicks Behavior",
                                "Other Behavior", "Specify other."])

            # Add Coordinate Related Fields - Coord_Units, Coord_System, Datum
            fieldLen = outDFObsOnly.shape[1]
            # Insert 'Coord_Units' = m
            outDFObsOnly.insert(fieldLen, "Coord_Units", "degree")
            # Insert 'Coord_System' = GCS
            outDFObsOnly.insert(fieldLen + 1, "Coord_System", "GCS")
            # Insert 'Datum' = WGS84
            outDFObsOnly.insert(fieldLen + 2, "Datum", "WGS84")

            # Append outDFObs to 'tbl_SNPL_Observations'
            # Pass final Query to be appended
            insertQuery = (f'INSERT INTO tbl_SNPL_Observations (Event_ID, SNPL_Data_ID, SNPL_Time, SNPL_Male,'
                           f' SNPL_Female, SNPL_Unk, SNPL_Hatchlings, SNPL_FLedglings, Number_Eggs, Nest_ID, SNPL_Notes,'
                           f' X_Coord, Y_Coord, Coord_Units, Coord_System, Datum) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,'
                           f' ?, ?, ?, ?, ?, ?)')

            cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
            dm.generalDMClass.appendDataSet(cnxn, outDFObsOnly, "tbl_SNPL_Observations", insertQuery,
                                            dmInstance)

            ############################
            # Process Behaviors Data going to 'tbl_SNPL_Behaviors'.  Will need to read in lookup tables tlu_Behavior and
            # tlu_BehaviorCategory, Parse out by Territory, Nest and Chick Behavior Fields, If value in specify other
            # this is being pushed to the 'tbl_SNPL_Behaviors' - 'Note' field.
            ############################

            outDFBehavior = process_Behaviors(etlInstance, dmInstance, outDFSubset)

            logMsg = f"Success ETL_SNPLPORE.py - process_Observations."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            return outDFObs

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_Observations: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_Bands(outDFDic, etlInstance, dmInstance, outDFSurvey, outDFObs):

        """
        ETL routine for the 'SNPL Bands Sub Form' form - table 'tbl_SNPL_Banded' and 'tbl_Check_BandData'



        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance
        :param outDFSurvey: Data frame output from the proces_Survey method.
        :param outDFObs: Data frame output from the process_Observations method.

        :return:outDFBands: Data Frame with the exported/imported data, to be used as needed.
        """

        try:

            # Export the Survey Dataframe from Dictionary List - Wild Card in Key is *Observations*
            inDF = None
            for key, df in outDFDic.items():
                if 'Band' in key:
                    inDF = df
                    break

            # Create initial dataframe subset
            outDFSubset = inDF[['ParentGlobalID', 'GlobalID', 'Left Leg', 'Specify other.', 'Right Leg',
                                'Specify other..1', 'SNPL Sex', 'SNPL Age', 'Band Notes', 'Chick Banding?', 'Chick % Dryness',
                                'Egg Tooth Presence', 'Yolk Sac Presence', 'USGS Band Number', 'SNPL Chick Notes']].rename(
                columns={'ParentGlobalID': 'SNPL_Data_ID',
                         'GlobalID': 'SNPL_Band_ID',
                         'Left Leg': 'Left_Leg',
                         'Specify other.': 'Specify_Other_Left',
                         'Right Leg': 'Right_Leg',
                         'Specify other..1': 'Specify_Other_Right',
                         'SNPL Sex': 'SNPL_Sex',
                         'SNPL Age': 'SNPL_Age',
                         'Band Notes': 'Band_Notes',
                         'Chick Banding?': 'Chick_Banding',
                         'Chick % Dryness': 'PctDryness',
                         'Egg Tooth Presence': 'EggToothPresence',
                         'Yolk Sac Presence': 'YolkSacPresence',
                         'USGS Band Number': 'USGSBandNumber'
                         })




            logMsg = f"Success ETL_SNPLPORE.py - process_Bands."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            return outDFBands

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_Bands: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def process_Predator(outDFDic, etlInstance, dmInstance, outDFSurvey):

        """
        ETL routine for the 'Predator Observations' form - table 'PredatorObservations_2'.
        The major of information on this form is pushed to the following tables:
        tbl_Predator_Survey


        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance
        :param outDFSurvey: Data frame output from the proces_Survey method.

        :return:outDFPredator: Data Frame with the exported/imported data
        """

        try:
            # Pass Output Data Frame
            outDFSurvey = []

            logMsg = f"Success ETL_SNPLPORE.py - process_Predators."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            return outDFPredator

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_Predators: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

def process_NestMasterInitial(etlInstance, dmInstance, outDFSurvey, outDFSubset):
    """
    Create the the Nest_ID record in the 'tbl_Nest_Master' table for all observations if it doesn't already exist.
    The 'tbl_Nest_Master' - 'Nest_ID' field in the 'tbl_SNPL_Observations' table must first be defined in the
    'tbl_Nest_Master' before any SNPL Observations can be appended.

    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance
    :param outDFSurvey: Survey data frame that was append to the
    :param outDFSubset: Observation dataframe that are been subset in 'process_Observations'

    :return:outDFNestIDNewAppend: Dataframe with the newly append Nests
    """

    try:
        outDFUniqueEventNest = outDFSubset[outDFSubset['Nest_ID'].notna()][['Event_ID', 'Nest_ID']].drop_duplicates()

        # Merge the Unique Nest_ID - Event and Survey Data frames
        merged_df = pd.merge(outDFSurvey, outDFUniqueEventNest, on='Event_ID')
        # Sort on Nest_ID and Start_Date
        merged_df = merged_df.sort_values(by=['Nest_ID', 'Start_Date'])

        # Retain the first record by 'Nest_ID'
        outDFNestIDFirst = merged_df.drop_duplicates(subset=['Nest_ID'], keep='first')


        # Join to Check which 'Nest_ID, Year, and Location_ID composite primary key is not present Left join.
        # Pull the Nest Master table
        inQuery = f"Select * FROM tbl_Nest_Master;"
        outDFNestMaster = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

        # Perform an outer left join - these are the records to be append
        outDFNestIDNew = pd.merge(outDFNestIDFirst, outDFNestMaster, on='Nest_ID', how='left', indicator=True)

        outDFNestIDNewAppend = outDFNestIDNew[['Location_ID_x', 'Nest_ID', "Start_Date"]].rename(
                    columns={'Location_ID_x': 'Location_ID'})

        # Add additional required fields - Created_By, DataProcessingLevelUser. Note DPL_ID is defaulting to 1, and DPL
        # data is defaulting to Now

        # Add and Calculate Year Field
        outDFNestIDNewAppend['Year'] = outDFNestIDNewAppend['Start_Date'].dt.year

        # Drop 'Start_Date
        outDFNestIDNewAppend = outDFNestIDNewAppend.drop(columns=['Start_Date'])

        fieldLen = outDFNestIDNewAppend.shape[1]
        # Add fields: Created_By, DataProcessingLevelUser
        outDFNestIDNewAppend.insert(fieldLen, "Created_By", etlInstance.inUser)
        outDFNestIDNewAppend.insert(fieldLen + 1, "DataProcessingLevelUser", etlInstance.inUser)

        # Append the New Nest_ID values
        insertQuery = (f'INSERT INTO tbl_Nest_Master (Location_ID, Nest_ID, Year, Created_By, DataProcessingLevelUser)'
                       f' VALUES (?, ?, ?, ?, ?)')

        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        dm.generalDMClass.appendDataSet(cnxn, outDFNestIDNewAppend, "tbl_Nest_Master", insertQuery,
                                        dmInstance)

        logMsg = f"Success process_NestMasterInitial - New Nest_Ids append to tbl_NestMaster."
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.info(logMsg)

        return outDFNestIDNewAppend

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - processSNPLContacts: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)


def processSNPLContacts(inDF, etlInstance, dmInstance):
    """
    Define Observers in SNPL Survey Form table xref_EventContacts
    Harvest Multi-select field 'Define Observers', if other, also harvest 'Specify Other' field in Survey .csv
    Lookup table for contacts is tlu_Contacts - Contact_ID being pushed to table xref_EventContacts

    :param inDF: Data Frame being processed
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return:
    """

    try:

        inDFContacts = inDF[['GlobalID', 'Define Observer(s)', 'Specify other.']].rename(
                    columns={'GlobalID': 'Event_ID',
                             'Define Observer(s)': 'Observers',
                             'Specify other.': 'Other'})

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
        inDFObserversParsed3.reset_index(drop=True)

        # Trim leading white spaces in the 'Observers' field
        inDFObserversParsed3['Observers'] = inDFObserversParsed3['Observers'].str.lstrip()


        ##################################
        # Parse the 'Other' field on ','
        # Retain only the records where Observers contains 'other'
        inObsOther = inDFContacts[inDFContacts['Observers'].str.contains('other')]
        inDFOthersParsed = inObsOther.assign(Observers=inObsOther['Other'].str.split(',')).explode('Observers')
        inDFOthersParsed2 = inDFOthersParsed.drop(['Other'], axis=1)

        # Reset Index
        inDFOthersParsed3 = inDFOthersParsed2.reset_index(drop=True)

        # Trim leading white spaces in the 'Observers' field
        inDFOthersParsed3['Observers'] = inDFOthersParsed3['Observers'].str.lstrip()

        ##################################
        # Combine both parsed dataframes for fields Observers and Others
        dfObserversOther = pd.concat([inDFObserversParsed3, inDFOthersParsed3], ignore_index=True)

        # Define First and Last Name Fields
        dfObserversOther.insert(2, "Last_Name", None)
        dfObserversOther.insert(3, "First_Name", None)

        # Add Field checking if '_' is in field Observers
        dfObserversOther['Underscore'] = dfObserversOther['Observers'].apply(lambda x: 'Yes' if '_' in x else 'No')
        ###############################
        # Define the 'First_Name' field
        # Parse the name before the '_' into the 'First_Name' field if 'Underscore' equals 'Yes'
        dfObserversOther['First_Name'] = dfObserversOther.apply(
            lambda row: row['Observers'].split('_')[0] if row['Underscore'] == 'Yes' else row['First_Name'], axis=1)
        # Parse the name before the ' ' into the 'First_Name' field if 'Underscore' equals 'No'
        dfObserversOther['First_Name'] = dfObserversOther.apply(
            lambda row: row['Observers'].split(' ')[0] if row['Underscore'] == 'No' else row['First_Name'], axis=1)

        ###############################
        # Define the 'Last_Name' field
        # Parse the name after the '_' into the 'Last_Name' field if 'Underscore' equals 'Yes'
        dfObserversOther['Last_Name'] = dfObserversOther.apply(
            lambda row: row['Observers'].split('_')[1] if row['Underscore'] == 'Yes' else row['Last_Name'], axis=1)
        # Parse the name after the ' ' into the 'Last_Name' field if 'Underscore' equals 'No'
        dfObserversOther['Last_Name'] = dfObserversOther.apply(
            lambda row: row['Observers'].split(' ')[1] if row['Underscore'] == 'No' else row['Last_Name'], axis=1)

        # Create a 'First_Last' name which will be the index on which the lookup will be performs
        dfObserversOther['First_Last'] = dfObserversOther['First_Name'] + '_' + dfObserversOther['Last_Name']

        # Add the Contact_ID field to be populated
        fieldLen = dfObserversOther.shape[1]
        # Insert 'DataProcesingLevelID' = 1
        dfObserversOther.insert(fieldLen-1, "Contact_ID", None)

        #######################################
        # Read in 'Lookup Table - tlu Contacts'
        inQuery = f"SELECT tlu_Contacts.Contact_ID, [First_Name] & '_' & [Last_Name] AS First_Last FROM tlu_Contacts;"

        outDFContactsLU = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

        # Apply the Lookup Code on the Two Data Frames
        dfObserversOtherwLK = dm.generalDMClass.applyLookupToDFField(dmInstance, outDFContactsLU,
                                                             "First_Last", "Contact_ID",
                                                             dfObserversOther, "First_Last",
                                                             "Contact_ID")

        # Check for Lookups not defined via an outer join.
        # If 'Contact_ID' is null then these are undefined contacts
        dfObserversNull = dfObserversOtherwLK[dfObserversOtherwLK['Contact_ID'].isna()]
        numRec = dfObserversNull.shape[0]
        if numRec >= 1:
            logMsg = f'WARNING there are {numRec} records without a defined tlu_Contacts Contact_ID value.'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.warning(logMsg)

            outPath = f'{etlInstance.outDir}\RecordsNoDefinedContact.csv'
            if os.path.exists(outPath):
                os.remove(outPath)

            dfObserversNull.to_csv(outPath, index=True)

            logMsg = (f'Exporting Records without a defined lookup see - {outPath} \n'
                      f'Exiting ETL_SNPLPORE.py - processSNPLContacts with out full completion.')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.warning(logMsg)
            exit()

        logMsg = f"Success ETL_SNPLPORE.py - processSNPLContacts."
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.info(logMsg)

        return dfObserversOtherwLK

    except Exception as e:

        logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - processSNPLContacts: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)

def process_Behaviors(etlInstance, dmInstance, outDFSubset):

    """
    Populates the 'tbl_SNPL_Behaviors' table from the SNPL Observation Forms.
    Parses out (i.e. multiple values possible, comma delimited) by fields 'Terriotry Behavior, Nest Behavior, Chick
    Behavior and Other Behavior Fields.

    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance
    :param outDFSubset: Observation dataframe that are been subset in 'process_Observations'

    :return:outDFBehavior_wOther: Dataframe with the appended Behaviors to 'tbl_SNPL_Behaviors'
    """

    try:
        # Define behavior fields to process
        fieldsToProcess = ['Territory Behavior', 'Nest Behavior', 'Chicks Behavior', 'Other Behavior']

        # Create the blank dataframe to be populated
        data = {
            'SNPL_Data_ID': ['Test'],
            'BehaviorClass': ['Test'],
            'Behavior': ['Test'],
            'Notes': ['Test']}

        outDFBehavior = pd.DataFrame(data)
        # Remove the setup record
        outDFBehavior = outDFBehavior[outDFBehavior['SNPL_Data_ID'] != 'Test']

        # Read in lookup 'tlu_Behavior'
        inQuery = f"Select * FROM tlu_Behavior;"
        outDFBehaviorLU = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

        # Iterate through the Fields
        for field in fieldsToProcess:
            # Subset to the desired fields to retain
            df_parsed = outDFSubset[['SNPL_Data_ID', field]].copy()

            # Split the 'Field' on the ',' delimiter
            df_parsed[field] = df_parsed[field].str.split(',')
            # Parse the input Field to get 1 record per delimiter via explode
            df_parsedExploded = df_parsed.explode(field)

            # Remove the parseExploded records that are nan
            df_parsedExplodedNoNA = df_parsedExploded.dropna(subset=[field])

            # If field = 'Other Behavior' also need to remove the 'other' records. The 'other' records are strings that
            # are added to the 'specify other.' field.  These 'specify other.' entries will be handled separately below.
            if field == 'Other Behavior':
                df_parsedExplodedNoNA = df_parsedExplodedNoNA[df_parsedExplodedNoNA[field] != 'other']
            # Join on the 'Field' to the Behavior lookup table, left join so can check if any missing lookups
            df_parsedExplodedwLk = pd.merge(df_parsedExplodedNoNA, outDFBehaviorLU, left_on=field, right_on='Behavior',
                                            how='left', indicator=True)

            # Check if any undefined lookup
            dfBehaviorNull = df_parsedExplodedwLk[df_parsedExplodedwLk['_merge'] != 'both']
            numRec = dfBehaviorNull.shape[0]
            if numRec >= 1:
                logMsg = f'WARNING there are {numRec} records without a defined tlu_Behavior - Behavior value.'
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.warning(logMsg)

                outPath = f'{etlInstance.outDir}\RecordsNotDefinedBehavior_{field}.csv'
                if os.path.exists(outPath):
                    os.remove(outPath)

                dfBehaviorNull.to_csv(outPath, index=True)

                logMsg = (f'Exporting Records without a defined Behavior lookup see - {outPath} \n'
                          f'Exiting ETL_SNPLPORE.py - process_Behaviors with out full completion.')
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.warning(logMsg)
                exit()

            # Subset to only the 'SNPL_Data_ID', 'BehaviorCategory', and 'Behavior' fields and rename as needed
            outDFBehaviorLoop = df_parsedExplodedwLk[['SNPL_Data_ID', 'BehaviorCategory', 'Behavior']].rename(
                columns={'BehaviorCategory': 'BehaviorClass'})

            # Append the records to the 'outDFBehavior' dataframe
            outDFBehavior = pd.concat([outDFBehavior, outDFBehaviorLoop], ignore_index=True)

        ##################################
        # Process the 'Other Behavior' field.  Will define as 'Behavior Category' = Territory, and Behavior = O-ON.
        # Will push the notes in 'Specify Other.' field to the 'tbl_SNPL_Beaviors' - 'Notes' field.

        inDFOther = outDFSubset[['SNPL_Data_ID', 'Other Behavior', 'Specify other.']]

        inDFOther = inDFOther[inDFOther['Other Behavior'] == 'other']

        # Remove and records where 'Specify Other.' field is null.
        inDFOtherNotNull = inDFOther[inDFOther['Specify other.'].notna()]

        # Subset to the desired fields
        outDFOtherAppend = inDFOtherNotNull[['SNPL_Data_ID', 'Specify other.']].rename(
            columns={'Specify other.': 'Notes'})

        # Add the BehaviorCategory field setting to default 'Other'
        outDFOtherAppend.insert(1, 'BehaviorClass', 'Other')

        # Add the Behavior field setting to default 'O-ON' which is defined as: Other Behaviors see Notes field in
        # tbl_SNPL_Behaviors
        outDFOtherAppend.insert(2, 'Behavior', 'O-ON')

        #################################################################
        # Append the 'outDFOtherAppend' to the 'outDFBehavior' data frame
        #################################################################
        outDFBehavior_wOther = pd.concat([outDFBehavior, outDFOtherAppend])

        # Remove nan values from the 'Notes' field
        cols_to_update = ["Notes"]
        for col in cols_to_update:
            outDFBehavior_wOther[col] = dm.generalDMClass.nan_to_none(outDFBehavior_wOther[col])

        # Append to tbl_SNPL_Behaviors
        insertQuery = (f'INSERT INTO tbl_SNPL_Behaviors (SNPL_Data_ID, BehaviorClass, Behavior, Notes)'
                       f' VALUES (?, ?, ?, ?)')

        cnxn = dm.generalDMClass.connect_DB_Access(etlInstance.inDBBE)
        dm.generalDMClass.appendDataSet(cnxn, outDFBehavior_wOther, "tbl_SNPL_Behaviors", insertQuery,
                                        dmInstance)

        logMsg = f"Success process_Behaviors - ETL_SNPLPORE.py"
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.info(logMsg)

        return outDFBehvior_wOther

    except Exception as e:

        logMsg = f'WARNING ERROR  - process_Behaviors - ETL_SNPLPORE.py: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)