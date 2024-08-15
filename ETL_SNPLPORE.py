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
            outDFSurvey = etl_SNPLPORE.proces_Survey(outDFDic, etlInstance, dmInstance)


            ######  STOPPED HERE 8/15/2024 - KRS
            # Process SNPL Observations Form
            ######
            outDFObs = etl_SNPLPORE.proces_Observations(outDFDic, etlInstance, dmInstance, outDFSurvey)

            ######
            # Process Bands Sub Form
            ######
            outDFBands = etl_SNPLPORE.proces_Bands(outDFDic, etlInstance, dmInstance, outDFSurvey, outDFObs)

            ######
            # Process Predator
            ######
            outDFPredator = etl_SNPLPORE.proces_Predator(outDFDic, etlInstance, dmInstance, outDFSurvey)



            # Pass Variable Success
            outETL = 'Success'

            logMsg = f"Success ETL_SNPLPORE.py - process_ETLSNPLPORE."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            return outETL

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_ETLSNPLPORE: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def proces_Survey(outDFDic, etlInstance, dmInstance):

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
                                    "%m/%d/%Y %H:%M", "na", "na", "na",
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

    def proces_Observations(outDFDic, etlInstance, dmInstance, outDFSurvey):

        """
        ETL routine for the 'SNPL Observation' form - table 'SNPL_Observations'.
        The major of information on this form is pushed to the following tables:
        tbl_SNPL_Observations
        xxxx

        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance
        :param outDFSurvey: Data frame output from the proces_Survey method.  Used is workflow processing.

        :return:outDFObs: Data Frame with the exported/imported Observation data, to be used as needed.
        """

        try:

            # Define final query
            insertQuery = f""



            logMsg = f"Success ETL_SNPLPORE.py - process_Observations."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            # Might Return a DF with a query of tblEvents aftern appending - TBD

            return outDFObs

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_Observations: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def proces_Bands(outDFDic, etlInstance, dmInstance, outDFSurvey, outDFObs):

        """
        ETL routine for the 'SNPL Bands Sub Form' form - table 'tbl_SNPL_Banded'.
        The major of information on this form is pushed to the following tables:
        tbl_SNPL_Banded


        :param outDFDic - Dictionary with all imported dataframes from the imported feature layer
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance
        :param outDFSurvey: Data frame output from the proces_Survey method.
        :param outDFObs: Data frame output from the process_Observations method.

        :return:outDFBands: Data Frame with the exported/imported data, to be used as needed.
        """

        try:
            # Pass Output Data Frame
            outDFBands = []

            logMsg = f"Success ETL_SNPLPORE.py - process_Bands."
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            return outDFBands

        except Exception as e:

            logMsg = f'WARNING ERROR  - ETL_SNPLPORE.py - process_Bands: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def proces_Predator(outDFDic, etlInstance, dmInstance, outDFSurvey):

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


def processSNPLContacts(inDF, etlInstance, dmInstance):
    """
    Define Observers in SNPL Survey Form table xref_EventContacts
    Harvet Mutli-select field 'Define Observers', if other, also harvest 'Specify Other' field in Survey .csv
    Lookup table for contacts is tlu_Contacts - Contact_ID being pushed to table xref_EventContacts


    :param inDF - Survey Data Frame with Event ID and Contacts
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return:outDFContacts: Data Frame with the contacts and associated event ID, will append to xref_EventContacts
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
