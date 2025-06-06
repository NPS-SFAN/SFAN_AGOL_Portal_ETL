"""
tests_etl.py

Unit Testing Script for the ETL Routines

Updates:
5/13/2025 - Added initial unit testing frame working for AppendDataset.  Adding Record Test Agreement for Pinn Elephant
Seals - tblEvents load.....many more unit tests needed.

"""
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd

import ETL_Salmonids_Smolts
import generalDM as dm
import ETL_PINN_Elephant as PElephant

class TestAppendDataSet(unittest.TestCase):
# Methods for Testing correct number of records appending to the respective destination table with passing the defined
# load schema.
    @patch('generalDM.logging.info')
    def test_record_count_PinnTblEvents(self, mock_log):
        # Unit Test on Mock DataFrame and schema being processed successfully appends the correct number of records
        # to the tblEvents for Elephant Seal Survey/Metadata Import.

        data = {
            "GlobalID": [1, 2],
            "ProjectCode": ["P001", "P002"],
            "StartDate": ["2025-01-01", "2025-02-01"],
            "EndDate": ["2025-01-02", "2025-02-02"],
            "StartTime": ["08:00", "09:00"],
            "EndTime": ["17:00", "18:00"],
            "CreatedDate": ["2025-01-01", "2025-02-01"],
            "CreatedBy": ["User1", "User2"],
            "DataProcessingLevelID": [1, 2],
            "DataProcessingLevelDate": ["2025-01-01", "2025-02-01"],
            "DataProcessingLevelUser": ["User1", "User2"],
            "Project": ["Project A", "Project B"],
            "ProtocolID": ["Protocol1", "Protocol2"]
        }

        df = pd.DataFrame(data)
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        insert_query = (f'INSERT INTO tblEvents (GlobalID, ProjectCode, StartDate, EndDate, StartTime, EndTime, '
                       f'CreatedDate, CreatedBy, DataProcessingLevelID, DataProcessingLevelDate, '
                       f'DataProcessingLevelUser, Project, ProtocolID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                       f'?)')

        dm_instance = MagicMock()

        # Call appendDataSet for table 1
        dm.generalDMClass.appendDataSet(mock_connection, df, 'tblEvents', insert_query, dm_instance)

        # Check logging.info calls for record count verification
        log_messages = [call_args[0][0] for call_args in mock_log.call_args_list]

        self.assertTrue(
            any("Record Count Verification: Expected=2, Inserted=2" in msg for msg in log_messages),
            "Expected record count verification log message not found in logging.info() calls."
        )

        # Add logic to confirm the test was successful
        print("Success 'test_record_count_PinnTblEvents' passed.")

    @patch('generalDM.logging.info')
    def test_record_count_PinnTblSealCount(self, mock_log):
        # Unit Test on Mock DataFrame and schema being processed successfully appends the correct number of records
        # to the tblSealCount for Elephant Seal Survey/Metadata Import.

        data = {
            "CreatedDate": ["2025-01-01", "2025-02-01"],
            "EventID": [1, 23],
            "ObservationTime": ["08:00", "09:00"],
            "LocationID": [107, 108],
            "MatureCode": ["User1", "User2"],
            "Enumeration": [1, 123],
            "QCNotes": [None, "Taxon Test: Kirk S"]}

        df = pd.DataFrame(data)
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        insert_query = insertQuery = (f'INSERT INTO tblSealCount (CreatedDate, EventID, ObservationTime, LocationID, '
                                      f'MatureCode, Enumeration, QCNotes) VALUES (?, ?, ?, ?, ?, ?, ?)')

        dm_instance = MagicMock()

        # Call appendDataSet for table 1
        dm.generalDMClass.appendDataSet(mock_connection, df, 'bsTable', insert_query, dm_instance)

        # Check logging.info calls for record count verification
        log_messages = [call_args[0][0] for call_args in mock_log.call_args_list]

        self.assertTrue(
            any("Record Count Verification: Expected=2, Inserted=2" in msg for msg in log_messages),
            "Expected record count verification log message not found in logging.info() calls."
        )

        # Add logic to confirm the test was successful
        print("Success 'test_record_count_PinnTblSealCount' passed.")

    @patch('generalDM.logging.info')
    def test_record_count_SalmonidsSmolt_Measurements(self, mock_log):
        # Unit Test on Mock DataFrame and schema being processed successfully appends the correct number of records
        # to the tbl_SmoltMeasurements Import.

        data = {
            "EventID": [1, 23],
            "SpeciesCode": ["CO", "SH"],
            "LifeStage": ["YOY", "YOY"],
            "FishTally": [3, 6],
            "ForkLength": [43, 34],
            "LengthCategoryID": [6, 4],
            "TotalWeight": [10.2, 4.5],
            "BagWeight": [2.1, 2.1],
            "FishWeight": [8.1, 3.3],
            "NewRecaptureCode": ['NPIT', 'ND'],
            "PITTag": [989005030193450, 989005030193451],
            "MarkColor": ['blue', 'orange'],
            "PriorSeason": [True, False],
            "Injured": [False, False],
            "Dead": [False, False],
            "Scales": [False, False],
            "Tissue": [True, False],
            "EnvelopeID": ["013-2006", "0143-2006"],
            "Comments": ["", "TestComments"],
            "QCFlag": ["", "DEO"],
            "QCNotes": ["", "Forgot to enter EvenlopeID Correctly"],
            "CreatedDate": ["2025-01-01", "2025-02-01"]}

        df = pd.DataFrame(data)
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        # Patch the connect_DB_Access function
        dm.generalDMClass.connect_DB_Access = MagicMock(return_value=mock_connection)

        dmInstance = MagicMock()
        etlInstance = MagicMock()
        etlInstance.inDBBE = "mock_connection_string"  # Fix the root cause

        ETL_Salmonids_Smolts.process_Measurements(df, etlInstance, dmInstance)

        # Check logging.info calls for record count verification
        log_messages = [call_args[0][0] for call_args in mock_log.call_args_list]

        self.assertTrue(
            any("Record Count Verification: Expected=2, Inserted=2" in msg for msg in log_messages),
            "Expected record count verification log message not found in logging.info() calls."
        )

        # Add logic to confirm the test was successful
        print("Success 'test_record_count_SalmonidsSmolt_Measurements' passed.")


    @patch('generalDM.logging.info')
    def test_record_count_SalmonidsSmolt_Counts(self, mock_log):
        # Unit Test on Mock DataFrame and schema being processed successfully appends the correct number of records
        # to the tbl_SmoltCounts Import.

        data = {
            "EventID": [1, 23],
            "SpeciesCode": ["CO", "SH"],
            "LifeStage": ["YOY", "YOY"],
            "Comments": ["", "TestComments"],
            "QCFlag": ["", "DEO"],
            "QCNotes": ["", "Forgot to enter EvenlopeID Correctly"],
            "CreatedDate": ["2025-01-01", "2025-02-01"],
            "FishTally": [3, 6],
            "Dead": [False, False]
        }

        df = pd.DataFrame(data)
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        # Patch the connect_DB_Access function
        dm.generalDMClass.connect_DB_Access = MagicMock(return_value=mock_connection)

        dmInstance = MagicMock()
        etlInstance = MagicMock()
        etlInstance.inDBBE = "mock_connection_string"  # Fix the root cause

        ETL_Salmonids_Smolts.process_Counts(df, etlInstance, dmInstance)

        # Check logging.info calls for record count verification
        log_messages = [call_args[0][0] for call_args in mock_log.call_args_list]

        self.assertTrue(
            any("Record Count Verification: Expected=2, Inserted=2" in msg for msg in log_messages),
            "Expected record count verification log message not found in logging.info() calls."
        )

        # Add logic to confirm the test was successful
        print("Success 'test_record_count_SalmonidsSmolt_Counts' passed.")

class TestETLTargetSchema(unittest.TestCase):
    #Methds for testing expected data types are compatiable with target schema (i.e. field type match)
    '''
    def test_schema_data_type_PinnTblSealCount(self):
        # Define expected schema for PinnTblSealCount load
        expected_dtypes = {
            'CreatedDate': 'datetime64',
            'EventID': 'int64',
            'ObservationTime': 'datetime64',
            'LocationID': 'int64',
            'MatureCode': 'object',
            'Enumeration': 'int64',
            'QCNotes': 'object'
        }

        # Mock input data the Count DataFrame
        mock_reader = MagicMock()
        mock_reader.read.return_value = pd.DataFrame({
            "CreatedDate": ["2025-01-01", "2025-02-01"],
            "EventID": [1, 23],
            "ObservationTime": ["08:00", "09:00"],
            "LocationID": [107, 108],
            "MatureCode": ["User1", "User2"],
            "Enumeration": [1, 123],
            "QCNotes": [None, "Taxon Test: Kirk S"]})

        # Mock Data for the Event Data Frame
        mock_reader2 = MagicMock()
        mock_reader.read.return_value = pd.DataFrame({
            'EventID': [1, 23],
            'GlobalID': [1, 345],
            'StartTime': ["08:00", "09:00"]})

        # Need to also emulate etlInstance and dmInstance - the process_Counts function should be Refactors - KRS
        # Run the ETL process
        result_df = PElephant.etl_PINNElephant.process_Counts(mock_reader, mock_reader2, etlInstance, dmInstance)

        # Check data types match the target schema
        for col, expected_dtype in expected_dtypes.items():
            with self.subTest(column=col):
                actual_dtype = str(result_df[col].dtype)
                self.assertEqual(actual_dtype, expected_dtype,
                                 f"Column '{col}' expected {expected_dtype}, got {actual_dtype}")
    '''

if __name__ == '__main__':
    unittest.main()
