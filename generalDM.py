"""
generalDM.py
General Data Management workflow related methods.  Consider migrating this to a more general SFAN_Data Management module.
"""
import os.path
import sys
from datetime import datetime
import traceback
import pyodbc
import pandas as pd
import win32com.client
import logging
import psutil
from zipfile import ZipFile
import glob

class generalDMClass:

    dateNow = datetime.now().strftime('%Y%m%d')

    def __init__(self, logFile):
        """
        Define the instantiated general Data Management instantiation attributes

        :param logFile: File path and name of .txt logFile
        :return: Instantiated generalDMClass with passed variable definitions and defined methods
        """
        self.logFileName = logFile

    def createLogFile(logFilePrefix, workspaceParent):
        """
        Creates a .txt logfile for the defined Name and in the defined Workspace directory. Checks if workspace exists
        will create if not.

        :param logFilePrefix:  LogFile Prefix Name
        :param workspaceParent: Parent directory for the workspace, will add a child 'workspace' in which the logfile
         will be created
        :return: logFile: Full Path Name of the created logfile
        """

        #################################
        # Checking for working directories
        ##################################
        workspace = workspaceParent + "\\workspace"
        if os.path.exists(workspace):
            pass
        else:
            os.makedirs(workspace)

        logFileName1 = f'{logFilePrefix}_logFile_{generalDMClass.dateNow}.txt'
        logFileName = os.path.join(workspace,logFileName1)

        # Check if logFile exists
        if os.path.exists(logFileName):
            pass
        else:
            logFile = open(logFileName, "w")  # Creating Log File
            logFile.close()

        return logFileName


    def messageLogFile(self, logMsg):

        """
        Write Message to Logfile - routine add a date/time Now string time stamp

        :param self:  dmInstance
        :param logMsg: String with the logfile message to be appended to the Self.logFileName
        :return:
        """

        try:
            #logFileName_LU = self.logFileName.name
            logFileName_LU = self.logFileName

            #Get Current Time
            messageTime = generalDMClass.timeFun()
            logMsg = f'{logMsg} - {messageTime}'
            print(logMsg)

            logFile = open(logFileName_LU, "a")
            logFile.write(logMsg + "\n")
            logFile.close()
        except:
            traceback.print_exc(file=sys.stdout)


    def timeFun():

        """
        Returns a date time now isoformat string using in error messages

        :return: messageTime: String with ISO format date time
        """

        from datetime import datetime
        b = datetime.now()
        messageTime = b.isoformat()

        return messageTime

    def connect_DB_Access(inDB):
        """
        Create connection to Access Database Via PYODBC connection.

        :param inDB: Full path and name to access database

        :return: cnxn: ODBC connection to access database
        """

        connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + inDB + ";")
        cnxn = pyodbc.connect(connStr)
        return cnxn

    def getLookUpValueAccess(self, cnxn, lookupTable, lookupField, lookupValue, lookupFieldValueFrom):
        """
        Find value in a lookup table using the passed variables, using a distinct clause expecting this to be used for
        a lookup table situation.

        :param instance: pass instance with relevant information
        :param cnxn: ODBC connection to access database
        :param lookupTable: lookup table in the cnxn database
        :param lookupField: field in the lookupTable to lookup
        :param lookupValueOut: values in the lookupField to be looked up
        :param lookupFieldValueFrom: field in the lookupTable from which to pull the lookup value

        :return: lookupValueOut: String with the lookup value
        """
        query = f"Select Distinct {lookupFieldValueFrom} from {lookupTable} Where {lookupField} = '{lookupValue}';"

        #Should be single value so will be series
        lookupValueDF = pd.read_sql(query, cnxn)

        #No value returned in lookup table - exit script
        if lookupValueDF.shape[0] == 0:
            logMsg = f'WARNING - No value returned in lookup table for - {lookupValue} - EXITING script at - getLookUpValueAccess'
            generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            sys.exit()

        #Convert lookup to series
        lookupValueSeries = lookupValueDF.iloc[0]

        #Convert the series values to a string
        lookupValueSeriesStr = lookupValueSeries.astype(str)

        lookupValueOut = lookupValueSeriesStr.str.cat(sep=', ')

        return lookupValueOut

    def connect_to_AcessDB_DF(query, inDB):

        """
        Connect to Access DB via PYODBC and perform defined query via pyodbc - return query in a dataframe

        :param query: query to be processed
        :param inDB: path to the access database being hit

        :return: queryDf: query output dataframe
        """
        connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + inDB + ";")
        cnxn = pyodbc.connect(connStr)

        try:
            queryDf = pd.read_sql(query, cnxn)


        except Exception as e:

            # Check if 'queryDf' imported try and import via ODBC cursor
            if 'queryDf' in locals() or 'queryDf' in globals():
                print("Variable 'queryDf' exists - imported via read_sql")
            else:
                print("Variable 'queryDf' does not exist - try and import query via pyodbc cursor")
                cursor = cnxn.cursor()

                # Execute the query
                cursor.execute(query)

                # Fetch all rows from the query
                rows = cursor.fetchall()

                # Fetch the column names from the cursor description
                columns = [column[0] for column in cursor.description]

                # Create a DataFrame from the fetched rows and column names
                queryDf = pd.DataFrame.from_records(rows, columns=columns)
                cursor.close()
                logMsg = "Import query - {query} via PYODBC - Cursor rather then pd.read_sql"
                logging.info(logMsg, exc_info=True)


            print(f"WARNING Error in 'connect_to_AccessDB_DF' for - {query} - {e}")
            logging.critical(logMsg, exc_info=True)
            exit()



        finally:
            cnxn.close()
            return queryDf


    def closeAccessDB():
        """
        Closes any open Microsoft Access databases using pywin32 to hit the Access COM interface

        :return: outClose - string denoting successfully closing of all access DBs.
        """

        try:
            # Find and close all Microsoft Access processes
            for proc in psutil.process_iter(['pid', 'name']):
                if proc.info['name'] and 'MSACCESS.EXE' in proc.info['name']:
                    try:
                        # Attempt to terminate the process
                        p = psutil.Process(proc.info['pid'])
                        p.terminate()
                        p.wait(timeout=5)
                        print(f"Closed Microsoft Access process with PID {proc.info['pid']}")
                    except psutil.NoSuchProcess:
                        print(f"No such process with PID {proc.info['pid']}")
                    except psutil.AccessDenied:
                        print(f"Access denied to terminate process with PID {proc.info['pid']}")
                    except psutil.TimeoutExpired:
                        print(f"Timeout expired while terminating process with PID {proc.info['pid']}")

            outClose = "Successfully Closed All Access DB's"
            return outClose

        except Exception as e:

            logMsg = (f'ERROR - An error occurred generalDMClass.closeAccessDB: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)



    def queryExistsDeleteODBC(queryName, inDBPath):
        """
        Check if query exists in the database MSysObjects table.  Must have Admin permissions to read 'MSys tables

        :param queryName: Name of query being pushed, will deleted first if exists
        :param inDBPath: path to database

        :return: query_exists: Variable defines if query exists on not (True|False)
        """
        inQuery = f"Select * FROM MSysObjects WHERE [Name] = '{queryName}';"

        outDFQueries = generalDMClass.connect_to_AcessDB_DF(inQuery, inDBPath)

        if len(queryName) > 0:
            query_exists = True
        else:
            query_exists = False

        return query_exists


    def queryExistsDelete(queryName, inDBPath):
        """
        Check if query exists in the database if yes delete, using pywin32 to hit the Access COM interface, ODBC
        doesn't have permissions to hit the 'MSYS' variables

        :param queryName: Name of query being pushed, will deleted first if exists
        :param inDBPath: path to database

        :return:
        """
        # Initialize the Access application
        access_app = win32com.client.Dispatch('Access.Application')

        # Open the Access database
        access_app.OpenCurrentDatabase(inDBPath)

        # Get the current database object
        db = access_app.CurrentDb()

        try:
            # Check if the query exists before attempting to delete it
            query_exists = False
            for query in db.QueryDefs:
                if query.Name == queryName:
                    query_exists = True
                    break

            if query_exists:
                # Delete the query
                db.QueryDefs.Delete(queryName)
                print(f"Query '{queryName}' has been deleted from the database.")
            else:
                print(f"Query '{queryName}' does not exist in the database.")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            # Close the database and quit Access
            access_app.CloseCurrentDatabase()
            access_app.Quit()

        # Clean up COM objects
        del access_app

    def pushQuery(inQuerySel, queryName, inDBPath):
        """
        Push SQL query defined in 'inQuerySel' to the output query 'queryName'. Uses PyWin32 library.

        :param inQuerySel: SQL Query defining the query to be pushed back to the backend instance
        :param queryName: Name of query being pushed, will deleted first if exists
        :param inDBPath: path to database

        :return:
        """
        # Initialize the Access application
        access_app = win32com.client.Dispatch('Access.Application')

        # Open the Access database
        access_app.OpenCurrentDatabase(inDBPath)

        # Get the current database object
        db = access_app.CurrentDb()

        try:
            # Create the new query
            new_query = db.CreateQueryDef(queryName, inQuerySel)
            print(f"Query '{queryName}' has been created in the database.")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            # Close the database and quit Access
            access_app.CloseCurrentDatabase()
            access_app.Quit()

        # Clean up COM objects
        del access_app

    def pushQueryODBC (inQuerySel, queryName, inDBPath):
        """
        Push SQL query defined in 'inQuerySel' to the output query 'queryName'. Using an ODBC Connection

        :param inQuerySel: SQL Query defining the query to be pushed back to the backend instance
        :param queryName: Name of query being pushed, will deleted first if exists
        :param inDBPath: path to database

        :return:
        """

        # Connect via ODBC to Access Database
        cnxn = generalDMClass.connect_DB_Access(inDBPath)

        # Create a cursor object
        cursor = cnxn.cursor()

        #Define the full query
        fullQuery = f"CREATE VIEW {queryName} AS {inQuerySel}"

        try:
            cursor.execute(fullQuery)
            cnxn.commit()
            logMsg = f"Query '{queryName}' has been created in the database."
            print(logMsg)
            logging.info(logMsg, exc_info=True)

        except Exception as e:
            print(f"Error: {e}")

        # Close the connection
        cnxn.close()

    def excuteQuery(inQuery, inDBBE):
        """
        Routine runs a defined SQL Query in the passed database, Query will be performing an 'Update', 'Append'
        or 'Make Table'.  Query is not retained in the database only is executed.
        Using PYODBC datbase connection

        :param inQuery: SQL Query defining the query to be pushed back to the backend instance
        :param inDBBE: Path to access backend database

        :return:
        """
        #Connect via ODBC to Access Database
        cnxn = generalDMClass.connect_DB_Access(inDBBE)

        try:
            # Create a cursor object
            cursor = cnxn.cursor()
            cursor.execute(inQuery)
            cnxn.commit()

        except Exception as e:
            print(f"An error occurred in execute query {e}")
            traceback.print_exc(file=sys.stdout)

        finally:
            # Close the database and quit Access
            cnxn.close()

    def queryDesc(queryName_LU, queryDecrip_LU, qcCheckInstance):
        """
        Add the query description to the passed query

        :param queryName_LU: Name of query being pushed, will deleted first if exists
        :param queryDesc: Query description to be added to the query
        :param qcCheckInstance: QC Check Instance (has Database paths, will used to define the front end query with
        the existing query.

        :return
        """

        # Initialize the Access application
        access_app = win32com.client.Dispatch('Access.Application')

        inDBPath = qcCheckInstance.inDBFE
        # Open the Access database
        access_app.OpenCurrentDatabase(inDBPath)

        # Get the current database object
        db = access_app.CurrentDb()

        # Get the query definition
        query_def = db.QueryDefs(queryName_LU)

        #Check that queryDescript_LU is less then 255 characters
        lenQueryDescription = len(queryDecrip_LU)
        if lenQueryDescription >255:
            logMsg = (f'WARNING Query Description length is - {lenQueryDescription} - must be less than 255 - existing'
                      f' script')
            print(logMsg)
            logging.error(logMsg, exc_info=True)
            exit()

        # Add the description property if it doesn't exist, or update it if it does
        try:
            query_def.Properties("Description").Value = queryDecrip_LU
        except Exception as e:
            # If the property does not exist, create it
            new_prop = query_def.CreateProperty("Description", 10, queryDecrip_LU)  # 10 is the constant for dbText
            query_def.Properties.Append(new_prop)

        # Clean up and close the database
        access_app.CloseCurrentDatabase()
        access_app.Quit()

        # Clean up COM objects
        del access_app

    def tableExistsDelete(tableName, inDBPath):
        """
        Check if table exists in the database if yes delete, using pywin32 to hit the Access COM interface, ODBC
        doesn't have permissions to hit the 'MSYS' variables

        :param tableName: Name of query being pushed, will deleted first if exists
        :param inDBPath: path to database

        :return:
        """

        # Initialize the Access application
        access_app = win32com.client.Dispatch('Access.Application')

        # Open the Access database
        access_app.OpenCurrentDatabase(inDBPath)

        # Get the current database object
        db = access_app.CurrentDb()

        try:
            # Check if the query exists before attempting to delete it
            tableExists = False
            for table in db.TableDefs:
                if table.Name == tableName:
                    tableExists = True
                    break

            if tableExists:
                # Delete the query
                db.TableDefs.Delete(tableName)
                print(f"Table '{tableName}' has been deleted from the database.")
            else:
                print(f"Table '{tableName}' does not exist in the database.")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            # Close the database and quit Access
            access_app.CloseCurrentDatabase()
            access_app.Quit()

        # Clean up COM objects
        del access_app

    def createTableFromDF(df, tableName, inDBPath):
        """
        From Passed Dataframe create new table in Access DB

        :param df: Data Frame to be created
        :param tableName: Name of table to be created
        :param inDBPath: Full path to backend database

        :return:
        """

        cnxn = generalDMClass.connect_DB_Access(inDBPath)

        # Get column names and types
        columns = df.columns
        dtypes = df.dtypes
        col_defs = []

        for column, dtype in zip(columns, dtypes):
            if dtype == 'int64':
                col_defs.append(f"[{column}] INTEGER")
            elif dtype == 'float64':
                col_defs.append(f"[{column}] DOUBLE")
            elif dtype == 'object':
                col_defs.append(f"[{column}] TEXT")
            elif dtype == 'datetime64[ns]':
                col_defs.append(f"[{column}] DATETIME")
            elif dtype == 'bool':
                col_defs.append(f"[{column}] YESNO")
            else:
                raise Exception(f"Unrecognized dtype: {dtype}")

        col_defs_str = ", ".join(col_defs)

        create_table_query = f"CREATE TABLE {tableName} ({col_defs_str})"
        # Create a cursor object
        cursor = cnxn.cursor()
        cursor.execute(create_table_query)

        # Insert data in the dataframe (i.e. df) into the created table
        for index, row in df.iterrows():
            insert_query = f"INSERT INTO {tableName} VALUES ({', '.join(['?' for _ in row])})"
            cursor.execute(insert_query, tuple(row))

        # Commit the transaction
        cnxn.commit()

        # Close the connection
        cursor.close()
        cnxn.close()

        print(f'Created Temp Table - {tableName}')

    def unZipZip(zipPath, outName, outDir):
        """
        Unzip passed zipfile to the output directory

        :param zipPath: Full path to zip file
        :param outName: Name of the output directory
        :param outDir: output Directory for unzipped files

        :return:
        """

        outZipPath = f'{outDir}\\{outName}'
        if os.path.exists(outZipPath):
            pass
        else:
            os.makedirs(outZipPath)

        # Unzip and export to the workspace
        with ZipFile(zipPath, 'r') as zip:
            print('Unzipping files')
            zip.extractall(path=outZipPath)


    def importFilesToDF(inDir):
        """
        Import files in passed folder to dataframe(s). Uses GLOB to get all files in the directory.
        Currently defined to import .csv, and .xlsx files

        :param inDir: Directory with files to be imported to dataframes

        :return:outDFDic: List of imported dataframes
        """
        #Dictionary for the output dataframes per file imported
        outDFDic = {}

        filePattern = f'{inDir}/*'
        fileList = glob.glob(filePattern)

        for file in fileList:
            file_name = os.path.splitext(os.path.basename(file))[0]
            fileType = os.path.splitext(os.path.basename(file))[1]

            if fileType == ".csv":
                df = pd.read_csv(file)
            elif fileType == ".xlsx":
                df = pd.read_excel(file)
            else:
                logMsg = f"WARNING fileType - {fileType} - is not defined - Exiting method 'importFileToDF."
                print (logMsg)
                logging.info(logMsg, exc_info=True)
                exit()

            #Add the dataframe with the filename as the key to the outDFDic
            outDFDic[file_name] = df
            logMsg = f"Successfully imported - {file_name} - to dataframe."
            print(logMsg)
            logging.info(logMsg)

        return outDFDic

    def appendDataSet(cnxn, dfToAppend, appendToTable, insertQuery, dmInstance):

        """
        Appends the pass insert query using the input data frame to append to the defined table. ODBC Connection
        is made to the defined table

        :param cnxn: ODBC database connection
        :param dfToAppend:  dataframe being appended
        :param appendToTable - table being appended to in the passed database
        :param insertQuery - query defining the append query
        :param dmInstance - data management instance

        :return:
        """

        try:

            # Create a cursor to execute SQL commands for Append
            cursor = cnxn.cursor()

            # Iterate over each row in the DataFrame and insert it into the table
            for index, row in dfToAppend.iterrows():
                values = tuple(row)
                cursor.execute(insertQuery, values)
                logMsg = f'Appended Records: {values}'
                generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.info(logMsg)
                # Commit the changes to the database
                cnxn.commit()

            # Close the cursor and database connection
            cursor.close()
            cnxn.close()

            logMsg = f'Records Successfully import to {appendToTable}'
            generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

        except Exception as e:

            logMsg = f'WARNING ERROR - "Exiting Error appendDataSet: {e}'
            generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)


    def appendDataSetwDic(cnxn, dfToAppend, appendToTable, fieldTypeDic, insertQuery, dmInstance):

        """
        Appends the pass insert query using the input data frame to append to the defined table. ODBC Connection
        is made to the defined table

        :param cnxn: ODBC database connection
        :param dfToAppend:  dataframe being appended
        :param appendToTable - table being appended to in the passed database
        :param fieldTypeDic: Field Definitions Dictionary
        :param insertQuery - query defining the append query
        :param dmInstance - data management instance

        :return:
        """

        try:

            # Create a cursor to execute SQL commands for Append
            cursor = cnxn.cursor()

            # Iterate over the dataframe rows
            for index, row in dfToAppend.iterrows():
                values_to_insert = []

                # Iterate over the fields in the dictionary
                for field, dtype in zip(fieldTypeDic['Field'], fieldTypeDic['Type']):
                    # Check if the field is numeric
                    if dtype.startswith('int') or dtype.startswith('float'):
                        value = None if pd.isna(row[field]) else row[field]
                    else:
                        value = row[field]

                    values_to_insert.append(value)

                # Debugging: Print to check the values being inserted
                print(f"Inserting: {values_to_insert}")

                # Execute the insert query
                cursor.execute(insertQuery, values_to_insert)
            # Commit the transaction
            cnxn.commit()

            # Close the connection
            cursor.close()
            cnxn.close()

            logMsg = f'Records Successfully import to {appendToTable}'
            generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

        except Exception as e:

            logMsg = f'WARNING ERROR - "Exiting Error appendDataSetwDic: {e}'
            generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def applyLookupToDFField(dmInstance, dfLookupTable, lookupField, lookupValue, dfIn, dflookupField, dfDefineField):
        """
        Define a field (i.e. dfDefineField) via a lookup table (i.e. dfLookupTable) and a join on two data frames.

        :param dmInstance: Data management Instance
        :param dfLookupTable: Imported data from the lookup table
        :param lookupField: field in the lookupTable to lookup
        :param lookupValue: field in the lookupTable with the value to be applied.
        :param dfIn: dataframe to be joined to the lookup table
        :param dflookupField: field in the dataframe to which the lookupTable join will be performed
        :param dfDefineField: field in the dataframe to which the lookupTable lookupField value will be updated/applied

        :return: outDF: Output dataframe with the updated lookupfield applied

        Updates: Updated Logic to do a join on the update field step - 20250513.
        """

        try:

            # Join via Merge Lookup data from to the passed dataframe via join fields
            merged_df = pd.merge(dfIn, dfLookupTable, how='left', left_on=dflookupField, right_on=lookupField,
                                 suffixes=("_src", "_lk"))

            lookupField_lk = f'{lookupValue}_lk'
            dfDefineField_src = f'{dfDefineField}_src'  # Field to be Updated in some scenarios
            # Update defined field with the lookup value via join on the 'dflookupField' to the 'dfIn' data frame
            if lookupField_lk in merged_df.columns:

                if dfDefineField_src in merged_df.columns:  # If the field to be updated is in both source and lookup
                    merged_df[dfDefineField_src] = merged_df[lookupField_lk]
                    # Rename field 'lookupField_src' back to 'lookupField'
                    merged_df = merged_df.rename(columns={dfDefineField_src: dfDefineField})

                elif dfDefineField in merged_df.columns:  # Normal Source, Lookup field scenario in the join table
                    merged_df[dfDefineField] = merged_df[lookupField_lk]

                else:
                    logMsg = f'Undefined Join Update Field Relationship - lookupField_y - Exiting Script'
                    logging.critical(logMsg, exc_info=True)
                    sys.exit(1)

            elif lookupValue in merged_df.columns: # Only field lookupValue once
                merged_df[dfDefineField] = merged_df[lookupField]

            else:
                logMsg = f'Undefined Join Update Field Relationship - Exiting Script'
                logging.critical(logMsg, exc_info=True)
                sys.exit(1)

            logMsg = f'Success applying lookup for lookup Field:{lookupField} - lookup Value Field: {lookupField}'
            generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            return merged_df

        except Exception as e:
            logMsg = (f'WARNING ERROR - applyLookupToDFField for lookup Field:{lookupField} - lookup Value Field:'
                      f' {lookupField} - {e}')
            generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def defineFieldTypesDF(dmInstance, fieldTypeDic, inDF):
        """
        For fields in the pass 'inDF' dataframe, set the fields in 'fieldTypeDic' to the defined field type.
        Note Date and Time fields are deferentiated with Time fields having a *Time* in the field name.
        T
        :param dmInstance: Data management Instance
        :param fieldTypeDic: Field type dictionary defining the fields and field type per field
        :param dfIn: dataframe to which the field type dictionary 'fieldTypeDic' will be applied

        :return: outDF: Dataframe with the field type crosswalk in fieldTypeDic applied
        """

        try:

            # Iterating over the dictionary to update field types
            for field, desired_type, dt_format in zip(fieldTypeDic['Field'], fieldTypeDic['Type'],
                                                     fieldTypeDic['DateTimeFormat']):
                # Check if the field exists in the dataframe and if the type does not match
                if field in inDF.columns:
                   current_type = inDF[field].dtype
                   if current_type != desired_type:
                       # Convert to the desired type
                       if desired_type == 'object':
                           inDF[field] = inDF[field].astype(str)
                       elif desired_type == 'datetime64' and dt_format != "na":
                           if dt_format == "%H:%M":
                               # Handle time conversion separately using datetime.strptime
                               inDF[field] = inDF[field].apply(
                                   lambda x: datetime.strptime(x, dt_format).time() if pd.notnull(x) else None)
                           # Date and DateTime formats - must define the dt_format to match the actual time in the
                           # survey format (e.g. if only date then %M/%D/%Y or with time included %m/%d/%Y %H:%M)
                           elif dt_format == '%m/%d/%Y':
                               # Handle date conversion
                               inDF[field] = pd.to_datetime(inDF[field], format=dt_format,
                                                                   errors='coerce')
                           elif dt_format == '%m/%d/%Y %I:%M:%S %p':
                               # Do Nothing
                               print(f'No Conversion for - {dt_format}')

                       elif desired_type == 'int64' or desired_type == 'int32':
                            inDF[field] = pd.to_numeric(inDF[field], errors='coerce', downcast='integer')

                       elif desired_type == 'float64' or desired_type == 'float32':
                           inDF[field] = pd.to_numeric(inDF[field], errors='coerce', downcast='float')

                       print(f"Field '{field}' converted from {current_type} to {desired_type} with format"
                             f" '{dt_format}'")

            # Output the updated dataframe
            print(inDF.dtypes)

            logMsg = f'Success applying field type crosswalk - defineFieldTypesDF'
            generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.info(logMsg)

            return inDF

        except Exception as e:
            logMsg = f'WARNING ERROR - applying field type crosswalk - defineFieldTypesDF-{e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def nan_to_none(x):
        """
        For passed value is Nans to Object none.  In Access Nan are imported as 1.#QNAN from panadas dataframes.
        :param x: Value being checked

        :return: x: value changed to None if was Nans
        """

        return x.astype(object).where(x.notna(), None)

    if __name__ == "__name__":
        logger.info("generalDM.py")