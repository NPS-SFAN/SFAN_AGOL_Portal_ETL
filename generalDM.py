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

logger = logging.getLogger(__name__)

class generalDMClass:

    dateNow = datetime.now().strftime('%Y%m%d')

    def __init__(self, logFile):
        """
        Define the instantiated general Data Management instantiation attributes

        :param logFile: File path and name of .txt logFile
        :return: zzzz
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

        :return:        """

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

    if __name__ == "__name__":
        logger.info("generalDM.py")