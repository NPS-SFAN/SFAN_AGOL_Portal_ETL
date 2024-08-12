"""
SFAN_AGOL_ETL.py
Parent SFAN ArcGIS Online (AGOL) and Portal Extract, Transform and Load (ETL) script.  From parent script routines are
defined by protocl for ETL of passed feature layers (e.g. Survey 123 or Arc Field Maps) to the respective protocol
database and table schema locations.

Output:

Python Environment: SFAN_AGOLEETL - Python 3.11
pywin32

Date Developed - August 2024
Created By - Kirk Sherrill - Data Scientist/Manager San Francisco Bay Area Network Inventory and Monitoring
"""

# Import Libraries
import pandas as pd
import pyodbc
import sys
import os
import session_info
import traceback
from datetime import datetime
import QC_Checks as qc
import generalDM as dm
import logging


# Get the logger
logger = logging.getLogger(__name__)

# Protocol Being Processes
protocol = 'SNPLPORE'   #(SNPLPORE|Salmonids|...)
# Access Backend Database for the protocol
inDBBE = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\SnowyPlovers_PORE\SNPLOVER\SNPL_IM\Data\Database\Dbase_BE\PORE_SNPL_BE_20240806.accdb'
# Access FrontEnd Database for the protocol
inDBFE = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\SnowyPlovers_PORE\SNPLOVER\SNPL_IM\Data\Database\PORE_SNPL_FrontEnd_20240806.accdb'
# Year Being Processed
inYear = 2023
# NPS User Name of person running the QC script.  This will be populated in the 'QA_USer' field of the 'tbl_QA_Results
inUser = 'ksherrill'

dateNow = datetime.now().strftime('%Y%m%d')
# Output Name, OutDir, Workspace and Logfile Name
outName = f'{protocol}_{inYear}_{dateNow}'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\SnowyPlovers_PORE\SNPLOVER\SNPL_IM\Data\ETL\2024'  # Directory Output Location

def main():
    logger = logging.getLogger(__name__)

    try:
        session_info.show()
        # Set option in pandas to not allow chaining (views) of dataframes, instead force copy to be performed.
        pd.options.mode.copy_on_write = True

        #Close any open Access Databases on the computer
        dm.generalDMClass.closeAccessDB()

        ###############
        # Define the etlInstance and dmInstance instances
        ################

        # Create the etlInstance instance
        etlInstance = qc.qcChecks(protocol=protocol, inDBBE=inDBBE, inDBFE=inDBFE, yearLU= inYear, inUser = inUser)

        # Print out the name space of the instance
        print(qcCheckInstance.__dict__)

        # Logfile will be saved in the workspace directory which is child of the fileDir - this is in addition to the
        # logger file 'ScriptProcessingError.log being created by the 'logger' configuration file via python.
        logFile = dm.generalDMClass.createLogFile(logFilePrefix=outName, workspaceParent=outDir)

        # Create the data management instance to  be used to define the logfile path and other general DM attributes
        dmInstance = dm.generalDMClass(logFile)

        ###############
        # Proceed to the Workflow to process the defined data validation routines
        ################

        # Go to QC Processing Routines
        qc.qcChecks.process_QCRequest(qcCheckInstance=qcCheckInstance, dmInstance=dmInstance)

        # Message Script Completed
        logMsg = f'Successfully Finished All QC Checks for - {protocol}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.info(logMsg)

    except Exception as e:

        logMsg = f'ERROR - "Exiting Error - QCChecks.py: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)

    finally:
        exit()

def timeFun():
    try:
        b = datetime.now()
        messageTime = b.isoformat()
        return messageTime
    except:
        print(f'Failed - timeFun')
        exit()


if __name__ == '__main__':

    #################################
    # Checking for Out Directories and Log File
    ##################################
    if os.path.exists(outDir):
        pass
    else:
        os.makedirs(outDir)

    # Run Main Code Bloc
    main()
