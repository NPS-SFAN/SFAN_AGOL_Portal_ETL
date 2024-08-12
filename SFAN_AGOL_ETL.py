"""
SFAN_AGOL_ETL.py
Parent SFAN ArcGIS Online (AGOL) and Portal Extract, Transform and Load (ETL) script.  From parent script routines are
defined by protocl for ETL of passed feature layers (e.g. Survey 123 or Arc Field Maps) to the respective protocol
database and table schema locations.

Output:

Python Environment: SFAN_AGOLEETL - Python 3.9, clone of the ArcGISPro 3.2 environment to all for ArcPY
pywin32

Date Developed - August 2024
Created By - Kirk Sherrill - Data Scientist/Manager San Francisco Bay Area Network Inventory and Monitoring
"""

# Import Libraries
import pandas as pd
import sys
import os
import session_info
import traceback
from datetime import datetime
import ETL as etl
import generalDM as dm
import ArcGIS_API as agl
import logging


# Get the logger
logger = logging.getLogger(__name__)

# Protocol Being Processes
protocol = 'SNPLPORE'   #(SNPLPORE|Salmonids|...)
# Access Backend Database for the protocol
inDBBE = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\SnowyPlovers_PORE\SNPLOVER\SNPL_IM\Data\Database\Dbase_BE\PORE_SNPL_BE_20240812 - Copy.accdb'

# Year Being Processed
inYear = 2023

#################################
# AGOL/Portal Variables to define
#################################
# Feature Layer ID on ArcGIS OnLine or Portal to be ETL
layerID = "d4e2ab1f95704d98b4174a5ba811ba80"
# URL to the AGOL or Portal Path to be processed
cloudPath = "https://nps.maps.arcgis.com"   #AGOL: https://nps.maps.arcgis.com, Portal: https://gisportal.nps.gov/portal

# Define if using a OAuth2.0 credential or the credentials via the ArcGISPro Environment
credentials = 'OAuth'    # ('OAuth'|'ArcGISPro')
# If processing with OAuth2.0 define the client ID. You will be prompted to pass your client Id
pythonApp_ID = 'VFfN107sG4W47jXo'   # If not using define as 'na' ('client ID'|'na')
# Python Environment - applicable if processing with ArcGISPro environment credentials, else will not be used
agolEnv = r'C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe'
#################################

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
        etlInstance = etl.etlInstance(protocol=protocol, inDBBE=inDBBE, yearLU=inYear, inUser=inUser)
        # Print the name space of the instance
        print(etlInstance.__dict__)

        # Create the generalArcGIS instance
        generalArcGIS = agl.generalArcGIS(agolEnv=agolEnv, layerID=layerID, cloudPath=cloudPath, credentials=credentials,
                                          pythonApp_ID=pythonApp_ID)
        # Print the name space of the instance
        print(generalArcGIS.__dict__)

        # Logfile will be saved in the workspace directory which is child of the fileDir - this is in addition to the
        # logger file 'ScriptProcessingError.log being created by the 'logger' configuration file via python.
        logFile = dm.generalDMClass.createLogFile(logFilePrefix=outName, workspaceParent=outDir)

        # Create the data management instance to  be used to define the logfile path and other general DM attributes
        dmInstance = dm.generalDMClass(logFile)

        ###############
        # Proceed to the Workflow to process the defined ETL Routines
        ################

        # Go to QC Processing Routines
        etl.etlInstance.process_ETLRequest(etlInstance=etlInstance, dmInstance=dmInstance)



        # Message Script Completed
        logMsg = f'Successfully Finished ETL Routine for - {protocol}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.info(logMsg)

    except Exception as e:

        logMsg = f'ERROR - "Exiting Error - SFAN_AGOL_ETL.py: {e}'
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
