"""
SFAN_AGOL_ETL.py
Parent SFAN ArcGIS Online (AGOL) and Portal Extract, Transform and Load (ETL) script.  From parent script routines are
defined by protocl for ETL of passed feature layers (e.g. Survey 123 or Arc Field Maps) to the respective protocol
database and table schema locations.

Workflow can be accomplished connecting to AGOL/Portal via an OAuth 2.0. Conversely if you have ArcGISPro installed you
can connect via the 'Pro' python environment installed on your computer, which will use your windows/active directory
credentials to connect to AGOL/Portal thus not needing a OAuth 2.0 token with subsequent workflow.

Note: When behind the NPS firewall at the office I seemingly have to use OAuth authentication because when using the
ArcGISPro credentials with the native ArcGISPro python environment I'm not able to retain my permission (i.e. not able
to download Feature Layers I own).   Conversely, when VPN connected at home the ArcGISPro credentials work fine.

As of 8/27/2024 - Snowy Plover PORE ETL workflow has been developed - KRS.
As of 10/23/2024 - Salmonids ElecrtoFishing ETL workflow has been developed - KRS.
As of 3/25/2025 - PCM ETL of Location Manual Information to Portal developed - KRS

Output:

Python Environment: arcgispro_py3pt3 - Python 3.11, clone of the ArcGISPro 3.3 environment to all for ArcPY
pywin32

Date Developed - August 2024
Development Status - Ongoing
Created By - Kirk Sherrill - Data Scientist/Manager San Francisco Bay Area Network Inventory and Monitoring
"""

# Import Libraries
import pandas as pd
import sys
import os
import traceback
import datetime
from datetime import datetime
import ETL as etl
import generalDM as dm
import ArcGIS_API as agl
import logging
import log_config

# Get the logger
logger = logging.getLogger(__name__)

# Protocol Being Processes
protocol = 'PCM-LocationsManual'   #(SNPLPORE|Salmonids-EFish|PCM-LocationsManual)
# Access Backend Database for the protocol
inDBBE = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\PlantCommunities\Data\Database\SFAN_PlantCommunities_BE_20250326 - Copy.accdb'
inDBFE = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\PlantCommunities\Data\Database\SFAN_PlantCommunities_FE_20250326.accdb'

# Year Being Processed
inYear = 2025

#################################
# AGOL/Portal Variables to define
#################################
# Feature Layer ID on ArcGIS OnLine or Portal to be ETL
layerID = "d4e2ab1f95704d98b4174a5ba811ba80"
# URL to the AGOL or Portal Path to be processed
cloudPath = f"https://geospatial.nps.gov/portal"   #AGOL: https://nps.maps.arcgis.com, New Portal: https://geospatial.nps.gov/portal

# Define if using a OAuth2.0 credential or the credentials via the ArcGISPro Environment
credentials = 'OAuth'    # ('OAuth'|'ArcGISPro')
# If processing with OAuth2.0 define the client ID. You will be prompted to pass your client Id
pythonApp_ID = 'gkdkWQhthVDXMpOL'   # If not using define as 'na' ('client ID'|'na')
#################################

# NPS User Name of person running the QC script.  This will be populated in the 'QA_USer' field of the 'tbl_QA_Results
inUser = 'ksherrill'
from datetime import datetime
dateNow = datetime.now().strftime('%Y%m%d')
# Output Name, OutDir, Workspace and Logfile Name
outName = f'{protocol}_{inYear}_{dateNow}'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\PlantCommunities\Data\PlotLocationManuals\2025'  # Directory Output Location

# Variable defines if the AGOL Feature layers needs to be downloaded, if 'No' then you are doing development and do not
# want/need to download each run of script, Hard Coded paths will need to be updates in the 'ArcGIS_API.py' -
# processFeatureLayer method when set to 'No'
AGOLDownload = 'Yes'  # ('Yes'|'No')

def main():
    logger = logging.getLogger(__name__)

    try:
        # Set option in pandas to not allow chaining (views) of dataframes, instead force copy to be performed.
        pd.options.mode.copy_on_write = True

        #Close any open Access Databases on the computer
        dm.generalDMClass.closeAccessDB()

        # Logfile will be saved in the workspace directory which is child of the fileDir - this is in addition to the
        # logger file 'ScriptProcessingError.log being created by the 'logger' configuration file via python.
        logFile = dm.generalDMClass.createLogFile(logFilePrefix=outName, workspaceParent=outDir)

        # Create the data management instance to  be used to define the logfile path and other general DM attributes
        dmInstance = dm.generalDMClass(logFile)

        ###############
        # Define the etlInstance and dmInstance instances
        ################

        # Create the etlInstance instance
        etlInstance = etl.etlInstance(protocol=protocol, inDBBE=inDBBE, inDBFE=inDBFE, flID=layerID, yearLU=inYear,
                                      inUser=inUser, outDir=outDir, AGOLDownload=AGOLDownload)
        # Print the name space of the instance
        print(etlInstance.__dict__)

        # Create the generalArcGIS instance
        generalArcGIS = agl.generalArcGIS(layerID=layerID, cloudPath=cloudPath, credentials=credentials,
                                          pythonApp_ID=pythonApp_ID)
        # Print the name space of the instance
        print(generalArcGIS.__dict__)

        ###############
        # Proceed to the Workflow to process the defined ETL Routines
        ################

        # Go to ETL Processing Routines
        etl.etlInstance.process_ETLRequest(generalArcGIS=generalArcGIS, etlInstance=etlInstance, dmInstance=dmInstance)

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

    #################################
    # Checking for working directories and Log File
    ##################################
    workspace = f"{outDir}\\workspace"
    if os.path.exists(workspace):
        pass
    else:
        os.makedirs(workspace)

    # Run Main Code Bloc
    main()
