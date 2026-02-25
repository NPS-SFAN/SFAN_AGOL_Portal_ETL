"""
ArcGIS_API.py
Methods for working within AGOL/Portal and the ArcGIS API.
"""
#Import Required Dependices
import os, sys, traceback
import generalDM as dm
import logging
import arcgis
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
import pandas as pd

class generalArcGIS:

    numArcGISInstances = 0

    def __init__(self, layerID, cloudPath, credentials, pythonApp_ID):

        """
        Define the instantiated generalArcGIS instantiation attributes
    
        :param layerID: AGOL/Portal ID reference value
        :param cloudPath: URL to the NPS or Portal 
        :param credentials: Defines if using OAuth 2.0 or ArcGISPro credentials
        :param pythonApp_ID: If using OAuth 2.0 will need to pass the client Id value
                 
         
        :return: generalArcGIS instance with passed variable definitions and defined methods
        """

        self.layerID = layerID
        self.cloudPath = cloudPath
        self.credentials = credentials
        self.pythonApp_ID = pythonApp_ID

        generalArcGIS.numArcGISInstances += 1

    def processFeatureLayer(generalArcGIS, etlInstance, dmInstance):
        """
        Workflow for processing/Pulling from AGOL/Portal the passed AGOL/Portal ID

        :param generalArcGIS: ArcGIS instance
        :param etlInstance: ETL processing instance
        :param dmInstance: Data Management instance

        :return: outDFDic - Dictionary with all imported dataframes from the imported feature layer
        """

        try:

            if etlInstance.AGOLDownload == 'Yes':

                # Connect to the Cloud via passed credentials workflow
                if generalArcGIS.credentials.lower() == 'oauth':
                    outGIS = connectAGOL_clientID(generalArcGIS=generalArcGIS, dmInstance=dmInstance)
                # Connect via ArcGISPro Environment
                else:
                    outGIS = connectAGOL_ArcGIS(generalArcGIS=generalArcGIS, dmInstance=dmInstance)

                # Import the feature layer
                outFeatureLayer = importFeatureLayer(outGIS, generalArcGIS, etlInstance, dmInstance)
                outzipPath = outFeatureLayer[0]
                outName = outFeatureLayer[1]

            # Use when developing - don't need to download the AGOl data each time
            elif etlInstance.AGOLDownload == 'No':
                # Hard Code the Imported AGOL/Portal data when debuging - turn off lines 53-63 above - crude I know.
                outzipPath = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\SnowyPlovers_PORE\SNPLOVER\SNPL_IM\Data\ETL\2025\Survey2025v1.2\SFAN_SNPLPORE_Survey2025v1.2_20250930-152732.zip'
                outName = 'SFAN_SNPLPORE_Survey2025v1.2_20250930-152732'

            # Extract Exported zip file and import .csv files to DBF files
            dm.generalDMClass.unZipZip(zipPath=outzipPath, outName=outName,outDir=etlInstance.outDir)
            # Path to Unzipped files
            fullPathZipped = f"{etlInstance.outDir}\\{outName}"
            # Import Extracted Files to Dataframes
            outDFDic = dm.generalDMClass.importFilesToDF(inDir=fullPathZipped)

            return outDFDic

        except Exception as e:

            logMsg = f'ERROR - processFeatureLayer - {generalArcGIS.cloudPath} - {etlInstance.flID}: {e}'
            print(logMsg)
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg)
            traceback.print_exc(file=sys.stdout)

    def download_layer_attachments(gis, item_id, out_Folder, layer_name, where="1=1"):
        """
        Download photo attachments from a specific layer.

        :param gis: Authenticated GIS object
        :param item_id: Hosted feature layer collection item ID
        :param out_Folder: Output folder for the imported photos
        :param layer_name: Layer name (e.g., 'NestRepeats')
        :param where: SQL filter required


        :return outDFPhotosDF - Dataframe define the photos that have been processed
        """
        try:
            if not os.path.exists(out_Folder):
                os.makedirs(out_Folder)

            flc_item = gis.content.get(item_id)
            flc = FeatureLayerCollection.fromitem(flc_item)

            # Find target layer
            target_layer = next(
                (lyr for lyr in flc.layers if lyr.properties.name == layer_name),
                None)

            if not target_layer:
                raise ValueError(f"Layer '{layer_name}' not found.")

            # Dataframe to be populated with the ID, PhotoName, and parentGlobalID - will join on Photo Nest to
            # define the records to be created in the 'tbl_Nest_Photos' table.
            outDFPhotosDF = pd.DataFrame(columns=['ID', 'tempPhotoName', 'ParentGlobalID'])

            # List to Hold the photo record values exported to a photo
            rows = []

            # Query features
            features = target_layer.query(where=where, out_fields="*").features

            for feature in features:
                oid = feature.attributes[target_layer.properties.objectIdField]
                attachments = target_layer.attachments.get_list(oid)

                # If attachment present download
                for att in attachments:
                    att_id = att["id"]
                    photoName1 = att["name"]
                    photoName = f'{att_id}_{photoName1}'
                    parentGlobalId = att['parentGlobalId']

                    downloaded_path = target_layer.attachments.download(
                        oid=oid,
                        attachment_id=att_id,
                        save_path=out_Folder)

                    # Build desired file name
                    new_path = os.path.join(out_Folder, photoName)
                    # List to String
                    dPathStr = downloaded_path[0]

                    # Rename file
                    os.rename(dPathStr, new_path)

                    # Add values to rows
                    rows.append({
                        'ID': att_id,
                        'tempPhotoName': photoName,
                        'ParentGlobalID': parentGlobalId
                    })

                    logMsg = f'Successfully downloaded photo - {photoName} - {parentGlobalId}'
                    print(logMsg)

            # If df already exists with same columns:
            outDFPhotosDF = pd.concat([outDFPhotosDF, pd.DataFrame(rows)], ignore_index=True)

            return outDFPhotosDF

        except Exception as e:

            logMsg = f'WARNING ERROR  - ArcGIS_API.py - download_layer_attachments: {e}'
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

def importFeatureLayer(outGIS, generalArcGIS, etlInstance, dmInstance):
    """
    Workflow for processing of the passed AGOL/Portal ID

    :param outGIS: GIS Connection
    :param generalArcGIS: ArcGIS instance
    :param etlInstance: ETL processing instance
    :param dmInstance: Data Management instance

    :return: outZipFull: full path to zip file of imported layer for AGOL/Portal
    dataTitle: tile of the feature layer that was imported
    """

    try:
        layerIDLU = generalArcGIS.layerID

        # Pull the desired AGOL content via the AGOL ID
        item = outGIS.content.get(layerIDLU)

        # Define DateTile for Feature Layer being exported
        dataTitle = item.title

        # Define Unique Name for item being exported - this will be exported to your AGOL Content home page thus
        # a unique name is desirable.
        from datetime import datetime
        dateTimeNow = datetime.now().strftime('%Y%m%d-%H%M%S')
        dataTileUnique = f'{item.title}_{dateTimeNow}'

        outZipFull = f'{etlInstance.outDir}\\{dataTileUnique}.zip'
        result = item.export(dataTileUnique, export_format='CSV', wait=True)

        outWorkDir = f'{etlInstance.outDir}'
        # Delete outZipFull if exists
        if os.path.exists(outZipFull):
            os.remove(outZipFull)
            logMsg = f'Deleted existing zip - {outZipFull}'
            print(logMsg)
            logging.info(logMsg)

        # Export Result to the zip file
        result.download(outWorkDir)

        # Add Log Messages
        logMsg = f'Successfully Downloaded from - {generalArcGIS.cloudPath} - {dataTileUnique}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.info(logMsg)
        traceback.print_exc(file=sys.stdout)

        return outZipFull, dataTileUnique

    except Exception as e:

        logMsg = f'ERROR - Downloading from - {generalArcGIS.cloudPath} - {etlInstance.flID}: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg)
        traceback.print_exc(file=sys.stdout)


def loadDataFrameToFeatureLayer(inDF, inDic, outGIS, etlPCMInstance):
    """
    Load the passed dataframe as a Feature_Layer to the defined AGOL/Portal Group. as the defined Feature

    :param inDF: Dataframe being processed
    :param inDic: Dictionary defining the Feature Layer Properties being pushed
    :param outGIS: GIS Connection to AGOL/Portal
    :param etlPCMInstance: ETL PCM Instance - use to define output folder and other info as needed

    :return: Return String denoting success or failure.
    """

    try:
        # Check if Feature Layer Exists, if yes delete
        featureLayerName = inDic.get("title")
        existing_items = outGIS.content.search(f'title:"{featureLayerName}"', item_type="Feature Layer")
        for item in existing_items:
            item.delete()  # Delete old layers
            logMsg = f'Deleted existing Feature Layer - {featureLayerName}'
            logging.info(logMsg)

        # Create the Feature Layer item
        feature_layer_item = outGIS.content.import_data(inDF, folder=etlPCMInstance.Folder)
        feature_layer_item.update(inDic)

        print(feature_layer_item.title)
        print(feature_layer_item.description)
        print(feature_layer_item.tags)

        #####################################
        # Define the Group to be shared with.
        #####################################
        # Get the Group ID via the Group Name
        group_search = outGIS.groups.search(etlPCMInstance.PortalTeam)
        if group_search:
            group_id = group_search[0].id  # Get the first matching group ID

            # Share theFeature Layer with the Group
            feature_layer_item.share(groups=[group_id])
            logMsg = f'Shared {group_search[0].title} with - {etlPCMInstance.PortalTeam}'
            logging.info(logMsg)

        else:
            logMsg = f'WARNING Group {etlPCMInstance.PortalTeam} - not found.'
            logging.warning(logMsg)

        #####################################
        # Rename the first Layer name to the title (assuming we are creating single feature (i.e. point, line) feature
        # layers.
        #####################################
        if feature_layer_item.layers:
            point_layer = feature_layer_item.layers[0]  # Get the first (child) layer
            new_layer_name = featureLayerName  # Define the new name

            update_params = {
                "name": new_layer_name
            }

            point_layer.manager.update_definition(update_params)  # Apply the name change

            print(f"Child Point Layer Renamed to: {new_layer_name}")
        else:
            print("No child layers found in the Feature Layer.")

        logMsg = f'Successfully Created Feature Layer: {feature_layer_item.title}'
        logging.info(logMsg)
        traceback.print_exc(file=sys.stdout)
        return logMsg

    except Exception as e:

        logMsg = f'ERROR - "Exiting Error loadDataFrameToFeatureLayer - ArcGIS_API.py: {e}'
        logging.critical(logMsg)
        traceback.print_exc(file=sys.stdout)


def connectAGOL_ArcGIS(generalArcGIS, dmInstance):
    """
    Connect to defined AGOL or Portal Path via the ArcGISPro Environment which will use the NPS AD credentials.
    This is an alternative to processing an Python Application ID in meethod 'connecdtAGOL_clientID'.

    Full Path to the ArcGISPro Python Environment .exe on your local computer often will be
     C:/Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3/python.exe.  If you are behind the VPN you can simply
     connect to the 'Pro' environment.

    :param generalArcGIS: generalArcGIS instance
    :param dmInstance: dmInstance instance

    :return: Return GIS Connection
    """
    pathToAGOL = generalArcGIS.cloudPath

    try:

        # Not Sure how to get a defined path for 'AGOL' or 'Portal' and include the '
        #gis = GIS(pathToAGOL, None, None, None, None, False, True,
        #          None, None, 'Pro')
        #gis = GIS(url=pathToAGOL, username='Pro')
        gis = GIS('Pro')

        print(f"Successfully connected to - {pathToAGOL}")

        return gis

    except Exception as e:

        logMsg = f'ERROR - "Exiting Error connectAGOl_ArcGIS - ArcGIS_API.py: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)


def connectAGOL_clientID(generalArcGIS, dmInstance):

    """
    Connect to defined AGOL or Portal Path via a passed Python Application OAuth 2.0 Credential

    :param generalArcGIS: generalArcGIS instance
    :param dmInstance: dmInstance instance

    :return: Return GIS Connection to AGOL or Portal for the current user
    """

    pathToAGOL = generalArcGIS.cloudPath
    pythonID = generalArcGIS.pythonApp_ID
    try:
        gis = GIS(pathToAGOL, client_id=pythonID)
        print(f"Successfully connected to - {pathToAGOL}")

        return gis
    except Exception as e:

        logMsg = f'ERROR - "Exiting Error connectAGOl_clientID - ArcGIS_API.py: {e}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
        logging.critical(logMsg, exc_info=True)
        traceback.print_exc(file=sys.stdout)


