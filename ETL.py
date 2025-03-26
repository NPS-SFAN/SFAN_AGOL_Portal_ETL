"""
ETL.py
Extract Transform and Load (ETL) Methods/Functions to be used for general AGOL/Portal ETL workflow.
"""
#Import Required Dependices
import os, sys, traceback
import generalDM as dm
import ArcGIS_API as agl
import logging
import ETL_SNPLPORE as SNPLP
import ETL_Salmonids_Electro as SEfish
import ETL_PCM_LocationsManualParking as PCMLOC
import log_config


logger = logging.getLogger(__name__)

class etlInstance:
    # Class Variables
    numETLInstances = 0

    def __init__(self, protocol, inDBBE, inDBFE, flID, yearLU, inUser, outDir, AGOLDownload):
        """
        Define the instantiated etlInstance attributes
        
        :param protocol: Name of the Protocol being processes
        :param inDBBE: Protocol Backend Access database full path
        :param inDBFE: Protocol Frontend Access database full path - used in PCM ETL Locations workflow.
        :param yearLU: Year being processed
        :param flID: Feature Layer ID
        :param inUser: NPS UserNam
        :param outDir: Output directory
        :param AGOLDownload: Define if the AGOL/Portal Feature Layers need to be download, used in when developing code.
        
        :return: instantiated self object
        """

        self.protocol = protocol
        self.inDBBE = inDBBE
        self.inDBFE = inDBFE
        self.flID = flID
        self.yearLU = yearLU
        self.inUser = inUser
        self.outDir = outDir
        self.AGOLDownload = AGOLDownload

        # Update the Class Variable
        etlInstance.numETLInstances += 1


    def process_ETLRequest(generalArcGIS, etlInstance, dmInstance):

        """
        General ETL workflow processing workflow steps.

        :param generalArcGIS: ArcGIS/Portal workflow instance
        :param etlInstance: ETL workflow instance
        :param dmInstance: data management instance which will have the logfile name

        :return:
        """

        try:
            #Configure Logging:
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

            # Create the protocol specific ETL instance
            if etlInstance.protocol == 'SNPLPORE':
                # Pull the Feature Layer for the defined return as dataframe(s) in list variable outDFList
                outDFDic = agl.generalArcGIS.processFeatureLayer(generalArcGIS, etlInstance, dmInstance)

                outETL = SNPLP.etl_SNPLPORE.process_ETLSNPLPORE(outDFDic, etlInstance, dmInstance)

            elif etlInstance.protocol == 'Salmonids-EFish':
                # Pull the Feature Layer for the defined return as dataframe(s) in list variable outDFList
                outDFDic = agl.generalArcGIS.processFeatureLayer(generalArcGIS, etlInstance, dmInstance)
                outETL = SEfish.etl_SalmonidsElectro.process_ETLElectro(outDFDic, etlInstance, dmInstance)

            elif etlInstance.protocol == 'PCM-LocationsManual':
                outETL = PCMLOC.etl_PCMLocations.process_PCMLocManual(etlInstance, dmInstance)

            else:
                logMsg = f"WARNING Protocol Specific Instance - {etlInstance.protocol} - has not been defined."
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.critical(logMsg, exc_info=True)
                traceback.print_exc(file=sys.stdout)
                sys.exit()

        except Exception as e:

            logMsg = f'ERROR - An error occurred process_ETLRequest: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg)
            traceback.print_exc(file=sys.stdout)