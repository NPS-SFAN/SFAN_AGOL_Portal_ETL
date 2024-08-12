"""
ETL.py
Extract Transform and Load (ETL) Methods/Functions to be used for general AGOL/Portal ETL workflow.
"""
#Import Required Dependices
import os, sys, traceback
import generalDM as dm
import ETL_SNPLPORE as SNPLP
import ArcGIS_API as agl
import logging

logger = logging.getLogger(__name__)

class etlInstance:
    # Class Variables
    numETLInstances = 0

    def __init__(self, protocol, inDBBE, flID, yearLU, inUser):
        """
        Define the instantiated etlInstance attributes
        
        :param protocol: Name of the Protocol being processes
        :param inDBBE: Protocol Backend Access database full path
        :param yearLU: Year being processed
        :param flID: Feature Layer ID
        :param inUser: NPS UserNam
        
        :return: instantiated self object
        """

        self.protocol = protocol
        self.inDBBE = inDBBE
        self.flID = flID
        self.yearLU = yearLU
        self.inUser = inUser

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

            # Pull the Feature Layer for the defined  return as dataframe(s) in list variable outDFList
            outDFList = agl.generalArcGIS.processFeatureLayer(generalArcGIS, etlInstance, dmInstance)





            # Create the protocol specific ETL instance
            if etlInstance.protocol == 'SNPLPORE':
                etlProtocolInstance = SNPLP.etl_SNPLPORE()
            else:
                logMsg = f"WARNING Protocol Specific Instance - {etlInstance.protocol} - has not been defined."
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.critical(logMsg, exc_info=True)
                traceback.print_exc(file=sys.stdout)
                sys.exit()






        except Exception as e:

            logMsg = (f'ERROR - An error occurred process_ETLRequest: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)