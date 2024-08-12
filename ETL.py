"""
ETL.py
Extract Transform and Load (ETL) Methods/Functions to be used for general AGOL/Portal ETL workflow.
"""
#Import Required Dependices
import pandas as pd
import glob, os, sys, traceback
import generalDM as dm
import QC_Checks_SNPLPORE as SNPLP
import logging
import log_config

logger = logging.getLogger(__name__)

class etlInstance:
    # Class Variables
    numETLInstances = 0


    def__init__(self, protocol, inDBBE, inDBFE, yearLU, inUser):
    """
    Define the instantiated etlInstance attributes
    
    :param protocol: Name of the Protocol being processes
    :param inDBBE: Protocol Backend Access database full path
    :param inDBFE: Protocol Frontend Access database full path
    :param yearLU: Year being processed
    :param inUser: NPS UserNam
    
    :return: instantiated self object
    """

    self.protocol = protocol
    self.inDBBE = inDBBE
    self.inDBFE = inDBFE
    self.yearLU = yearLU
    self.inUser = inUser

    # Update the Class Variable
    etlInstance.numETLInstances += 1


    def process_ETLRequest(etlInstance, dmInstance):

        """
        General ETL workflow processing workflow steps.

        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return:
        """

        try:
            #Configure Logging:
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

            # Create the protocol specific ETL instance
            if etlInstance.protocol == 'SNPLPORE':
                etlProtocolInstance = SNPLP.ETLProtcol_SNPLPORE()
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