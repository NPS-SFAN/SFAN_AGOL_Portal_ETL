"""
ETL_QCValidation.py
Extract Transform and Load (ETL) Quality Control Validation Routines.
"""
#Import Required Dependices
import os, sys, traceback
import generalDM as dm
import ArcGIS_API as agl
import logging
import ETL_SNPLPORE as SNPLP
import ETL_Salmonids_Electro as SEfish
import ETL_Salmonids_Smolts as SSmolt
import ETL_PCM_LocationsManualParking as PCMLOC
import ETL_PINN_Elephant as PElephant
from datetime import datetime

class etlInstance_QC:
    # Class Variables
    numETLInstances = 0

    def __init__(self):
        """
        Define the QC Protocol instantiation attributes

        :param TBD
        :return: zzzz
        """

        # Define Instance Variables
        numETLInstances += 1
    def process_ETLValidation(etlInstance, dmInstance, inDF, qcValidationFields):

        """
        Setup Method to process QC Validation routines for the pass fields by dataset/monitoring component.

        :param etlInstance: ETL workflow instance
        :param dmInstance: data management instance which will have the logfile name\
        :param inDF: List with QC Validation Fields to be processed
        :param qcValidationFields: List with QC Validation Fields to be processed

        :return: outDF: Passed inDF that has been through the QC validation checks
        """

        try:

            # Iterate through the fields to be QC validated - New 2/9/2026 - Needs to be tested
            outDF = inDF  # start with the original
            changed_any = False  # track whether anything changed

            for field in qcValidationFields:

                if field == 'LengthCategoryID':
                    outDF_new, changed = etlInstance_QC.qc_LengthCategoryID(
                        etlInstance, dmInstance, outDF, field
                    )

                elif field == 'FishWeight':
                    outDF_new, changed = etlInstance_QC.qc_FishWeight(
                        etlInstance, dmInstance, outDF, field
                    )

                else:
                    logMsg = (
                        f"WARNING QC validation failed for "
                        f"{etlInstance.protocol} - field - {field}."
                    )
                    dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                    logging.critical(logMsg, exc_info=True)
                    sys.exit()

                # only version if something actually changed
                if changed:
                    outDF = outDF_new
                    changed_any = True

            return outDF

        except Exception as e:

            logMsg = f'ERROR - An error occurred ETL_QCValidation: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg)
            traceback.print_exc(file=sys.stdout)

    def qc_LengthCategoryID(etlInstance, dmInstance, inDF, field):

        """
        QC Validation for LengthCategoryID fields in the Summer Measurements and Smolt Measurement tables.
        Lookup table 'tluLengthCategories' review via lookup to confirm the correct LengthCatID value is entered.

        :param etlInstance: ETL workflow instance
        :param dmInstance: data management instance which will have the logfile name\
        :param inDF: List with QC Validation Fields to be processed
        :param field: Field in 'inDF' being validated

        :return:
        """

        try:

            # Read in the 'tluLengthCategories' lookup table
            # Read in the tluDevices lookup table
            # Import Event Table to define the EventID via the GlobalID
            inQuery = f"SELECT tluLengthCategories.* FROM tluLengthCategories;"

            # Import Event Table with defined EventID
            tluLengthCategories_DF = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, etlInstance.inDBBE)

            # Via the 'ForkLength' variable lookup the 'LengthCategoryID' value
            outDFwLookUp = etlInstance_QC.lookup_length_category_id(tluLengthCategories_DF, inDF)

            # Variables to push
            flag = "CFCETL"
            note = "Updated - LengthCategoryID during initial ETL QC Validation"
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # rows needing correction
            mask_update = (
                    outDFwLookUp["LengthCategoryID"] != outDFwLookUp["LengthCategoryID_Lookup"]
            )

            countUpdate = mask_update.sum()

            if countUpdate >= 1:

                # update the corrected value
                outDFwLookUp.loc[mask_update, "LengthcategoryID"] = inDF.loc[
                    mask_update, "LengthCategoryID_LU"
                ]

                # ---- QCField ----
                outDFwLookUp.loc[mask_update, "QCFlag"] = (
                    outDFwLookUp.loc[mask_update, "QCFlag"]
                    .fillna(flag)
                    .where(
                        outDFwLookUp.loc[mask_update, "QCFlag"].isna(),
                        outDFwLookUp.loc[mask_update, "QCFlag"] + ";" + flag
                    )
                )

                # ---- QCNotes ----
                outDFwLookUp.loc[mask_update, "QCNotes"] = (
                    outDFwLookUp.loc[mask_update, "QCNotes"]
                    .fillna(f"{note} - {now}")
                    .where(
                        outDFwLookUp.loc[mask_update, "QCNotes"].isna(),
                        outDFwLookUp.loc[mask_update, "QCNotes"] + " | " + f"{note} - {now}"
                    )
                )

                logMsg = (f'WARNING - Updated - {countUpdate} - Length Category ID records in '
                          f'- {etlInstance.protocol} - QC validation - qc_LengthCategoryID')
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.critical(logMsg)

            else:
                logMsg = (f'No Updates - for Length Category ID records in '
                          f'- {etlInstance.protocol} - QC validation - qc_LengthCategoryID')

                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.info(logMsg)

            return outDFwLookUp

        except Exception as e:

            logMsg = f'ERROR - ETL_QCValidation - qc_LengthCategoryID: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg)
            traceback.print_exc(file=sys.stdout)

    def lookup_length_category_id(tluLengthCategories_DF, InDF):
        """
        Return LengthCategoryID for scalar field that Field is >= Low and Field <= High.
        Returns None if field is NaN or doesn't fall in any interval.
        """

        import numpy as np
        import pandas as pd

        # Ensure numeric types (optional but recommended)
        InDF["ForkLength"] = pd.to_numeric(InDF["ForkLength"], errors="coerce")
        tlu = tluLengthCategories_DF.copy()
        tlu["Low"] = pd.to_numeric(tlu["Low"], errors="coerce")
        tlu["High"] = pd.to_numeric(tlu["High"], errors="coerce")

        # Numpy arrays for vectorized computation
        fork = InDF["ForkLength"].to_numpy()
        low = tlu["Low"].to_numpy()
        high = tlu["High"].to_numpy()
        cats = tlu["LengthCategoryID"].to_numpy()

        # Build an (n_fish x n_categories) matrix of membership
        # True if ForkLength is within [Low, High] (inclusive)
        membership = (fork[:, None] >= low[None, :]) & (fork[:, None] <= high[None, :])

        # Handle matches:
        has_match = membership.any(axis=1)  # at least one category matches
        first_match_idx = membership.argmax(axis=1)  # take the first match (leftmost)

        # Create the result, NaN where no match
        result = np.where(has_match, cats[first_match_idx], np.nan)

        # Assign back to InDF
        InDF["LengthCategoryID_Lookup"] = result

        # Replace NaN with None in LengthCategoryID
        InDF["LengthCategoryID_Lookup"] = InDF["LengthCategoryID_Lookup"].where(
            InDF["LengthCategoryID_Lookup"].notna(), None)

        InDF = InDF.replace([np.nan, 'nan'], None)

        return InDF

    def qc_FishWeight(etlInstance, dmInstance, inDF, field):

        """
        QC Validation for Fish Weight field. Fish Weight is Total Weight - Bag Weight

        :param etlInstance: ETL workflow instance
        :param dmInstance: data management instance which will have the logfile name\
        :param inDF: List with QC Validation Fields to be processed
        :param field: Field in 'inDF' being validated

        :return:
        """

        try:

            # Variables to push if QC validation fails
            flag = "CFCETL"
            note = "Updated - Fish Weight during initial ETL QC Validation"
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # rows needing correction
            mask_update = (
                    inDF["FishWeight"].notna() &
                    inDF["TotalWeight"].notna() &
                    inDF["BagWeight"].notna() &
                    (inDF[field] != (inDF["TotalWeight"] - inDF["BagWeight"]))
            )

            countUpdate = mask_update.sum()

            if countUpdate >= 1:

                # Calculate FishWeight = TotalWeight - BagWeight for the rows in mask_update
                inDF.loc[mask_update, field] = (
                        inDF.loc[mask_update, "TotalWeight"] - inDF.loc[mask_update, "BagWeight"]
                )

                # ---- QCField ----
                inDF.loc[mask_update, "QCFlag"] = (
                    inDF.loc[mask_update, "QCFlag"]
                    .fillna(flag)
                    .where(
                        inDF.loc[mask_update, "QCFlag"].isna(),
                        inDF.loc[mask_update, "QCFlag"] + ";" + flag
                    )
                )

                # ---- QCNotes ----
                inDF.loc[mask_update, "QCNotes"] = (
                    inDF.loc[mask_update, "QCNotes"]
                    .fillna(f"{note} - {now}")
                    .where(
                        inDF.loc[mask_update, "QCNotes"].isna(),
                        inDF.loc[mask_update, "QCNotes"] + " | " + f"{note} - {now}"
                    )
                )

                logMsg = (f'WARNING - Updated - {countUpdate} - {field} records in '
                          f'- {etlInstance.protocol} - QC validation - qc_FishWeight')
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.critical(logMsg)

            else:
                logMsg = (f'No Updates - for {field} records in '
                          f'- {etlInstance.protocol} - QC validation - qc_FishWeight')

                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.info(logMsg)

            return inDF

        except Exception as e:

            logMsg = f'ERROR - ETL_QCValidation - qc_LengthCategoryID: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg)
            traceback.print_exc(file=sys.stdout)







