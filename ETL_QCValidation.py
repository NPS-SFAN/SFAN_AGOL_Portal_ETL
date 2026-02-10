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
import numpy as np

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

        Flagging with 'CFCETL' flag -Calculated field value was corrected during extract transform and load
        quality control validation check.

        :param etlInstance: ETL workflow instance
        :param dmInstance: data management instance which will have the logfile name\
        :param inDF: List with QC Validation Fields to be processed
        :param field: Field in 'inDF' being validated

        :return:outDFwLookUp - dataframe with the updates 'LengthCategoryID' field
         changed - variable defining if the inDF passed in the outDFwLookup has been changed - True/False
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

            # Define an Index where the Length Category ID lookups are not equal
            left = outDFwLookUp["LengthCategoryID"]
            right = outDFwLookUp["LengthCategoryID_Lookup"]

            both_missing = left.isna() & right.isna()
            equal_values = left.fillna("__NULL__").eq(right.fillna("__NULL__"))
            mask_update = ~(both_missing | equal_values)
            outDFwLookUp = outDFwLookUp.replace([np.nan, 'nan'], None)

            # Get Count of Records to be updated
            countUpdate = mask_update.sum()

            if countUpdate >= 1:

                # Define changed denoting the Input and Output data frame has been updated
                changed = True

                # Variables to push
                flag = "CFCETL"
                now = datetime.now().strftime("%Y-%m-%d")
                note = f"Updated - LengthCategoryID during initial ETL QC Validation - {now}"

                # update the corrected value
                outDFwLookUp.loc[mask_update, "LengthCategoryID"] = outDFwLookUp.loc[
                    mask_update, "LengthCategoryID_Lookup"
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
                    .fillna(f"{note}")
                    .where(
                        outDFwLookUp.loc[mask_update, "QCNotes"].isna(),
                        outDFwLookUp.loc[mask_update, "QCNotes"] + " | " + f"{note}"
                    )
                )

                logMsg = (f'WARNING - Updated - {countUpdate} - Length Category ID records in '
                          f'- {etlInstance.protocol} - QC validation - qc_LengthCategoryID')
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.critical(logMsg)

                # Drop the 'LengthCategoryID_Lookup'
                outDFwLookUp = outDFwLookUp.drop(columns=['LengthCategoryID_Lookup'])

            else:
                logMsg = (f'No Updates - for Length Category ID records in '
                          f'- {etlInstance.protocol} - QC validation - qc_LengthCategoryID')

                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.info(logMsg)

            return outDFwLookUp, changed

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

        # Convert all nan to None
        InDF = InDF.replace([np.nan, 'nan'], None)

        return InDF

    def qc_FishWeight(etlInstance, dmInstance, inDF, field):

        """
        QC Validation for Fish Weight field. Fish Weight is Total Weight - Bag Weight.
        Updating if the QC Validation Calulated Fish Weight if >=0.001.
        Flagging with 'CFCETL' flag -Calculated field value was corrected during extract transform and load
        quality control validation check.

        :param etlInstance: ETL workflow instance
        :param dmInstance: data management instance which will have the logfile name\
        :param inDF: List with QC Validation Fields to be processed
        :param field: Field in 'inDF' being validated

        :return:
        inDF - dataframe with the updates 'LengthCategoryID' field
        changed - variable defining if the inDF passed in the outDFwLookup has been changed - True/False

        """

        try:

            # Variables to push if QC validation fails
            inDF['FishWeight_QC'] = inDF["TotalWeight"] - inDF["BagWeight"]
            inDF['FishWeight_Dif'] = inDF["FishWeight"] - inDF["FishWeight_QC"]

            # If the Absolute Difference is >=0.001 then update
            mask_update = inDF["FishWeight_Dif"].abs() >= 0.001

            countUpdate = mask_update.sum()

            if countUpdate >= 1:
                # Define Dataframe has been updated
                changed = True

                flag = "CFCETL"
                now = datetime.now().strftime("%Y-%m-%d")
                note = f"Updated - Fish Weight during initial ETL QC Validation - {now}"

                # Update Fish Weight
                inDF.loc[mask_update, field] = inDF.loc[
                    mask_update, "FishWeight_QC"]

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
                    .fillna(f"{note}")
                    .where(
                        inDF.loc[mask_update, "QCNotes"].isna(),
                        inDF.loc[mask_update, "QCNotes"] + " | " + f"{note}"
                    )
                )

                logMsg = (f'WARNING - Updated - {countUpdate} - {field} records in '
                          f'- {etlInstance.protocol} - QC validation - qc_FishWeight')
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.critical(logMsg)

                # Drop the 'FishWeight_QC' and 'FishWeight_Dif' fields
                inDF = inDF.drop(columns=['FishWeight_QC', 'FishWeight_Dif'])

            else:
                logMsg = (f'No Updates - for {field} records in '
                          f'- {etlInstance.protocol} - QC validation - qc_FishWeight')

                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.info(logMsg)

            return inDF, changed

        except Exception as e:

            logMsg = f'ERROR - ETL_QCValidation - qc_LengthCategoryID: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg)
            traceback.print_exc(file=sys.stdout)


