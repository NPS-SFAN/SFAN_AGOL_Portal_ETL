# SFAN_AGOL_Portal ETL
San Francisco Bay Area Network AGOL and Portal Extract Transform and Load routines.  
As of 8/27/2024 - Snowy Plover PORE ETL workflow has been developed - KRS.\
As of 10/23/2024 - Salmonids ElecrtoFishing ETL workflow has been developed - KRS.\
As of 3/25/2025 - PCM ETL of Location Manual Information to Portal is developed - KRS.\
As of 5/25/2025 - Elephant Seal ETL is developed - KRS.\
As of 6/3/2025 - Salmonids Smolts ETL workflow developed - KRS

## SFAN_AGOL_Portal_ETL.py
Parent SFAN ArcGIS Online (AGOL) and Portal Extract, Transform and Load (ETL) script.  From parent script routines are
defined by protocl for ETL of passed feature layers (e.g. Survey 123 or Arc Field Maps) to the respective protocol
database and table schema locations.  Processing can also be for ETL from databases to the AGOL/Portal platforms.

Workflow can be accomplished connecting to AGOL/Portal via an OAuth 2.0. Conversely if you have ArcGISPro installed you
can connect via the 'Pro' python environment installed on your computer, which will use your windows/active directory
credentials to connect to AGOL/Portal thus not needing a OAuth 2.0 token with subsequent workflow.

Note: When behind the NPS firewall at the office I seemingly have to use OAuth authentication because when using the
ArcGISPro credentials with the native ArcGISPro python environment I'm not able to retain my permission (i.e. not able
to download Feature Layers I own).   Conversely, when VPN connected at home the ArcGISPro credentials work fine.

## ETL.py
Extract Transform and Load (ETL) Methods/Functions to be used for general AGOL/Portal ETL workflow.

## ArcGIS_API.py
Methods for working within AGOL/Portal and the ArcGIS API.

## generalDM.py
General Data Management workflow related methods.

# Scripts from AGOL/Portal to Databases (e.g. Survey 123 to Databases)
## ETL_SNPLPORE.py
Methods/Functions to be used for Snowy Plover PORE ETL workflow.

## ETL_PCM_LocationsManualParking.py
ETL workflow pulling the Locations Manual/Parking information from the PCM Frontend Database and exporting (i.e. ETL) as
two Feature Layers to the AGOL/Portal.

## ETL_Salmonids_Smolts.py
Methods/Functions to be used for Salmonids Smolts ETL workflow.

## ETL_Salmonids_Electro.py
Methods/Functions to be used for Salmonids Electrofishing ETL workflow.

## ETL_PINN_Elephant.py
Methods/Functions to be used for Pinnipeds Elephant Seal ETL workflow.

# Scripts from Databases to AGOL/Portal
## ETL_PCM_LocationsManualParking.py
ETL workflow pulling the Locations Manual/Parking information from the Plan Communities Frontend Database and exporting (i.e. ETL) as
two Feature Layers to AGOL/Portal.  Workflow going from Access Database to AGOL/Portal.

# General Files
## tests/test_etl.py
ETL Unit Testing Script - needs to be further developed

