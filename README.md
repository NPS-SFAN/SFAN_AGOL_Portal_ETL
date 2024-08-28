# SFAN_AGOL_ETL
San Francisco Bay Area Network AGOL Extract Transform and Load Routines for Survey 123 and Arc Field Maps.  
As of 8/27/2024 - Snowy Plover PORE ETL workflow has been developed - KRS.

## SFAN_AGOL_ETL.py
Parent SFAN ArcGIS Online (AGOL) and Portal Extract, Transform and Load (ETL) script.  From parent script routines are
defined by protocl for ETL of passed feature layers (e.g. Survey 123 or Arc Field Maps) to the respective protocol
database and table schema locations.

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

## ETL_SNPLPORE.py
Methods/Functions to be used for Snowy Plover PORE ETL workflow.
