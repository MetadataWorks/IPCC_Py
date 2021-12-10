This script is used to extract details from DKRZ XML files and map them to the MetadataWorks bulk import sheet (for IPCC Schema v1).

To get the xml files for DKRZ, you need to set up a TomCat instance, and install the JOAI harvester.  Details are here: 

OAI Server for OAI/PMH harvesting (endpoint: http://dm-oai.dkrz.de:8080/oai <http://dm-oai.dkrz.de:8080/oai>):
We map our internal metadata format into different standards:
- Dublin Core
- DIF
- OpenAire's version of the DataCite standard
- ISO 19115/19139 (rather outdated and needs tidy-up, not recommendable): We will possibly move towards the new recommendation from INSPIRE - GeoDCAT, but we currently don't have the capacity to do that.

Most metadata sets include only DOI-ed data (old IPCC data have no DOIs) and at the same time further WDC Climate data. The exception is the DDC set, I created about two weeks ago, after we had received initial information on your plans:
- DDC subset in OpenAire-DataCite standard: setspec:'openaire-ddc'

Please check that out. It's the best we can currently provide. In comparison to DataCite's schema, OpenAire allows non-DOI identifiers and makes some additional metadata fields mandatory. Further documentation on that metadata standard:
- OpenAire's documentation: https://guidelines.openaire.eu/en/latest/data/index.html <https://guidelines.openaire.eu/en/latest/data/index.html>
- DataCite's metadata schema: http://schema.datacite.org/ <http://schema.datacite.org/>

Details are:
Repository Name: Name it what ever you like
Repository Base URL: http://dm-oai.dkrz.de:8080/oai/provider
Metadata Format being harvested: 'oai_datacite'

Once the XML files are harvest (1 per dataset) this script will populate an excel spreadsheet in the same format as the Bulk importer spreadsheet.

Two final steps must be complete in ExceL:
1) Copy this data into the Bulk Import Excel Template
2) Find all "NaN" and replace with a blank cell


