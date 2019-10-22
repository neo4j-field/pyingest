# pyingest
A script for loading CSV and JSON files into a Neo4j database written in Python3.  It performs well due to several factors:
* Records are grouped into configurable-sized chunks before ingest
* For CSV files, we leverage the optimized CSV parsing capabilities of the Pandas library
* For JSON files, we use a streaming JSON parser (ijson) to avoid reading the entire document into memory

## Usage
`python3 ingest.py <config>`

The config is a YAML file described below

## How it works
* The configuration file is read into memory
* Any `pre_ingest` cypher statements are run
* File-based ingests (in the `files` stanza of config) are run 
* Any `post_ingest` cypher statements are run
* **NB** - All values are read from the source file as strings.  If you need a different type in the database, you should do the appropriate type conversion in the cypher ingest statement.  

## Configuration 
The following parameters may be configured:
* `server_uri`: Address of Neo4j driver (**required**)
* `admin_user`: Username of Neo4j user (**required**)
* `admin_pass`: Password for Neo4j user (**required**)
* `pre_ingest`: List of cypher statements to be run before the file ingests
* `post_ingest`: List of cypher statements to be run after the file ingests
* `files`: Describes ingestion of a file - one stanza for each file.  If a file needs to be processed more than once, just use it again in a separate stanza.  Parameters for `files` are discussed below.
### File ingest parameters
* `url`: Path to the file (**required**)
* `cql`: Cypher statement to be run (**required**)
* `chunk_size`: Number of records to batch together for ingest (**Default**: 1000)
* `type`: Type of file being ingested.  (**One of** csv|json).  This parameter is not required.  If missing, the script will guess based on the file extension, defaulting to CSV.
* `field_separator`: (**CSV only**) Character separating values in a line of CSV. (**Default**: ,)
* `compression`: Type of compression used on file (**One of** gzip|zip|none) (**Default**: none)
* `skip_file`: If true, do not process this file.  (**One of** true|false) (**Default**: false)
* `skip_records`: Skip specified number of records while ingesting the file.  (**Default**: 0)

## Additional info
The pyingest script is backed by an Integration Test suite written in Java that leverages the Neo4j test harness.  Please see the javadoc on the `IngestPyIT.java` file for details about how this script is tested.