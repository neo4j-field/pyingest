# pyingest
A script for loading CSV and JSON files into a Neo4j database written in Python3.  It performs well due to several factors:
* Records are grouped into configurable-sized chunks before ingest
* For CSV files, we leverage the optimized CSV parsing capabilities of the Pandas library
* For JSON files, we use a streaming JSON parser (ijson) to avoid reading the entire document into memory

## Installation
* You will need to have Python 3 and compatible version of Pip installed.
* Then run `pip3 install -r requirements.txt` to obtain dependencies
* If you do not have a yaml module installed, you may need to run `pip3 install pyyaml`

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
### Parquet specific parameters
* `parquet_suffix_whitelist`: Pyingest will attempt to read files in the **uri** folder with this suffix(es) (**Default**: None (i.e. will attempt to read all files in the folder)).  Pyingest expects a comma-separated list in quotes
* `parquet_suffix_blacklist`: Pyingest will ignore files with this suffix(es) when it reads from the **uri** folder  (**Default**: None (i.e. will attempt to read all files in the folder)).  Pyingest expects a comma-separated list in quotes
* `parquet_partition_filter`: Snippet of python code to run to determine which (if any) filter to apply on the partition.  For this to work, **parquet_as_dataset** must be set to `True`.  Additionally, the python code **MUST** assign the `self.filter` to the partitioning function.  Here is an example of a legal entry:
`parquet_partition_filter: 'self.filter = lambda x: True if x["station"] == "402265" else False'`  (**Default**: None)
* `parquet_columns`: If this parameter is set, only the specified columns will be retrieved.  (**Default**: None (i.e. all columns will be retrieved)).  Pyingest expects a comma-separated string in quotes
* `parquet_as_dataset`: If set to `True`, the parquet folder will be treated as a dataset rather than single files.  It **must** be set to True to enable `parquet_partition_filter`.  In other cases, the default value of `False` is fine
* `parquet_start_from_mod_date`: Read only files in the **uri** folder whose last modified date is after the value specified.  The format **MUST** be `MM/dd/yy HH:mm:ss` (e.g. 8/31/20 17:22:30) (**Default**: None (i.e. read all the files in the folder))
* `parquet_up_to_mod_date`: Read only files in the **uri** folder whose last modified date is before the value specified.  The format **MUST** be `MM/dd/yy HH:mm:ss` (e.g. 8/31/20 17:22:30) (**Default**: None (i.e. read all the files in the folder))
* `parquet_s3_additional_args`: Additional arguments to be passed to the parquet reader.  These **must** be specified in a quoted string where entries are separated by commas and keys are separated from values by a colon.  Here is an example:
`    parquet_s3_additional_args: "ServerSideEncryption:aws:kms,SSEKMSKeyId: YOUR_KMY_KEY_ARN"
`

## Additional info
The pyingest script is backed by an Integration Test suite written in Java that leverages the Neo4j test harness.  Please see the javadoc on the `IngestPyIT.java` file for details about how this script is tested.
