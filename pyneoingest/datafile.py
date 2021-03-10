# -*- coding: utf-8 -*-
"""Module use to load the data file configuration to memory.

An instance of the DataFile class contains all the configuration
parameters for a specific data file if it complies with all the
characteristics provided below.
 - The path provided exist and is readable by the user.
 - The file contains the expected configuration parameters.
 - The file extension exists and in a supported format.
"""

import logging
import sys
import os
import pathlib
import distutils
from urllib.parse import urlparse
from util import checkRequiredKeys, lower_dict_keys, url_exists


class DataFile():
    """Class used to model a data file.

    An instance of this class contains all the configuration
    parameters for a specific data file.

    Attributes
    ----------
    data_file : dict
        Contains the configuration parameters for a data file.
    url : str
        String containing the URL of the datafile.
    cql: str
        String containing the Cypher query used to load this data file.
    scheme : str
        String containing the url scheme
    netloc : str
        String containing the network location of the data file.
    path : str
        String with the full path of the data file.
    format : str
        Data file extension.
    skip_records : int
        Number of records or lines to skip in the data file.
    chuck_size : int
        Number of records per chunk of the data file to process.
    file_sep : str
        Data file field delimiter.
    skip : bool
        Whether to skip this data file from being process.
    """
    def __init__(self, data_file):
        """Class constructor.

        Used to create an intance of the data files that are correct.
        Otherwise it thows the appropiate exception.

        Parameters
        ----------
        data_file : str
            Full path of the log file to write to.

        Raises
        ------
        ValueError
            When the data file is missing the required configuration
            parameters.
        FileNotFoundError
            When the data file cannot to access.
        TypeError
            When the data file does not contains the file extension or
            when the extension is not supported.
        """
        self.data_file = lower_dict_keys(data_file)
        self.__required_keys = ['url', 'cql']
        self.__file_keys = data_file.keys()
        missing_keys = checkRequiredKeys(
            self.__required_keys, self.__file_keys)
        if len(missing_keys) > 0:
            raise ValueError(
                'Missing the following configuration key(s): '
                + ','.join(missing_keys))
        self.url = data_file['url']
        self.cql = data_file['cql']
        url_parsed = urlparse(self.url)
        self.scheme = url_parsed.scheme
        self.netloc = url_parsed.netloc
        self.path = url_parsed.path
        self.formats = [f.replace('.','').lower() for f in
                        pathlib.Path(self.url).suffixes]
        self.format = pathlib.Path(
            self.url).suffix.replace('.', '').lower()
        if not url_exists(self.path):
            raise FileNotFoundError(self.url)
        self.__supported_formats = ['gzip', 'zip', 'csv', 'txt', 'json']
        self.skip_records = data_file.get('skip_records') or 0
        self.chunk_size = data_file.get('chunk_size') or 1000
        self.field_sep = data_file.get('field_separator') or ','
        self.skip = data_file.get('skip_file') or False
        if self.format == '':
            raise TypeError(
                'Error reading url {0}: No file extension found'
                .format(self.url))
        if self.format not in self.__supported_formats:
            raise TypeError(
                ('Error reading url {0}: File format {1} '
                 'not supported, only {2} are allowed')
                .format(self.url, self.format,
                        self.__supported_formats))
