# -*- coding: utf-8 -*-
"""Module use to process the YAML configuration file.

This module process the configuration file after verifying
that it complies with all the characteristics provided below.
Otherwise it will raise an exception and stop the execution of the script.
 - The path provided exist and is readable by the user.
 - The file is properly formatted as a YAML file.
 - The file contains the expected configuration parameters.
"""

import sys
import yaml
import logging
from yaml.parser import ParserError
from util import lower_dict_keys, checkRequiredKeys


class LoadConfig(object):
    """Class used to represent the configuration file.

        An instance of this class contains all the information in the
        configuration file. If the required configuration parameters are not
        included in configuration an execption is raised and the execution
        is terminated.

        Attributes
        ----------
        configuration_file : str
            String with the location of the configuration file.
        config_keys : list
            List of parameters in the configuration file.
        server_uri : str
            Server URI connection string.
        database : str
            Name of the database to use for the load.
        admin_user: str
            User name to use in the connection to the database.
        admin_pass: str
            User password to use in the connection to the database.
        pre_ingest : list
            List of strings containing the Cypher statements to be executed
            prior to the data load.
        post_ingest : list
            List of strings containing the Cypher statements to be executed
            after the data is loaded.
        data_files_config : list
            List of dictionaries containing the data files configuration
            parameters.
    """

    def __init__(self, configuration_file, log_file):
        """Class constructor.

        Instantiate the class with the configuration parameters if the
        configuration file is correct. Otherwise it raises an exeption and
        terminate the execution of the script.

        Parameters
        ----------
        configuration_file : str
            Path of the configuration file.
        log_file : str
            Full path of the log file to write to.
        """
        logging.root.handlers = []
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.__error_messages = {
            'FileNotFoundError': 'File not found: ',
            'ParserError': 'Wrong configuration file format: ',
            'ValueError': 'Missing the following configuration key(s): '
        }
        self.__required_keys = ['server_uri', 'admin_user', 'admin_pass']
        self.configuration_file = configuration_file
        self.load_config()
        self.config_keys = self.configuration_object.keys()
        self.pre_ingest = self.configuration_object.get('pre_ingest')
        self.post_ingest = self.configuration_object.get('post_ingest')
        self.database = self.configuration_object.get('database')
        self.admin_user = self.configuration_object.get('admin_user')
        self.admin_pass = self.configuration_object.get('admin_pass')
        self.server_uri = self.configuration_object.get('server_uri')
        self.data_files_config = self.configuration_object.get('files') or []
        try:
            missing_keys = checkRequiredKeys(
                self.__required_keys, self.config_keys)
            if len(missing_keys) > 0:
                raise ValueError()
        except ValueError as e:
            logging.error(
                self.__error_messages[e.__class__.__name__] + ','.join(
                    missing_keys))
            sys.exit(1)

    def load_config(self):
        """Load the configuration file.

        Load the configuration file to memory if the file is readable and
        formatted propertly. Otherwise it raised exemptions and terminate the
        execution of the script.
        """
        try:
            with open(self.configuration_file) as config_file:
                self.configuration_object = lower_dict_keys(
                    yaml.load(config_file, yaml.SafeLoader))
        except FileNotFoundError as e:
            logging.error(self.__error_messages[e.__class__.__name__] + str(e))
            sys.exit(1)
        except ParserError as e:
            logging.error(self.__error_messages[e.__class__.__name__] + str(e))
            sys.exit(1)
