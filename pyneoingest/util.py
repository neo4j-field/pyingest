# -*- coding: utf-8 -*-
"""Module containing utility functions used by other modules in the package."""

from datetime import datetime
import os


def lower_dict_keys(dictionary):
    """Convert the keys of a dictionary to lower case.

    Provided a dictionary, it returns the dictionary with
    the same values but with the keys in lower case.

    Parameters
    ----------
    dictionary : dict
        Source dictionary.

    Returns
    -------
    dict
        Dictionary with the same values as the source dictionary but all keys
        are converted to lower case.
    """
    lower_keys = [key.lower() for key in dictionary.keys()]
    return dict(zip(lower_keys, dictionary.values()))


def checkRequiredKeys(required_keys, keys):
    """Verify if the elements in one list are contained in the other list.

    Verify if the elements of the required_keys_list are contained in the keys
    list.

    Parameters
    ----------
    required_keys : list
        List containing the elements to search for.
    keys : list
        List containing elements to search from.

    Returns
    -------
    missing_keys
        List of elements in the required_keys that are not included in keys.
    """
    missing_keys = [key for key in required_keys if key not in keys]
    return missing_keys


def get_file_name(file_type, file_parts):
    """Create a string representation of a file name.

   Parameters
   __________
   file_type : str
        The file extension
   file_parts : list
        List of strings that are use as part of the file separeted by
        underscore

    Returns
    _______
    str
        String combining the file parts provided by underscores
        and appending the date and time the function was executed.

    Examples
    ________
    >>> file_name = get_file_name('csv',['this','is','the','file','name'])
    >>> print(file_name)
    this_is_the_file_name_20210209_170259.csv
    """
    file_name_parts = file_parts
    file_name_parts.append(datetime.today().strftime('%Y%m%d'))
    file_name_parts.append(datetime.now().strftime('%H%M%S'))
    file_name = '_'.join(file_name_parts) + '.' + file_type
    return file_name


def url_exists(file_loc):
    return os.path.exists(file_loc)
