"""
Configures google ads performance pipeline
"""
from data_integration.parallel_tasks.files import ReadMode


def input_file_version():
    """A suffix that is added to input files, denoting a version of the data format"""
    return 'v3'


def read_mode():
    """Determines the mode for specifying which files from a list of files to load"""
    return ReadMode.ONLY_CHANGED
