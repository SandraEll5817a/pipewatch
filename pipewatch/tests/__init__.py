# pipewatch tests package

import os
import sys


def get_test_data_path(filename):
    """Return the absolute path to a test data file.

    Args:
        filename: The name of the file within the test data directory.

    Returns:
        The absolute path to the specified test data file.
    """
    return os.path.join(os.path.dirname(__file__), "data", filename)
