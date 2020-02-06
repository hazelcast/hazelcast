import numpy as np


def transform_list(input_list):
    """
    Uses NumPy to transform a list of numbers into a list of their square
    roots.

    :param input_list: the list with input items
    :return: the list with input items' square roots
    """
    num_list = [float(it) for it in input_list]
    sqrt_list = np.sqrt(num_list)
    return [str(it) for it in sqrt_list]
