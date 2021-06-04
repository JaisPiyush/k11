import pickle
import os
from posixpath import dirname


def get_categories():
    file_path = os.path.abspath(dirname(__file__))
    file_name = "categories.bin"
    path = os.path.join(file_path, file_name)

    with open(path, 'rb') as file:
        return pickle.load(file)
