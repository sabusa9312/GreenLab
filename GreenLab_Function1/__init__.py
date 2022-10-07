import logging
import azure.functions as func
from azure.storage.blob import ContainerClient

import sys
import os
from sys import path
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)
from get_file import *


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name_file = req.params.get('file')
    
    if not name_file:
        return func.HttpResponse("Attenzione parametro mancante!", status_code=200)

    if name_file:
        result = get_file(name_file)
        return func.HttpResponse(result, status_code=200)

