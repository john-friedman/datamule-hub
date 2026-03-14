import os

def get_api_key():
    key = os.environ.get('DATAMULE_API_KEY')
    if not key:
        raise EnvironmentError("DATAMULE_API_KEY environment variable is not set.")
    return key


api_key = get_api_key()