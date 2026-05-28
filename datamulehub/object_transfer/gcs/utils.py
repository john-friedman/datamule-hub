import os
import platform

def set_adc_credentials():
    if platform.system() == 'Windows':
        path = os.path.join(os.environ['APPDATA'], 'gcloud', 'application_default_credentials.json')
    else:
        path = os.path.expanduser('~/.config/gcloud/application_default_credentials.json')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path


def _get_storage(gcs_credentials, session):
    from gcloud.aio.storage import Storage
    if 'service_file' in gcs_credentials:
        return Storage(service_file=gcs_credentials['service_file'], session=session)
    else:
        set_adc_credentials()
        return Storage(session=session)