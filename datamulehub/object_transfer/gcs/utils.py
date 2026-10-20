from datetime import datetime, timezone

from google.cloud import storage as gcs


CHUNK_SIZE = 8 * 1024 * 1024


def _get_gcs_client(gcs_credentials):
    if "service_file" in gcs_credentials:
        return gcs.Client.from_service_account_json(gcs_credentials["service_file"])
    return gcs.Client()


def _upload_response(bucket, key, response, url):
    blob = bucket.blob(key)
    blob.metadata = {
        "source-url": url,
        "transfer-date": datetime.now(timezone.utc).isoformat(),
    }
    size = 0
    with blob.open(
        "wb", chunk_size=CHUNK_SIZE,
        content_type=response.headers.get("Content-Type", "application/octet-stream"),
    ) as output:
        while True:
            chunk = response.read(CHUNK_SIZE)
            if not chunk:
                break
            output.write(chunk)
            size += len(chunk)
        expected = response.headers.get("Content-Length")
        if expected is not None and size != int(expected):
            raise IOError(f"Incomplete source download: expected {expected} bytes, received {size}.")
    return size
