import urllib.request
import urllib.parse
from tqdm import tqdm
import json

from .datasets import DATASET_NAME_MAP
from .api_key import api_key

def download_dataset(dataset, filename=None):
    # Map dataset name to official name
    dataset_name = DATASET_NAME_MAP.get(dataset)
    if not dataset_name:
        raise ValueError(f"Unknown dataset: {dataset}")
    
    # Get download URL from API
    api_url = f"https://api.datamule.xyz/dataset/{urllib.parse.quote(dataset_name)}?api_key={api_key}"

    req = urllib.request.Request(
        api_url,
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    )
    
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        raise Exception(f"API request failed: {error_body}")
    
    if not data.get('success'):
        raise Exception(f"API error: {data.get('error', 'Unknown error')}")
    
    billing = data.get('metadata', {}).get('billing', {})
    cost = billing.get('total_charge', 0)
    remaining_balance = billing.get('remaining_balance')

    download_url = data['data']['download_url']
    size_gb = data['data']['size_gb']
    
    # Extract filename from URL if not provided
    if filename is None:
        parsed = urllib.parse.urlparse(download_url)
        query_params = urllib.parse.parse_qs(parsed.query)
        path = query_params.get('path', [''])[0]
        filename = urllib.parse.unquote(path.split('/')[-1])
        if not filename:
            filename = f"{dataset}.download"
    
    print(f"Downloading {dataset} ({size_gb:.2f} GB)...")
    
    download_req = urllib.request.Request(
        download_url,
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    )
    
    try:
        with urllib.request.urlopen(download_req) as response:
            total_size = int(response.headers.get('Content-Length', 0))
            
            with open(filename, 'wb') as f, tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                desc=filename
            ) as pbar:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
                    pbar.update(len(chunk))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        raise Exception(f"Download failed: {error_body}")
    
    print(f"Downloaded to {filename}")
    if cost is not None:
        print(f"- Cost: ${cost:.4f} | Remaining balance: ${remaining_balance:.2f}")