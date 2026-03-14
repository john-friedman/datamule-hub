import asyncio
import aiohttp
import ssl
import time
from tqdm import tqdm
from .api_key import api_key as API_KEY

_total_cost = 0
_remaining_balance = None

async def _fetch_page(session, database, params, page=1, page_size=25000):
    url = f"https://api.datamule.xyz/{database}"
    
    query_params = params.copy()
    query_params["page"] = page
    query_params["pageSize"] = page_size
    query_params["api_key"] = API_KEY
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    async with session.get(url, params=query_params, headers=headers) as response:
        data = await response.json()
        if not data.get('success'):
            raise ValueError(f"API request failed: {data.get('error')}")
        
        billing = data.get('metadata', {}).get('billing', {})
        page_cost = billing.get('total_charge', 0)
        remaining_balance = billing.get('remaining_balance')
        
        pagination = data.get('metadata', {}).get('pagination', {})
        
        result_data = data.get('data', [])
        if isinstance(result_data, dict) and 'data' in result_data:
            result_data = result_data['data']
        
        return result_data, pagination, page_cost, remaining_balance


async def _execute_query(database, **kwargs):
    global _total_cost, _remaining_balance
    _total_cost = 0

    page_size = kwargs.pop('page_size', 25000)
    quiet = kwargs.pop('quiet', False)

    params = {}
    for key, value in kwargs.items():
        if value is None:
            continue
        elif isinstance(value, list):
            params[key] = ','.join([str(val) for val in value])
        elif isinstance(value, tuple):
            params[f"{key}_START"] = value[0]
            params[f"{key}_END"] = value[1]
        else:
            params[key] = value

    start_time = time.time()
    total_items = 0
    pages_processed = 0
    results = []

    connector = aiohttp.TCPConnector(ssl=ssl.create_default_context())
    async with aiohttp.ClientSession(connector=connector) as session:
        if not quiet:
            pbar = tqdm(unit="page", bar_format="{desc}: {n_fmt} {unit} [{elapsed}<{remaining}, {rate_fmt}{postfix}]")
            pbar.set_description("Fetching data")

        current_page = 1
        has_more = True

        while has_more:
            page_results, pagination, page_cost, _remaining_balance = await _fetch_page(
                session, database, params, page=current_page, page_size=page_size
            )

            results.extend(page_results)
            _total_cost += page_cost
            pages_processed += 1
            total_items += len(page_results)

            if not quiet:
                pbar.set_description(f"Fetching data (page {current_page})")
                pbar.set_postfix_str(f"cost=${_total_cost:.4f} | balance=${_remaining_balance:.2f}")
                pbar.update(1)

            has_more = pagination.get('hasMore', False)
            current_page += 1

            if pages_processed == 1 and not quiet:
                records_per_page = pagination.get('currentPageRecords', len(page_results))
                if records_per_page > 0:
                    pbar.write(f"Retrieved {records_per_page} records (page 1) - Fetching additional pages...")
                else:
                    pbar.write("No records found matching criteria")
                    break

        if not quiet:
            pbar.close()
            elapsed_time = time.time() - start_time
            print("\nQuery complete:")
            print(f"- Retrieved {total_items} records across {pages_processed} pages")
            print(f"- Total cost: ${_total_cost:.4f}")
            print(f"- Remaining balance: ${_remaining_balance:.2f}")
            print(f"- Time: {elapsed_time:.1f} seconds")

    return results


def query_database(database, **kwargs):
    return asyncio.run(_execute_query(database=database, **kwargs))


