import os

def get_api_key(api_key=None):
    key = api_key or os.environ.get('DATAMULE_API_KEY')
    if not key:
        raise EnvironmentError("DATAMULE_API_KEY environment variable is not set.")
    return key


def _format_api_key_credits(billing):
    if "remaining_api_key_credits" not in billing:
        return ""
    remaining = billing["remaining_api_key_credits"]
    value = "Unlimited" if remaining is None else f"${float(remaining):.4f}"
    return f" | API key credits left: {value}"


api_key = os.environ.get('DATAMULE_API_KEY')
