import requests
import json

def get_alpha_vantage_raw_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error ocurred: {e}")
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")