import time
import requests
import json
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def send_request(payload, headers, proxy):
    return send_request_with_proxy(payload, headers, proxy)

def send_request_with_proxy(payload, headers, proxy):
    url = "https://airline-api.naver.com/graphql"    
    try:
        response = requests.post(
            url, 
            proxies=proxy, 
            json=payload, 
            headers=headers, 
            verify=False, 
            timeout=(10, 10)
        )

        if type(response)==bool:
            print(f'{proxy}->ip 밴당함')
            return None
        # response.raise_for_status()
        try:
            return response.json()
        except Exception as e:
            return None

    except Exception as e:
        return None