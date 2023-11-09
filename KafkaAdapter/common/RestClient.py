import requests
import json

class RestClient:


    def restapi_post_normal(self, url, body):
        try:
            headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
            response = requests.post(url, headers=headers, data=json.dumps(body, ensure_ascii=False, indent="\t"))
            return response
        except Exception as err:
            raise

    def restapi_get_normal(self, url):
        try:
            response = requests.get(url, allow_redirects=True)
            return response
        except Exception as err:
            raise

