import requests
from requests_toolbelt import MultipartEncoder
from requests.structures import CaseInsensitiveDict
import json

class RestClient:
    def restapi_get(self, url):
        try:
            #headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
            headers = CaseInsensitiveDict()
            headers["Accept"] = "application/json"
            headers["Authorization"] = "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJQRVRfVE9LRU5fUEFTU1dPUkRfYWRtaW4iLCJhdXRoIjoiUk9MRV9TVVBFUl9BRE1JTixST0xFX0FETUlOIiwibG9naW5UeXBlIjoiUEFTU1dPUkQiLCJsb2dpbklkIjoiYWRtaW4iLCJjbGllbnRVbmlxdWVJZCI6IiIsImV4cCI6MTcyNjE1MDU2OX0.bxHwupW1vqrR8ZQ7AZ3SkepMl-WCbeWIYdtu2cBsGzMRIUpClv72TS17v17xGeU9n2CqMO4Igm9RmLz3kH1GDA"
            response = requests.get(url, headers=headers)
            return response
        except Exception as err:
            raise

    def restapi_auth_post(self, url, body):
        try:
            #headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
            headers = CaseInsensitiveDict()
            headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
            headers["Authorization"] = "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJQRVRfVE9LRU5fUEFTU1dPUkRfYWRtaW4iLCJhdXRoIjoiUk9MRV9TVVBFUl9BRE1JTixST0xFX0FETUlOIiwibG9naW5UeXBlIjoiUEFTU1dPUkQiLCJsb2dpbklkIjoiYWRtaW4iLCJjbGllbnRVbmlxdWVJZCI6IiIsImV4cCI6MTcyNjE1MDU2OX0.bxHwupW1vqrR8ZQ7AZ3SkepMl-WCbeWIYdtu2cBsGzMRIUpClv72TS17v17xGeU9n2CqMO4Igm9RmLz3kH1GDA"
            response = requests.post(url, headers=headers, data=json.dumps(body, ensure_ascii=False, indent="\t"))
            return response
        except Exception as err:
            raise

    def restapi_auth_patch(self, url, body):
        try:
            #headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
            headers = CaseInsensitiveDict()
            headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
            #headers["Accept"] = "application/json"
            headers["Authorization"] = "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJQRVRfVE9LRU5fUEFTU1dPUkRfYWRtaW4iLCJhdXRoIjoiUk9MRV9TVVBFUl9BRE1JTixST0xFX0FETUlOIiwibG9naW5UeXBlIjoiUEFTU1dPUkQiLCJsb2dpbklkIjoiYWRtaW4iLCJjbGllbnRVbmlxdWVJZCI6IiIsImV4cCI6MTcyNjE1MDU2OX0.bxHwupW1vqrR8ZQ7AZ3SkepMl-WCbeWIYdtu2cBsGzMRIUpClv72TS17v17xGeU9n2CqMO4Igm9RmLz3kH1GDA"
            response = requests.patch(url, headers=headers, data=json.dumps(body, ensure_ascii=False, indent="\t"))
            return response
        except Exception as err:
            raise

    def restapi_post(self, url, body):
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

    def restapi_post_normal_multi(self, url, data):
        try:
            m = MultipartEncoder(fields=data)
            headers = {'Content-Type': m.content_type}
            response = requests.post(url, headers=headers, data=m)
            return response
        except Exception as err:
            raise
