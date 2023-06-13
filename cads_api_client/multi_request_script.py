
import json
import logging
import os

from cads_api_client import ApiClient
from cads_api_client.multi_request import download_multiple_requests

# env vars
CADS_API_ROOT_URL = os.getenv("CADS_API_ROOT_URL", "http://cds2-dev.copernicus-climate.eu/api")

# logging
format = "%(asctime)s - %(levelname)s - %(name)s - %(threadName)s - %(message)s"
datefmt = "%H:%M:%S"
logging.basicConfig(format=format, level=logging.INFO, datefmt=datefmt)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)
logging.getLogger("multiurl.base").setLevel(logging.WARNING)

# params
MAX_DOWNLOADS = 2
MAX_UPDATES = 5

# init
client = ApiClient(url=CADS_API_ROOT_URL, key="00112233-4455-6677-c899-aabbccddeeff")
collection = client.collection("reanalysis-era5-pressure-levels")
requests = [
    dict(product_type="reanalysis",
         variable="temperature",
         pressure_level="1",
         year=str(year),
         month=['01'],
         day=['01'],
         time="06:00",
         #target="test02.grib",
         accepted_licences=[{"id": "licence-to-use-copernicus-products", "revision": 12}])
    for year in range(2014, 2024)]

print(f"Requests: {len(requests)}\n"\
      f"Concurrent updates: {MAX_UPDATES}\n"\
      f"Concurrent downloads: {MAX_DOWNLOADS}")


client = ApiClient(url=CADS_API_ROOT_URL, key="00112233-4455-6677-c899-aabbccddeeff")
collection = client.collection("reanalysis-era5-pressure-levels")
download_multiple_requests(collection, requests)
