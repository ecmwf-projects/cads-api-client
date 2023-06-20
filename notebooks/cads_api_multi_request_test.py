# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.5
#   kernelspec:
#     display_name: conda-py310-cads_api_client
#     language: python
#     name: conda-py310-cads_api_client
# ---

# %load_ext autoreload
# %autoreload 2

import json
import logging
import os

from cads_api_client import ApiClient
# from cads_api_client.multi_retrieve import multi_retrieve

# env vars
CADS_API_ROOT_URL = os.getenv("CADS_API_ROOT_URL", "http://cds2-dev.copernicus-climate.eu/api")

# logging
format = "%(asctime)s - %(levelname)s - %(name)s - %(threadName)s - %(message)s"
datefmt = "%H:%M:%S"
logging.basicConfig(format=format, level=logging.INFO, datefmt=datefmt)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)
logging.getLogger("multiurl.base").setLevel(logging.WARNING)
logging.getLogger("multiurl.http").setLevel(logging.WARNING)

# params
MAX_DOWNLOADS = 2
MAX_UPDATES = 5
TARGET = "/tmp"

# init
client = ApiClient(base_url=CADS_API_ROOT_URL, key="00112233-4455-6677-c899-aabbccddeeff")
collection_id = "reanalysis-era5-pressure-levels"
accepted_licences = [{"id": "licence-to-use-copernicus-products", "revision": 12}]
requests = [
    dict(product_type="reanalysis",
         variable="temperature",
         pressure_level="1",
         year=str(year),
         month=['01'],
         day=['01'],
         time="06:00",
         # accepted_licences=[{"id": "licence-to-use-copernicus-products", "revision": 12}]
         ) for year in range(2014, 2024)]

print(f"Requests: {len(requests)}\n"\
      f"Concurrent updates: {MAX_UPDATES}\n"\
      f"Concurrent downloads: {MAX_DOWNLOADS}")

client = ApiClient(base_url=CADS_API_ROOT_URL, key="00112233-4455-6677-c899-aabbccddeeff")
results = client.retrieve(collection_id=collection_id, requests=requests, accepted_licenses=accepted_licences,
                          target=TARGET, max_updates=MAX_UPDATES, max_downloads=MAX_DOWNLOADS)


print("YEAR | " + "{:>20}".format("HASH") + " | PATH")
for request_hash, result in results.items():
    request, job, download = result.get("request"), result.get("job"), result.get("download", {})
    print(f"{request['year']} | {request_hash:20} | {download.get('path')}")

results


