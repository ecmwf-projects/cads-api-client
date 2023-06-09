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

import concurrent.futures
import logging
import queue
import random
import threading
import time
from cads_api_client import ApiClient
from cads_api_client.processing import ProcessingFailedError
import os
import json

CADS_API_ROOT_URL = os.getenv("CADS_API_ROOT_URL", "http://cds2-dev.copernicus-climate.eu/api")

#format = "%(asctime)s - %(levelname)7s - %(name)-24s - %(threadName)s - %(message)s"
format = "%(asctime)s - %(levelname)s - %(name)s - %(threadName)s - %(message)s"
datefmt = "%H:%M:%S"
logging.basicConfig(format=format, level=logging.INFO, datefmt=datefmt)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)

client = ApiClient(url=CADS_API_ROOT_URL, key="00112233-4455-6677-c899-aabbccddeeff")
collection = client.collection("reanalysis-era5-pressure-levels")

# +
conf, fn = {}, "multi-request.json"
if os.path.exists(fn):
    with open(fn) as fin:
        conf = json.load(fin)

NUM_YEARS = conf.get("NUM_YEARS", 10)
MAX_DOWNLOADS = conf.get("MAX_DOWNLOADS", 2)
MAX_UPDATES = conf.get("MAX_UPDATES", 5)
DOWNLOAD_TIME = conf.get("DOWNLOAD_TIME", 5)

print(f"Requests: {NUM_YEARS}\n"\
      f"Concurrent status updates: {MAX_UPDATES}\n"\
      f"Concurrent downloads: {MAX_DOWNLOADS}\n"\
      f"Download time: {DOWNLOAD_TIME}\n")
# -

requests = [
    dict(product_type="reanalysis", 
         variable="temperature", 
         pressure_level="1", 
         year=str(year), 
         month=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
         day=[
             '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12',
             '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24',
             '25', '26', '27', '28', '29', '30', '31',
         ],
         time="06:00",
         target="test02.grib",
         accepted_licences=[{"id": "licence-to-use-copernicus-products", "revision": 12}]
) for year in range(2024-NUM_YEARS, 2024)]


# +
def send_request(kwargs):
    job = collection.submit(**kwargs)
    job_id = job.response.json()["jobID"]
    logging.debug(f"{job_id} - Adding")
    return job


def update_job(job, queue):
    #logging.debug(f"qsize={queue.qsize()}")
    job_id = job.response.json()["jobID"]
    
    t0 = time.time()
    logging.debug(f"{job_id} - Waiting on result")
    try:
        job_status = job.wait_on_result()
    except ProcessingFailedError:
        job_status = 'failed'
    logging.debug(f"{job_id} - Status={job_status} ({time.time() - t0:.0f}s)")

    #logging.debug(f"qsize={queue.qsize()}")
    if job_status in ('failed', 'successful'):
        queue.put(job)
        #logging.debug(f"qsize={queue.qsize()} [updated]")
    
    return job_id, job_status
    

def producer(queue, jobs):
    logging.debug("Producer starting")
    global event

    # cons: still updating when the queue is full / not updating when a slot in the queue is released
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_UPDATES) as executor:
        update_futures = executor.map(update_job, jobs, [queue]*len(jobs))
    
    print('\n'.join(f"{job_id}: {job_status}" for job_id, job_status in list(update_futures)))
    
    event.set()


def consumer(queue):
    logging.debug("Consumer starting")
    global event
    while not event.is_set() or not queue.empty():
        job = queue.get()
        job_id = job.response.json()["jobID"]
        # download
        logging.debug(f"consumer - {job_id} - qsize={queue.qsize()} - Downloading")
        time.sleep(DOWNLOAD_TIME)
        logging.debug(f"consumer - {job_id} - qsize={queue.qsize()} - Downloaded")



# +
# %%time

#######################
t0 = time.time()
#######################

# send requests concurrently
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    jobs = list(executor.map(send_request, requests))

#######################
t1 = time.time()
#######################

# producer / consumer
pipeline = queue.Queue(maxsize=MAX_DOWNLOADS)
event = threading.Event()
#lock = threading.Lock()
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOADS+1) as executor:
    executor.submit(producer, pipeline, jobs)
    for i in range(MAX_DOWNLOADS):
        executor.submit(consumer, pipeline)

#######################
t2 = time.time()
print(f"Requests: {len(jobs)}\n"\
      f"Concurrent status updates: {MAX_UPDATES}\n"\
      f"Concurrent downloads: {MAX_DOWNLOADS}\n"\
      f"Download time: {DOWNLOAD_TIME}\n"\
      f"Submit requests: {t1-t0:.2f}s\n"\
      f"Download: {t2-t1:.2f}s")
#######################

print("DONE")
# -

