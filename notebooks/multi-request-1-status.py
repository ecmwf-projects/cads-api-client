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

import concurrent.futures
import logging
import queue
import random
import threading
import time
from cads_api_client import ApiClient
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


def update_job(job):
    job_json = job.response.json()
    job_id = job_json["jobID"]
    #job_status = job_json["status"]
    job_status = job._robust_status()
    logging.debug(f"{job_id} - Updated")
    return job, job_status
    

def producer(queue, jobs):
    logging.debug("Producer starting")
    global event
    while len(jobs):
        if queue.qsize() < MAX_DOWNLOADS:  # avoid updating status if queue is full even if there are no jobs ready to be executed
                                           # (we do not use the built-in limit mechanism, but the queue class is still useful because 
                                           #  prevents race conditions when putting and getting jobs into the queue)
            #print("="*20)
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_UPDATES) as executor:
                map_res = executor.map(update_job, jobs)
            # thread safe?
            jobs_s = list(map_res)
            jobs_ready = [job[0] for job in jobs_s if job[1] in ('failed', 'successful')]
            jobs_not_ready = [job[0] for job in jobs_s if job[1] not in ('failed', 'successful')]
            logging.debug(f"qsize={queue.qsize()}, {len(jobs):02d} jobs ({len(jobs_ready)} ready, {len(jobs_not_ready)} not ready)")
            if len(jobs_ready):
                job = jobs_ready.pop()  # first available
                queue.put(job)
                jobs = jobs_ready + jobs_not_ready
                # note that producer runs on a single thread so there are no race conditions
                logging.debug(f"qsize={queue.qsize()}, {len(jobs):02d} jobs ({len(jobs_ready)} ready, {len(jobs_not_ready)} not ready) [updated]")
        else:
            logging.debug(f"qsize={queue.qsize()} FULL QUEUE")
        
        time.sleep(1)
    
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


