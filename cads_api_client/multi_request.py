
import concurrent.futures
import logging
import queue
import threading

from cads_api_client.processing import ProcessingFailedError


QUEUE_GET_PUT_TIMEOUT_S = 10


# API client calls
def _submit_and_wait(collection, request, downloads_queue):

    # submit request
    job = collection.submit(**request)  # TODO: set timeout
    job_id = job.response.json()["jobID"]
    logging.debug(f"{job_id} - Adding")

    # wait on result
    logging.debug(f"{job_id} - Waiting on result")
    try:
        job_status = job.wait_on_result()   # TODO: set timeout
    except ProcessingFailedError:
        job_status = 'failed'
    logging.debug(f"{job_id} - Job {job_status}")

    if job_status in ('successful'):
        downloads_queue.put(job, timeout=QUEUE_GET_PUT_TIMEOUT_S)
    
    return job_id, job_status


def _download(job, *args, **kwargs):
    job_id = job.response.json()["jobID"]
    logging.debug(f"{job_id} - Downloading")
    path = job._download_result(*args, **kwargs)
    logging.debug(f"{job_id} - Downloaded")
    return job_id, path


# producer/consumer pattern
def _producer(collection, requests_queue, downloads_queue, end_event):
    logging.debug("Producer starting")
    results = []
    while not requests_queue.empty():
        request = requests_queue.get(timeout=QUEUE_GET_PUT_TIMEOUT_S)
        job_id, job_status = _submit_and_wait(collection, request, downloads_queue)
        results.append((job_id, job_status))
    end_event.set()
    return results


def _consumer(downloads_queue, end_event):
    logging.debug("Consumer starting")
    results = []
    while not end_event.is_set() or not downloads_queue.empty():
        job = downloads_queue.get(timeout=QUEUE_GET_PUT_TIMEOUT_S)
        job_id, download_status = _download(job)
        results.append((job_id, download_status))
    return results


def download_multiple_requests(collection, requests, MAX_UPDATES=10, MAX_DOWNLOADS=2):

    requests_q = queue.Queue()
    downloads_q = queue.Queue(maxsize=MAX_DOWNLOADS)
    # we still need an event in case all requests are extracted from the queue but they are not fed into the
    # downloads queue yet (see stop condition for consumer)
    end_event = threading.Event()

    for request in requests:
        requests_q.put(request)

    # producer / consumer
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_UPDATES+MAX_DOWNLOADS) as executor:
        for i in range(MAX_UPDATES):
            producer_results = executor.submit(_producer, collection, requests_q, downloads_q, end_event)
        for i in range(MAX_DOWNLOADS):
            consumer_results = executor.submit(_consumer, downloads_q, end_event)

    return producer_results, consumer_results



