
import concurrent.futures
import logging
import queue
import threading

from cads_api_client.processing import ProcessingFailedError


QUEUE_GET_PUT_TIMEOUT_S = 10


# API client calls
def _submit_and_wait(collection, request, downloads_queue, *args, **kwargs):

    # submit request
    job = collection.submit(**request)  # TODO: set timeout
    job_id = job.response.json()["jobID"]
    logging.debug(f"{job_id} - Adding")

    # wait on result
    logging.debug(f"{job_id} - Waiting on result")
    try:
        job_status = job.wait_on_result(*args, **kwargs)   # TODO: set timeout
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
def _producer(collection, requests_queue, downloads_queue, end_event, *args, **kwargs):
    logging.debug("Producer starting")
    results = []
    while not requests_queue.empty():
        request = requests_queue.get(timeout=QUEUE_GET_PUT_TIMEOUT_S)
        job_id, job_status = _submit_and_wait(collection, request, downloads_queue, *args, **kwargs)
        results.append((job_id, job_status))
    end_event.set()
    return results


def _consumer(downloads_queue, end_event, *args, **kwargs):
    logging.debug("Consumer starting")
    results = []
    while not end_event.is_set() or not downloads_queue.empty():
        job = downloads_queue.get(timeout=QUEUE_GET_PUT_TIMEOUT_S)
        job_id, download_status = _download(job, *args, **kwargs)
        results.append((job_id, download_status))
    return results


def _format_results(p_futures, c_futures):
    # producer_results, consumer_results = tuple(map(
    #     lambda lst_futures: [res for fut in lst_futures for res in fut.result()],
    #     [p_futures, c_futures]))
    _c_path_map: dict = {job_id: path for c_fut in c_futures for job_id, path in c_fut.result()}
    p_res = {
        job_id: (job_status, _c_path_map.get(job_id))
        for p_fut in p_futures
        for job_id, job_status in p_fut.result()
    }
    return p_res


def multi_retrieve(collection, requests,
                   target, retry_options,
                   max_updates, max_downloads):

    # initialize queues and events for concurrency
    requests_q = queue.Queue()
    downloads_q = queue.Queue(maxsize=max_downloads)
    # we still need an event in case all requests are extracted from the queue but they are not fed into the
    # downloads queue yet (see stop condition for consumer)
    end_event = threading.Event()

    # put requests into queue
    for request in requests:
        requests_q.put(request)

    # producer / consumer
    p_futures, c_futures = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_updates + max_downloads) as executor:
        for i in range(max_updates):
            p_futures.append(
                executor.submit(_producer, collection, requests_q, downloads_q, end_event,
                                retry_options=retry_options)
            )

        for i in range(max_downloads):
            c_futures.append(
                executor.submit(_consumer, downloads_q, end_event,
                                target=target, retry_options=retry_options)
            )

    results = _format_results(p_futures=p_futures, c_futures=c_futures)

    return results



