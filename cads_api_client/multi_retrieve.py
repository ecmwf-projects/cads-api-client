
import concurrent.futures
import logging
import queue
import threading
import _queue

from cads_api_client.processing import ProcessingFailedError


QUEUE_GET_PUT_TIMEOUT_S = 10


def _hash(request: dict):
    return hash(', '.join(f"{k}={v}" for k, v in request.items()))


# API client calls
def _submit_and_wait(collection, request: dict, downloads_queue, *args, **kwargs):

    # submit request
    req_id = _hash(request)
    logging.debug(f"{req_id} - Sending request")
    job = collection.submit(**request)  # TODO: set timeout

    # wait on result
    logging.debug(f"{req_id} - {job.id} - Waiting on result")
    try:
        job_status = job.wait_on_result(*args, **kwargs)   # TODO: set timeout
    except ProcessingFailedError:
        job_status = "failed"

    logging.debug(f"{req_id} - {job.id} - Job {job_status}")
    if job_status == "successful":
        downloads_queue.put(job, timeout=QUEUE_GET_PUT_TIMEOUT_S)

    return job


def _download(job, *args, **kwargs):
    logging.debug(f"{job.id} - Downloading")
    path = job._download_result(*args, **kwargs)
    logging.debug(f"{job.id} - Downloaded")
    return path


# producer/consumer pattern
def _producer(collection, requests_queue, downloads_queue, end_event, *args, **kwargs):
    logging.debug("Producer starting")
    results = []
    while not requests_queue.empty():
        request = requests_queue.get(timeout=QUEUE_GET_PUT_TIMEOUT_S)
        # handle exception inside task because a single job that fails could give error for the entire task (thread)
        # TODO decorator?
        try:
            res, good = _submit_and_wait(collection, request, downloads_queue, *args, **kwargs), True
        except BaseException as e:
            res, good = str(e), False
        results.append((good, request, res))
    end_event.set()
    return results


def _consumer(downloads_queue, end_event, *args, **kwargs):
    logging.debug("Consumer starting")
    results = []
    while not end_event.is_set() or not downloads_queue.empty():
        job = downloads_queue.get(timeout=QUEUE_GET_PUT_TIMEOUT_S)
        # handle exception inside task because a single job that fails could give error for the entire task (thread)
        try:
            res, good = _download(job, *args, **kwargs), True
        except BaseException as e:
            res, good = str(e), False
        results.append((good, job, res))
    return results


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


def _format_results(p_futures, c_futures):

    _c_path_map, results = {}, {}

    for c_fut in c_futures:
        try:
            c_results = c_fut.result()
            for good, job, res in c_results:
                if good:
                    _c_path_map.update({job.id: {"path": res}})
                else:
                    _c_path_map.update({job.id: {"exception": res}})
        except _queue.Empty:
            logging.debug("_queue.Empty")

    for p_fut in p_futures:
        try:
            p_results = p_fut.result()
            for good, req, res in p_results:
                if good:
                    results.update({_hash(req): {
                        "request": req,
                        "job": {"id": res.id, "status": res.status},
                        "download": _c_path_map.get(res.id)
                    }})
                else:
                    results.update({_hash(req): {
                        "request": req,
                        "job": {"exception": res}
                    }})
        except _queue.Empty:
            logging.debug("_queue.Empty")

    return results
