import concurrent.futures
import logging
import queue
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, TypeVar

import _queue

from .processing import ProcessingFailedError, Remote

Collection = TypeVar("Collection")


# params
QUEUE_GET_PUT_TIMEOUT_S = 10


# utils
@dataclass
class TaskResult:
    good: bool
    input: Any
    output: Any


def _hash(request: dict):
    return hash(", ".join(f"{k}={v}" for k, v in request.items()))


# API client calls
def _submit_and_wait(
    collection: Collection, request: dict, *args, **kwargs
) -> Tuple[Remote, str]:
    # submit request
    req_id = _hash(request)
    logging.debug(f"{req_id} - Sending request")
    job = collection.submit(**request)  # TODO: set timeout
    # req_uid = job.request_uid

    # wait on result
    logging.debug(f"{req_id} - {job.id} - Waiting on result")
    try:
        job_status = job.wait_on_result(*args, **kwargs)  # TODO: set timeout
    except ProcessingFailedError:
        job_status = "failed"

    logging.debug(f"{req_id} - {job.id} - Job {job_status}")

    return job, job_status


def _download(job: Remote, *args, **kwargs) -> str:
    logging.debug(f"{job.id} - Downloading")
    path = job._download_result(*args, **kwargs)
    logging.debug(f"{job.id} - Downloaded")
    return path


# producer/consumer pattern
def _producer(
    collection: Collection,
    requests_queue: queue.Queue,
    downloads_queue: queue.Queue,
    working_queue: queue.Queue,
    *args,
    **kwargs,
) -> List[TaskResult]:
    logging.debug("Producer starting")
    results = []
    while not requests_queue.empty():
        working_queue.put(True)
        request = requests_queue.get(timeout=QUEUE_GET_PUT_TIMEOUT_S)
        # handle exception inside task because a single job that fails could give error for the
        # entire task (thread)
        # TODO decorator?
        try:
            res, good = (
                _submit_and_wait(collection, request, downloads_queue, *args, **kwargs),
                True,
            )
            job, job_status = res
            if job_status == "successful":
                downloads_queue.put(job, timeout=QUEUE_GET_PUT_TIMEOUT_S)
        except BaseException as e:
            res, good = str(e), False
        results.append(TaskResult(good=good, input=request, output=res))
        working_queue.get()

    # end_event.set()
    return results


def _consumer(
    downloads_queue: queue.Queue, working_q: queue.Queue, *args, **kwargs
) -> List[TaskResult]:
    logging.debug("Consumer starting")
    results = []
    while not working_q.empty() or not downloads_queue.empty():
        job = downloads_queue.get()  # no timeout for get as queue can be empty
        # handle exception inside task because a single job that fails could give error for the
        # entire task (thread)
        # TODO decorator?
        try:
            res, good = _download(job, *args, **kwargs), True
        except BaseException as e:
            res, good = str(e), False
        results.append(TaskResult(good=good, input=job, output=res))
    return results


# multi-threading orchestrator
def multi_retrieve(
    collection: Collection,
    requests: List[dict],
    target_folder: Optional[str] = ".",
    retry_options: Dict[str, Any] = {},
    max_submit: int = 10,
    max_download: int = 2,
):
    # initialize queues and events for concurrency
    requests_q = queue.Queue()
    downloads_q = queue.Queue()
    # we don't need maxsize as concurrency is handled by the number  of threads instantiated in the pool.
    working_q = queue.Queue(maxsize=max_submit)
    # edge cases:
    # a) when all requests are extracted from the requests queue and not yet fed into the downloads queue,
    #    we cannot use the fact that both the queues are empty as a stop condition.
    # b) when the download queue is empty and the last request has been extracted from the requests queue,
    #    but not yet submitted into the downloads queue (still waiting for results) we cannot set an end
    #    event in one of the threads to be used a stop condition (consumers would end while the last producer
    #    hasn't terminated yet) that's why we need to monitor the execution status of the producer threads
    #    through the working queue.

    # put requests into queue
    for request in requests:
        requests_q.put(request)

    # producer / consumer
    p_futures, c_futures = [], []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_submit + max_download
    ) as executor:
        for i in range(max_submit):
            p_futures.append(
                executor.submit(
                    _producer,
                    collection,
                    requests_q,
                    downloads_q,
                    working_q,
                    retry_options=retry_options,
                )
            )

        for i in range(max_download):
            c_futures.append(
                executor.submit(
                    _consumer,
                    downloads_q,
                    working_q,
                    target_folder=target_folder,
                    retry_options=retry_options,
                )
            )

    results = _format_results(p_futures=p_futures, c_futures=c_futures)

    return results


def _format_results(p_futures, c_futures):
    _c_path_map, results = {}, {}

    for c_fut in c_futures:
        try:
            c_results = c_fut.result()
            for r in c_results:
                good, job, res = r.good, r.input, r.output
                if good:  # successful downloads
                    _c_path_map.update({job.id: {"path": res}})
                else:  # failed downloads
                    _c_path_map.update({job.id: {"exception": res}})
        except _queue.Empty:
            logging.debug("_queue.Empty")

    for p_fut in p_futures:
        try:
            p_results = p_fut.result()
            for r in p_results:
                good, req, res = r.good, r.input, r.output
                if good:  # successful job requests
                    results.update(
                        {
                            _hash(req): {
                                "request": req,
                                "job": {"id": res.id, "status": res.status},
                                "download": _c_path_map.get(res.id, {}),
                            }
                        }
                    )
                else:  # failed job requests
                    results.update(
                        {_hash(req): {"request": req, "job": {"exception": res}}}
                    )
        except _queue.Empty:
            logging.debug("_queue.Empty")

    return results
