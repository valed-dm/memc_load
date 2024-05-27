import gzip
import logging
import os
import threading
from queue import Queue
from typing import Dict, List

import memcache
from tqdm import tqdm

from utils.dot_rename import dot_rename
from utils.line_generator import line_generator
from utils.worker import Worker

NORMAL_ERR_RATE = 0.01


def process_file(
        file_path: str,
        device_memc: Dict[str, str],
        dry_run: bool,
        num_threads: int
) -> None:
    results = {'processed': 0, 'errors': 0}
    results_lock = threading.Lock()
    queue: Queue = Queue(maxsize=num_threads * 2)

    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)

    pbar = tqdm(total=total_lines, desc=f"Processing {os.path.basename(file_path)}")

    memc_clients = {addr: memcache.Client([addr]) for addr in device_memc.values()}

    threads: List[Worker] = []
    for _ in range(num_threads):
        thread = Worker(queue, device_memc, dry_run, results_lock, results, pbar, memc_clients)
        thread.start()
        threads.append(thread)

    for line in line_generator(file_path):
        queue.put(line)
        if queue.qsize() >= num_threads * 2:
            queue.join()

    queue.join()

    for _ in range(num_threads):
        queue.put(None)
    for thread in threads:
        thread.join()

    pbar.close()

    err_rate = float(results['errors']) / results['processed'] if results['processed'] else 1
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfully loaded" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))

    dot_rename(file_path)
