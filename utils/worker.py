import logging
import threading
from queue import Queue
from typing import Dict, List

import memcache
from tqdm import tqdm

from apps.appsinstalled import AppsInstalled
from apps.appsinstalled import insert_appsinstalled
from apps.appsinstalled import parse_appsinstalled


class Worker(threading.Thread):
    def __init__(
            self,
            queue: Queue,
            device_memc: Dict[str, str],
            dry_run: bool,
            results_lock: threading.Lock,
            results: Dict[str, int],
            pbar: tqdm,
            memc_clients: Dict[str, memcache.Client],
            batch_size: int = 1000
    ) -> None:
        threading.Thread.__init__(self)
        self.queue = queue
        self.device_memc = device_memc
        self.dry_run = dry_run
        self.results_lock = results_lock
        self.results = results
        self.pbar = pbar
        self.memc_clients = memc_clients
        self.batch_size = batch_size

    def run(self) -> None:
        batch = []
        while True:
            line = self.queue.get()
            if line is None:
                if batch:
                    self.process_batch(batch)
                break
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                with self.results_lock:
                    self.results['errors'] += 1
                self.queue.task_done()
                self.pbar.update(1)
                continue
            batch.append(appsinstalled)
            if len(batch) >= self.batch_size:
                self.process_batch(batch)
                batch = []
            self.queue.task_done()
            self.pbar.update(1)
        if batch:
            self.process_batch(batch)

    def process_batch(self, batch: List[AppsInstalled]) -> None:
        memc_addr = self.device_memc.get(batch[0].dev_type)
        if not memc_addr:
            logging.error("Unknown device type: %s" % batch[0].dev_type)
            with self.results_lock:
                self.results['errors'] += len(batch)
            return
        memc_client = self.memc_clients[memc_addr]
        if insert_appsinstalled(memc_client, batch, self.dry_run):
            with self.results_lock:
                self.results['processed'] += len(batch)
        else:
            with self.results_lock:
                self.results['errors'] += len(batch)
