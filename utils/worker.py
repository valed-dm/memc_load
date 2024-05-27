import logging
import threading
from queue import Queue
from typing import Dict

import memcache
from tqdm import tqdm

from apps.appsinstalled import parse_appsinstalled, insert_appsinstalled


class Worker(threading.Thread):
    def __init__(
            self,
            queue: Queue,
            device_memc: Dict[str, str],
            dry_run: bool,
            results_lock: threading.Lock,
            results: Dict[str, int],
            pbar: tqdm,
            memc_clients: Dict[str, memcache.Client]
    ) -> None:
        threading.Thread.__init__(self)
        self.queue = queue
        self.device_memc = device_memc
        self.dry_run = dry_run
        self.results_lock = results_lock
        self.results = results
        self.pbar = pbar
        self.memc_clients = memc_clients

    def run(self) -> None:
        while True:
            line = self.queue.get()
            if line is None:
                break
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                with self.results_lock:
                    self.results['errors'] += 1
                self.queue.task_done()
                self.pbar.update(1)
                continue
            memc_addr = self.device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                logging.error("Unknown device type: %s" % appsinstalled.dev_type)
                with self.results_lock:
                    self.results['errors'] += 1
                self.queue.task_done()
                self.pbar.update(1)
                continue
            memc_client = self.memc_clients[memc_addr]
            if insert_appsinstalled(memc_client, appsinstalled, self.dry_run):
                with self.results_lock:
                    self.results['processed'] += 1
            else:
                with self.results_lock:
                    self.results['errors'] += 1
            self.queue.task_done()
            self.pbar.update(1)
