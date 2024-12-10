import collections
import logging
from typing import List, Optional

import memcache

from apps import appsinstalled_pb2

AppsInstalled = collections.namedtuple(
    "AppsInstalled",
    ["dev_type", "dev_id", "lat", "lon", "apps"]
)


def insert_appsinstalled(
        memc_client: memcache.Client,
        batch: List[AppsInstalled],
        dry_run: bool = False
) -> bool:
    try:
        packed_batch = {}
        for appsinstalled in batch:
            ua = appsinstalled_pb2.UserApps()
            ua.lat = appsinstalled.lat
            ua.lon = appsinstalled.lon
            ua.apps.extend(appsinstalled.apps)
            key = f"{appsinstalled.dev_type}:{appsinstalled.dev_id}"
            packed = ua.SerializeToString()
            if dry_run:
                logging.debug("%s -> %s" % (key, str(ua).replace("\n", " ")))
            else:
                packed_batch[key] = packed
        if not dry_run:
            memc_client.set_multi(packed_batch)
    except Exception as exc:
        logging.exception(f"Cannot write to memc: {exc}")
        return False
    return True


def parse_appsinstalled(line: str) -> Optional[AppsInstalled]:
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return None
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return None
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isdigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
        return None
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)
