import collections
import logging
from typing import Optional

import memcache

from apps import appsinstalled_pb2

AppsInstalled = collections.namedtuple(
    "AppsInstalled",
    ["dev_type", "dev_id", "lat", "lon", "apps"]
)


def insert_appsinstalled(memc_client: memcache.Client, appsinstalled: AppsInstalled, dry_run: bool = False) -> bool:
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    try:
        if dry_run:
            logging.debug("%s -> %s" % (key, str(ua).replace("\n", " ")))
        else:
            memc_client.set(key, packed)
    except Exception as exc:
        logging.exception("Cannot write to memc: %s" % exc)
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
