import logging
import os
import sys
import time

import psutil
from dotenv import load_dotenv
from httpx import Client, HTTPTransport

logging.basicConfig(
    format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)

load_dotenv(".env")
sys.path.append(".")
from cuckoo import Query
from cuckoo.utils import Prop
from regions import AssetRegion


def get_bytes_received():
    bytes_rcv = psutil.net_io_counters(pernic=True)["wlp0s20f3"].bytes_recv / 1024**2
    return round(bytes_rcv, 3)


def print_bytes_received():
    print(f"Bytes received total: {round(get_bytes_received()-BASE_BYTES_RCV,3)} MB.")


def get_mem_used():
    mem_used = psutil.Process(os.getpid()).memory_info().rss / 1024**2
    # mem_used = psutil.virtual_memory().used / 1024**2
    return round(mem_used, 3)


def print_mem_used():
    print(f"Mem used total: {round(get_mem_used()-BASE_MEM_USED,3)} MB.")


if __name__ == "__main__":
    i = 0
    t0 = time.time()
    logging.basicConfig()
    logging.getLogger().setLevel(logging.ERROR)

    BASE_BYTES_RCV = get_bytes_received()
    BASE_MEM_USED = get_mem_used()
    print_bytes_received()
    print_mem_used()
    with Client(
        timeout=None,
        mounts={
            "https://": HTTPTransport(retries=5),
            "http://": HTTPTransport(retries=5),
        },
    ) as session:
        for ar in (
            Query(AssetRegion, session=session, logger=logging.getLogger("PERF TEST"))
            .many(
                where={},
                # where=Prop("site_uuid").eq_("78831eb5-8a5a-4372-97ae-4e8d9be734b3"),
                # limit=1,
                limit=500000,
            )
            .yielding(["uuid", "geometry"])
        ):
            i += 1
            if i % 10000 == 0:
                print(i)
                print_bytes_received()
                print_mem_used()

    t1 = time.time()
    print("======================")
    print(f"Total time = {round(t1 - t0, 2)} s.")
    print_bytes_received()
    print_mem_used()
