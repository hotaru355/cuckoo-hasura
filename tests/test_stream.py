import logging
import sys
from uuid import UUID, uuid4

from dotenv import load_dotenv
from httpx import AsyncClient, Client

logging.basicConfig(
    format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)

load_dotenv(".env")
from cuckoo import Insert, Query
from cuckoo.utils import Prop

sys.path.append(".")
from regions import AssetRegion

if __name__ == "__main__":
    i = 0
    for ar in (
        Query(AssetRegion)
        .many(
            where=Prop("site_uuid").eq_("78831eb5-8a5a-4372-97ae-4e8d9be734b3"),
            limit=1,
            # limit=500001,
        )
        .yielding(["uuid", "geometry"])
    ):
        i += 1
        if i % 10000 == 0:
            print(i, ar.uuid)
