from typing import Optional

from httpx import AsyncClient, Client

from .constants import HASURA_DEFAULT_CONFIG, CuckooConfig, GlobalCuckooConfig


class Cuckoo:
    _global_config: GlobalCuckooConfig = {
        "session": None,
        "session_async": None,
        "cuckoo_config": HASURA_DEFAULT_CONFIG.copy(),
    }

    @classmethod
    def configure(
        cls,
        cuckoo_config: Optional[CuckooConfig] = None,
        *,
        session: Optional[Client] = None,
        session_async: Optional[AsyncClient] = None,
    ):
        for name, prop in [
            (
                "cuckoo_config",
                {
                    **cls._global_config["cuckoo_config"],
                    **(cuckoo_config if cuckoo_config else {}),
                },
            ),
            ("session", session),
            ("session_async", session_async),
        ]:
            if prop is not None:
                cls._global_config[name] = prop

    @classmethod
    def cuckoo_config(cls, *config: CuckooConfig):
        merged_config = Cuckoo._global_config["cuckoo_config"].copy()
        for cfg in config:
            merged_config.update(cfg)

        return merged_config

    @classmethod
    def session(cls):
        return cls._global_config["session"]

    @classmethod
    def session_async(cls):
        return cls._global_config["session_async"]
