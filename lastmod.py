from typing import Optional, NamedTuple

from dbhash import AbstractDbHash


class LastmodInfo(NamedTuple):
    url: str
    etag: Optional[str] = None
    last_modified: Optional[str] = None


class Lastmod:
    def __init__(self, dbhash: AbstractDbHash):
        self.dbhash = dbhash

    def set_info(self, info: LastmodInfo, commit=True) -> None:
        self.dbhash.set_or_delete(
            f'last_modified:{info.url}', info.last_modified)

        self.dbhash.set_or_delete(
            f'etag:{info.url}', info.etag)

    def get_info(self, url: str) -> LastmodInfo:
        return LastmodInfo(
            url=url,
            etag=self.dbhash.get(f'etag:{url}'),
            last_modified=self.dbhash.get(f'last_modified:{url}')
        )
