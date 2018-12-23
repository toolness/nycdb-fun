from typing import Optional, NamedTuple, Iterable, Any
import sqlite3
from sqlite3 import Connection, Cursor


class LastmodInfo(NamedTuple):
    url: str
    etag: Optional[str] = None
    last_modified: Optional[str] = None


class Lastmod:
    def __init__(self, conn: Connection, table: str='lastmod', param_subst: str='?'):
        self.table = table
        self.param_subst = param_subst
        self.conn = conn
        self._init_db()

    def _init_db(self) -> None:
        self.exec_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                url text PRIMARY KEY NOT NULL,
                etag text,
                last_modified text
            )
            """
        )

    def exec_sql(self, sql: str, params: Iterable[Any]=tuple()) -> Cursor:
        sql = sql.replace('?', self.param_subst)
        cur = self.conn.cursor()
        cur.execute(sql, params)
        return cur

    def set_info(self, info: LastmodInfo, commit=True) -> None:
        if self.get_info(info.url):
            self.exec_sql(f"UPDATE {self.table} SET etag = ?, last_modified = ? WHERE url = ?",
                          (info.etag, info.last_modified, info.url))
        else:
            self.exec_sql(
                f"INSERT INTO {self.table} (url, etag, last_modified) VALUES (?, ?, ?)", info)
        if commit:
            self.conn.commit()

    def get_info(self, url: str) -> Optional[LastmodInfo]:
        cur = self.exec_sql(
            f"SELECT url, etag, last_modified FROM {self.table} WHERE url = ?", (url,))
        result = cur.fetchone()
        return None if result is None else LastmodInfo(*result)
