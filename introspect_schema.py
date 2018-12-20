"""\
Output information about NYCDB's schema.

Usage:
  introspect_schema.py

Options:
  -h --help                 Show this screen.

Environment variables:
  DATABASE_URL              The Postgres URL to the NYC-DB instance.
"""

import os
import re
from typing import Dict
from pathlib import Path
from dataclasses import dataclass
import json
import dotenv
import requests
import yaml
import psycopg2

dotenv.load_dotenv()


MY_DIR = Path(__file__).parent.resolve()

DATA_DIR = MY_DIR / 'data'

DATASETS_YML = DATA_DIR / "datasets.yml"

DATASETS_YML_URL = "https://raw.githubusercontent.com/aepyornis/nyc-db/master/src/nycdb/datasets.yml"

API_VIEW_REGEX = re.compile(r"^(https:\/\/data\.cityofnewyork\.us\/api\/views\/[0-9A-Za-z\-]+)")


@dataclass
class ColumnMeta:
    field_name: str
    name: str
    description: str


@dataclass
class TableMeta:
    table_name: str
    name: str
    description: str
    columns: Dict[str, ColumnMeta]


def download(url: str, dest: Path):
    print(f"Downloading {url}.")
    dest.write_bytes(requests.get(url).content)


def download_table_metadata():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not DATASETS_YML.exists():
        download(DATASETS_YML_URL, DATASETS_YML)
    datasets = yaml.load(DATASETS_YML.read_text())
    for dataset_name, dataset in datasets.items():
        for fileinfo in dataset['files']:
            url = fileinfo['url']
            stem = Path(fileinfo['dest']).stem
            match = API_VIEW_REGEX.match(url)
            if match:
                meta_url = match.group(1)
                metafile = DATA_DIR / f"{stem}.json"
                if not metafile.exists():
                    download(meta_url, metafile)
                meta = json.loads(metafile.read_text(encoding='utf-8'))
                columns: Dict[str, ColumnMeta] = {}
                for colmeta in meta['columns']:
                    field_name = colmeta['fieldName']
                    columns[field_name] = ColumnMeta(
                        field_name=field_name,
                        name=colmeta['name'],
                        description=colmeta.get('description', '')
                    )
                table = TableMeta(
                    table_name=stem,
                    name=meta['name'],
                    description=meta['description'],
                    columns=columns
                )
                print(table)
                print(stem, meta_url)


def main():
    download_table_metadata()
    nycdb = psycopg2.connect(os.environ['DATABASE_URL'])
    with nycdb.cursor() as cur:
        cur.execute("SELECT * FROM information_schema.columns WHERE table_schema = 'public'")


if __name__ == '__main__':
    main()
