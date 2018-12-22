"""\
Output information about NYCDB's schema.

Usage:
  introspect_schema.py [--toc]

Options:
  -h --help                 Show this screen.
  --toc                     Add a table of contents.

Environment variables:
  DATABASE_URL              The Postgres URL to the NYC-DB instance.
"""

import os
import re
from typing import Dict, Any, NamedTuple, List, Optional, Callable
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum
import textwrap
import json
import dotenv
import docopt
import requests
import yaml
import psycopg2

dotenv.load_dotenv()


MY_DIR = Path(__file__).parent.resolve()

DATA_DIR = MY_DIR / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

DATASETS_YML = DATA_DIR / "datasets.yml"

DATASETS_YML_URL = "https://raw.githubusercontent.com/aepyornis/nyc-db/master/src/nycdb/datasets.yml"

API_VIEW_REGEX = re.compile(r"^(https:\/\/data\.cityofnewyork\.us\/api\/views\/[0-9A-Za-z\-]+)")
API_VIEW_SOURCE = 'the City of New York API metadata'


class DataType(Enum):
    array = 'ARRAY'
    character = 'character'
    text = 'text'
    integer = 'integer'
    bigint = 'bigint'
    smallint = 'smallint'
    date = 'date'
    boolean = 'boolean'
    numeric = 'numeric'
    json = 'json'
    time = 'time without time zone'


@dataclass
class ColumnMeta:
    name: str
    verbose_name: str = ''
    description: str = ''
    data_type: Optional[DataType] = None
    data_subtype: Optional[DataType] = None
    is_nullable: bool = False
    is_in_db_schema: bool = False


@dataclass
class TableMeta:
    name: str
    verbose_name: str = ''
    description: str = ''
    description_source: str = ''
    dataset: str = ''
    columns: Dict[str, ColumnMeta] = field(default_factory=dict)
    is_in_db_schema: bool = False


class DatasetMeta(NamedTuple):
    name: str
    tables: List[TableMeta]


def wrap(text: str, initial_indent: str='', subsequent_indent: Optional[str]=None) -> str:
    if subsequent_indent is None:
        subsequent_indent = initial_indent
    return textwrap.fill(
        text,
        initial_indent=initial_indent,
        subsequent_indent=subsequent_indent,
        break_on_hyphens=False,
        break_long_words=False
    )


def download(url: str, dest: Path):
    print(f"Downloading {url}.")
    dest.write_bytes(requests.get(url).content)


def download_datasets_yml() -> Dict[str, Any]:
    if not DATASETS_YML.exists():
        download(DATASETS_YML_URL, DATASETS_YML)
    return yaml.load(DATASETS_YML.read_text())


def clean_description(desc: str) -> str:
    desc = desc.strip()
    if desc and not desc.endswith('.'):
        desc += '.'
    return desc


def download_table_metadata(datasets_yml: Dict[str, Any]) -> Dict[str, TableMeta]:
    all_tables: Dict[str, TableMeta] = {}
    for dataset_name, dataset in datasets_yml.items():
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
                        field_name,
                        verbose_name=colmeta['name'],
                        description=clean_description(colmeta.get('description', ''))
                    )
                table = TableMeta(
                    stem,
                    verbose_name=meta['name'],
                    description=clean_description(meta['description']),
                    description_source=API_VIEW_SOURCE,
                    dataset=dataset_name,
                    columns=columns
                )
                all_tables[table.name] = table
    return all_tables


def introspect_schema_and_populate_table_metadata(tables: Dict[str, TableMeta]):
    nycdb = psycopg2.connect(os.environ['DATABASE_URL'])
    table_schema = "public"
    with nycdb.cursor() as cur:
        cur.execute(
            f"SELECT table_name, column_name, is_nullable, data_type, dtd_identifier "
            f"FROM information_schema.columns "
            f"WHERE table_schema = '{table_schema}' "
            f"ORDER BY table_name, ordinal_position"
        )
        for table_name, column_name, is_nullable, data_type, dtd_identifier in cur.fetchall():
            if table_name not in tables:
                tables[table_name] = TableMeta(table_name)
            table = tables[table_name]
            table.is_in_db_schema = True
            if column_name not in table.columns:
                table.columns[column_name] = ColumnMeta(column_name)
            column = table.columns[column_name]
            column.is_nullable = True if is_nullable == 'YES' else False
            column.data_type = DataType(data_type)
            if column.data_type == DataType.array:
                cur.execute(f"SELECT data_type FROM information_schema.element_types AS e "
                            f"WHERE e.object_schema = '{table_schema}' AND "
                            f"e.object_name = '{table_name}' AND "
                            f"e.object_type = 'TABLE' AND "
                            f"e.collection_type_identifier = '{dtd_identifier}'")
                array_type = DataType(cur.fetchone()[0])
                column.data_subtype = array_type
            column.is_in_db_schema = True

    for table_name, table in list(tables.items()):
        if not table.is_in_db_schema:
            del tables[table_name]
        for column_name, column in list(table.columns.items()):
            if not column.is_in_db_schema:
                del table.columns[column_name]


def populate_table_metadata_with_dataset_names(tables: Dict[str, TableMeta],
                                               datasets_yml: Dict[str, Any]):
    for dataset_name, dataset in datasets_yml.items():
        schema = dataset['schema']
        if not isinstance(schema, list):
            schema = [schema]
        for s in schema:
            table_name = s['table_name']
            tables[table_name].dataset = dataset_name


def create_datasets_metadata(datasets_yml: Dict[str, Any],
                             tables: Dict[str, TableMeta]) -> List[DatasetMeta]:
    return [
        DatasetMeta(
            name=dataset_name,
            tables=[t for t in tables.values() if t.dataset == dataset_name]
        )
        for dataset_name in datasets_yml
    ]


def slugify(value: str) -> str:
    return value.lower().replace(' ', '-').replace('`', '')


def document_datasets(datasets: List[DatasetMeta], show_toc: bool=True):
    print("# NYC-DB schema")
    print("\nThis documentation was automatically generated by a Python script.")
    print("\nNote that unless otherwise specified, all columns are nullable.")

    dataset_title: Callable[[DatasetMeta], str] = lambda d: f"The `{d.name}` dataset"
    table_title: Callable[[TableMeta], str] = lambda t: f"The `{t.name}` table"
    toclink: Callable[[str], str] = lambda title: f"[{title}](#{slugify(title)})"

    if show_toc:
        # Note that this table of contents links to anchors that will only
        # be defined if the markdown is posted to GitHub.
        for dataset in datasets:
            print(f"* {toclink(dataset_title(dataset))}")
            for table in dataset.tables:
                print(f"  * {toclink(table_title(table))}")

    for dataset in datasets:
        print(f"\n## {dataset_title(dataset)}")
        for table in dataset.tables:
            print(f"\n### {table_title(table)}")
            if table.description:
                print(f"\nFrom {table.description_source}:\n")
                print(wrap(table.description, "> "))
            print(f"\nThis table has the following columns:\n")
            for column in table.columns.values():
                article_adj = "A" if column.is_nullable else "A required"
                assert column.data_type is not None
                if column.data_type == DataType.array:
                    assert column.data_subtype is not None
                    dtype = f"{column.data_subtype.value} array"
                else:
                    dtype = column.data_type.value
                print(wrap(
                    f"* `{column.name}` - {article_adj} {dtype} value.\n",
                    "  ",
                    "    "
                ))
                if column.description:
                    desc = wrap(column.description, "    > ")
                    print(f"\n{desc}\n")


def main():
    args = docopt.docopt(__doc__)

    datasets_yml = download_datasets_yml()
    tables = download_table_metadata(datasets_yml)
    introspect_schema_and_populate_table_metadata(tables)
    populate_table_metadata_with_dataset_names(tables, datasets_yml)
    datasets = create_datasets_metadata(datasets_yml, tables)
    document_datasets(datasets, show_toc=args['--toc'])


if __name__ == '__main__':
    main()
