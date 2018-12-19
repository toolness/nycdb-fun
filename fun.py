"""\
Find some information about your landlord.

Usage:
  fun.py <address>

Options:
  -h --help                 Show this screen.

Environment variables:
  DATABASE_URL              The Postgres URL to the NYC-DB instance.
"""

import os
import sys
from collections import namedtuple
from typing import List
import docopt
import dotenv
import psycopg2

import geocoding


dotenv.load_dotenv()


def friendly_fetchall(cursor, name: str):
    colnames: List[str] = []
    for desc in cursor.description:
        colname = desc[0]
        while colname in colnames:
            colname += '_'
        colnames.append(colname)

    Result = namedtuple(name, colnames)  # type: ignore
    return [Result(*r) for r in cursor.fetchall()]  # type: ignore


def friendly_execute(cursor, sql: str, name: str='Row'):
    cursor.execute(sql)
    return friendly_fetchall(cursor, name=name)


def get_count(cursor, sql) -> int:
    cursor.execute(f"SELECT COUNT(*) {sql}")
    return cursor.fetchone()[0]


def main():
    args = docopt.docopt(__doc__)

    address: str = args['<address>']

    features = geocoding.search(address)
    if not features:
        print(f"Unable to find geolocation info for '{address}'.")
        sys.exit(1)
    
    props = features[0].properties
    bbl = props.pad_bbl
    print(f"Found {props.label} (BBL {bbl}).")

    nycdb = psycopg2.connect(os.environ['DATABASE_URL'])

    with nycdb.cursor() as cur:
        hpd_viols = get_count(cur, f"FROM hpd_violations WHERE bbl = '{bbl}'")
        dob_viols = get_count(cur, f"FROM dob_violations WHERE bbl = '{bbl}'")
        print(f"The property has {hpd_viols} HPD violations and {dob_viols} DOB violations.")

        docs = friendly_execute(
            cur,
            f"SELECT * "
            f"FROM real_property_legals AS rpl, real_property_master AS rpm "
            f"WHERE rpl.bbl = '{bbl}' AND "
            f"rpl.documentid = rpm.documentid "
            f"ORDER BY rpm.recordedfiled",
            name='ACRISDocument'
        )
        for d in docs:
            amt = f" for ${d.docamount:,} ({d.pcttransferred}% transferred)" if d.docamount else ""
            print(
                f"On {d.docdate or d.recordedfiled} a {d.doctype}{amt} "
                "was signed between:"
            )
            parties = friendly_execute(
                cur,
                f"SELECT * FROM real_property_parties as rpp "
                f"WHERE rpp.documentid = '{d.documentid}'",
                name='ACRISParty'
            )
            for p in parties:
                print(f"  {p.name} / {p.address1} / {p.address2} / {p.city} {p.state} {p.country}")


if __name__ == '__main__':
    main()
