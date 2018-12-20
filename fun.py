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


RESERVED_NAMES = ['class']


def friendly_fetchall(cursor, name: str):
    colnames: List[str] = []
    for desc in cursor.description:
        colname = desc[0]
        while colname in colnames + RESERVED_NAMES:
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
        num_hpd_viols = get_count(cur, f"FROM hpd_violations WHERE bbl = '{bbl}'")
        num_dob_viols = get_count(cur, f"FROM dob_violations WHERE bbl = '{bbl}'")
        print(f"The property has {num_hpd_viols} HPD violations and {num_dob_viols} DOB violations.")

        if num_hpd_viols:
            hpd_viols = friendly_execute(
                cur,
                f"SELECT * FROM hpd_violations WHERE bbl = '{bbl}' ORDER BY inspectiondate DESC LIMIT 10"
            )
            print("Here are some HPD violations:")
            for v in hpd_viols:
                print(f"  * {v.inspectiondate} {v.novdescription}")

        if num_dob_viols:
            dob_viols = friendly_execute(
                cur,
                f"SELECT * FROM dob_violations WHERE bbl = '{bbl}' ORDER BY issuedate DESC LIMIT 10"
            )
            print("Here are some DOB violations:")
            for v in dob_viols:
                print(f"  * {v.issuedate} {v.description}")

        plutos = friendly_execute(
            cur,
            f"SELECT * FROM pluto_18v1 WHERE bbl = '{bbl}'"
        )
        for pluto in plutos:
            print(f"The property has {pluto.numfloors} floors and was built in {pluto.yearbuilt}.")

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
                party = " / ".join(filter(None, [
                    p.name, p.address1, p.address2, p.city, p.state,
                    p.country if p.country != "US" else None
                ]))
                print(f"  {party}")


if __name__ == '__main__':
    main()
