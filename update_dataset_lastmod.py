import sqlite3
from typing import Dict
import requests
import yaml

import dbhash
import lastmod
import introspect_schema


def main():
    datasets_yml = introspect_schema.download_datasets_yml()
    conn = sqlite3.connect('dataset_lastmod_dbhash.db')
    storage = dbhash.SqlDbHash(conn, 'lastmod')
    lm = lastmod.Lastmod(storage)
    print("Processing all dataset files.")
    for dataset in datasets_yml.values():
        for fileinfo in dataset['files']:
            url = fileinfo['url']
            filename = fileinfo['dest']

            print(f"\nProcessing {filename}.")

            lminfo = lm.get_info(url)
            headers: Dict[str, str] = {}
            if lminfo.etag:
                print(f"  etag: {lminfo.etag}")
                headers['If-None-Match'] = lminfo.etag
            if lminfo.last_modified:
                print(f"  last modified: {lminfo.last_modified}")
                headers['If-Modified-Since'] = lminfo.last_modified
            print(f"  Fetching from {url}...")
            res = requests.get(url, headers=headers, stream=True)
            print(f"  Got HTTP {res.status_code}.")
            if res.status_code == 200:
                print(f"\n  *** DOWNLOADING {filename} ***\n")
                etag = res.headers.get('ETag')
                last_modified = res.headers.get('Last-Modified')
                lminfo = lastmod.LastmodInfo(
                    url=url,
                    etag=etag,
                    last_modified=last_modified
                )
                print(f"  Updating etag={etag}, last_modified={last_modified}.")
                lm.set_info(lminfo)
            res.close()


if __name__ == '__main__':
    main()
