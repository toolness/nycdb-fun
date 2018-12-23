import sqlite3
from typing import Dict
import requests
import yaml

import lastmod
import introspect_schema


def main():
    datasets_yml = introspect_schema.download_datasets_yml()
    conn = sqlite3.connect('dataset_lastmod.db')
    lm = lastmod.Lastmod(conn)
    for dataset in datasets_yml.values():
        for fileinfo in dataset['files']:
            url = fileinfo['url']
            filename = fileinfo['dest']
            lminfo = lm.get_info(url)
            headers: Dict[str, str] = {}
            if lminfo is not None:
                print(f"Found {lminfo}")
                if lminfo.etag:
                    headers['If-None-Match'] = lminfo.etag
                if lminfo.last_modified:
                    headers['If-Modified-Since'] = lminfo.last_modified
            print(f"Fetching {url} ({filename})")
            res = requests.get(url, headers=headers, stream=True)
            print(f"Got HTTP {res.status_code} for {filename}")
            if res.status_code == 200:
                print(f"\n*** DOWNLOADING {filename} ***\n")
                lminfo = lastmod.LastmodInfo(
                    url=url,
                    etag=res.headers.get('ETag'),
                    last_modified=res.headers.get('Last-Modified')
                )
                print(f"Updating {lminfo}")
                lm.set_info(lminfo)
            res.close()


if __name__ == '__main__':
    main()
