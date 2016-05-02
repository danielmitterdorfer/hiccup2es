#!/usr/bin/env python3

import re
import json
import argparse
import urllib3
try:
    import certifi
except ImportError:
    certifi = None

__version__ = "0.0.1"

BULK_SIZE = 5000

if certifi:
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
else:
    http = urllib3.PoolManager()


def parse_args():
    p = argparse.ArgumentParser(prog="hiccup2es", description="Import jHiccup logs to Elasticsearch")
    p.add_argument('--version', action='version', version="%(prog)s " + __version__)
    p.add_argument(
        "--input-file",
        help="jHiccup CSV file that should be imported",
        required=True)
    p.add_argument(
        "--file-type",
        help="Whether the provided file is the application's hiccup file or the control file (default: application)",
        choices=["application", "control"],
        default="application"
    )
    p.add_argument(
        "--create-index",
        help="Create a new index in Elasticsearch (default: False)",
        default=False,
        action="store_true")
    p.add_argument(
        "--index-name",
        help="Elasticsearch hiccup index (default: hiccups)",
        default="hiccups"
    )
    p.add_argument(
        "--type-name",
        help="Elasticsearch hiccup type (default: hiccup)",
        default="hiccup"
    )
    p.add_argument(
        "--protocol",
        help="Protocol to use to connect to Elasticsearch (default:http)",
        default="http",
        choices=["http", "https"]
    )
    p.add_argument(
        "--host",
        help="Elasticsearch host name (default:localhost)",
        default="localhost"
    )
    p.add_argument(
        "--port",
        help="Elasticsearch HTTP port (default: 9200)",
        default=9200
    )
    p.add_argument(
        "--user",
        help="User name for basic authentication",
        default=""
    )
    p.add_argument(
        "--password",
        help="Password for basic authentication",
        default=""
    )
    return p.parse_args()


def as_bytes(data):
    return bytearray(data, encoding="UTF-8")


def auth_header(user, password):
    if user and password:
        return urllib3.make_headers(basic_auth="%s:%s" % (user, password))
    else:
        return None


def create_index(endpoint, user, password, index, type_name):
    url = "%s/%s" % (endpoint, index)

    with open('mapping.json', 'r') as mapping_file:
        mapping = mapping_file.read().replace("##TYPE_NAME##", type_name)

    response = http.urlopen(method="PUT", url=url, headers=auth_header(user, password), body=as_bytes(mapping))
    if response.status >= 300:
        raise RuntimeError("Could not create index [%s]. Elasticsearch returned HTTP status [%s] (content: [%s])"
                           % (index, response.status, response.data))


# Beware, this is just a very basic bulk API, we just check HTTP status but not whether the bulk failed!
def send_bulk(endpoint, user, password, data):
    url = "%s/_bulk" % endpoint

    response = http.urlopen(method="POST", url=url, headers=auth_header(user, password), body=as_bytes("\n".join(data)))
    # this is *NOT* a guarantee that everything went smooth but it should do for now
    if response.status >= 300:
        raise RuntimeError("Could not bulk index. Elasticsearch returned HTTP status [%s]. (content: [%s])"
                           % (response.status, response.data))


# This is the file format that we expect:

# [Interval percentile log between 0.000 and <Infinite> seconds (relative to StartTime)]
# [StartTime: 1460655200.273 (seconds since epoch), Thu Apr 14 19:33:20 CEST 2016]
# "Timestamp","Int_Count","Int_50%","Int_90%","Int_Max","Total_Count","Total_50%","Total_90%","Total_99%","Total_99.9%","Total_99.99%","Total_Max"
# ...
#
def main():
    args = parse_args()

    endpoint = "%s://%s:%s" % (args.protocol, args.host, args.port)

    if args.create_index:
        create_index(endpoint, args.user, args.password, args.index_name, args.type_name)

    start_time_pattern = re.compile("StartTime: (\d.*) \(.*")
    bulk_data = []

    with open(args.input_file) as f:
        start_timestamp = None
        while True:
            line = f.readline()
            if line == "":
                break
            elif line.startswith("#[StartTime"):
                match = start_time_pattern.search(line)
                if not match:
                    raise RuntimeError("Could not extract start timestamp from [%s]" % line)
                start_timestamp = float(match.group(1))
            elif not (line.startswith("#") or line.startswith("\"")):
                if not start_timestamp:
                    raise RuntimeError("No valid start timestamp detected (currently reading line [%s])" % line)

                tup = line.strip().split(",")
                d = {
                    "timestamp": round((start_timestamp + float(tup[0])) * 1000),
                    "relative-time-seconds": float(tup[0]),
                    "int_count": tup[1],
                    "int_50": tup[2],
                    "int_90": tup[3],
                    "int_100": tup[4],
                    "type": args.file_type
                }
                bulk_data.append('{ "index" : { "_index" : "%s", "_type" : "%s"} }' % (args.index_name, args.type_name))
                bulk_data.append(json.dumps(d))

                if len(bulk_data) > BULK_SIZE:
                    send_bulk(endpoint, args.user, args.password, bulk_data)
                    bulk_data = []

    # also send last chunk
    if len(bulk_data) > 0:
        send_bulk(endpoint, args.user, args.password, bulk_data)


if __name__ == "__main__":
    main()
