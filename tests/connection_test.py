import argparse
import logging

import signac


parser = argparse.ArgumentParser()
parser.add_argument('hostname', type=str, default='testcfg', nargs='?')
parser.add_argument('-d', '--debug', action='store_true')
args = parser.parse_args()

logging.basicConfig(
    level=logging.DEBUG if args.debug else logging.INFO)

signac.get_database('testing', hostname=args.hostname).testing.find_one()
signac.get_database('testing', hostname=args.hostname).testing.find_one()
signac.get_database('testing', hostname=args.hostname).testing.find_one()
