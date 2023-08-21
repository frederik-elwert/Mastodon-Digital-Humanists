#!/usr/bin/env python3
import sys
import argparse
import logging
from urllib.request import urlopen
import csv
from pathlib import Path

SHEET_ID = '15Ak4VHCnr_4Stkzkz-_RhOgX-jCJMt0oq0GN9XXDaKo'
CSV_URL_TEMPLATE = ('https://docs.google.com/spreadsheets/d/{key}/gviz/'
                    'tq?tqx=out:csv')
IN_COLUMNS = ['timestamp', 'name', 'masto', 'twitter', 'bio']
OUTFILE = Path(__file__).parent / '..' / 'resources' / 'users.csv'
OUT_COLUMNS = ['account', 'name', 'link']


def get_google_sheet():
    with urlopen(CSV_URL_TEMPLATE.format(key=SHEET_ID)) as csvfile:
        # urlopen() returns binary content, so first decode it
        content = csvfile.read().decode('utf-8')
        lines = content.splitlines()
        # We pass our own field names, so skip the original header row
        lines.pop(0)
        reader = csv.DictReader(lines, fieldnames=IN_COLUMNS)
        return list(reader)


def convert_row(row):
    account = row['masto']
    name = row['name']
    account_parts = account.split('@')
    # Test if account name is correct
    if len(account_parts) != 3:
        raise ValueError(f'Malformed account: {account}')
    link = f'https://{ account_parts[2] }/@{ account_parts[1] }'
    return {'account': account,
            'name': name,
            'link': link}


def update_from_google_sheet(data, outfile):
    with outfile.open('w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=OUT_COLUMNS)
        writer.writeheader()
        for row in data:
            try:
                new_row = convert_row(row)
            except ValueError as e:
                logging.warning(f'Failed to parse row: {e}')
                continue
            writer.writerow(new_row)


def main():
    # Parse commandline arguments
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-v', '--verbose', action='store_true')
    arg_parser.add_argument('-o', '--outfile', default=OUTFILE,
                            type=Path)
    args = arg_parser.parse_args()
    # Set up logging
    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.ERROR
    logging.basicConfig(level=level)
    # Return exit value
    data = get_google_sheet()
    update_from_google_sheet(data, args.outfile)
    return 0


if __name__ == '__main__':
    sys.exit(main())
