#!/usr/bin/env python3
import sys
import os
import argparse
import logging
import csv
import re

from urllib.request import urlopen
from pathlib import Path
from datetime import datetime, timedelta

from mastodon import Mastodon
from mastodon.errors import MastodonNotFoundError
from dotenv import load_dotenv

load_dotenv()

SHEET_ID = '15Ak4VHCnr_4Stkzkz-_RhOgX-jCJMt0oq0GN9XXDaKo'
CSV_URL_TEMPLATE = ('https://docs.google.com/spreadsheets/d/{key}/gviz/'
                    'tq?tqx=out:csv')
IN_COLUMNS = ['timestamp', 'name', 'masto', 'twitter', 'bio']
OUTFILE = Path(__file__).parent / '..' / 'resources' / 'users.csv'
OUT_COLUMNS = ['account', 'name', 'link']
MAX_WEEKS_SINCE_POST = 8


def get_google_sheet():
    with urlopen(CSV_URL_TEMPLATE.format(key=SHEET_ID)) as csvfile:
        # urlopen() returns binary content, so first decode it
        content = csvfile.read().decode('utf-8')
        lines = content.splitlines()
        # We pass our own field names, so skip the original header row
        lines.pop(0)
        reader = csv.DictReader(lines, fieldnames=IN_COLUMNS)
        data = list(reader)
        data.reverse()
        return data


def convert_row(row):
    account = row['masto']
    if m := re.match(r'https://(?P<server>.*)/@(?P<user>.*)', account):
        pass
    elif m := re.match(r'@?(?P<user>.*)@(?P<server>.*)', account):
        pass
    else:
        raise ValueError(f'Malformed account: {account}')
    user = m.group('user')
    server = m.group('server')
    return {'account': f'@{ user }@{ server }',
            'name': row['name'],
            'link': f'https://{ server }/@{ user }'}


def account_active(client, acct, delta):
    try:
        # Pause a little not to overload the server
        account = client.account_lookup(acct)
    except MastodonNotFoundError:
        logging.debug(f'Account {acct} not found on server')
        return False
    last_date = account['last_status_at']
    if last_date is None:
        # No posts
        return False
    return datetime.now() - delta < last_date


def update_from_google_sheet(data, outfile, *, delta=None):
    if delta is not None:
        client = Mastodon(api_base_url=os.getenv('MASTODON_INSTANCE'),
                          access_token=os.getenv('MASTODON_ACCESS_TOKEN'),
                          ratelimit_method='pace')
    with outfile.open('w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=OUT_COLUMNS)
        writer.writeheader()
        for row in data:
            try:
                new_row = convert_row(row)
            except ValueError as e:
                logging.warning(f'Failed to parse row: {e}')
                continue
            # Test for account activity
            if delta is not None:
                # API does not take leading @
                acct = new_row['account'].lstrip('@')
                if not account_active(client, acct, delta):
                    logging.info(f'Inactive account: @{acct}')
                    continue
            writer.writerow(new_row)


def main():
    # Parse commandline arguments
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-v', '--verbose', action='store_true')
    arg_parser.add_argument('-o', '--outfile', default=OUTFILE,
                            type=Path)
    arg_parser.add_argument('-m', '--max-weeks', default=MAX_WEEKS_SINCE_POST,
                            type=int)
    args = arg_parser.parse_args()
    # Set up logging
    if args.verbose:
        level = logging.INFO
    else:
        level = logging.ERROR
    logging.basicConfig(level=level)
    # Return exit value
    if args.max_weeks:
        delta = timedelta(weeks=args.max_weeks)
    else:
        delta = None
    data = get_google_sheet()
    update_from_google_sheet(data, args.outfile, delta=delta)
    return 0


if __name__ == '__main__':
    sys.exit(main())
