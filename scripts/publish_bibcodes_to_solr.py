#!/usr/bin/python2
# -*- coding: utf-8 -*-

from adsmp import app, tasks
from adsmp.models import Records
from sqlalchemy.orm import load_only
import argparse
import sys, os


def get_all_bibcodes():
    with app.session_scope() as session:
        for r in session.query(Records).options(load_only(['bibcode'])).all():
            yield r.bibcode


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('bibcodes', nargs='*',
                        help='bibcodes to publish')

    parser.add_argument(
        '--from-file',
        nargs=1,
        default=None,
        dest='bibcode_file',
        type=str,
        help='Load bibcodes from file, one bibcode per line',
        )

    parser.add_argument('--whole-database', default=False,
                        dest='whole_database', action='store_true')

    parser.add_argument(
        '--bibcodes-per-message',
        nargs=1,
        default=100,
        dest='bibcodes_per_message',
        type=int,
        help='Publish N bibcodes at a time',
        )

    args = parser.parse_args()

    if args.whole_database:
        for bibcode in get_all_bibcodes():
            tasks.task_route_record.delay(bibcode)
    elif args.bibcode_file:
        with open(args.bibcode_file[0]) as fp:
            for l in fp.readlines():
                l = l.strip()
                if l and not l.startswith('#'):
                    tasks.task_route_record.delay(l)
    else:
        if not args.bibcodes and not args.whole_database:
            raise Exception('Not enough arguments given')
        for b in args.bibcodes:
            tasks.task_route_record.delay(b)
    

if __name__ == '__main__':
    main()
