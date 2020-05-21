#!/usr/bin/env python

import os
import adsputils
import argparse
import warnings
import json
from requests.packages.urllib3 import exceptions
warnings.simplefilter('ignore', exceptions.InsecurePlatformWarning)
import time
import requests
from difflib import SequenceMatcher
from pyrabbit.api import Client as PyRabbitClient
from urlparse import urlparse

from adsputils import setup_logging, get_date
from adsmp.models import KeyValue, Records
from adsmp import tasks, solr_updater, validate
from sqlalchemy.orm import load_only
from sqlalchemy.orm.attributes import InstrumentedAttribute

# ============================= INITIALIZATION ==================================== #

from adsputils import setup_logging, load_config
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)
logger = setup_logging('run.py', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))

app = tasks.app

# =============================== FUNCTIONS ======================================= #

def _print_record(bibcode):
    with app.session_scope() as session:
        print 'stored by us:', bibcode
        r = session.query(Records).filter_by(bibcode=bibcode).first()
        if r:
            print json.dumps(r.toJSON(), indent=2, default=str, sort_keys=True)
        else:
            print 'None'
        print '-' * 80

        print 'as seen by SOLR'
        solr_doc = solr_updater.transform_json_record(r.toJSON())
        print json.dumps(solr_doc, indent=2, default=str, sort_keys=True)
        print '=' * 80

def diagnostics(bibcodes):
    """
    Show information about what we have in our storage.

    :param: bibcodes - list of bibcodes
    """

    if not bibcodes:
        print 'Printing 3 randomly selected records (if any)'
        bibcodes = []
        with app.session_scope() as session:
            for r in session.query(Records).limit(3).all():
                bibcodes.append(r.bibcode)

    for b in bibcodes:
        _print_record(b)


    with app.session_scope() as session:
        for x in dir(Records):
            if isinstance(getattr(Records, x), InstrumentedAttribute):
                print '# of %s' % x, session.query(Records).filter(getattr(Records, x) != None).count()

    print 'sending test bibcodes to the queue for reindexing'
    tasks.task_index_records.delay(bibcodes, force=True, update_solr=True, update_metrics=True, update_links=True,
                                   ignore_checksums=True)



def print_kvs():
    """Prints the values stored in the KeyValue table."""
    print 'Key, Value from the storage:'
    print '-' * 80
    with app.session_scope() as session:
        for kv in session.query(KeyValue).order_by('key').yield_per(100):
            print kv.key, kv.value


def reindex(since=None, batch_size=None, force_indexing=False, update_solr=True, update_metrics=True,
            update_links=True, force_processing=False, ignore_checksums=False):
    """
    Initiates routing of the records (everything that was updated)
    since point in time T.
    """
    if force_indexing:
        key = 'last.reindex.forced'
    else:
        key = 'last.reindex.normal'

    if update_solr and update_metrics:
        pass # default
    elif update_solr:
        key = key + '.solr-only'
    else:
        key = key + '.metrics-only'


    previous_since = None
    now = get_date()
    if since is None:
        with app.session_scope() as session:
            kv = session.query(KeyValue).filter_by(key=key).first()
            if kv is None:
                since = get_date('1972')
                kv = KeyValue(key=key, value=now.isoformat())
                session.add(kv)
            else:
                since = get_date(kv.value)
                previous_since = since
                kv.value = now.isoformat()
            session.commit()
    else:
        since = get_date(since)


    logger.info('Sending records changed since: %s', since.isoformat())
    sent = 0
    last_bibcode = None
    year_zero = adsputils.get_date('1972')

    try:
        # select everything that was updated since
        batch = []
        with app.session_scope() as session:
            for rec in session.query(Records) \
                .filter(Records.updated >= since) \
                .options(load_only(Records.bibcode, Records.updated, Records.processed)) \
                .yield_per(100):

                if rec.processed is None:
                    processed = year_zero
                else:
                    processed = get_date(rec.processed)
                updated = get_date(rec.updated)

                if not force_processing and processed > updated:
                    continue # skip records that were already processed

                sent += 1
                if sent % 1000 == 0:
                    logger.debug('Sending %s records', sent)

                if not batch_size or batch_size < 0:
                    batch.append(rec.bibcode)
                elif batch_size > len(batch):
                    batch.append(rec.bibcode)
                else:
                    batch.append(rec.bibcode)
                    tasks.task_index_records.delay(batch, force=force_indexing, update_solr=update_solr,
                                                   update_metrics=update_metrics, update_links=update_links,
                                                   ignore_checksums=ignore_checksums)
                    batch = []
                    last_bibcode = rec.bibcode

        if len(batch) > 0:
            tasks.task_index_records.delay(batch, force=force_indexing, update_solr=update_solr, update_metrics=update_metrics,
                                           commit=force_indexing, ignore_checksums=ignore_checksums)
        elif force_indexing and last_bibcode:
            # issue one extra call with the commit
            tasks.task_index_records.delay([last_bibcode], force=force_indexing, update_solr=update_solr, update_metrics=update_metrics,
                                           commit=force_indexing, ignore_checksums=ignore_checksums)

        logger.info('Done processing %s records', sent)
    except Exception, e:
        if previous_since:
            logger.error('Failed while submitting data to pipeline, resetting timestamp back to: %s', previous_since)
            with app.session_scope() as session:
                kv = session.query(KeyValue).filter_by(key=key).first()
                kv.value = previous_since
                session.commit()
        else:
            logger.error('Failed while submitting data to pipeline')
        raise e


def rebuild_collection(collection_name):
    """
    Will grab all recs from the database and send them to solr
    """
    # first, fail if we can not monitor queue length before we queue anything
    u = urlparse(app.conf['OUTPUT_CELERY_BROKER'])
    rabbitmq = PyRabbitClient(u.hostname + ':' + str(u.port + 10000), u.username, u.password)
    if not rabbitmq.is_alive('master_pipeline'):
        logger.error('failed to connect to rabbitmq with PyRabbit to monitor queue')
        sys.exit(1)

    now = get_date()
    if collection_name.startswith('http'):
        solr_urls = [collection_name]
    else:
        solr_urls = []
        urls = app.conf['SOLR_URLS']
        for u in urls:
            parts = u.split('/')
            parts[-2] = collection_name
            solr_urls.append('/'.join(parts))

    logger.info('Sending all records to: %s', ';'.join(solr_urls))
    sent = 0

    batch = []
    _tasks = []
    with app.session_scope() as session:
        # master db only contains valid documents, indexing task will make sure that incomplete docs are rejected
        for rec in session.query(Records) \
            .options(load_only(Records.bibcode, Records.updated, Records.processed)) \
            .yield_per(1000):

            sent += 1
            if sent % 1000 == 0:
                logger.debug('Sending %s records', sent)

            batch.append(rec.bibcode)
            if len(batch) > 1000:
                t = tasks.task_rebuild_index.delay(batch, force=True, update_solr=True,
                                           update_metrics=False, update_links=False,
                                           ignore_checksums=True, solr_targets=solr_urls)
                _tasks.append(t)
                batch = []

    if len(batch) > 0:
        t = tasks.task_rebuild_index.delay(batch, force=True, update_solr=True,
                                           update_metrics=False, update_links=False,
                                           ignore_checksums=True, solr_targets=solr_urls)
        _tasks.append(t)

    logger.info('Done queueing bibcodes for rebuilding collection %s', (collection_name))
    # now wait for queue to empty
    queue_length = 1
    while queue_length > 0:
        queue_length = rabbitmq.get_queue_depth('master_pipeline', 'rebuild-index')
        stime = queue_length * 0.1
        logger.info('Waiting %s for rebuild-collection tasks to finish, queue_length %s, sent %s' % (stime, queue_length, sent))
        time.sleep(stime)

    logger.info('Done rebuilding collection %s, sent %s records', (collection_name, sent))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process user input.')

    parser.add_argument('-d',
                        '--diagnostics',
                        dest='diagnostics',
                        action='store_true',
                        help='Show diagnostic message')

    parser.add_argument('-b',
                        '--bibcodes',
                        dest='bibcodes',
                        action='store',
                        help='List of bibcodes separated by spaces')

    parser.add_argument('-f',
                        '--force_indexing',
                        dest='force_indexing',
                        action='store_true',
                        default=False,
                        help='Forces indexing of documents as soon as we receive them.')

    parser.add_argument('-o',
                        '--force_processing',
                        dest='force_processing',
                        action='store_true',
                        default=False,
                        help='Submits records for processing even if they dont have any new updates (use this to rebuild index).')

    parser.add_argument('-s',
                        '--since',
                        dest='since',
                        action='store',
                        default=None,
                        help='Starting date for reindexing')

    parser.add_argument('-k',
                        '--kv',
                        dest='kv',
                        action='store_true',
                        default=False,
                        help='Show current values of KV store')

    parser.add_argument('-r',
                        '--index',
                        nargs='?',
                        dest='reindex',
                        action='store',
                        const = 'sml',
                        default='sml',
                        help='Sent all updated documents to SOLR/Postgres (you can combine with --since).' +
                        'Default is to update both solr and metrics. You can choose what to update.' +
                        '(s = update solr, m = update metrics, l = update link resolver)')

    parser.add_argument('--delete',
                        dest='delete',
                        action='store_true',
                        default=False,
                        help='delete a file of bibcodes')

    parser.add_argument('-e',
                        '--batch_size',
                        dest='batch_size',
                        action='store',
                        default=100,
                        type=int,
                        help='How many records to process/index in one batch')

    parser.add_argument('-c',
                        '--validate_solr',
                        dest='validate',
                        action='store_true',
                        help='Compares two SOLR instances for the given bibcodes or file of bibcodes')

    parser.add_argument('-n',
                        '--filename',
                        dest='filename',
                        action='store',
                        help='File containing a list of bibcodes, one per line')

    parser.add_argument('--ignore_checksums',
                        dest='ignore_checksums',
                        action='store_true',
                        default=False,
                        help='Update persistent store even when checksum match says it is redundant')

    parser.add_argument('-a',
                        '--augment',
                        dest='augment',
                        action='store_true',
                        default=False,
                        help='sends bibcodes to augment affilation pipeline, works with --filename')
    parser.add_argument('--solr-collection',
                       dest='solr_collection',
                       default='collection2',
                       action='store',
                       help='name of solr collection, currently only used by rebuild collection')
    parser.add_argument('-x',
                        '--rebuild-collection',
                        action='store_true',
                        default=False,
                        help='Will send all solr docs for indexing to another collection; by purpose this task is synchronous. You can send the name of the collection or the full url to the solr instance incl http via --solr-collection')

    args = parser.parse_args()

    if args.bibcodes:
        args.bibcodes = args.bibcodes.split(' ')

    if args.kv:
        print_kvs()

    logger.info('Executing run.py: %s', args)

    if args.diagnostics:
        diagnostics(args.bibcodes)

    elif args.validate:
        fields = ('abstract', 'ack', 'aff', 'alternate_bibcode', 'alternate_title', 'arxiv_class', 'author',
                  'author_count', 'author_facet', 'author_facet_hier', 'author_norm', 'bibgroup', 'bibgroup_facet',
                  'bibstem', 'bibstem_facet', 'body', 'citation', 'citation_count', 'cite_read_boost', 'classic_factor',
                  'comment', 'copyright', 'data', 'data_count', 'data_facet', 'database', 'date', 'doctype',
                  'doctype_facet_hier',
                  'doi', 'eid', 'editor', 'email', 'entry_date', 'esources', 'facility', 'first_author', 'first_author_facet_hier',
                  'first_author_norm', 'fulltext_mtime', 'grant', 'grant_facet_hier', 'id', 'identifier', 'indexstamp',
                  'isbn', 'issn', 'issue', 'keyword', 'keyword_facet', 'keyword_norm', 'keyword_schema', 'lang',
                  'links_data', 'metadata_mtime', 'metrics_mtime', 'nedid', 'nedtype', 'ned_object_facet_hier',
                  'nonbib_mtime',
                  'origin', 'orcid_mtime', 'orcid', 'orcid_pub', 'orcid_user', 'orcid_other', 'page', 'page_range',
                  'page_count', 'property', 'pub', 'pub_raw', 'pubdate', 'pubnote', 'read_count', 'reader', 'recid',
                  'reference', 'simbad_object_facet_hier', 'simbid', 'simbtype', 'title', 'update_timestamp', 'vizier',
                  'vizier_facet', 'volume', 'year')

        ignore_fields = ('id', 'indexstamp', 'fulltext_mtime', 'links_data', 'metadata_mtime', 'metrics_mtime',
                         'nonbib_mtime', 'orcid_mtime', 'recid', 'update_timestamp')

        new_fields = ('data_count', 'editor', 'entry_date', 'esources', 'nedid', 'nedtype', 'ned_object_facet_hier', 'origin',
                      'page_count', 'page_range')

        d = validate.Validate(fields, ignore_fields, new_fields)
        d.compare_solr(bibcodelist=args.bibcodes,filename=args.filename)

    elif args.delete:
        if args.filename:
            print 'deleting bibcodes from file via queue'
            bibs = []
            with open(args.filename, 'r') as f:
                for line in f:
                    bibcode = line.strip()
                    if bibcode:
                        tasks.task_delete_documents(bibcode)
        else:
            print 'please provide a file of bibcodes to delete via -n'

    elif args.augment:
        if args.filename:
            with open(args.filename, 'r') as f:
                for line in f:
                    bibcode = line.strip()
                    if bibcode:
                        # read db record for current aff value, send to queue
                        # aff values omes from bib pipeline

                        app.request_aff_augment(bibcode)

    elif args.rebuild_collection:
        rebuild_collection(args.solr_collection)
    elif args.reindex:
        update_solr = 's' in args.reindex.lower()
        update_metrics = 'm' in args.reindex.lower()
        update_links = 'l' in args.reindex.lower()

        if args.filename:
            print 'sending bibcodes from file to the queue for reindexing'
            bibs = []
            with open(args.filename) as f:
                for line in f:
                    bibcode = line.strip()
                    if bibcode:
                        bibs.append(bibcode)
                    if len(bibs) >= 100:
                        tasks.task_index_records.delay(bibs, force=True,
                                                       update_solr=update_solr, update_metrics=update_metrics,
                                                       update_links = update_links, ignore_checksums=args.ignore_checksums)
                        bibs = []
                if len(bibs) > 0:
                    tasks.task_index_records.delay(bibs, force=True,
                                                   update_solr=update_solr, update_metrics=update_metrics,
                                                   update_links = update_links, ignore_checksums=args.ignore_checksums)
                    bibs = []
        else:
            print 'sending bibcode since date to the queue for reindexing'
            reindex(since=args.since, batch_size=args.batch_size, force_indexing=args.force_indexing,
                    update_solr=update_solr, update_metrics=update_metrics,
                    update_links = update_links, force_processing=args.force_processing, ignore_checksums=args.ignore_checksums)
