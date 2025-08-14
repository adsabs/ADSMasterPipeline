#!/usr/bin/env python

import os
import sys
import adsputils
import argparse
import warnings
import json
from requests.packages.urllib3 import exceptions
warnings.simplefilter('ignore', exceptions.InsecurePlatformWarning)
import time
from pyrabbit.api import Client as PyRabbitClient

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from adsputils import setup_logging, get_date, load_config
from adsmp.models import KeyValue, Records
from adsmp import tasks, solr_updater, validate #s3_utils
from sqlalchemy.orm import load_only
from sqlalchemy.orm.attributes import InstrumentedAttribute

# ============================= INITIALIZATION ==================================== #
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)

logger = setup_logging('run.py', proj_home=proj_home,
                       level=config.get('LOGGING_LEVEL', 'DEBUG'),
                       attach_stdout=config.get('LOG_STDOUT', False))

app = tasks.app

# =============================== FUNCTIONS ======================================= #


def _print_record(bibcode):
    with app.session_scope() as session:
        print('stored by us:', bibcode)
        r = session.query(Records).filter_by(bibcode=bibcode).first()
        if r:
            print(json.dumps(r.toJSON(), indent=2, default=str, sort_keys=True))
        else:
            print('None')
        print('-' * 80)

        print('as seen by SOLR')
        solr_doc = solr_updater.transform_json_record(r.toJSON())
        print(json.dumps(solr_doc, indent=2, default=str, sort_keys=True))
        print('=' * 80)


def diagnostics(bibcodes):
    """
    Show information about what we have in our storage.

    :param: bibcodes - list of bibcodes
    """

    if not bibcodes:
        print('Printing 3 randomly selected records (if any)')
        bibcodes = []
        with app.session_scope() as session:
            for r in session.query(Records).limit(3).all():
                bibcodes.append(r.bibcode)

    for b in bibcodes:
        _print_record(b)

    with app.session_scope() as session:
        for x in dir(Records):
            if isinstance(getattr(Records, x), InstrumentedAttribute):
                print('# of %s' % x, session.query(Records).filter(getattr(Records, x) != None).count())

    print('sending test bibcodes to the queue for reindexing')
    tasks.task_index_records.apply_async(
        args=(bibcodes,),
        kwargs={
           'force': True,
           'update_solr': True,
           'update_metrics': True,
           'update_links': True,
           'ignore_checksums': True,
           'update_processed': False,
           'priority': 0
        },
        priority=0
    )


def print_kvs():
    """Prints the values stored in the KeyValue table."""
    print('Key, Value from the storage:')
    print('-' * 80)
    with app.session_scope() as session:
        for kv in session.query(KeyValue).order_by('key').yield_per(100):
            print(kv.key, kv.value)


def reindex(since=None, batch_size=None, force_indexing=False, update_solr=True, update_metrics=True,
            update_links=True, force_processing=False, ignore_checksums=False, solr_targets=None,
            update_processed=True, priority=0):
    """
    Initiates routing of the records (everything that was updated)
    since point in time T.
    """
    if force_indexing:
        key = 'last.reindex.forced'
    else:
        key = 'last.reindex.normal'

    if update_solr and update_metrics:
        pass  # default
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
                    continue  # skip records that were already processed

                sent += 1
                if sent % 1000 == 0:
                    logger.debug('Sending %s records', sent)

                if not batch_size or batch_size < 0:
                    batch.append(rec.bibcode)
                elif batch_size > len(batch):
                    batch.append(rec.bibcode)
                else:
                    batch.append(rec.bibcode)
                    tasks.task_index_records.apply_async(
                        args=(batch,),
                        kwargs={
                           'force': force_indexing,
                           'update_solr': update_solr,
                           'update_metrics': update_metrics,
                           'update_links': update_links,
                           'ignore_checksums': ignore_checksums,
                           'solr_targets': solr_targets,
                           'update_processed': update_processed,
                           'priority': priority
                        },
                        priority=priority
                    )
                    batch = []
                    last_bibcode = rec.bibcode

        if len(batch) > 0:
            tasks.task_index_records.apply_async(
                args=(batch,),
                kwargs={
                   'force': force_indexing,
                   'update_solr': update_solr,
                   'update_metrics': update_metrics,
                   'update_links': update_links,
                   'commit': force_indexing,
                   'ignore_checksums': ignore_checksums,
                   'solr_targets': solr_targets,
                   'update_processed': update_processed,
                   'priority': priority
                },
                priority=priority
            )
        elif force_indexing and last_bibcode:
            # issue one extra call with the commit
            tasks.task_index_records.apply_async(
                args=([last_bibcode],),
                kwargs={
                   'force': force_indexing,
                   'update_solr': update_solr,
                   'update_metrics': update_metrics,
                   'update_links': update_links,
                   'commit': force_indexing,
                   'ignore_checksums': ignore_checksums,
                   'solr_targets': solr_targets,
                   'update_processed': update_processed,
                   'priority': priority
                },
                priority=priority
            )
        logger.info('Done processing %s records', sent)
    except Exception as e:
        if previous_since:
            logger.error('Failed while submitting data to pipeline, resetting timestamp back to: %s', previous_since)
            with app.session_scope() as session:
                kv = session.query(KeyValue).filter_by(key=key).first()
                kv.value = previous_since
                session.commit()
        else:
            logger.error('Failed while submitting data to pipeline')
        raise e


def collection_to_urls(collection_name):
    solr_urls = []
    urls = app.conf['SOLR_URLS']
    if collection_name:
        if collection_name.startswith('http'):
            return [collection_name]

        for u in urls:
            parts = u.split('/')
            parts[-2] = collection_name
            solr_urls.append('/'.join(parts))
    else:
        solr_urls = urls[:]

    # if collection is named without full URL
    # and config listed two SOLR targets (on the same server)
    # we'll end up with duplicates and be sending the same
    # data to same collection N times; to avoid that
    # we'll unique the list of targets

    return list(set(solr_urls))


def delete_obsolete_records(older_than, batch_size=1000):
    """
    Will delete records without bib_data that are in the db
    and that are older than `older_than` timestamp
    """

    if not older_than:
        raise Exception('This operation requires a valid timestamp')

    old = get_date(older_than)
    logger.info('Going to delete records without bib_data older than: %s', old.isoformat())

    deleted = 0
    bibcodes = []

    # because delete_by_bibcode issues commit (which expires session)
    # we must harvest them before (and close our session); it's not
    # very fast, but we don't mind
    with app.session_scope() as session:
        for rec in session.query(Records) \
                          .options(load_only(Records.bibcode)) \
                          .filter(Records.updated <= old) \
                          .filter(Records.bib_data.is_(None)) \
                          .yield_per(batch_size):

            bibcodes.append(rec.bibcode)

    while bibcodes:
        bibcode = bibcodes.pop()
        if app.delete_by_bibcode(bibcode):
            logger.debug("Deleted record: %s", bibcode)
            deleted += 1
        else:
            logger.warn("Failed to delete: %s (this only happens if the rec cannot be found)", bibcode)

    logger.info("Deleted {} obsolete records".format(deleted))

def process_all_scixid(batch_size, flag):
    logger.info('Starting process_all_scixid with flag: %s', flag)
    if flag == 'update-all':
        flag = 'update'
    elif flag == 'force-all':
        flag = 'force'
    elif flag == 'reset-all':
        flag = 'reset'
    logger.info('Converted flag to: %s', flag)
    
    sent = 0
    batch = []
    _tasks = []
    with app.session_scope() as session:
        # load all records from RecordsDB
        for rec in session.query(Records) \
                        .options(load_only(Records.bibcode)) \
                        .yield_per(batch_size):

            sent += 1
            if sent % 1000 == 0:
                logger.debug('Sending %s records', sent)

            batch.append(rec.bibcode)
            if len(batch) >= batch_size:
                logger.info('Sending batch of %s records with flag %s', len(batch), flag)
                t = tasks.task_update_scixid.delay(batch, flag)
                _tasks.append(t)
                batch = []
    
    if len(batch) > 0:
        logger.info('Sending final batch of %s records with flag %s', len(batch), flag)
        t = tasks.task_update_scixid.delay(batch, flag)
        logger.debug('Sending %s records', len(batch))
        _tasks.append(t)
    logger.info('Completed process_all_scixid, sent total of %s records', sent)


def rebuild_collection(collection_name, batch_size):
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
    solr_urls = collection_to_urls(collection_name)

    logger.info('Sending all records to: %s', ';'.join(solr_urls))
    sent = 0

    batch = []
    _tasks = []
    with app.session_scope() as session:
        # master db only contains valid documents, indexing task will make sure that incomplete docs are rejected
        for rec in session.query(Records) \
                          .options(load_only(Records.bibcode)) \
                          .yield_per(batch_size):

            sent += 1
            if sent % 1000 == 0:
                logger.debug('Sending %s records', sent)

            batch.append(rec.bibcode)
            if len(batch) > batch_size:
                t = tasks.task_rebuild_index.delay(batch, solr_targets=solr_urls)
                _tasks.append(t)
                batch = []

    if len(batch) > 0:
        t = tasks.task_rebuild_index.delay(batch, solr_targets=solr_urls)
        _tasks.append(t)

    logger.info('Done queueing bibcodes for rebuilding collection %s', collection_name)
    # now wait for rebuild-index queue to empty
    queue_length = 1
    while queue_length > 0:
        queue_length = rabbitmq.get_queue_depth('master_pipeline', 'rebuild-index')
        stime = max(queue_length * 0.1, 10.0)
        logger.info('Waiting %s for rebuild-index queue to empty, queue_length %s, sent %s' % (stime, queue_length, sent))
        time.sleep(stime)
    logger.info('Completed waiting %s for rebuild-index queue to empty, queue_length %s, sent %s' % (stime, queue_length, sent))

    # now wait for index-solr queue to empty
    queue_length = 1
    while queue_length > 0:
        queue_length = rabbitmq.get_queue_depth('master_pipeline', 'index-solr')
        stime = queue_length * 0.1
        logger.info('Waiting %s for index-solr queue to empty, queue_length %s, sent %s' % (stime, queue_length, sent))
        time.sleep(stime)
    logger.info('Completed waiting %s for index-solr queue to empty, queue_length %s, sent %s' % (stime, queue_length, sent))

    logger.info('Done rebuilding collection %s, sent %s records', collection_name, sent)


def reindex_failed_bibcodes(app, update_processed=True):
    """from status field in records table we compute what failed"""
    bibs = []
    count = 0
    with app.session_scope() as session:
        for rec in session.query(Records) \
                          .filter(Records.status.notin_(['success', 'retrying'])) \
                          .filter(Records.bib_data.isnot(None)) \
                          .options(load_only(Records.bibcode, Records.status)) \
                          .yield_per(1000):
            logger.info('Reindexing previously failed bibcode %s, previous status: %s', rec.bibcode, rec.status)
            bibs.append(rec.bibcode)
            count += 1
            rec.status = 'retrying'
            if len(bibs) >= 100:
                session.commit()
                tasks.task_index_records.apply_async(
                    args=(bibs,),
                    kwargs={
                       'force': True,
                       'update_solr': True,
                       'update_metrics': True,
                       'update_links': True,
                       'ignore_checksums': True,
                       'update_processed': update_processed,
                       'priority': 0
                    },
                    priority=0
                )
                bibs = []
        if bibs:
            session.commit()
            tasks.task_index_records.apply_async(
                args=(bibs,),
                kwargs={
                   'force': True,
                   'update_solr': True,
                   'update_metrics': True,
                   'update_links': True,
                   'ignore_checksums': True,
                   'update_processed': update_processed,
                   'priority': 0
                },
                priority=0
            )
            bibs = []
        logger.info('Done reindexing %s previously failed bibcodes', count)

def manage_sitemap(bibcodes, action):
    """
    Submit sitemap management task to Celery worker
    
    Actions:
    - 'add': add/update record info to sitemap table if bibdata_updated is newer than filename_lastmoddate
    - 'force-update': force update sitemap table entries for given bibcodes  
    - 'remove': remove bibcodes from sitemap table (TODO: not implemented)
    - 'delete-table': delete all contents of sitemap table and backup files
    - 'update-robots': force update robots.txt files for all sites
    
    Returns:
        str: Celery task ID for monitoring task progress
    """
    result = tasks.task_manage_sitemap.apply_async(args=(bibcodes, action))
    print(f"Sitemap management task submitted: {result.id}")
    print(f"Action: {action}")
    if action in ['add', 'remove', 'force-update']:
        print(f"Processing {len(bibcodes)} bibcodes")
    print("Task is running in the background...")
    return result.id

def update_sitemap_files():
    """
    Submit sitemap files update task to Celery worker
    Updates all sitemap files for records with update_flag = True
    
    Returns:
        str: Celery task ID for monitoring task progress
    """
    result = tasks.task_update_sitemap_files.apply_async()
    print(f"Sitemap files update task submitted: {result.id}")
    print("Task is running in the background...")
    return result.id

# def sync_sitemap_to_s3():
#     """
#     Manually sync all sitemap files to S3
#     """
#     sitemap_dir = app.sitemap_dir
#     print(f"Syncing sitemap files from {sitemap_dir} to S3...")
    
    # success = s3_utils.sync_sitemap_files_to_s3(app.conf, sitemap_dir)
    # if success:
    #     print("S3 sync completed successfully")
    # else:
    #     print("S3 sync failed - check logs for details")
    #     sys.exit(1)

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
                        help='Datestamp used in indexing and other operations')

    parser.add_argument('--delete_obsolete',
                        dest='delete_obsolete',
                        action='store_true',
                        default=False,
                        help='Delete records without bib_data that are in the db and that are older than `since` timestamp.')

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
                        const='sml',
                        default='sml',
                        help='Sent all updated documents to SOLR/Postgres (you can combine with --since).' +
                        'Default is to update both solr and metrics. You can choose what to update.' +
                        '(s = update solr, m = update metrics, l = update link resolver)')

    parser.add_argument('--index_failed',
                        dest='index_failed',
                        action='store_true',
                        help='reindex bibcodes that failed during previous reindex run, updates solr, metrics and resolver')

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
                        default=None,
                        action='store',
                        help='name of solr collection, defaults to None (which means the pipeline will send data to whatever is set in config); set to collectionX if you want to multiplex data to all SOLR_URL targets (assuming they are different servers) and all of them to receive data into collectionX; set with full URL i.e. http://server/collection1/update if you want to force sending data to a specific machine')
    parser.add_argument('-x',
                        '--rebuild-collection',
                        action='store_true',
                        default=False,
                        help='Will send all solr docs for indexing to another collection; by purpose this task is synchronous. You can send the name of the collection or the full url to the solr instance incl http via --solr-collection')
    parser.add_argument('--priority',
                        dest='priority',
                        action='store',
                        default=0,
                        type=int,
                        help='priority to use in queue, typically cron jobs use a high priority.  preferred values are 0 to 10 where 10 is the highest priority (https://docs.celeryproject.org/en/stable/userguide/calling.html#advanced-options)')
    parser.add_argument('--update-processed',
                        action='store_true',
                        default=False,
                        dest='update_processed',
                        help='update processed timestamps and other state info in records table when a record is indexed')
    parser.add_argument('--manage-sitemap',
                        action='store_true',
                        default=False,
                        dest='manage_sitemap',
                        help='populate sitemap table for list of bibcodes')
    parser.add_argument('--action',
                        default=False,
                        choices=['add', 'delete-table', 'force-update', 'remove', 'update-robots', 'bootstrap'],
                        help='action: add (add bibcodes), force-update (force update bibcodes), remove (remove bibcodes), delete-table (clear sitemap table), update-robots (force update robots.txt files), bootstrap (initialize sitemaps for all existing records)')
    parser.add_argument('--update-sitemap-files',
                        action='store_true',
                        default=False,
                        dest='update_sitemap_files',
                        help='update sitemap files for records with update_flag = True in sitemap table')

    args = parser.parse_args()

    if args.bibcodes:
        args.bibcodes = args.bibcodes.split(' ')

    if args.kv:
        print_kvs()

    logger.info('Executing run.py: %s', args)

    # uff: this whole block needs refactoring (as is written, it only allows for single operation)
    if args.diagnostics:
        diagnostics(args.bibcodes)

    elif args.delete_obsolete:
        delete_obsolete_records(args.since, batch_size=args.batch_size)

    elif args.validate:
        fields = ('abstract', 'ack', 'aff', 'alternate_bibcode', 'alternate_title', 'arxiv_class', 'author',
                  'author_count', 'author_facet', 'author_facet_hier', 'author_norm', 'bibgroup', 'bibgroup_facet',
                  'bibstem', 'bibstem_facet', 'body', 'citation', 'citation_count', 'cite_read_boost', 'classic_factor',
                  'comment', 'copyright', 'data', 'data_count', 'data_facet', 'database', 'date', 'doctype',
                  'doctype_facet_hier',
                  'doi', 'eid', 'editor', 'email', 'entry_date', 'esources', 'facility', 'first_author', 'first_author_facet_hier',
                  'first_author_norm', 'fulltext_mtime', 'grant', 'grant_facet_hier', 'id', 'identifier', 'indexstamp',
                  'ISBN', 'ISSN', 'issue', 'keyword', 'keyword_facet', 'keyword_norm', 'keyword_schema', 'lang',
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
        d.compare_solr(bibcodelist=args.bibcodes, filename=args.filename)

    elif args.delete:
        if args.filename:
            print('deleting bibcodes from file via queue')
            bibcodes = []
            with open(args.filename, 'r') as f:
                for line in f:
                    bibcode = line.strip()
                    if bibcode:
                        tasks.task_delete_documents(bibcode)
        else:
            print('please provide a file of bibcodes to delete via -n')

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
        rebuild_collection(args.solr_collection, args.batch_size)
    elif args.index_failed:
        reindex_failed_bibcodes(app, args.update_processed)
    elif args.manage_sitemap:

        # Validate required action parameter
        if not args.action:
            print("Error: --action is required when using --populate-sitemap-table")
            print("Available actions: add, remove, force-update, delete-table, update-robots, bootstrap")
            sys.exit(1)
        
        action = args.action
        
        # Get bibcodes from file or command line
        bibcodes = []
        if args.filename:
            with open(args.filename) as f:
                for line in f:
                    bibcode = line.strip()
                    if bibcode:
                        bibcodes.append(bibcode)
        elif args.bibcodes:
            bibs = args.bibcodes
        if args.scix_id_flag:
            flag = args.scix_id_flag
        else:
            flag = ''
        if flag == 'update-all' or flag == 'force-all' or flag == 'reset-all':
            batch_size = args.batch_size
            process_all_scixid(batch_size, flag)
        else:
            tasks.task_update_scixid(bibs, flag = flag)
            bibcodes = args.bibcodes
        
        # Validate that actions requiring bibcodes have them
        if action in ['add', 'remove', 'force-update']:
            if not bibcodes:
                print(f"Error: --action {action} requires bibcodes")
                print("Provide bibcodes using --bibcodes or --filename")
                sys.exit(1)
        
        manage_sitemap(bibcodes, action)
    elif args.update_sitemap_files:
        update_sitemap_files()
    # elif args.sync_sitemap_s3:
    #     sync_sitemap_to_s3()
    elif args.update_scixid:
        if args.filename:
            bibs = []
            with open(args.filename) as f:
                for line in f:
                    bibcode = line.strip()
                    if bibcode:
                        bibs.append(bibcode)
        elif args.bibcodes:
            bibs = args.bibcodes
        if args.scix_id_flag:
            flag = args.scix_id_flag
        else:
            flag = ''

        if flag == 'update-all' or flag == 'force-all' or flag == 'reset-all':
            batch_size = args.batch_size
            process_all_scixid(batch_size, flag)
        else:
            tasks.task_update_scixid(bibs, flag = flag)
    elif args.reindex:
        update_solr = 's' in args.reindex.lower()
        update_metrics = 'm' in args.reindex.lower()
        update_links = 'l' in args.reindex.lower()
        solr_urls = collection_to_urls(args.solr_collection)
        print('reindexing to solr url ' + str(solr_urls))

        if args.filename:
            print('sending bibcodes from file to the queue for reindexing')
            bibcodes = []
            with open(args.filename) as f:
                for line in f:
                    bibcode = line.strip()
                    if bibcode:
                        bibcodes.append(bibcode)
                    if len(bibcodes) >= 100:
                        tasks.task_index_records.apply_async(
                            args=(bibcodes,),
                            kwargs={
                                'force': True,
                                'update_solr': update_solr,
                                'update_metrics': update_metrics,
                                'update_links': update_links,
                                'ignore_checksums': args.ignore_checksums,
                                'solr_targets': solr_urls,
                                'priority': args.priority,
                                'update_processed': args.update_processed
                            },
                            priority=args.priority
                        )
                        bibcodes = []
                if len(bibcodes) > 0:
                    tasks.task_index_records.apply_async(
                        args=(bibcodes,),
                        kwargs={
                            'force': True,
                            'update_solr': update_solr,
                            'update_metrics': update_metrics,
                            'update_links': update_links,
                            'ignore_checksums': args.ignore_checksums,
                            'solr_targets': solr_urls,
                            'priority': args.priority,
                            'update_processed': args.update_processed
                        },
                        priority=args.priority
                    )
                    bibcodes = []
        else:
            print('sending bibcode since date to the queue for reindexing')
            reindex(since=args.since, batch_size=args.batch_size, force_indexing=args.force_indexing,
                    update_solr=update_solr, update_metrics=update_metrics,
                    update_links=update_links, force_processing=args.force_processing, ignore_checksums=args.ignore_checksums,
                    solr_targets=solr_urls, update_processed=args.update_processed, priority=args.priority)



