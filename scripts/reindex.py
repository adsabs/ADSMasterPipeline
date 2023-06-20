from __future__ import division
# purpose of this script is to rebuild a new solr collection
# it will automatically activate it (by swapping cores)

from builtins import str
from past.utils import old_div
import datetime
import os
import sys
import pickle
import requests
import time
import json

proj_home = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if proj_home not in sys.path:
    sys.path.append(proj_home)

from subprocess import PIPE, Popen
from adsmp import tasks
from adsputils import setup_logging, load_config


config = load_config(proj_home=proj_home)
logger = setup_logging('rebuild', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))
lockfile = os.path.abspath(proj_home + '/rebuild.locked')


def get_solr_url(path):
    # use what is configured in current config, but only the first instance
    urls = config.get('SOLR_URLS')
    s = urls[0].split('/collection')[0]
    if path.startswith('/'):
        return s + path
    else:
        return s + '/' + path


cores_url = get_solr_url('/admin/cores')
update_url = get_solr_url('/collection2/update')
mbean_url = get_solr_url('/collection2/admin/mbeans?stats=true&wt=json')

def assert_different(dirname1, dirname2):
    assert dirname1 != dirname2

def assert_same(dirname1, dirname2):
    assert dirname1 == dirname2

def run():
    # it is important that we do not run multiple times
    if os.path.exists(lockfile):
        logger.error('Lockfile %s already exists; exiting! (if you want to proceed, delete the file)' % (lockfile,))
        data = read_lockfile(lockfile)
        for k,v in data.items():
            logger.error('%s=%s' % (k, v,))
        sys.exit(1)
    else:
        data = {}

    try:
        # verify both cores are there
        cores = requests.get(cores_url + '?wt=json').json()
        if set(cores['status'].keys()) != set(['collection1', 'collection2']):
            raise Exception('we dont have both cores available')

        assert_different(cores['status']['collection2']['dataDir'], cores['status']['collection1']['dataDir'])

        logger.info('We are starting the indexing into collection2; once finished; we will automatically activate the new core')

        logger.info('First, we will delete all documents from collection2')
        r = requests.post(update_url + '?commit=true&waitSearcher=true', data='<delete><query>*:*</query></delete>', headers={'Content-Type': 'text/xml'}, timeout=60*60)
        r.raise_for_status()
        logger.info('Done deleting all docs from collection2')

        cores = requests.get(cores_url + '?wt=json').json()
        if set(cores['status'].keys()) != set(['collection1', 'collection2']):
            raise Exception('We dont have both cores available')

        now = time.time()
        data['start'] = now
        write_lockfile(lockfile, data)

        command = 'python3 run.py --rebuild-collection --solr-collection collection2 --batch_size 1000 >> %s/logs/reindex.log' % (proj_home,)
        retcode, stdout, stderr = execute(command, cwd=proj_home)

        if retcode != 0:
            data['error'] = '%s failed with retcode=%s\nstderr:\n%s' % (command, retcode, stderr,)
            write_lockfile(lockfile, data)
            logger.error('stderr=%s' % (stderr))
            raise Exception('%s failed with retcode=%s\nstderr:\n%s' % (command, retcode, stderr,))

        logger.info('Successfully finished indexing in %s secs' % (time.time() - now,))

        # wait for solr workers to complete final messages
        monitor_solr_writes()

        # issue commit
        commit_time = datetime.datetime.utcnow()
        r = requests.get(update_url + '?commit=true&waitSearcher=false')
        r.raise_for_status()
        logger.info('Issued async commit to SOLR')

        # wait for solr commit to complete
        solr_error_count = 0
        finished = False
        while not finished:
            r = requests.get(mbean_url)
            if r.status_code != 200:
                solr_error_count += 1
                if solr_error_count > 2:
                    r.raise_for_status()
            else:
                beans = r.json()[u'solr-mbeans']
                for bean in beans:
                    if type(bean) is dict and 'searcher' in bean:
                        t = str_to_datetime(bean['searcher']['stats']['SEARCHER.searcher.registeredAt'])
                        logger.info('waiting for solr commit to complete, commit_time: %s, searcher registeredAt: %s' % (commit_time, t,))
                        if t > commit_time:
                            finished = True
                        time_waiting = datetime.datetime.utcnow() - commit_time
                        if (time_waiting.seconds > (3600 * 3)):
                            logger.warn('Solr commit running for over three hours, aborting')
                            raise
            if not finished:
                time.sleep(30)
        logger.info('Solr has registered a new searcher')

        logger.info('Waiting for the new collection to have a minimum number of commited documents')
        # all went well, verify the numDocs is similar to the previous collection
        min_committed_docs = os.environ.get('MIN_COMMITTED_DOCS', 17500000)
        min_index_size = os.environ.get('MIN_INDEX_SIZE', 200) # GB
        for _ in range(24): # Check every 5 minutes for 2 hours max
            time.sleep(300)
            verified, verified_msg = verify_collection2_size(cores_url, min_committed_docs, min_index_size)
            if verified:
                break
        if verified:
            logger.info(verified_msg)
        else:
            raise Exception(verified_msg)


        # all is well; swap the cores!
        r = requests.get(cores_url + '?action=SWAP&core=collection2&other=collection1&wt=json')
        r.raise_for_status()
        logger.info('Swapped collection1 with collection2')

        logger.info('Going to sleep for few secs...')
        time.sleep(30)

        # verify the new core is loaded
        new_cores = requests.get(cores_url + '?wt=json').json()
        assert_same(cores['status']['collection2']['dataDir'], new_cores['status']['collection1']['dataDir'])
        logger.info('Verified the new collection is in place')

        logger.info('Deleting the lock; congratulations on your new solr collection!')
        os.remove(lockfile)
    except Exception as e:
        logger.exception('Failed: we will keep the process permanently locked')
        data['last-exception'] = str(e)
        write_lockfile(lockfile, data)
        sys.exit(1)


def execute(command, **kwargs):
    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE, **kwargs)
    out, err = p.communicate()
    return (p.returncode, out, err)


def read_lockfile(lockfile):
    with open(lockfile, 'rb') as f:
        return pickle.load(f)


def write_lockfile(lockfile, data):
    with open(lockfile, 'wb') as f:
        pickle.dump(data, f)


def verify_collection2_size(cores_url, min_committed_docs, min_index_size):
    # Try to get info from solr
    try:
        response = requests.get(cores_url + '?wt=json')
        #response.raise_for_status()  # Raise an exception for non-2xx status codes
        cores = response.json()
    except (requests.exceptions.RequestException, json.decoder.JSONDecodeError, ValueError, TypeError) as e:
        return (False, str(e))
    # Extract key values
    data = cores.get('status', {}).get('collection2', {})
    num_docs = data.get('index', {}).get('numDocs', 0)
    index_size = data.get('index', {}).get('sizeInBytes', 0) / (1024*1024*1024.0) # GB
    #
    logger.info('New collection has {} committed entries and the index size is {:.2f} GB'.format(num_docs, index_size))
    if num_docs <= min_committed_docs:
        return (False, 'Too few committed documents in the new index: {}'.format(num_docs))
    if index_size <= min_index_size:
        return (False, 'The new index is suspiciously small: {:.2f} GB'.format(index_size))
    return (True, 'Successfully verified the new collection')


def str_to_datetime(s):
    """convert utc date string into a datetime object

    note: code in this file uses is timezone naive
    """
    try:
        d = datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        d = datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ')
    return d


def monitor_solr_writes():
    """detect when master pipeline workers have completed

    we wait for the number of pending docs on collection2 to not change for 2 minutes
    docsPending is available from the mbeans
    """
    previous_docs_pending = -1
    consecutive_match_count = 0
    solr_error_count = 0
    finshed = False
    logger.info('starting to monitor docsPending on solr')
    while not finshed:
        r = requests.get(mbean_url)
        if r.status_code != 200:
            solr_error_count += 1
            if solr_error_count > 2:
                r.raise_for_status()
        else:
            beans = r.json()[u'solr-mbeans']
            for bean in beans:
                if type(bean) is dict and 'updateHandler' in bean:
                    current_docs_pending = bean['updateHandler']['stats']['UPDATE.updateHandler.docsPending']
            if current_docs_pending == previous_docs_pending:
                consecutive_match_count += 1
            else:
                consecutive_match_count = 0
            previous_docs_pending = current_docs_pending
            if consecutive_match_count > 4:
                finshed = True
            else:
                logger.info('monitoring docsPending with current_docs_pending {}, previous_docs_pending {}, consecutive_match_count {}'.format(current_docs_pending, previous_docs_pending, consecutive_match_count))
                time.sleep(30)
    logger.info('completed monitoring of docsPending on solr with current_docs_pending {}, previous_docs_pending {}, consecutive_match_count {}'.format(current_docs_pending, previous_docs_pending, consecutive_match_count))


if __name__ == '__main__':
    run()
