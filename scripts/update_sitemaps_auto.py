#!/usr/bin/env python
"""
Wrapper script for automatic sitemap updates with lockfile protection.

"""

import os
import sys
import pickle
import time
import argparse
import re
import json
from subprocess import PIPE, Popen

proj_home = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if proj_home not in sys.path:
    sys.path.append(proj_home)

from adsputils import setup_logging, load_config
from celery.result import AsyncResult
from adsmp import tasks

config = load_config(proj_home=proj_home)
logger = setup_logging('sitemap_auto_update', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))

lockfile = os.path.abspath(proj_home + '/sitemap_auto_update.locked')


def read_lockfile(lockfile):
    with open(lockfile, 'rb') as f:
        return pickle.load(f)


def write_lockfile(lockfile, data):
    with open(lockfile, 'wb') as f:
        pickle.dump(data, f)


def execute(command, **kwargs):
    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE, **kwargs)
    out, err = p.communicate()
    return (p.returncode, out, err)


def monitor_workflow(workflow_id, start_time):
    """
    Monitor Celery workflow until completion.
    Returns True if successful, raises Exception if failed.
    """
    result = AsyncResult(workflow_id, app=tasks.app)
    
    check_interval = 30  # seconds
    last_log_time = time.time()
    log_interval = 300  # Log every 5 minutes
    max_duration = 12 * 3600  # 12 hours in seconds
    warning_logged = False
    
    logger.info('Monitoring workflow %s...' % workflow_id)
    
    while not result.ready():
        time.sleep(check_interval)
        current_time = time.time()
        elapsed = current_time - start_time
        
        if current_time - last_log_time >= log_interval:
            logger.info('Workflow still running... (elapsed: %.1f minutes)' % (elapsed / 60,))
            last_log_time = current_time
        
        # Warn if taking too long
        if elapsed > max_duration and not warning_logged:
            logger.warning('Workflow has been running for over 12 hours (%.1f hours)!' % (elapsed / 3600,))
            logger.warning('This is unusually long - check for stuck tasks or performance issues')
            warning_logged = True
    
    # Check if successful
    if result.successful():
        logger.info('Workflow completed successfully')
        return True
    else:
        error_msg = 'Workflow failed: %s' % str(result.info)
        logger.error(error_msg)
        raise Exception(error_msg)


def run(days_back=1):
    # Check for existing lockfile
    if os.path.exists(lockfile):
        logger.error('Lockfile %s already exists; exiting! (if you want to proceed, delete the file)' % (lockfile,))
        data = read_lockfile(lockfile)
        for k, v in data.items():
            logger.error('%s=%s' % (k, v,))
        sys.exit(1)
    else:
        data = {}

    try:
        now = time.time()
        data['start'] = now
        data['operation'] = 'sitemap_auto_update'
        data['days_back'] = days_back
        write_lockfile(lockfile, data)

        logger.info('Starting automatic sitemap update (looking back %d days)' % days_back)
        logger.info('This may take several hours depending on the number of updated records')

        # Execute command and capture workflow ID from output
        command = 'python3 run.py --update-sitemaps-auto --days-back %d' % days_back
        retcode, stdout, stderr = execute(command, cwd=proj_home)

        if retcode != 0:
            data['error'] = '%s failed with retcode=%s\nstderr:\n%s' % (command, retcode, stderr.decode())
            write_lockfile(lockfile, data)
            logger.error('stderr=%s' % (stderr.decode(),))
            raise Exception('%s failed with retcode=%s\nstderr:\n%s' % (command, retcode, stderr.decode()))

        # Parse workflow ID from stdout (JSON log format)
        stdout_str = stdout.decode()
        workflow_id = None
        
        # Try to parse as JSON first
        for line in stdout_str.split('\n'):
            if 'Submitted sitemap workflow:' in line:
                try:
                    log_entry = json.loads(line)
                    message = log_entry.get('message', '')
                    # Extract UUID from message
                    uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
                    match = re.search(uuid_pattern, message)
                    if match:
                        workflow_id = match.group(0)
                        break
                except (json.JSONDecodeError, ValueError):
                    # Fall back to simple parsing
                    uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
                    match = re.search(uuid_pattern, line)
                    if match:
                        workflow_id = match.group(0)
                        break
        
        if not workflow_id:
            logger.info('No workflow was started (no records to update)')
            logger.info('Operation completed in %s secs' % (time.time() - now,))
            os.remove(lockfile)
            return

        logger.info('Workflow ID: %s' % workflow_id)
        data['workflow_id'] = workflow_id
        write_lockfile(lockfile, data)

        # Monitor workflow until completion
        monitor_workflow(workflow_id, now)

        logger.info('Successfully finished sitemap auto-update in %s secs (%.1f minutes)' % 
                   (time.time() - now, (time.time() - now) / 60))

        # Success - remove lockfile
        logger.info('Deleting the lock; sitemap auto-update completed successfully!')
        os.remove(lockfile)
        
    except Exception as e:
        logger.exception('Failed: we will keep the process permanently locked')
        data['last-exception'] = str(e)
        data['failed_at'] = time.time()
        write_lockfile(lockfile, data)
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Automatic sitemap update with lockfile protection')
    parser.add_argument('--days-back', dest='days_back', type=int, default=1,
                        help='Number of days to look back for updated records (default: 1)')
    args = parser.parse_args()
    
    run(days_back=args.days_back)
