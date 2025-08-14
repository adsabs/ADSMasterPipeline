from __future__ import absolute_import, unicode_literals
from past.builtins import basestring
import os
import time
import adsputils
from adsmp import app as app_module
from adsmp import solr_updater
from adsmp.models import Records
from kombu import Queue
from adsmsg.msg import Msg
from sqlalchemy import exc
from sqlalchemy.orm import load_only


from adsmp import templates
# from adsmp import s3_utils
from kombu import Queue
from adsmsg.msg import Msg
from sqlalchemy import create_engine, MetaData, Table, exc, insert
from sqlalchemy.orm import sessionmaker
from adsmp.models import SitemapInfo
from collections import defaultdict
import pdb
# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSMasterPipelineCelery('master-pipeline', proj_home=proj_home, local_config=globals().get('local_config', {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('update-record', app.exchange, routing_key='update-record'),
    Queue('index-records', app.exchange, routing_key='index-records'),
    Queue('rebuild-index', app.exchange, routing_key='rebuild-index'),
    Queue('delete-records', app.exchange, routing_key='delete-records'),
    Queue('index-solr', app.exchange, routing_key='index-solr'),
    Queue('index-metrics', app.exchange, routing_key='index-metrics'),
    Queue('index-data-links-resolver', app.exchange, routing_key='index-data-links-resolver'),
    Queue('manage-sitemap', app.exchange, routing_key='manage-sitemap'),
    Queue('generate-single-sitemap', app.exchange, routing_key='generate-single-sitemap'),
    Queue('update-sitemap-files', app.exchange, routing_key='update-sitemap-files'),
    Queue('update-scixid', app.exchange, routing_key='update-scixid'),
)


# ============================= TASKS ============================================= #

@app.task(queue='update-record')
def task_update_record(msg):
    """Receives payload to update the record.

    @param msg: protobuff that contains at minimum
        - bibcode
        - and specific payload
    """
    logger.debug('Updating record: %s', msg)
    status = app.get_msg_status(msg)
    type = app.get_msg_type(msg)
    bibcodes = []

    if status == 'deleted':
        if type == 'metadata':
            task_delete_documents(msg.bibcode)
        elif type == 'nonbib_records':
            for m in msg.nonbib_records: # TODO: this is very ugly, we are repeating ourselves...
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'nonbib_data', None)
                if record:
                    logger.debug('Deleted %s, result: %s', type, record)
        elif type == 'metrics_records':
            for m in msg.metrics_records:
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'metrics', None)
                if record:
                    logger.debug('Deleted %s, result: %s', type, record)
        else:
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, type, None)
            if record:
                logger.debug('Deleted %s, result: %s', type, record)

    elif status == 'active':
        # save into a database
        # passed msg may contain details on one bibcode or a list of bibcodes
        if type == 'nonbib_records':
            for m in msg.nonbib_records:
                m = Msg(m, None, None) # m is a raw protobuf, TODO: return proper instance from .nonbib_records
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'nonbib_data', m.toJSON())
                if record:
                    logger.debug('Saved record from list: %s', record)
        elif type == 'metrics_records':
            for m in msg.metrics_records:
                m = Msg(m, None, None)
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'metrics', m.toJSON(including_default_value_fields=True))
                if record:
                    logger.debug('Saved record from list: %s', record)
        elif type == 'augment':
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, 'augment',
                                        msg.toJSON(including_default_value_fields=True))
            if record:
                logger.debug('Saved augment message: %s', msg)

        else:
            # here when record has a single bibcode
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, type, msg.toJSON())
            if record:
                logger.debug('Saved record: %s', record)
            if type == 'metadata':
                # with new bib data we request to augment the affiliation
                # that pipeline will eventually respond with a msg to task_update_record
                logger.debug('requesting affilation augmentation for %s', msg.bibcode)
                app.request_aff_augment(msg.bibcode)

    else:
        logger.error('Received a message with unclear status: %s', msg)

@app.task(queue='update-scixid')
def task_update_scixid(bibcodes, flag):
    """Receives bibcodes to add scix id to the record.

    @param 
    bibcodes: single bibcode or list of bibcodes to update
    flag: 'update' - update records to have a new scix_id if they do not already have one
          'force' - force reset scix_id and assign new scix_ids to all bibcodes
          'reset' - reset scix_id to None for all bibcodes
    """
    logger.info('Starting task_update_scixid with flag: %s for %s bibcodes', flag, len(bibcodes))
    if flag not in ['update', 'force', 'reset']:
        logger.error('task_update_scixid flag can only have the values "update" or "force"')
        return

    for bibcode in bibcodes:
        logger.debug('Processing bibcode: %s with flag: %s', bibcode, flag)

        with app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is None:
                logger.error('Bibcode %s does not exist in Records DB', bibcode)
                continue

            logger.debug('Current scix_id for %s: %s', bibcode, r.scix_id)
            logger.debug('Has bib_data: %s', bool(r.bib_data))

            if flag == 'update':
                if not r.scix_id:
                    if r.bib_data:
                        r.scix_id = "scix:" + app.generate_scix_id(r.bib_data)
                        try:
                            session.commit()
                            logger.debug('Bibcode %s has been assigned a new scix id: %s', bibcode, r.scix_id)
                        except exc.IntegrityError:
                            logger.exception('error in app.update_storage while updating database for bibcode %s', bibcode)
                            session.rollback()
                    else:
                        r.scix_id = None
                        session.commit()
                        logger.debug('Bibcode %s has no bib_data, scix_id is set to None', bibcode)
                else:
                    logger.debug('Bibcode %s already has a scix id assigned: %s', bibcode, r.scix_id)

            if flag == 'force': 
                if r.bib_data:
                    old_id = r.scix_id
                    r.scix_id = "scix:" + app.generate_scix_id(r.bib_data)
                    try:
                        session.commit()
                        logger.debug('Bibcode %s scix_id changed from %s to %s', bibcode, old_id, r.scix_id)
                    except exc.IntegrityError:
                        logger.exception('error in app.update_storage while updating database for bibcode %s', bibcode)
                        session.rollback()
                else:
                    r.scix_id = None
                    session.commit()
                    logger.debug('Bibcode %s has no bib_data, scix_id is set to None', bibcode)

            if flag == 'reset':
                old_id = r.scix_id
                r.scix_id = None
                session.commit()
                logger.debug('Bibcode %s scix_id reset from %s to None', bibcode, old_id)

@app.task(queue='rebuild-index')
def task_rebuild_index(bibcodes, solr_targets=None):
    """part of feature that rebuilds the entire solr index from scratch

    note that which collection to update is part of the url in solr_targets
    """
    reindex_records(bibcodes, force=True, update_solr=True, update_metrics=False, update_links=False, commit=False,
                    ignore_checksums=True, solr_targets=solr_targets, update_processed=False, priority=0)


@app.task(queue='index-records')
def task_index_records(bibcodes, force=False, update_solr=True, update_metrics=True, update_links=True, commit=False,
                       ignore_checksums=False, solr_targets=None, update_processed=True, priority=0):
    """
    Sends data to production systems: solr, metrics and resolver links
    Only does send if data has changed
    This task is (normally) called by the cronjob task
    (that one, quite obviously, is in turn started by cron)
    Use code also called by task_rebuild_index,
    """
    reindex_records(bibcodes, force=force, update_solr=update_solr, update_metrics=update_metrics, update_links=update_links, commit=commit,
                    ignore_checksums=ignore_checksums, solr_targets=solr_targets, update_processed=update_processed)


@app.task(queue='index-solr')
def task_index_solr(solr_records, solr_records_checksum, commit=False, solr_targets=None, update_processed=True):
    app.index_solr(solr_records, solr_records_checksum, solr_targets, commit=commit, update_processed=update_processed)


@app.task(queue='index-metrics')
def task_index_metrics(metrics_records, metrics_records_checksum, update_processed=True):
    # todo: create insert and update lists before queuing?
    app.index_metrics(metrics_records, metrics_records_checksum)


@app.task(queue='index-data-links-resolver')
def task_index_data_links_resolver(links_data_records, links_data_records_checksum, update_processed=True):
    app.index_datalinks(links_data_records, links_data_records_checksum, update_processed=update_processed)


def reindex_records(bibcodes, force=False, update_solr=True, update_metrics=True, update_links=True, commit=False,
                    ignore_checksums=False, solr_targets=None, update_processed=True, priority=0):
    """Receives bibcodes that need production store updated
    Receives bibcodes and checks the database if we have all the
    necessary pieces to push to production store. If not, then postpone and
    send later.
    we consider a record to be ready for solr if these pieces were updated
    (and were updated later than the last 'processed' timestamp):
        - bib_data
        - nonbib_data
        - orcid_claims
    if the force flag is true only bib_data is needed
    for solr, 'fulltext' is not considered essential; but updates to fulltext will
    trigger a solr_update (so it might happen that a document will get
    indexed twice; first with only metadata and later on incl fulltext)
    """

    if isinstance(bibcodes, basestring):
        bibcodes = [bibcodes]

    if not (update_solr or update_metrics or update_links):
        raise Exception('Hmmm, I dont think I let you do NOTHING, sorry!')

    logger.debug('Running index-records for: %s', bibcodes)
    solr_records = []
    metrics_records = []
    links_data_records = []
    solr_records_checksum = []
    metrics_records_checksum = []
    links_data_records_checksum = []
    links_url = app.conf.get('LINKS_RESOLVER_UPDATE_URL')

    if update_solr:
        fields = None  # Load all the fields since solr records grab data from almost everywhere
    else:
        # Optimization: load only fields that will be used
        fields = ['bibcode', 'augments_updated', 'bib_data_updated', 'fulltext_updated', 'metrics_updated', 'nonbib_data_updated', 'orcid_claims_updated', 'processed']
        if update_metrics:
            fields += ['metrics', 'metrics_checksum']
        if update_links:
            fields += ['nonbib_data', 'bib_data', 'datalinks_checksum']

    # check if we have complete record
    for bibcode in bibcodes:
        r = app.get_record(bibcode, load_only=fields)

        if r is None:
            logger.error('The bibcode %s doesn\'t exist!', bibcode)
            continue

        augments_updated = r.get('augments_updated', None)
        bib_data_updated = r.get('bib_data_updated', None)
        fulltext_updated = r.get('fulltext_updated', None)
        metrics_updated = r.get('metrics_updated', None)
        nonbib_data_updated = r.get('nonbib_data_updated', None)
        orcid_claims_updated = r.get('orcid_claims_updated', None)

        year_zero = '1972'
        processed = r.get('processed', adsputils.get_date(year_zero))
        if processed is None:
            processed = adsputils.get_date(year_zero)

        is_complete = all([bib_data_updated, orcid_claims_updated, nonbib_data_updated])

        if is_complete or (force is True and bib_data_updated):
            if force is False and all([
                    augments_updated and augments_updated < processed,
                    bib_data_updated and bib_data_updated < processed,
                    nonbib_data_updated and nonbib_data_updated < processed,
                    orcid_claims_updated and orcid_claims_updated < processed
                   ]):
                logger.debug('Nothing to do for %s, it was already indexed/processed', bibcode)
                continue
            if force:
                logger.debug('Forced indexing of: %s (metadata=%s, orcid=%s, nonbib=%s, fulltext=%s, metrics=%s, augments=%s)' %
                             (bibcode, bib_data_updated, orcid_claims_updated, nonbib_data_updated, fulltext_updated,
                              metrics_updated, augments_updated))
            # build the solr record
            if update_solr:
                solr_payload = solr_updater.transform_json_record(r)
                # ADS microservices assume the identifier field exists and contains the canonical bibcode:
                if 'identifier' not in solr_payload:
                    solr_payload['identifier'] = []
                if 'bibcode' in solr_payload and solr_payload['bibcode'] not in solr_payload['identifier']:
                    solr_payload['identifier'].append(solr_payload['bibcode'])
                logger.debug('Built SOLR record for %s', solr_payload['bibcode'])
                solr_checksum = app.checksum(solr_payload)
                if ignore_checksums or r.get('solr_checksum', None) != solr_checksum:
                    solr_records.append(solr_payload)
                    solr_records_checksum.append(solr_checksum)
                else:
                    logger.debug('Checksum identical, skipping solr update for: %s', bibcode)

            # get data for metrics
            if update_metrics:
                metrics_payload = r.get('metrics', None)
                metrics_checksum = app.checksum(metrics_payload or '')
                if (metrics_payload and ignore_checksums) or (metrics_payload and r.get('metrics_checksum', None) != metrics_checksum):
                    metrics_payload['bibcode'] = bibcode
                    logger.debug('Got metrics: %s', metrics_payload)
                    metrics_records.append(metrics_payload)
                    metrics_records_checksum.append(metrics_checksum)
                else:
                    logger.debug('Checksum identical or no metrics data available, skipping metrics update for: %s', bibcode)

            if update_links and links_url:
                datalinks_payload = app.generate_links_for_resolver(r)
                if datalinks_payload:
                    datalinks_checksum = app.checksum(datalinks_payload)
                    if ignore_checksums or r.get('datalinks_checksum', None) != datalinks_checksum:
                        links_data_records.append(datalinks_payload)
                        links_data_records_checksum.append(datalinks_checksum)
        else:
            # if forced and we have at least the bib data, index it
            if force is True:
                logger.warn('%s is missing bib data, even with force=True, this cannot proceed', bibcode)
            else:
                logger.debug('%s not ready for indexing yet (metadata=%s, orcid=%s, nonbib=%s, fulltext=%s, metrics=%s, augments=%s)' %
                             (bibcode, bib_data_updated, orcid_claims_updated, nonbib_data_updated, fulltext_updated,
                              metrics_updated, augments_updated))
    if solr_records:
        task_index_solr.apply_async(
            args=(solr_records, solr_records_checksum,),
            kwargs={
               'commit': commit,
               'solr_targets': solr_targets,
               'update_processed': update_processed
            }
        )
    if metrics_records:
        task_index_metrics.apply_async(
            args=(metrics_records, metrics_records_checksum,),
            kwargs={
               'update_processed': update_processed
            }
        )
    if links_data_records:
        task_index_data_links_resolver.apply_async(
            args=(links_data_records, links_data_records_checksum,),
            kwargs={
               'update_processed': update_processed
            }
        )


@app.task(queue='delete-records')
def task_delete_documents(bibcode):
    """Delete document from SOLR and from our storage.
    @param bibcode: string
    """
    logger.debug('To delete: %s', bibcode)
    app.delete_by_bibcode(bibcode)
    deleted, failed = solr_updater.delete_by_bibcodes([bibcode], app.conf['SOLR_URLS'])
    if len(failed):
        logger.error('Failed deleting documents from solr: %s', failed)
    if len(deleted):
        logger.debug('Deleted SOLR docs: %s', deleted)

    if app.metrics_delete_by_bibcode(bibcode):
        logger.debug('Deleted metrics record: %s', bibcode)
    else:
        logger.debug('Failed to deleted metrics record: %s', bibcode)

@app.task(queue='manage-sitemap') 
def task_manage_sitemap(bibcodes, action):
    """
    Executes the actions below for the given bibcodes
    
    Actions:
    - 'add': add/update record info to sitemap table if bibdata_updated is newer than filename_lastmoddate
    - 'force-update': force update sitemap table entries for given bibcodes
    - 'remove': remove bibcodes from sitemap table (TODO: not implemented)
    - 'delete-table': delete all contents of sitemap table and backup files
    - 'update-robots': force update robots.txt files for all sites
    """

    sitemap_dir = app.sitemap_dir

    if action == 'delete-table':
        # reset and empty all entries in sitemap table
        app.delete_contents(SitemapInfo)

        # move all sitemap files to a backup directory
        app.backup_sitemap_files(sitemap_dir)
        return
    
    elif action == 'update-robots':
        logger.info('Force updating robots.txt files for all sites')
        success = update_robots_files(True)
        if success:
            logger.info('robots.txt files updated successfully')
        else:
            logger.error('Failed to update robots.txt files')
            raise Exception('Failed to update robots.txt files')
        return
    
    elif action == 'bootstrap':
        logger.info('Bootstrapping sitemaps for all existing records...')
        
        with app.session_scope() as session:
            # Check if SitemapInfo already has records
            existing_sitemap_count = session.query(SitemapInfo).count()
            if existing_sitemap_count > 0:
                logger.warning(f'SitemapInfo table already has {existing_sitemap_count} records')
                logger.warning('Use "force-update" action if you want to update existing sitemap records')
                return
            
            # Get total count for progress tracking (optional - can be expensive on large tables)
            try:
                total_records = session.query(Records).count()
                logger.info(f'Found {total_records:,} total records in database')
            except Exception as e:
                logger.warning(f'Could not get total record count: {e}. Progress tracking disabled.')
                total_records = None
            
            if total_records == 0:
                logger.info('No records found in Records table - nothing to bootstrap')
                return
            
            batch_size = app.conf.get('SITEMAP_BOOTSTRAP_BATCH_SIZE', 50000)  # Increased from 10000
            max_records_per_sitemap = app.conf.get('MAX_RECORDS_PER_SITEMAP', 10000)
            processed = 0
            successful_count = 0
            failed_count = 0
            last_id = 0
            
            # Pre-calculate sitemap filename assignments
            current_file_index = 1
            records_in_current_file = 0
            
            logger.info('Processing records in bulk batches of %d...', batch_size)
            logger.info('Max records per sitemap file: %d', max_records_per_sitemap)
            logger.info('Using keyset pagination for efficient processing of large datasets')
            
            while True:
                # Use keyset pagination instead of OFFSET/LIMIT for O(1) performance
                records_batch = (
                    session.query(Records.id, Records.bibcode, Records.bib_data_updated)
                    .filter(Records.id > last_id)
                    .order_by(Records.id)
                    .limit(batch_size)
                    .all()
                )
                
                if not records_batch:
                    break  # No more records
                
                # Prepare bulk insert data
                bulk_data = []
                for record in records_batch:
                    try:
                        # Calculate sitemap filename efficiently
                        if records_in_current_file >= max_records_per_sitemap:
                            current_file_index += 1
                            records_in_current_file = 0
                        
                        sitemap_filename = f'sitemap_bib_{current_file_index}.xml'
                        records_in_current_file += 1
                        
                        # Create bulk insert record
                        bulk_data.append({
                            'record_id': record.id,
                            'bibcode': record.bibcode,
                            'bib_data_updated': record.bib_data_updated,
                            'scix_id': None,
                            'sitemap_filename': sitemap_filename,
                            'filename_lastmoddate': None,
                            'update_flag': True
                        })
                        successful_count += 1
                        
                    except Exception as e:
                        logger.error('Failed to prepare record %s: %s', record.bibcode, str(e))
                        failed_count += 1
                        continue
                
                # Bulk insert the entire batch 
                if bulk_data:
                    try:
                        # Use session.execute with insert() for better performance
                        insert_sitemap = insert(SitemapInfo)
                        session.execute(insert_sitemap, bulk_data)
                        session.commit()
                        logger.debug('Bulk inserted %d records', len(bulk_data))
                    except Exception as e:
                        logger.error('Bulk insert failed for batch: %s', str(e))
                        session.rollback()
                        failed_count += len(bulk_data)
                        successful_count -= len(bulk_data)
                
                # Update last_id for next iteration
                last_id = records_batch[-1].id
                processed += len(records_batch)
                
                # Log progress less frequently to reduce overhead
                if processed % (batch_size * 5) == 0 or not records_batch:  # Log every 5 batches
                    if total_records:
                        progress_pct = (processed / total_records) * 100
                        logger.info('Bootstrapped %d/%d records (%.1f%%) - %d successful, %d failed', 
                                  processed, total_records, progress_pct, successful_count, failed_count)
                    else:
                        logger.info('Bootstrapped %d records - %d successful, %d failed', 
                                  processed, successful_count, failed_count)
            
            logger.info('Bootstrap completed: %d successful, %d failed out of %d total records', 
                       successful_count, failed_count, processed)
            logger.info('All records marked with update_flag=True')
            logger.info('Run --update-sitemap-files to generate sitemap XML files')
        return
    
    elif action == 'remove':
        affected_files = set()
        files_to_delete = set()  # Track files that become empty
        removed_count = 0
        
        with app.session_scope() as session:
            for bibcode in bibcodes:
                sitemap_info = session.query(SitemapInfo).filter_by(bibcode=bibcode).first()
                if sitemap_info:
                    affected_files.add(sitemap_info.sitemap_filename)
                    session.delete(sitemap_info)
                    removed_count += 1
            
            # Check which files now have 0 records left
            for filename in affected_files:
                remaining_count = session.query(SitemapInfo).filter_by(sitemap_filename=filename).count()
                if remaining_count == 0:
                    files_to_delete.add(filename)
                else:
                    # Mark remaining records for regeneration
                    session.query(SitemapInfo).filter_by(sitemap_filename=filename).update({'update_flag': True})
            
            # Delete empty sitemap files from disk (all sites)
            if files_to_delete:
                sites_config = app.conf.get('SITES', {})
                sitemap_dir = app.sitemap_dir
                
                for filename in files_to_delete:
                    for site_key in sites_config.keys():
                        site_filepath = os.path.join(sitemap_dir, site_key, filename)
                        if os.path.exists(site_filepath):
                            os.remove(site_filepath)
                            app.logger.info(f'Deleted empty sitemap file: {site_filepath}')
                            
                            # # Delete from S3 if enabled
                            # s3_success = s3_utils.delete_sitemap_file_from_s3(
                            #     app.conf, site_key, filename
                            # )
                            # if not s3_success:
                            #     logger.warning('Failed to delete %s from S3 for site %s', filename, site_key)
        
        app.logger.info(f'Removed {removed_count} bibcodes, deleted {len(files_to_delete)} empty files')
        session.commit()

    elif action in ['add', 'force-update']:
        if isinstance(bibcodes, basestring):
            bibcodes = [bibcodes]

        logger.debug('Updating sitemap info for: %s', bibcodes)
        fields = ['id', 'bibcode', 'bib_data_updated']
        sitemap_records = []

        # update all record_id from records table into sitemap table
        successful_count = 0
        failed_count = 0
        
        for bibcode in bibcodes:
            try:
                record = app.get_record(bibcode, load_only=fields)
                sitemap_info = app.get_sitemap_info(bibcode) 

                if record is None:
                    logger.error('The bibcode %s doesn\'t exist!', bibcode)
                    failed_count += 1
                    continue

                # Create sitemap record data structure with default None values for filename_lastmoddate and sitemap_filename
                sitemap_record = {
                        'record_id': record.get('id'), # Records object uses attribute access
                        'bibcode': record.get('bibcode'), # Records object uses attribute access  
                        'bib_data_updated': record.get('bib_data_updated', None),
                        'filename_lastmoddate': None, 
                        'sitemap_filename': None,
                        'update_flag': False
                    }
                
                # New sitemap record
                if sitemap_info is None:            
                    sitemap_record['update_flag'] = True
                    sitemap_records.append((sitemap_record['record_id'], sitemap_record['bibcode']))
                    app._populate_sitemap_table(sitemap_record) 
                    

                else:
                    # Sitemap record exists, update it 
                    sitemap_record['filename_lastmoddate'] = sitemap_info.get('filename_lastmoddate', None)
                    sitemap_record['sitemap_filename'] = sitemap_info.get('sitemap_filename', None)

                    bib_data_updated = sitemap_record.get('bib_data_updated', None) 
                    file_modified = sitemap_record.get('filename_lastmoddate', None)

                    # If action is 'add' and bibdata was updated, or if action is 'force-update', set update_flag to True
                    # Sitemap files will need to be updated in task_update_sitemap_files
                    if action == 'force-update':
                        sitemap_record['update_flag'] = True
                    elif action == 'add':
                        # Sitemap file has never been generated OR data updated since last generation
                        if file_modified is None or (file_modified and bib_data_updated and bib_data_updated > file_modified):
                            sitemap_record['update_flag'] = True
                    
                    app._populate_sitemap_table(sitemap_record, sitemap_info)
                
                successful_count += 1
                logger.debug('Successfully processed sitemap for bibcode: %s', bibcode)
                
            except Exception as e:
                failed_count += 1
                logger.error('Failed to populate sitemap table for bibcode %s: %s', bibcode, str(e))
                # Continue to next bibcode instead of crashing
                continue
        
        logger.info('Sitemap population completed: %d successful, %d failed out of %d total bibcodes', 
                    successful_count, failed_count, len(bibcodes)) 
        logger.info('%s Total sitemap records created: %s', len(sitemap_records), sitemap_records)

# TODO: Add directory names: about, help, blog  
# TODO: Need to query github to find when above dirs are updated: https://docs.github.com/en/rest?apiVersion=2022-11-28
# TODO: Need to generate an API token from ADStailor (maybe)
def update_robots_files(force_update=False):
    """Update robots.txt files"""
    
    try:
        # Get sites configuration
        sites_config = app.conf.get('SITES', {})
        if not sites_config:
            logger.error('No SITES configuration found for robots.txt generation')
            return False
            
        sitemap_dir = app.sitemap_dir
        updated_sites = 0
        
        for site_key, site_config in sites_config.items():
            try:
                # Create site-specific directory if it doesn't exist
                site_output_dir = os.path.join(sitemap_dir, site_key)
                if not os.path.exists(site_output_dir):
                    os.makedirs(site_output_dir)
                
                robots_filepath = os.path.join(site_output_dir, 'robots.txt')
                sitemap_url = site_config.get('sitemap_url', '')
                
                # Check if robots.txt needs updating
                needs_update = force_update or not os.path.exists(robots_filepath)
                
                if not needs_update:
                    # Check if content has changed
                    try:
                        with open(robots_filepath, 'r', encoding='utf-8') as f:
                            existing_content = f.read()
                        expected_content = templates.render_robots_txt(sitemap_url)
                        needs_update = existing_content.strip() != expected_content.strip()
                    except Exception:
                        needs_update = True  # If we can't read it, recreate it
                
                if needs_update:
                    robots_content = templates.render_robots_txt(sitemap_url)
                    with open(robots_filepath, 'w', encoding='utf-8') as f:
                        f.write(robots_content)
                    
                    # # Upload to S3 if enabled
                    # s3_success = s3_utils.upload_sitemap_file_to_s3(
                    #     app.conf, robots_filepath, site_key, 'robots.txt'
                    # )
                    # if not s3_success:
                    #     logger.warning('Failed to upload robots.txt to S3 for site %s', site_key)
                    
                    logger.info('Updated robots.txt for site %s at %s', 
                              site_config.get('name', site_key), robots_filepath)
                    updated_sites += 1
                else:
                    logger.debug('robots.txt for site %s is up to date', site_config.get('name', site_key))
                    
            except Exception as e:
                logger.error('Failed to update robots.txt for site %s: %s', site_key, str(e))
                continue
        
        logger.info('Robots.txt update completed: %d sites updated', updated_sites)
        return True
        
    except Exception as e:
        logger.error('Failed to update robots.txt files: %s', str(e))
        return False

def update_sitemap_index():
    """Generate sitemap index files for all configured sites"""
    
    try:
        # Get sites configuration
        sites_config = app.conf.get('SITES', {})
        if not sites_config:
            logger.error('No SITES configuration found for sitemap index generation')
            return False
            
        sitemap_dir = app.sitemap_dir
        updated_sites = 0
        
        with app.session_scope() as session:
            # Get all distinct sitemap filenames from database
            sitemap_files = (
                session.query(SitemapInfo.sitemap_filename.distinct())
                .filter(SitemapInfo.sitemap_filename.isnot(None))
                .all()
            )
            
            if not sitemap_files:
                logger.warning('No sitemap files found in database for index generation')
                return True
            
            # Flatten the list of tuples into a list of strings
            sitemap_filenames = [filename[0] for filename in sitemap_files]
            
            for site_key, site_config in sites_config.items():
                try:
                    # Create site-specific directory if it doesn't exist
                    site_output_dir = os.path.join(sitemap_dir, site_key)
                    if not os.path.exists(site_output_dir):
                        os.makedirs(site_output_dir)
                    
                    # Build sitemap entries for this site
                    base_url = site_config.get('base_url', 'https://ui.adsabs.harvard.edu/')
                    sitemap_entries = []
                    
                    for filename in sitemap_filenames:
                        # Check if the actual sitemap file exists for this site
                        sitemap_filepath = os.path.join(site_output_dir, filename)
                        if os.path.exists(sitemap_filepath):
                            # Get file modification time
                            mtime = os.path.getmtime(sitemap_filepath)
                            lastmod_date = time.strftime('%Y-%m-%d', time.gmtime(mtime))
                            
                            # Create sitemap entry
                            sitemap_url = f"{base_url.rstrip('/')}"
                            entry = templates.format_sitemap_entry(sitemap_url, filename, lastmod_date)
                            sitemap_entries.append(entry)
                    
                    if sitemap_entries:
                        # Generate sitemap index content
                        index_content = templates.render_sitemap_index(''.join(sitemap_entries))
                        
                        # Write sitemap index file
                        # Ex: <sitemap><loc>https://ui.adsabs.harvard.edu/sitemap/sitemap_bib_1.xml</loc><lastmod>2023-01-01</lastmod></sitemap>
                        index_filepath = os.path.join(site_output_dir, 'sitemap_index.xml')
                        with open(index_filepath, 'w', encoding='utf-8') as f:
                            f.write(index_content)
                        
                        # # Upload to S3 if enabled
                        # s3_success = s3_utils.upload_sitemap_file_to_s3(
                        #     app.conf, index_filepath, site_key, 'sitemap_index.xml'
                        # )
                        # if not s3_success:
                        #     logger.warning('Failed to upload sitemap_index.xml to S3 for site %s', site_key)
                        
                        logger.info('Generated sitemap index for site %s with %d entries at %s', 
                                  site_config.get('name', site_key), len(sitemap_entries), index_filepath)
                        updated_sites += 1
                    else:
                        logger.warning('No sitemap files found for site %s', site_config.get('name', site_key))
                        
                except Exception as e:
                    logger.error('Failed to generate sitemap index for site %s: %s', site_key, str(e))
                    continue
        
        logger.info('Sitemap index generation completed: %d sites updated', updated_sites)
        return True
        
    except Exception as e:
        logger.error('Failed to generate sitemap index files: %s', str(e))
        return False
 
@app.task(queue='generate-single-sitemap')
def task_generate_single_sitemap(sitemap_filename, record_ids):
    """Worker task: Generate a single sitemap file for all configured sites given record ids"""
    
    try:
        # Get sites configuration
        sites_config = app.conf.get('SITES', {})
        if not sites_config:
            logger.error('No SITES configuration found')
            return False
            
        sitemap_dir = app.sitemap_dir
        
        with app.session_scope() as session:
            # Get all records for this specific file
            file_records = (
                session.query(SitemapInfo)
                .filter(SitemapInfo.id.in_(record_ids))
                .all()
            )
            
            if not file_records:
                logger.warning('No records found for sitemap file %s', sitemap_filename)
                return False
            
            logger.debug('Processing sitemap file %s with %d records', sitemap_filename, len(file_records))
            
            # Generate file for each configured site
            successful_sites = 0
            for site_key, site_config in sites_config.items():
                try:
                    # Create site-specific directory only if it doesn't exist
                    # Ex: /app/logs/sitemap/ads/ or /app/logs/sitemap/scix/
                    site_output_dir = os.path.join(sitemap_dir, site_key) 
                    if not os.path.exists(site_output_dir):
                        os.makedirs(site_output_dir)
                    
                    # Ex: /app/logs/sitemap/ads/sitemap_bib_1.xml or /app/logs/sitemap/scix/sitemap_bib_1.xml
                    site_filepath = os.path.join(site_output_dir, sitemap_filename)
                    
                    # Ex: https://ui.adsabs.harvard.edu/abs/{bibcode}
                    # Get site-specific URL pattern
                    abs_url_pattern = site_config.get('abs_url_pattern', 'https://ui.adsabs.harvard.edu/abs/{bibcode}')
                    
                    url_entries = []
                    for info in file_records:
                        # Grab the lastmod date from the bib_data_updated field, if present
                        lastmod_date = info.bib_data_updated.date() if info.bib_data_updated else adsputils.get_date().date()
                        # Use site-specific URL pattern
                        # Ex: https://ui.adsabs.harvard.edu/abs/2023ApJ...123..456A/abstract
                        url_entry = templates.format_url_entry(info.bibcode, lastmod_date, abs_url_pattern)
                        url_entries.append(url_entry)
                    
                    # Write the site-specific XML file
                    # Ex: <url><loc>https://ui.adsabs.harvard.edu/abs/2023ApJ...123..456A/abstract</loc><lastmod>2023-01-01</lastmod></url>
                    sitemap_content = templates.render_sitemap_file(''.join(url_entries))
                    with open(site_filepath, 'w', encoding='utf-8') as file:
                        file.write(sitemap_content)
                    
                    # # Upload to S3 if enabled
                    # s3_success = s3_utils.upload_sitemap_file_to_s3(
                    #     app.conf, site_filepath, site_key, sitemap_filename
                    # )
                    # if not s3_success:
                    #     logger.warning('Failed to upload %s to S3 for site %s', sitemap_filename, site_key)
                    
                    logger.debug('Successfully generated %s for site %s with %d records', 
                               site_filepath, site_config.get('name', site_key), len(url_entries))
                    successful_sites += 1
                    
                except Exception as e:
                    logger.error('Failed to generate sitemap file %s for site %s: %s', 
                               sitemap_filename, site_key, str(e))
                    continue
            
            # Update database records only after all sites are processed successfully
            if successful_sites > 0:
                for info in file_records:
                    # Filename lastmoddate is updated to current date and time
                    info.filename_lastmoddate = adsputils.get_date()
                    info.update_flag = False
                
                session.commit()
                logger.debug('Successfully generated %s for %d sites', sitemap_filename, successful_sites)
                return True
            else:
                logger.error('Failed to generate %s for any sites', sitemap_filename)
                return False
            
    except Exception as e:
        logger.error('Failed to generate sitemap file %s: %s', sitemap_filename, str(e))
        return False

@app.task(queue='update-sitemap-files') 
def task_update_sitemap_files():
    """Orchestrator task: Updates robots.txt first, then spawns parallel tasks for each sitemap file"""
    
    try:
        logger.info('Starting sitemap workflow')
        
        # Step 1: Update robots.txt files (only if necessary)
        logger.info('Updating robots.txt files...')
        success = update_robots_files(True)  # Simple direct call
        
        if not success:
            logger.warning('robots.txt update failed, but continuing with sitemap generation')
        
        # Step 2: Generate sitemap files
        logger.info('Starting sitemap file generation')
        
        # Find distinct sitemap files that need updating
        with app.session_scope() as session:
            files_needing_update_subquery = (
                session.query(SitemapInfo.sitemap_filename.distinct())
                .filter(SitemapInfo.update_flag == True)
            )
            # Get all records for all the sitemap files above (even if they do not need updating)
            all_records = (
                session.query(SitemapInfo)
                .filter(SitemapInfo.sitemap_filename.in_(files_needing_update_subquery))
                .all()
            )
            
            if not all_records:
                logger.info('No sitemap files need updating')
                return
            
            logger.info('Found %d records to process in sitemap files', len(all_records))
            
            # Group records by filename. Ex: {'sitemap_bib_1.xml': [1, 2, 3], 'sitemap_bib_2.xml': [4, 5, 6]}
            files_dict = defaultdict(list)
            for record in all_records:
                files_dict[record.sitemap_filename].append(record.id)  
        
        # Spawn parallel Celery tasks - one per file
        logger.info('Spawning %d parallel tasks for sitemap files', len(files_dict))
        task_results = []
        
        for sitemap_filename, record_ids in files_dict.items():
            # Launch async task for this file
            result = task_generate_single_sitemap.apply_async(
                args=(sitemap_filename, record_ids)
            )
            task_results.append((sitemap_filename, result))
            logger.debug('Spawned task for %s with %d records', sitemap_filename, len(record_ids))
        
        # Wait for all parallel tasks to complete
        successful_files = 0
        failed_files = 0
        
        for sitemap_filename, result in task_results:
            try:
                # Wait for this specific task to complete (with timeout)
                success = result.get(timeout=300)  # 5 minute timeout per file
                if success:
                    successful_files += 1
                    logger.debug('Successfully completed %s', sitemap_filename)
                else:
                    failed_files += 1
                    logger.error('Task returned False for %s', sitemap_filename)
            except Exception as e:
                failed_files += 1
                logger.error('Task failed for %s: %s', sitemap_filename, str(e))
        
        logger.info('Sitemap generation completed: %d successful, %d failed', 
                   successful_files, failed_files)
        
        # Generate index files after all individual files are done
        logger.info('Generating sitemap index files')
        index_success = update_sitemap_index()  
        
        if not index_success:
            logger.error('Sitemap index generation failed')
        else:
            logger.info('Sitemap workflow completed successfully')
        
    except Exception as e:
        logger.error('Error in orchestrator task: %s', str(e))
        raise

if __name__ == '__main__':
    app.start()
