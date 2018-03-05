
from __future__ import absolute_import, unicode_literals
import adsputils
from adsmp import app as app_module
from adsmp import solr_updater
from kombu import Queue
import math
import requests
import json
from adsmsg import MetricsRecord, NonBibRecord
from adsmsg.msg import Msg

# ============================= INITIALIZATION ==================================== #

app = app_module.ADSMasterPipelineCelery('master-pipeline')
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('update-record', app.exchange, routing_key='update-record'),
    Queue('index-records', app.exchange, routing_key='route-record'),
    Queue('delete-records', app.exchange, routing_key='delete-records'),
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
                logger.debug('Deleted %s, result: %s', type, app.update_storage(m.bibcode, 'nonbib_data', None))
        elif type == 'metrics_records':
            for m in msg.metrics_records:
                bibcodes.append(m.bibcode)
                logger.debug('Deleted %s, result: %s', type, app.update_storage(m.bibcode, 'metrics', None))
        else:
            bibcodes.append(msg.bibcode)
            logger.debug('Deleted %s, result: %s', type, app.update_storage(msg.bibcode, type, None))
        
    elif status == 'active':
        
        # save into a database
        # passed msg may contain details on one bibcode or a list of bibcodes
        if type == 'nonbib_records':
            for m in msg.nonbib_records:
                m = Msg(m, None, None) # m is a raw protobuf, TODO: return proper instance from .nonbib_records
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'nonbib_data', m.toJSON())
                logger.debug('Saved record from list: %s', record)
        elif type == 'metrics_records':
            for m in msg.metrics_records:
                m = Msg(m, None, None)
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'metrics', m.toJSON(including_default_value_fields=True))
                logger.debug('Saved record from list: %s', record)
        else:
            # here when record has a single bibcode
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, type, msg.toJSON())
            logger.debug('Saved record: %s', record)
    
    else:
        logger.error('Received a message with unclear status: %s', msg)



@app.task(queue='index-records')
def task_index_records(bibcodes, force=False, update_solr=True, update_metrics=True, update_links=True, commit=False):
    """
    This task is (normally) called by the cronjob task
    (that one, quite obviously, is in turn started by cron)
    
    Receives the bibcode of a document that was updated.
    (note: we could have sent the full record however we don't
    do it because the messages might be delayed and we can have
    multiple workers updating the same record; so we want to
    look into the database and get the most recent version)


    Receives bibcodes and checks the database if we have all the
    necessary pieces to push to solr. If not, then postpone and
    push later.

    We consider a record to be 'ready' if those pieces were updated
    (and were updated later than the last 'processed' timestamp):

        - bib_data
        - nonbib_data
        - orcid_claims

    'fulltext' is not considered essential; but updates to fulltext will
    trigger a solr_update (so it might happen that a document will get
    indexed twice; first with only metadata and later on incl fulltext)

    This function also updates metrics and links data.  A full metrics record
    comes from nonbib data pipeline in metrics record and it is written to 
    metrics sql database.  A full links data record comes from data pipeline 
    in nonbib_data and it is sent to links resolver's update endpoint.
    """
    
    if not (update_solr or update_metrics or update_links):
        raise Exception('Hmmm, I dont think I let you do NOTHING, sorry!')

    logger.debug('Running index-records for: %s', bibcodes)

    batch = []
    batch_insert = []
    batch_update = []
    recs_to_process = set()
    links_data = []
    links_bibcodes = []
    links_url = app.conf.get('LINKS_RESOLVER_UPDATE_URL')

    #check if we have complete record
    for bibcode in bibcodes:
        r = app.get_record(bibcode)
    
        if r is None:
            logger.error('The bibcode %s doesn\'t exist!', bibcode)
            continue
    
        bib_data_updated = r.get('bib_data_updated', None)
        orcid_claims_updated = r.get('orcid_claims_updated', None)
        nonbib_data_updated = r.get('nonbib_data_updated', None)
        fulltext_updated = r.get('fulltext_updated', None)
        metrics_updated = r.get('metrics_updated', None)
    
        year_zero = '1972'
        processed = r.get('processed', adsputils.get_date(year_zero))
        if processed is None:
            processed = adsputils.get_date(year_zero)

        logger.info('update_links = %s , url = %s, nonbib record = %s ', update_links, links_url, r.get('nonbib_data'))
        if update_links and 'nonbib_data' in r and links_url:
            # handle data links issue, force arg does not apply
            nb = json.loads(r.get('nonbib_data'))
            if 'data_links_rows' in nb:
                # send json version of DataLinksRow to update endpoint on links resolver
                # need to optimize and not send one record at a time
                tmp = {'bibcode': bibcode, 'data_links_rows': nb['data_links_rows']}
                links_data.append(tmp)
                links_bibcodes.append(bibcode)
            
        is_complete = all([bib_data_updated, orcid_claims_updated, nonbib_data_updated])
    
        if is_complete or (force is True and bib_data_updated):
            
            if force is False and all([
                   bib_data_updated and bib_data_updated < processed,
                   orcid_claims_updated and orcid_claims_updated < processed,
                   nonbib_data_updated and nonbib_data_updated < processed
                   ]):
                logger.debug('Nothing to do for %s, it was already indexed/processed', bibcode)
                continue
            
            if force:
                logger.debug('Forced indexing of: %s (metadata=%s, orcid=%s, nonbib=%s, fulltext=%s, metrics=%s)' % \
                            (bibcode, bib_data_updated, orcid_claims_updated, nonbib_data_updated, fulltext_updated, \
                             metrics_updated))

            # build the solr record
            if update_solr:
                d = solr_updater.transform_json_record(r)
                logger.debug('Built SOLR: %s', d)
                batch.append(d)
                recs_to_process.add(bibcode)
                
            # get data for metrics
            if update_metrics:
                m = r.get('metrics', None)
                if m:
                    m['bibcode'] = bibcode
                    logger.debug('Got metrics: %s', m) 
                    recs_to_process.add(bibcode)
                    if r.get('processed'):
                        batch_update.append(m)
                    else:
                        batch_insert.append(m)

        else:
            # if forced and we have at least the bib data, index it
            if force is True:
                logger.warn('%s is missing bib data, even with force=True, this cannot proceed', bibcode)
            else:
                logger.debug('%s not ready for indexing yet (metadata=%s, orcid=%s, nonbib=%s, fulltext=%s, metrics=%s)' % \
                            (bibcode, bib_data_updated, orcid_claims_updated, nonbib_data_updated, fulltext_updated, \
                             metrics_updated))
                
        
    failed_bibcodes = []
    if len(batch):
        failed_bibcodes = app.reindex(batch, app.conf.get('SOLR_URLS'), commit=commit)
    
    if failed_bibcodes and len(failed_bibcodes):
        logger.warn('Some bibcodes failed: %s', failed_bibcodes)
        failed_bibcodes = set(failed_bibcodes)
        
        # when solr_urls > 1, some of the servers may have successfully indexed
        # but here we are refusing to pass data to metrics db; this seems the 
        # right choice because there is only one metrics db (but if we had many,
        # then we could differentiate) 
                
        batch_insert = filter(lambda x: x['bibcode'] not in failed_bibcodes, batch_insert)
        batch_update = filter(lambda x: x['bibcode'] not in failed_bibcodes, batch_update)
        
        recs_to_process = recs_to_process - failed_bibcodes
        if len(failed_bibcodes):
            app.mark_processed(failed_bibcodes, type=None, status='solr-failed')
    
    
    
    if len(batch_insert) or len(batch_update):
        metrics_done, exception = app.update_metrics_db(batch_insert, batch_update)
        
        metrics_failed = recs_to_process - set(metrics_done)
        if len(metrics_failed):
            app.mark_processed(metrics_failed, type=None, status='metrics-failed')
    
        # mark all successful documents as done
        app.mark_processed(metrics_done, type=None, status='success')
        
        if exception:
            raise exception # will trigger retry
    else:
        app.mark_processed(recs_to_process, type=None, status='success')
    
    if len(links_data):
        r = requests.put(links_url, data = links_data)
        if r.status_code == 200:
            logger.info('send %s datalinks to %s including %s', len(links_data), links_url, links_data[0])
            app.mark_processed(links_bibcodes, type='links', status='success')
        else:
            logger.error('error sending links to %s, error = %s, sent data = %s ', links_url, r.text, tmp)
            app.mark_processed(links_bibcodes, type=None, status='links-failed')

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


if __name__ == '__main__':
    app.start()
