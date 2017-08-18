
from __future__ import absolute_import, unicode_literals
import adsputils
from adsmp import app as app_module
from adsmp import solr_updater
from kombu import Queue
import math
from adsmsg import MetricsRecord, NonBibRecord

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
    
    if status == 'deleted':
        task_delete_documents(msg.bibcode)
    elif status == 'active':
        type = app.get_msg_type(msg)
        bibcodes = []
        
        # save into a database
        # passed msg may contain details on one bibcode or a list of bibcodes
        if type == 'nonbib_records':
            for m in msg.nonbib_records:
                m = NonBibRecord.deserializer(m.SerializeToString())
                t = app.get_msg_type(m)
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, t, m.toJSON())
                logger.debug('Saved record from list: %s', record)
        elif type == 'metrics_records':
            for m in msg.metrics_records:
                m = MetricsRecord.deserializer(m.SerializeToString())
                t = app.get_msg_type(m)
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, t, m.toJSON())
                logger.debug('Saved record from list: %s', record)
        else:
            # here when record has a single bibcode
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, type, msg.toJSON())
            logger.debug('Saved record: %s', record)
    
        # trigger futher processing
        task_index_records.delay(bibcodes)
    else:
        logger.error('Received a message with unclear status: %s', msg)


@app.task(queue='index-records')
def task_index_records(bibcodes, force=False, update_solr=True, update_metrics=True):
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

    """
    
    if not (update_solr or update_metrics):
        raise Exception('Hmmm, I dont think I let you do NOTHING, sorry!')

    logger.debug('Running after-update for: %s', bibcodes)
    batch = []
    batch_insert = []
    batch_update = []
    
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
            # It was never sent to Solr
            processed = adsputils.get_date(year_zero)
    
        is_complete = all([bib_data_updated, orcid_claims_updated, nonbib_data_updated])
    
        if is_complete or (force is True and bib_data_updated):
            
            if force is False and all([bib_data_updated and bib_data_updated < processed,
                   orcid_claims_updated and orcid_claims_updated < processed,
                   nonbib_data_updated and nonbib_data_updated < processed]):
                logger.debug('Nothing to do for %s, it was already indexed/processed', bibcode)
                continue
            
            if force:
                logger.warn('Forced indexing of: %s (metadata=%s, orcid=%s, nonbib=%s, fulltext=%s, metrics=%s)' % \
                            (bibcode, bib_data_updated, orcid_claims_updated, nonbib_data_updated, fulltext_updated, \
                             metrics_updated))

            # build the solr record
            if update_solr:
                batch.append(solr_updater.transform_json_record(r))
            # get data for metrics
            if update_metrics:
                m = r.get('metrics', None)
                if m:
                    m['bibcode'] = bibcode 
                    if r.get('processed'):
                        batch_update.append(m)
                    else:
                        batch_insert(m)
        else:
            # if forced and we have at least the bib data, index it
            if force is True:
                logger.warn('%s is missing bib data, even with force=True, this cannot proceed', bibcode)
            else:
                logger.debug('%s not ready for indexing yet', bibcode)
                
        
        failed_bibcodes = None
        if len(batch):
            failed_bibcodes = app.reindex(batch, app.conf.get('SOLR_URLS'))
        
        if failed_bibcodes and len(failed_bibcodes):
            failed_bibcodes = set(failed_bibcodes)
            # when solr_urls > 1, some of the servers may have successfully indexed
            # but here we are refusing to pass data to metrics db; this seems the 
            # right choice because there is only one metrics db (but if we had many,
            # then we could differentiate) 
                    
            batch_insert = filter(lambda x: x['bibcode'] not in failed_bibcodes, batch_insert)
            batch_update = filter(lambda x: x['bibcode'] not in failed_bibcodes, batch_update)
        
        if len(batch_insert) or len(batch_update):
            app.update_metrics_db(batch_insert, batch_update)
        



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



if __name__ == '__main__':
    app.start()
