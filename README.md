[![Build Status](https://travis-ci.org/adsabs/ADSMasterPipeline.svg)](https://travis-ci.org/adsabs/ADSMasterPipeline)
[![Coverage Status](https://coveralls.io/repos/adsabs/ADSMasterPipeline/badge.svg)](https://coveralls.io/r/adsabs/ADSMasterPipeline)

# adsmp

## Short Summary

This pipeline is collecting results from the sub-ordinate pipelines (bibliographic, non-bibliographic, fulltext, orcid claims, metrics). It also updates production data stores: SOLR, Metrics DB and link resolver.


## Queues and objects

    - update-record: input from the 'other' pipelines is collected here
    - index-record: internal queue, it forwards data to solr/metrics
    - delete-record: removes from solr/metrics db
    - rebuild-index: used during weekly rebuild of solr
    - index-solr: internal queue of records to send to solr
    - index-metrics: internal queue of records to send to metrics on aws
    - index-data-links-resolver: internal queue of records to send to data links resolver

## Setup (recommended)

    `$ virtualenv python`
    `$ source python/bin/activate`
    `$ pip install -r requirements.txt`
    `$ pip install -r dev-requirements.txt`
    `$ vim local_config.py` # edit, edit
    `$ alembic upgrade head` # initialize database

## Important note

The pipeline will NOT send anything to SOLR/Metrics DB/data links resolver by default. You should trigger the update using a cronjob. There are two important modes:

    - normal mode (`python run.py -r`): will discover all updates that happened since the last invocation
        of the normal mode and will send them to the `index-records` queue; the parameter force will be set to False; hence only documents that have both metadata, orcid claims, and non-bib data will get sent to solr
        
    - pushy mode (`python run.py -r -f`) will discover all updates since the last invocation of the 'pushy' mode; and will send them to `index-records` queue and set force=True; this will force the worker to submit data to solr immediately (so effectively, this means any update to a record triggers push). Regardless, we always wait till we have bibliographic metadata.
        
 It is **imperative** that both modes of operation be used together in the 24h cycle. The normal mode will ignore some (many)
 records - so you must call the `pushy` mode at least at the end of the quiet period. The suggested schedule is the following (all times UTC):
 
  00:00-05:00 | normal mode, invoked every 5 mins
  05:00-06:00 | catch updates, invoke forced mode
  06:01-17:00 | --- do nothing, solr replication is not happening
  17:01-22:00 | normal mode, invoked every 5 mins
  22:01-23:59 | forced mode, catch updates 


## Testing

Always write unittests (even: always write unitests first!). Travis will run automatically. On your desktop run:

    `$ py.test`
    

## Maintainer(s)

Roman, Sergi, Steve
