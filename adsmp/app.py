from __future__ import absolute_import, unicode_literals
from past.builtins import basestring
import os
from collections import defaultdict
from itertools import chain
from . import exceptions
from adsmp.models import ChangeLog, IdentifierMapping, MetricsBase, MetricsModel, Records
from adsmsg import OrcidClaims, DenormalizedRecord, FulltextUpdate, MetricsRecord, NonBibRecord, NonBibRecordList, MetricsRecordList, AugmentAffiliationResponseRecord, AugmentAffiliationRequestRecord
from adsmsg.msg import Msg
from adsputils import ADSCelery, create_engine, sessionmaker, scoped_session, contextmanager
from sqlalchemy.orm import load_only as _load_only
from sqlalchemy import Table, bindparam
import adsputils
import json
from adsmp import solr_updater
from adsmp import templates
from adsputils import serializer
from sqlalchemy import exc
from multiprocessing.util import register_after_fork
import zlib
import requests
from copy import deepcopy
import sys
from sqlalchemy.dialects.postgresql import insert
from SciXPipelineUtils import scix_id

class ADSMasterPipelineCelery(ADSCelery):

    def __init__(self, app_name, *args, **kwargs):
        ADSCelery.__init__(self, app_name, *args, **kwargs)
        # this is used for bulk/efficient updates to metrics db
        self._metrics_engine = self._metrics_session = None
        if self._config.get('METRICS_SQLALCHEMY_URL', None):
            self._metrics_engine = create_engine(self._config.get('METRICS_SQLALCHEMY_URL', 'sqlite:///'),
                                                 echo=self._config.get('SQLALCHEMY_ECHO', False))
            _msession_factory = sessionmaker()
            self._metrics_session = scoped_session(_msession_factory)
            self._metrics_session.configure(bind=self._metrics_engine)

            MetricsBase.metadata.bind = self._metrics_engine
            self._metrics_table = Table('metrics', MetricsBase.metadata, autoload=True, autoload_with=self._metrics_engine)
            register_after_fork(self._metrics_engine, self._metrics_engine.dispose)

            insert_columns = {
                'an_refereed_citations': bindparam('an_refereed_citations', required=False),
                'an_citations': bindparam('an_citations', required=False),
                'author_num': bindparam('author_num', required=False),
                'bibcode': bindparam('bibcode'),
                'citations': bindparam('citations', required=False),
                'citation_num': bindparam('citation_num', required=False),
                'downloads': bindparam('downloads', required=False),
                'reads': bindparam('reads', required=False),
                'refereed': bindparam('refereed', required=False, value=False),
                'refereed_citations': bindparam('refereed_citations', required=False),
                'refereed_citation_num': bindparam('refereed_citation_num', required=False),
                'reference_num': bindparam('reference_num', required=False),
                'rn_citations': bindparam('rn_citations', required=False),
                'rn_citation_data': bindparam('rn_citation_data', required=False),
            }
            self._metrics_table_upsert = insert(MetricsModel).values(insert_columns)
            # on insert conflict we specify which columns update
            update_columns = {
                'an_refereed_citations': getattr(self._metrics_table_upsert.excluded, 'an_refereed_citations'),
                'an_citations': getattr(self._metrics_table_upsert.excluded, 'an_citations'),
                'author_num': getattr(self._metrics_table_upsert.excluded, 'author_num'),
                'citations': getattr(self._metrics_table_upsert.excluded, 'citations'),
                'citation_num': getattr(self._metrics_table_upsert.excluded, 'citation_num'),
                'downloads': getattr(self._metrics_table_upsert.excluded, 'downloads'),
                'reads': getattr(self._metrics_table_upsert.excluded, 'reads'),
                'refereed': getattr(self._metrics_table_upsert.excluded, 'refereed'),
                'refereed_citations': getattr(self._metrics_table_upsert.excluded, 'refereed_citations'),
                'refereed_citation_num': getattr(self._metrics_table_upsert.excluded, 'refereed_citation_num'),
                'reference_num': getattr(self._metrics_table_upsert.excluded, 'reference_num'),
                'rn_citations': getattr(self._metrics_table_upsert.excluded, 'rn_citations'),
                'rn_citation_data': getattr(self._metrics_table_upsert.excluded, 'rn_citation_data')}
            self._metrics_table_upsert = self._metrics_table_upsert.on_conflict_do_update(index_elements=['bibcode'], set_=update_columns)

    @property
    def sitemap_dir(self):
        """Get the sitemap directory from configuration"""
        return self.conf.get('SITEMAP_DIR', '/app/logs/sitemap/')

    def update_storage(self, bibcode, type, payload):
        """Update the document in the database, every time
        empty the solr/metrics processed timestamps.

        returns the sql record as a json object or an error string """
        if not isinstance(payload, basestring):
            payload = json.dumps(payload)

        with self.session_scope() as session:
            record = session.query(Records).filter_by(bibcode=bibcode).first()
            if record is None:
                record = Records(bibcode=bibcode)
                session.add(record)
            now = adsputils.get_date()
            oldval = None
            if type == 'metadata' or type == 'bib_data':
                oldval = record.bib_data
                record.bib_data = payload
                record.bib_data_updated = now
                # tasks.populate_sitemap_table(record, 'add') #TODO: Review this 
            elif type == 'nonbib_data':
                oldval = record.nonbib_data
                record.nonbib_data = payload
                record.nonbib_data_updated = now
            elif type == 'orcid_claims':
                oldval = record.orcid_claims
                record.orcid_claims = payload
                record.orcid_claims_updated = now
            elif type == 'fulltext':
                oldval = 'not-stored'
                record.fulltext = payload
                record.fulltext_updated = now
            elif type == 'metrics':
                oldval = 'not-stored'
                record.metrics = payload
                record.metrics_updated = now
            elif type == 'augment':
                # payload contains new value for affilation fields
                # r.augments holds a dict, save it in database
                oldval = 'not-stored'
                record.augments = payload
                record.augments_updated = now
            else:
                raise Exception('Unknown type: %s' % type)
            session.add(ChangeLog(key=bibcode, type=type, oldvalue=oldval))

            record.updated = now
            out = record.toJSON()
            try:
                session.flush()
                if not r.scix_id and r.bib_data:
                    r.scix_id = "scix:" + str(self.generate_scix_id(r.bib_data))
                    out = r.toJSON()
                session.commit()
                return out
            except exc.IntegrityError:
                self.logger.exception('error in app.update_storage while updating database for bibcode {}, type {}'.format(bibcode, type))
                session.rollback()
                raise

    def generate_scix_id(self, bib_data):
        return scix_id.generate_scix_id(bib_data) 

    def delete_by_bibcode(self, bibcode):
        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is not None:
                session.add(ChangeLog(key='bibcode:%s' % bibcode, type='deleted', oldvalue=serializer.dumps(r.toJSON())))
                session.delete(r)
                session.commit()
                return True

    def rename_bibcode(self, old_bibcode, new_bibcode):
        assert old_bibcode and new_bibcode
        assert old_bibcode != new_bibcode

        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=old_bibcode).first()
            if r is not None:

                t = session.query(IdentifierMapping).filter_by(key=old_bibcode).first()
                if t is None:
                    session.add(IdentifierMapping(key=old_bibcode, target=new_bibcode))
                else:
                    while t is not None:
                        target = t.target
                        t.target = new_bibcode
                        t = session.query(IdentifierMapping).filter_by(key=target).first()

                session.add(ChangeLog(key='bibcode:%s' % new_bibcode, type='renamed', oldvalue=r.bibcode, permanent=True))
                r.bibcode = new_bibcode
                session.commit()
            else:
                self.logger.error('Rename operation, bibcode doesnt exist: old=%s, new=%s', old_bibcode, new_bibcode)

    def get_record(self, bibcode, load_only=None):
        if isinstance(bibcode, list):
            out = []
            with self.session_scope() as session:
                q = session.query(Records).filter(Records.bibcode.in_(bibcode))
                if load_only:
                    q = q.options(_load_only(*load_only))
                for r in q.all():
                    out.append(r.toJSON(load_only=load_only))
            return out
        else:
            with self.session_scope() as session:
                q = session.query(Records).filter_by(bibcode=bibcode)
                if load_only:
                    q = q.options(_load_only(*load_only))
                r = q.first()
                if r is None:
                    return None
                return r.toJSON(load_only=load_only)

    def get_changelog(self, bibcode):
        out = []
        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is not None:
                out.append(r.toJSON())
            to_collect = [bibcode]
            while len(to_collect):
                for x in session.query(IdentifierMapping).filter_by(key=to_collect.pop()).yield_per(100):
                    to_collect.append(x.target)
                    out.append(x.toJSON())
        return out

    def get_msg_type(self, msg):
        """Identifies the type of this supplied message.

        :param: Protobuf instance
        :return: str
        """

        if isinstance(msg, OrcidClaims):
            return 'orcid_claims'
        elif isinstance(msg, DenormalizedRecord):
            return 'metadata'
        elif isinstance(msg, FulltextUpdate):
            return 'fulltext'
        elif isinstance(msg, NonBibRecord):
            return 'nonbib_data'
        elif isinstance(msg, NonBibRecordList):
            return 'nonbib_records'
        elif isinstance(msg, MetricsRecord):
            return 'metrics'
        elif isinstance(msg, MetricsRecordList):
            return 'metrics_records'
        elif isinstance(msg, AugmentAffiliationResponseRecord):
            return 'augment'

        else:
            raise exceptions.IgnorableException('Unkwnown type {0} submitted for update'.format(repr(msg)))

    def get_msg_status(self, msg):
        """Identifies the type of this supplied message.

        :param: Protobuf instance
        :return: str
        """

        if isinstance(msg, Msg):
            status = msg.status
            if status == 1:
                return 'deleted'
            else:
                return 'active'
        else:
            return 'unknown'

    def index_solr(self, solr_docs, solr_docs_checksum, solr_urls, commit=False, update_processed=True):
        """Sends documents to solr. It will update
        the solr_processed timestamp for every document which succeeded.

        :param: solr_docs - list of json objects (solr documents)
        :param: solr_urls - list of strings, solr servers.
        """
        self.logger.debug('Updating solr: num_docs=%s solr_urls=%s', len(solr_docs), solr_urls)
        # batch send solr update
        out = solr_updater.update_solr(solr_docs, solr_urls, ignore_errors=True)
        errs = [x for x in out if x != 200]

        if len(errs) == 0:
            if update_processed:
                self.mark_processed([x['bibcode'] for x in solr_docs], 'solr', checksums=solr_docs_checksum, status='success')
        else:
            self.logger.error('%s docs failed indexing', len(errs))
            failed_bibcodes = []
            # recover from errors by sending docs one by one
            for doc, checksum in zip(solr_docs, solr_docs_checksum):
                try:
                    self.logger.error('trying individual update_solr %s', doc)
                    solr_updater.update_solr([doc], solr_urls, ignore_errors=False, commit=commit)
                    if update_processed:
                        self.mark_processed((doc['bibcode'],), 'solr', checksums=(checksum,), status='success')
                    self.logger.debug('%s success', doc['bibcode'])
                except Exception as e:
                    # if individual insert fails,
                    # and if 'body' is in excpetion we assume Solr failed on body field
                    # then we try once more without fulltext
                    # this bibcode needs to investigated as to why fulltext/body is failing
                    failed_bibcode = doc['bibcode']           
                    if 'body' in str(e) or 'not all arguments converted during string formatting' in str(e):
                        tmp_doc = dict(doc)
                        tmp_doc.pop('body', None)
                        try:
                            solr_updater.update_solr([tmp_doc], solr_urls, ignore_errors=False, commit=commit)
                            if update_processed:
                                self.mark_processed((doc['bibcode'],), 'solr', checksums=(checksum,), status='success')
                            self.logger.debug('%s success without body', doc['bibcode'])
                        except Exception as e:
                            self.logger.exception('Failed posting bibcode %s to Solr even without fulltext (urls: %s)', failed_bibcode, solr_urls)
                            failed_bibcodes.append(failed_bibcode)
                    else:
                        # here if body not in error message do not retry, just note as a fail
                        self.logger.error('Failed posting individual bibcode %s to Solr\nurls: %s, offending payload %s, error is %s', failed_bibcode, solr_urls, doc, e)
                        failed_bibcodes.append(failed_bibcode)
            # finally update postgres record
            if failed_bibcodes and update_processed:
                self.mark_processed(failed_bibcodes, 'solr', checksums=None, status='solr-failed')

    def mark_processed(self, bibcodes, type, checksums=None, status=None):
        """
        Updates the timesstamp for all documents that match the bibcodes.
        Optionally also sets the status (which says what actually happened
        with the document) and the checksum.
        Parameters bibcodes and checksums are expected to be lists of equal
        size with correspondence one to one, except if checksum is None (in
        this case, checksums are not updated).
        """

        # avoid updating whole database (when the set is empty)
        if len(bibcodes) < 1:
            return

        now = adsputils.get_date()
        updt = {'processed': now}
        if status:
            updt['status'] = status
        self.logger.debug('Marking docs as processed: now=%s, num bibcodes=%s', now, len(bibcodes))
        if type == 'solr':
            timestamp_column = 'solr_processed'
            checksum_column = 'solr_checksum'
        elif type == 'metrics':
            timestamp_column = 'metrics_processed'
            checksum_column = 'metrics_checksum'
        elif type == 'links':
            timestamp_column = 'datalinks_processed'
            checksum_column = 'datalinks_checksum'
        else:
            raise ValueError('invalid type value of %s passed, must be solr, metrics or links' % type)
        updt[timestamp_column] = now
        checksums = checksums or [None] * len(bibcodes)
        for bibcode, checksum in zip(bibcodes, checksums):
            updt[checksum_column] = checksum
            with self.session_scope() as session:
                session.query(Records).filter_by(bibcode=bibcode).update(updt, synchronize_session=False)
        session.commit()

    def get_metrics(self, bibcode):
        """Helper method to retrieve data from the metrics db

        @param bibcode: string
        @return: JSON structure if record was found, {} else
        """

        if not self._metrics_session:
            raise Exception('METRCIS_SQLALCHEMU_URL not set!')

        with self.metrics_session_scope() as session:
            x = session.query(MetricsModel).filter(MetricsModel.bibcode == bibcode).first()
            if x:
                return x.toJSON()
            else:
                return {}

    @contextmanager
    def metrics_session_scope(self):
        """Provides a transactional session - ie. the session for the
        current thread/work of unit.

        Use as:

            with session_scope() as session:
                o = ModelObject(...)
                session.add(o)
        """

        if self._metrics_session is None:
            raise Exception('DB not initialized properly, check: METRICS_SQLALCHEMY_URL')

        # create local session (optional step)
        s = self._metrics_session()

        try:
            yield s
            s.commit()
        except:
            s.rollback()
            raise
        finally:
            s.close()

    def index_metrics(self, batch, batch_checksum, update_processed=True):
        """Writes data into the metrics DB.
        :param: batch - list of json objects to upsert into the metrics db
        :return: tupple (list-of-processed-bibcodes, exception)
        It tries hard to avoid raising exceptions; it will return the list
        of bibcodes that were successfully updated. It will also update
        metrics_processed timestamp in the records table for every bibcode that succeeded.
        """
        if not self._metrics_session:
            raise Exception('You cant do this! Missing METRICS_SQLALACHEMY_URL?')

        self.logger.debug('Updating metrics db: len(batch)=%s', len(batch))
        with self.metrics_session_scope() as session:
            if len(batch):
                trans = session.begin_nested()
                try:
                    # bulk upsert
                    trans.session.execute(self._metrics_table_upsert, batch)
                    trans.commit()
                    if update_processed:
                        self.mark_processed([x['bibcode'] for x in batch], 'metrics', checksums=batch_checksum, status='success')
                except exc.SQLAlchemyError as e:
                    # recover from errors by upserting data one by one
                    trans.rollback()
                    self.logger.error('Metrics insert batch failed, will upsert one by one %s recs', len(batch))
                    failed_bibcodes = []
                    for x, checksum in zip(batch, batch_checksum):
                        try:
                            trans.session.execute(self._metrics_table_upsert, [x])
                            trans.commit()
                            if update_processed:
                                self.mark_processed((x['bibcode'],), 'metrics', checksums=(checksum,), status='success')
                        except Exception as e:
                            failed_bibcode = x['bibcode']
                            self.logger.exception('Failed posting individual bibcode %s to metrics', failed_bibcode)
                            failed_bibcodes.append(failed_bibcode)
                    if failed_bibcodes and update_processed:
                        self.mark_processed(failed_bibcodes, 'metrics', checksums=None, status='metrics-failed')
                except Exception as e:
                    trans.rollback()
                    self.logger.error('DB failure: %s', e)
                    if update_processed:
                        self.mark_processed([x['bibcode'] for x in batch], 'metrics', checksums=None, status='metrics-failed')

    def index_datalinks(self, links_data, links_data_checksum, update_processed=True):
        # todo is failed right?
        links_url = self.conf.get('LINKS_RESOLVER_UPDATE_URL')
        api_token = self.conf.get('ADS_API_TOKEN', '')
        if len(links_data):
            # bulk put request
            bibcodes = [x['bibcode'] for x in links_data]
            r = requests.put(links_url, data=json.dumps(links_data), headers={'Authorization': 'Bearer {}'.format(api_token)})
            if r.status_code == 200:
                self.logger.info('sent %s datalinks to %s including %s', len(links_data), links_url, links_data[0])
                if update_processed:
                    self.mark_processed(bibcodes, 'links', checksums=links_data_checksum, status='success')
            else:
                # recover from errors by issuing put requests one by one
                self.logger.error('error sending links to %s, error = %s', links_url, r.text)
                failed_bibcodes = []
                for data, checksum in zip(links_data, links_data_checksum):
                    r = requests.put(links_url, data=json.dumps([data]), headers={'Authorization': 'Bearer {}'.format(api_token)})
                    if r.status_code == 200:
                        self.logger.info('sent 1 datalinks to %s for bibcode %s', links_url, data.get('bibcode'))
                        if update_processed:
                            self.mark_processed(bibcodes, 'links', checksums=links_data_checksum, status='success')
                    else:
                        self.logger.error('error sending individual links to %s for bibcode %s, error = %s', links_url, data.get('bibcode'), r.text)
                        failed_bibcodes.append(data['bibcode'])
                if failed_bibcodes and update_processed:
                    self.mark_processed(failed_bibcodes, 'links', status='links-failed')

    def metrics_delete_by_bibcode(self, bibcode):
        with self.metrics_session_scope() as session:
            r = session.query(MetricsModel).filter_by(bibcode=bibcode).first()
            if r is not None:
                session.delete(r)
                session.commit()
                return True

    def checksum(self, data, ignore_keys=('mtime', 'ctime', 'update_timestamp')):
        """
        Compute checksum of the passed in data. Preferred situation is when you
        give us a dictionary. We can clean it up, remove the 'ignore_keys' and
        sort the keys. Then compute CRC on the string version. You can also pass
        a string, in which case we simple return the checksum.
        
        @param data: string or dict
        @param ignore_keys: list of patterns, if they are found (anywhere) in
            the key name, we'll ignore this key-value pair
        @return: checksum
        """
        assert isinstance(ignore_keys, tuple)

        if isinstance(data, basestring):
            if sys.version_info > (3,):
                data_str = data.encode('utf-8')
            else:
                data_str = unicode(data)
            return hex(zlib.crc32(data_str) & 0xffffffff)
        else:
            data = deepcopy(data)
            # remove all the modification timestamps
            for k, v in list(data.items()):
                for x in ignore_keys:
                    if x in k:
                        del data[k]
                        break
            if sys.version_info > (3,):
                data_str = json.dumps(data, sort_keys=True).encode('utf-8')
            else:
                data_str = json.dumps(data, sort_keys=True)
            return hex(zlib.crc32(data_str) & 0xffffffff)

    def request_aff_augment(self, bibcode, data=None):
        """send aff data for bibcode to augment affiliation pipeline

        set data parameter to provide test data"""
        if data is None:
            rec = self.get_record(bibcode)
            if rec is None:
                self.logger.warning('request_aff_augment called but no data at all for bibcode {}'.format(bibcode))
                return
            bib_data = rec.get('bib_data', None)
            if bib_data is None:
                self.logger.warning('request_aff_augment called but no bib data for bibcode {}'.format(bibcode))
                return
            aff = bib_data.get('aff', None)
            author = bib_data.get('author', '')
            data = {
                'bibcode': bibcode,
                "aff": aff,
                "author": author,
            }
        if data and data['aff']:
            message = AugmentAffiliationRequestRecord(**data)
            self.forward_message(message)
            self.logger.debug('sent augment affiliation request for bibcode {}'.format(bibcode))
        else:
            self.logger.debug('request_aff_augment called but bibcode {} has no aff data'.format(bibcode))

    def generate_links_for_resolver(self, record):
        """use nonbib or bib elements of database record and return links for resolver and checksum"""
        # nonbib data has something like
        #  "data_links_rows": [{"url": ["http://arxiv.org/abs/1902.09522"]
        # bib data has json string for:
        # "links_data": [{"access": "open", "instances": "", "title": "", "type": "preprint",

        #                 "url": "http://arxiv.org/abs/1902.09522"}]

        resolver_record = None   # default value to return
        bibcode = record.get('bibcode')
        nonbib = record.get('nonbib_data', {})
        if type(nonbib) is not dict:
            nonbib = {}    # in case database has None or something odd
        nonbib_links = nonbib.get('data_links_rows', None)
        if nonbib_links:
            # when avilable, prefer link info from nonbib
            resolver_record = {'bibcode': bibcode,
                               'data_links_rows': nonbib_links}
        else:
            # as a fallback, use link from bib/direct ingest
            bib = record.get('bib_data', {})
            if type(bib) is not dict:
                bib = {}
            bib_links_record = bib.get('links_data', None)
            if bib_links_record:
                try:
                    bib_links_data = json.loads(bib_links_record[0])
                    url = bib_links_data.get('url', None)
                    if url:
                        # need to change what direct sends
                        url_pdf = url.replace('/abs/', '/pdf/')
                        resolver_record = {'bibcode': bibcode,
                                           'data_links_rows': [{'url': [url],
                                                                'title': [''], 'item_count': 0,
                                                                'link_type': 'ESOURCE',
                                                                'link_sub_type': 'EPRINT_HTML'},
                                                               {'url': [url_pdf],
                                                                'title': [''], 'item_count': 0,
                                                                'link_type': 'ESOURCE',
                                                                'link_sub_type': 'EPRINT_PDF'}]}
                except (KeyError, ValueError):
                    # here if record holds unexpected value
                    self.logger.error('invalid value in bib data, bibcode = {}, type = {}, value = {}'.format(bibcode, type(bib_links_record), bib_links_record))
        return resolver_record

    
    def populate_sitemap_table(self, sitemap_record, sitemap_info=None): 
        """Populate the sitemap with the given record id and action. 
        Only used for 'add' and 'force-update' actions. 

        :param sitemap_record: dictionary with record data
        :param sitemap_info: existing SitemapInfo object if updating
        """

        with self.session_scope() as session:
            # If the sitemap_record is new, add it to the database 
            if sitemap_info is None:
                sitemap_info = SitemapInfo(
                    record_id=sitemap_record.get('record_id'),
                    bibcode=sitemap_record.get('bibcode'),
                    scix_id=sitemap_record.get('scix_id'),
                    bib_data_updated=sitemap_record.get('bib_data_updated'),
                    sitemap_filename=sitemap_record.get('sitemap_filename'),
                    filename_lastmoddate=sitemap_record.get('filename_lastmoddate'),
                    update_flag=sitemap_record.get('update_flag', False)
                )
                session.add(sitemap_info)
                
                # Handle sitemap filename assignment for new records
                # get list of all sitemap filenames from the SiteMapInfo table
                sitemap_files = session.query(SitemapInfo.sitemap_filename).all()
                
                # flatten the list of tuples into a list of strings
                sitemap_files = list(chain.from_iterable(sitemap_files))
                
                # remove None values from the list
                sitemap_files = list(filter(lambda sitemap_file: sitemap_file is not None, sitemap_files))

                if sitemap_files:
                    # split all the filenames to get the index of the latest sitemap file
                    sitemap_file_indices = [int(sitemap_file.split('_bib_')[-1].split('.')[0]) for sitemap_file in sitemap_files]
                    latest_index = max(sitemap_file_indices)
                    sitemap_file_latest = 'sitemap_bib_{}.xml'.format(latest_index)

                    # Check the number of records assigned to sitemap_file_latest
                    sitemap_file_latest_count = session.query(SitemapInfo).filter_by(sitemap_filename=sitemap_file_latest).count()
                
                    # TODO: Why is the max number of records per sitemap file so low? 
                    if sitemap_file_latest_count >= self.conf.get('MAX_RECORDS_PER_SITEMAP', 3): 
                        latest_index += 1
                        sitemap_filename = 'sitemap_bib_{}.xml'.format(latest_index)
                    else:
                        sitemap_filename = sitemap_file_latest
                    
                    sitemap_info.sitemap_filename = sitemap_filename
                else:
                    sitemap_info.sitemap_filename = 'sitemap_bib_1.xml'
                    
                
            else:
                # For existing records, update only the fields that can change
                # The sitemap_record already contains the preserved values from sitemap_info
                update_data = {}
                
                # Always update these fields if they exist in sitemap_record
                for field in ['bib_data_updated', 'sitemap_filename', 'filename_lastmoddate']:
                    if field in sitemap_record:
                        update_data[field] = sitemap_record[field]
                
                # Always update update_flag (this is the key field for force-update)
                update_data['update_flag'] = sitemap_record.get('update_flag', False)
                
                session.query(SitemapInfo).filter_by(record_id=sitemap_record['record_id']).update(update_data)
               
            try:
                session.commit() 
            except exc.IntegrityError as e:
                self.logger.error('Could not update sitemap table for record_id: %s', sitemap_record['record_id'])
                session.rollback()
                raise
            except Exception as e:
                session.rollback()
                raise

    # def update_sitemap_files(self):
    #     """Update sitemap files for all configured sites"""
    #     sites_config = self.conf.get('SITES', {})
    #     if not sites_config:
    #         self.logger.error('No SITES configuration found')
    #         return
            
    #     sites_to_process = list(sites_config.keys())
    #     self.logger.info('Updating sitemap files for sites: %s', ', '.join(sites_to_process))
        
    #     # Update files for each site
    #     for site_key in sites_to_process:
    #         self._update_sitemap_files_for_site(site_key)
        
    #     # Update database records once after all sites are processed
    #     self._update_sitemap_database_records()

    # def _update_sitemap_files_for_site(self, site):
    #     """Update sitemap files for a specific site"""
    #     sites_config = self.conf.get('SITES', {})
    #     site_config = sites_config.get(site, {})
    #     abs_url_pattern = site_config.get('abs_url_pattern', 'https://ui.adsabs.harvard.edu/abs/{bibcode}/abstract')
    #     sitemap_dir = self.sitemap_dir
        
    #     self.logger.info('Processing sitemap files for site: %s', site_config.get('name', site))
        
    #     try:
    #         with self.session_scope() as session:
    #             # Get all filenames for files that have at least one update_flag=True
    #             files_needing_update_subquery = (
    #                 session.query(SitemapInfo.sitemap_filename.distinct())
    #                 .filter(SitemapInfo.update_flag == True)
    #             )
                
    #             # Get all sitemap records in files that have at least one update_flag=True record
    #             all_records = (
    #                 session.query(SitemapInfo)
    #                 .filter(SitemapInfo.sitemap_filename.in_(files_needing_update_subquery))
    #                 .all()
    #             )
                
    #             if not all_records:
    #                 self.logger.info('No sitemap files need updating for site %s', site)
    #                 return
                
    #             self.logger.info('Found %d records to process in sitemap files for site %s', len(all_records), site)
                
    #             # Group records by filename
    #             files_dict = defaultdict(list)
    #             for record in all_records:
    #                 if record.sitemap_filename:
    #                     files_dict[record.sitemap_filename].append(record)
                
    #             # Process each file and create site-specific version
    #             successful_files = 0
    #             for sitemap_filename, file_records in files_dict.items():
    #                 try:
    #                     # Create site-specific directory and use original filename
    #                     site_output_dir = os.path.join(sitemap_dir, site)
    #                     os.makedirs(site_output_dir, exist_ok=True)
    #                     site_filename = os.path.join(site_output_dir, sitemap_filename)
                        
    #                     url_entries = []
    #                     for info in file_records:
    #                         lastmod_date = info.bib_data_updated.date() if info.bib_data_updated else adsputils.get_date().date()
    #                         url_entry = templates.format_url_entry(info.bibcode, lastmod_date, abs_url_pattern)
    #                         url_entries.append(url_entry)
                        
    #                     # Write the site-specific XML file
    #                     sitemap_content = templates.render_sitemap_file(''.join(url_entries))
    #                     with open(site_filename, 'w', encoding='utf-8') as file:
    #                         file.write(sitemap_content)
                        
    #                     self.logger.debug('Successfully wrote %d records to %s for site %s', len(url_entries), site_filename, site)
    #                     successful_files += 1
                        
    #                 except Exception as e:
    #                     self.logger.error('Failed to process sitemap file %s for site %s: %s', sitemap_filename, site, str(e))
    #                     continue
                
    #             self.logger.info('Sitemap file update completed for site %s: %d files processed successfully', site, successful_files)
                
    #     except Exception as e:
    #         self.logger.error('Error in update_sitemap_files for site %s: %s', site, str(e))
    #         raise
    
    # def _update_sitemap_database_records(self):
    #     """Update database records after sitemap files have been generated for all sites"""
    #     try:
    #         with self.session_scope() as session:
    #             # Find all records that had update_flag=True and update them
    #             updated_records = session.query(SitemapInfo).filter(
    #                 SitemapInfo.update_flag == True
    #             ).all()
                
    #             if not updated_records:
    #                 return
                    
    #             current_time = adsputils.get_date()
    #             for record in updated_records:
    #                 record.filename_lastmoddate = current_time
    #                 record.update_flag = False
                
    #             session.commit()
    #             self.logger.info('Updated %d database records after sitemap generation', len(updated_records))
                
    #     except Exception as e:
    #         self.logger.error('Error updating sitemap database records: %s', str(e))
    #         raise
        
    # def get_sitemap_info(self, bibcode):
    #     """Get sitemap info for given bibcode.

    #     :param bibcode: bibcode of record to be updated
    #     """
    #     with self.session_scope() as session:
    #         info = session.query(SitemapInfo).filter_by(bibcode=bibcode).first()
    #         if info is None:
    #             return None
    #         return info.toJSON()
        
    # def delete_contents(self, table):
    #     """Delete all contents of the table

    #     :param table: string, name of the table
    #     """
    #     with self.session_scope() as session:
    #         session.query(table).delete()
    #         session.commit()
    
    # def backup_sitemap_files(self, directory):
    #     """Backup the sitemap files to the given directory

    #     :param dir: string, directory to backup the sitemap files
    #     """

    #     # move dir to /app/tmp 
    #     date = adsputils.get_date()
    #     tmpdir = '/app/logs/tmp/sitemap_{}_{}_{}-{}/'.format(date.year, date.month, date.day, date.time())
    #     os.system('mkdir -p {}'.format(tmpdir))
    #     os.system('mv {}/* {}/'.format(directory, tmpdir))
    #     return
    
    # def create_robot_txt_file(self, site=None):
    #     """Create robots.txt files for specified sites

    #     :param site: Site identifier(s) - can be a string ('ads'), list (['ads', 'scix']), or None (defaults to all sites)
    #     """
    #     sites_config = self.conf.get('SITES', {})
    #     if not sites_config:
    #         self.logger.error('No SITES configuration found')
    #         return
            
    #     # Default to all sites if none specified
    #     if site is None:
    #         sites_to_process = list(sites_config.keys())
    #     elif isinstance(site, str):
    #         sites_to_process = [site] if site in sites_config else []
    #         if not sites_to_process:
    #             self.logger.error('Site %s not found in config. Available: %s', site, list(sites_config.keys()))
    #             return
    #     elif isinstance(site, list):
    #         sites_to_process = [s for s in site if s in sites_config]
    #         invalid_sites = [s for s in site if s not in sites_config]
    #         if invalid_sites:
    #             self.logger.warning('Invalid sites ignored: %s. Available: %s', invalid_sites, list(sites_config.keys()))
    #     else:
    #         self.logger.error('Site parameter must be string, list, or None')
    #         return
            
    #     sitemap_dir = self.sitemap_dir
        
    #     for site_key in sites_to_process:
    #         site_config = sites_config[site_key]
    #         sitemap_url = site_config['sitemap_url']
    #         robots_content = templates.render_robots_txt(sitemap_url)
            
    #         # Create site-specific directory
    #         site_output_dir = os.path.join(sitemap_dir, site_key)
    #         os.makedirs(site_output_dir, exist_ok=True)
    #         robots_filepath = os.path.join(site_output_dir, 'robots.txt')
            
    #         with open(robots_filepath, 'w') as file:
    #             file.write(robots_content)
                
    #         self.logger.info('Created robots.txt for site %s at %s', site_config['name'], robots_filepath)
    #     return
    
    # def create_sitemap_index(self, site=None):
    #     """Create sitemap index files for specified sites

    #     :param site: Site identifier(s) - can be a string ('ads'), list (['ads', 'scix']), or None (defaults to all sites in config)
    #     """
    #     sites_config = self.conf.get('SITES', {})
    #     if not sites_config:
    #         self.logger.error('No SITES configuration found')
    #         return
            
    #     # Default to all sites if none specified
    #     if site is None:
    #         sites_to_process = list(sites_config.keys())
    #     elif isinstance(site, str):
    #         sites_to_process = [site] if site in sites_config else []
    #         if not sites_to_process:
    #             self.logger.error('Site %s not found in config. Available: %s', site, list(sites_config.keys()))
    #             return
    #     elif isinstance(site, list):
    #         sites_to_process = [s for s in site if s in sites_config]
    #         invalid_sites = [s for s in site if s not in sites_config]
    #         if invalid_sites:
    #             self.logger.warning('Invalid sites ignored: %s. Available: %s', invalid_sites, list(sites_config.keys()))
    #     else:
    #         self.logger.error('Site parameter must be string, list, or None')
    #         return
            
    #     sitemap_dir = self.sitemap_dir
        
    #     for site_key in sites_to_process:
    #         site_config = sites_config[site_key]
    #         sitemap_url = site_config['sitemap_url']
            
    #         sitemap_entries = []
    #         with self.session_scope() as session:
    #             # Get unique sitemap filenames 
    #             sitemapinfo = session.query(SitemapInfo.sitemap_filename,func.count('*').label('count')).group_by(
    #                                         SitemapInfo.sitemap_filename).order_by(
    #                                             func.min(SitemapInfo.id).asc()).all()

    #             #unique sitemap filenames  
    #             sitemap_filenames = list(map(lambda item: item[0], sitemapinfo))

    #             for filename in sitemap_filenames:
    #                 # Get the last modified date of the sitemap file
    #                 lastmoddate = session.query(SitemapInfo.filename_lastmoddate).filter_by(sitemap_filename=filename).first()
                    
    #                 # Format the sitemap entry
    #                 try:
    #                     entry = templates.format_sitemap_entry(sitemap_url, filename, str(lastmoddate[0].date()))
    #                     sitemap_entries.append(entry)
    #                 except:
    #                     self.logger.error('Error processing %s for sitemap_index.xml for site %s', filename, site_key)
    #                     continue
            
    #         sitemap_content = templates.render_sitemap_index(''.join(sitemap_entries))
            
    #         # Create site-specific directory and use standard filename
    #         site_output_dir = os.path.join(sitemap_dir, site_key)
    #         os.makedirs(site_output_dir, exist_ok=True)
    #         index_filepath = os.path.join(site_output_dir, 'sitemap_index.xml')
            
    #         with open(index_filepath, 'w') as file:
    #             file.write(sitemap_content)
                
    #         self.logger.info('Created sitemap_index.xml for site %s at %s with %d sitemap files', 
    #                        site_config['name'], index_filepath, len(sitemap_entries))
    #     return
    
    # def generate_all_sitemap_files(self):
    #     """Generate all sitemap files, indexes, and robots.txt for all sites"""
    #     self.logger.info('Starting complete sitemap generation for all sites')
        
    #     # 1. Update sitemap files for all sites
    #     self.update_sitemap_files()
        
    #     # 2. Create sitemap indexes for all sites  
    #     self.create_sitemap_index()
        
    #     # 3. Create robots.txt files for all sites
    #     self.create_robot_txt_file()
        
    #     self.logger.info('Completed sitemap generation for all sites')
