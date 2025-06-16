from __future__ import absolute_import, unicode_literals
from past.builtins import basestring
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
from adsputils import serializer
from sqlalchemy import exc
from multiprocessing.util import register_after_fork
import zlib
import requests
from copy import deepcopy
import sys
from sqlalchemy.dialects.postgresql import insert
import re
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

    def update_storage(self, bibcode, type, payload):
        """Update the document in the database, every time
        empty the solr/metrics processed timestamps.

        returns the sql record as a json object or an error string """
        if not isinstance(payload, basestring):
            payload = json.dumps(payload)

        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is None:
                r = Records(bibcode=bibcode)
                session.add(r)
            now = adsputils.get_date()
            oldval = None
            if type == 'metadata' or type == 'bib_data':
                oldval = r.bib_data
                r.bib_data = payload
                r.bib_data_updated = now
            elif type == 'nonbib_data':
                oldval = r.nonbib_data
                r.nonbib_data = payload
                r.nonbib_data_updated = now
            elif type == 'orcid_claims':
                oldval = r.orcid_claims
                r.orcid_claims = payload
                r.orcid_claims_updated = now
            elif type == 'fulltext':
                oldval = 'not-stored'
                r.fulltext = payload
                r.fulltext_updated = now
            elif type == 'metrics':
                oldval = 'not-stored'
                r.metrics = payload
                r.metrics_updated = now
            elif type == 'augment':
                # payload contains new value for affilation fields
                # r.augments holds a dict, save it in database
                oldval = 'not-stored'
                r.augments = payload
                r.augments_updated = now
            else:
                raise Exception('Unknown type: %s' % type)
            session.add(ChangeLog(key=bibcode, type=type, oldvalue=oldval))
            r.updated = now
            out = r.toJSON()
            try:
                session.flush()
                if not r.scix_id:
                    r.scix_id = "scix:" + str(self.generate_scix_id(r.id))
                    out = r.toJSON()
                session.commit()
                return out
            except exc.IntegrityError:
                self.logger.exception('error in app.update_storage while updating database for bibcode {}, type {}'.format(bibcode, type))
                session.rollback()
                raise

    def generate_scix_id(self, number):
        return scix_id.encode(number) 

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
        # Links data can come from two sources with different formats:
        #
        # 1. Nonbib data in either:
        #    - Old format with "data_links_rows":
        #      [{"url": ["http://arxiv.org/abs/1902.09522"], "title": [""], "item_count": 0}]
        #    - New format with "links": 
        #      {"DATA": {"url": ["http://arxiv.org/abs/1902.09522"], "title": [""], "count": 0}}
        #
        # 2. Bib data with "links_data" as JSON string:
        #    [{"access": "open", 
        #      "instances": "",
        #      "title": "",
        #      "type": "preprint",
        #      "url": "http://arxiv.org/abs/1902.09522"}]
        #
        # We prioritize nonbib data when available, falling back to bib data if needed.
        # We also assume it will be in either the old or new format, but not both.
        # If there is no relevant data, we return None.

        
        resolver_record = None   # default value to return
        bibcode = record.get('bibcode')
        nonbib = record.get('nonbib_data', {})
        # New format 
        nonbib_new_links = nonbib.get('links', {}) if nonbib else {}

        resolver_record = {'bibcode': bibcode,
                           'links':  nonbib_new_links}

        if not isinstance(nonbib, dict):
            nonbib = {}    # in case database has None or something odd
        
        # Old format links are in the "data_links_rows" key
        nonbib_old_links = nonbib.get('data_links_rows', [])

        # If nonbib data but in old format, transform into new format
        if not nonbib_new_links and nonbib_old_links:
            # transform into new format
            resolver_record['links'] = self._transform_old_links_to_new_format(nonbib_old_links)
        
        # If not nonbib data in any format, use link from bib/direct ingest
        elif not nonbib_new_links:
            bib = record.get('bib_data', {})
            if not isinstance(bib, dict):
                bib = {} 

            bib_links_record = bib.get('links_data', None)
            
            
            if bib_links_record:
                try:
                    bib_links_data = json.loads(bib_links_record[0])
                    url = bib_links_data.get('url', None)
                    if url:
                        # Need to change what direct sends
                        url_pdf = url.replace('/abs/', '/pdf/')
                        default_data_links_rows = {'data_links_rows': [{'url': [url],
                                                                'title': [''], 'item_count': 0,
                                                                'link_type': 'ESOURCE',
                                                                'link_sub_type': 'EPRINT_HTML'},
                                                               {'url': [url_pdf],
                                                                'title': [''], 'item_count': 0,
                                                                'link_type': 'ESOURCE',
                                                                'link_sub_type': 'EPRINT_PDF'}]}
                        # Transform into new format
                        resolver_record['links'] = self._transform_old_links_to_new_format(default_data_links_rows['data_links_rows'])
                except (KeyError, ValueError):
                    # here if record holds unexpected value
                    self.logger.error('invalid value in bib data, bibcode = {}, type = {}, value = {}'.format(bibcode, type(bib_links_record), bib_links_record))
                    return
        
        # Only populate links if it has relevant data, otherwise return None
        if resolver_record.get('links', {}):
            # Now populate any missing link information from all available sources
            resolver_record = self._populate_links_structure(record, resolver_record)
            return resolver_record
        
        

    def _transform_old_links_to_new_format(self, data_links_rows):
        """Transform the old links format to the new links format"""
        
        link_type_mapping = {
            'DATA': 'DATA',
            'ESOURCE': 'ESOURCE',
            'ASSOCIATED': 'ASSOCIATED',
            'INSPIRE': 'INSPIRE',
            'LIBRARYCATALOG': 'LIBRARYCATALOG',
            'PRESENTATION': 'PRESENTATION'
        }

        new_links = {
                        "ARXIV": [],
                        "DOI": [],
                        "DATA": {},
                        "ESOURCE": {},
                        "ASSOCIATED": {
                            "url": [],
                            "title": [],
                            "count": 0
                        },
                        "INSPIRE": {
                            "url": [],
                            "title": [],
                            "count": 0
                        },
                        "LIBRARYCATALOG": {
                            "url": [],
                            "title": [],
                            "count": 0
                        },
                        "PRESENTATION": {
                            "url": [],
                            "title": [],
                            "count": 0
                        },
                        "ABSTRACT": False,
                        "CITATIONS": False,
                        "GRAPHICS": False,
                        "METRICS": False,
                        "OPENURL": False, 
                        "REFERENCES": False,
                        "TOC": False,
                        "COREAD": False 
                    }
        
        for row in data_links_rows:
            link_type = row.get('link_type', '')
            if link_type not in link_type_mapping:
                continue

            mapped_type = link_type_mapping[link_type]

            if mapped_type in ('DATA', 'ESOURCE'):
                sub_type = row.get('link_sub_type', '')
                if sub_type not in new_links[mapped_type]:
                    new_links[mapped_type][sub_type] = {
                        'url': [],
                        'title': [],
                        'count': 0
                    }
                if 'url' in row:
                    new_links[mapped_type][sub_type]['url'].extend(row['url'])
                if 'title' in row:
                    new_links[mapped_type][sub_type]['title'].extend(row['title'])
                if 'item_count' in row:
                    new_links[mapped_type][sub_type]['count'] = row['item_count']
            
            else:
                if 'url' in row:
                    new_links[mapped_type]['url'].extend(row['url'])
                if 'title' in row:
                    new_links[mapped_type]['title'].extend(row['title'])
                if 'item_count' in row:
                    new_links[mapped_type]['count'] = row['item_count']
                
        return new_links
    
    def is_arxiv_id(self, identifier):

        
        identifier = str(identifier).lower()

        patterns = [
            r'^arxiv:\d{4}\.\d{5}$',                     # arXiv:2502.20510
            r'^arxiv:[a-z\-]+/\d{7}$',                   # arXiv:astro-ph/0610305
            r'^10\.48550/arxiv\.\d{4}\.\d{5}$',          # 10.48550/arXiv.2502.20510
            r'^10\.48550/arxiv\.[a-z\-]+/\d{7}$'         # 10.48550/arXiv.astro-ph/0610305
        ]

        return any(re.match(pat, identifier) for pat in patterns)
        
    
    def is_doi_id(self, identifier):
        # Normalize whitespace and case
        identifier = str(identifier).strip().lower()

        # Regex pattern for DOI
        doi_pattern = re.compile(r'^10\.\d{4,9}/\S+$', re.IGNORECASE)
        return bool(doi_pattern.match(identifier))

    
    def _extract_data_components(self, record):
        """Extract and validate data components from a record.
        
        Args:
            record (dict): The complete record with all available components
            
        Returns:
            tuple: Contains (bibcode, bib_data, fulltext, metrics, nonbib, orcid_claims)
        """
        bibcode = record.get('bibcode')
        
        # Extract and validate all needed components
        bib_data = record.get('bib_data', {})
        if not isinstance(bib_data, dict):
            bib_data = {}
            
        fulltext = record.get('fulltext', {})
        if not isinstance(fulltext, dict):
            fulltext = {}
            
        metrics = record.get('metrics', {})
        if not isinstance(metrics, dict):
            metrics = {}
        
        nonbib = record.get('nonbib_data', {})
        if not isinstance(nonbib, dict):
            nonbib = {}
            
        orcid_claims = record.get('orcid_claims', {})
        if not isinstance(orcid_claims, dict):
            orcid_claims = {}
            
        return bibcode, bib_data, fulltext, metrics, nonbib, orcid_claims
    
    def _populate_identifiers(self, record, resolver_record, links):
        """Populate the identifier list and extract ARXIV and DOI links.
        
        Args:
            record (dict): The complete record with all available components from the database
            resolver_record (dict): The resolver record to update with identifiers
            links (dict): The links structure to update with ARXIV and DOI information
            
        Returns:
            dict: The updated links structure with ARXIV and DOI fields populated
        """
        bibcode, bib_data, _, _, _, _ = self._extract_data_components(record)
        
        # Collect all identifiers from all sources
        identifiers = self._collect_identifiers(bibcode, bib_data)
        resolver_record['identifier'] = identifiers
            
        # Process identifiers for ARXIV and DOI fields
        arxiv_ids = set()
        doi_ids = set()
        
        for identifier in identifiers:
                
            # ARXIV identifiers
            if identifier and self.is_arxiv_id(identifier):
                arxiv_ids.add(identifier)
                
            # DOI identifiers
            if identifier and self.is_doi_id(identifier):
                doi_ids.add(identifier)
                
        # Initialize ARXIV and DOI fields if they don't exist or are not lists
        if 'ARXIV' not in links or not isinstance(links['ARXIV'], list):
            links['ARXIV'] = []
        
        if 'DOI' not in links or not isinstance(links['DOI'], list):
            links['DOI'] = []
        
        # Add identifiers to links structure
        if arxiv_ids:
            links['ARXIV'].extend(list(arxiv_ids))
            
        if doi_ids:
            links['DOI'].extend(list(doi_ids))
        
        return links
    
    def _populate_links_structure(self, record, resolver_record):
        '''Finally populates missing data like identifier, arxiv, doi, abstract, graphics, metrics, citations and references.
        
        Args:
            record (dict): The complete record with all available components from the database
            resolver_record (dict): The partially populated resolver record with base links structure
            
        Returns:
            dict: The fully populated resolver record with all available link information
        '''
      
        links = resolver_record.get('links', {})
        
        # Extract all necessary components
        _, bib_data, fulltext, metrics, nonbib, _ = self._extract_data_components(record)
        
        # Populate identifiers and extract ARXIV and DOI links
        links = self._populate_identifiers(record, resolver_record, links)
            
        # Always set ABSTRACT to True
        links['ABSTRACT'] = True

        # Set CITATIONS flag if citations are present in metrics or nonbib
        if ('citation_num' in metrics and metrics['citation_num'] > 0) or \
           ('citation_count' in nonbib and nonbib['citation_count'] > 0):
            links['CITATIONS'] = True
            
        # Set GRAPHICS flag if fulltext contains graphics indicators
        links['GRAPHICS'] = True

        # Set METRICS flag if metrics data is available
        if metrics and any(key in metrics for key in ['citation_num', 'downloads', 'reads']):
            links['METRICS'] = True

        # Always set OPENURL to True
        links['OPENURL'] = True
            
        # Set REFERENCES flag if references are present in nonbib or metrics indicates references
        if ('reference' in nonbib and len(nonbib['reference']) > 0) or \
           (metrics and 'reference_num' in metrics and metrics['reference_num'] > 0):
            links['REFERENCES'] = True
            
        # Check for TOC flag
        if 'property' in nonbib and isinstance(nonbib['property'], list) and 'TOC' in nonbib['property']:
            links['TOC'] = True
        elif bib_data.get('metadata', {}).get('properties', {}).get('doctype', {}).get('content') == 'toc':
            links['TOC'] = True
            
        # Always set COREAD to True 
        links['COREAD'] = True
            
        # Update the links in the resolver record
        resolver_record['links'] = links
        
        return resolver_record
        
    def _collect_identifiers(self, bibcode, bib_data):
            """Collect identifiers from all available sources.
            
            Args:
                bibcode (str): The bibcode of the record
                bib_data (dict): The bibliographic data dictionary
                nonbib (dict): The non-bibliographic data dictionary
                
            Returns:
                set: A set of all collected identifiers
            """
            identifiers = set()
            
            # 1. Add bibcode itself as an identifier
            if bibcode:
                identifiers.add(bibcode)
                
            # 2. Get identifiers from bib_data
            if 'identifier' in bib_data:
                bib_identifiers = bib_data.get('identifier', [])
                if isinstance(bib_identifiers, list):
                    identifiers.update([id for id in bib_identifiers if id])

            # 3. Get any additional identifiers from alternate_bibcode
            if 'alternate_bibcode' in bib_data:
                alt_bibcodes = bib_data.get('alternate_bibcode', [])
                if isinstance(alt_bibcodes, list):
                    identifiers.update([id for id in alt_bibcodes if id])
                    
            return list(identifiers)
                