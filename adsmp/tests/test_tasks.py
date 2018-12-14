import sys
import os
import json

from mock import patch, Mock, MagicMock
import unittest
from adsmp import app, tasks
from adsmp.models import Base, Records
from adsputils import get_date
from adsmsg import DenormalizedRecord, FulltextUpdate, NonBibRecord, NonBibRecordList, MetricsRecord, MetricsRecordList,AugmentAffiliationResponseRecord
from adsmsg.orcid_claims import OrcidClaims

import mock
import copy

class CopyingMock(mock.MagicMock):
    def _mock_call(_mock_self, *args, **kwargs):
        return super(CopyingMock, _mock_self)._mock_call(*copy.deepcopy(args), **copy.deepcopy(kwargs))

class TestWorkers(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = os.path.join(os.path.dirname(__file__), '../..')
        self._app = tasks.app
        self.app = app.ADSMasterPipelineCelery('test', local_config=\
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'SQLALCHEMY_ECHO': False,
            'SOLR_URLS': ['http://foo.bar.com/solr/v1'],
            'METRICS_SQLALCHEMY_URL': None,
            'LINKS_RESOLVER_UPDATE_URL': 'http://localhost:8080/update',
            'ADS_API_TOKEN': 'api_token'
            })
        tasks.app = self.app # monkey-patch the app object
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()


    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()
        tasks.app = self._app


    def test_task_update_record(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            tasks.task_update_record(DenormalizedRecord(bibcode='2015ApJ...815..133S'))
            self.assertFalse(next_task.called)
            
        
        with patch('adsmp.solr_updater.delete_by_bibcodes', return_value=[('2015ApJ...815..133S'), ()]) as solr_delete, \
            patch.object(self.app, 'metrics_delete_by_bibcode', return_value=True) as metrics_delete:
            tasks.task_update_record(DenormalizedRecord(bibcode='2015ApJ...815..133S', status='deleted'))
            self.assertTrue(solr_delete.called)
            self.assertTrue(metrics_delete.called)
            
    
    def test_task_update_record_delete(self):

        for x, cls in (('fulltext', FulltextUpdate), ('orcid_claims', OrcidClaims)):
            self.app.update_storage('bibcode', x, {'foo': 'bar'})
            self.assertEquals(self.app.get_record('bibcode')[x]['foo'], 'bar')
            with patch('adsmp.tasks.task_index_records.delay') as next_task:
                tasks.task_update_record(cls(bibcode='bibcode', status='deleted'))
                self.assertEquals(self.app.get_record('bibcode')[x], None)
                self.assertTrue(self.app.get_record('bibcode'))
        
        recs = NonBibRecordList()
        recs.nonbib_records.extend([NonBibRecord(bibcode='bibcode', status='deleted').data])
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            tasks.task_update_record(recs)
            self.assertEquals(self.app.get_record('bibcode')['metrics'], None)
            self.assertTrue(self.app.get_record('bibcode'))
            
        with patch('adsmp.tasks.task_delete_documents') as next_task:
            tasks.task_update_record(DenormalizedRecord(bibcode='bibcode', status='deleted'))
            self.assertTrue(next_task.called)
            self.assertTrue(next_task.call_args[0], ('bibcode',))


    def test_task_update_record_fulltext(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            tasks.task_update_record(FulltextUpdate(bibcode='2015ApJ...815..133S', body='INTRODUCTION'))
            self.assertEquals(self.app.get_record(bibcode='2015ApJ...815..133S')['fulltext']['body'], 'INTRODUCTION')
            self.assertFalse(next_task.called)

    def test_task_update_record_nonbib(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            tasks.task_update_record(NonBibRecord(bibcode='2015ApJ...815..133S', read_count=9))
            self.assertEquals(self.app.get_record(bibcode='2015ApJ...815..133S')['nonbib_data']['read_count'], 9)
            self.assertFalse(next_task.called)

    def test_task_update_record_nonbib_list(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            recs = NonBibRecordList()
            nonbib_data = {'bibcode': '2003ASPC..295..361M', 'boost': 3.1}
            nonbib_data2 = {'bibcode': '3003ASPC..295..361Z', 'boost': 3.2}
            rec = NonBibRecord(**nonbib_data)
            rec2 = NonBibRecord(**nonbib_data2)
            recs.nonbib_records.extend([rec._data, rec2._data])
            tasks.task_update_record(recs)
            self.assertFalse(next_task.called)


    def test_task_update_record_augments(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            d = {
                u"aff": [
                    u"Purdue University (United States)",
                    u"Purdue University (United States)",
                    u"Purdue University (United States)"
                ], 
                u"aff_abbrev": [
                    u"NA",
                    u"NA",
                    u"NA"
                ], 
                u"aff_canonical": [
                    u"-", 
                    u"-", 
                    u"-"
                ],
                u"aff_facet": [],
                u"aff_facet_hier": [],
                u"aff_id": [],
                u"author": [
                    u"Mikhail, E. M.",
                    u"Kurtz, M. K.",
                    u"Stevenson, W. H."
                ], 
                u"bibcode": u"1971SPIE...26..187M"
            }
            tasks.task_update_record(AugmentAffiliationResponseRecord(**d))
            db_rec = self.app.get_record(bibcode='1971SPIE...26..187M')
            db_rec['augments'].pop('status')
            self.maxDiff = None
            self.assertDictEqual(db_rec['augments'], d)


            self.assertFalse(next_task.called)

    def test_task_update_record_augments_list(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            recs = NonBibRecordList()
            nonbib_data = {'bibcode': '2003ASPC..295..361M', 'boost': 3.1}
            nonbib_data2 = {'bibcode': '3003ASPC..295..361Z', 'boost': 3.2}
            rec = NonBibRecord(**nonbib_data)
            rec2 = NonBibRecord(**nonbib_data2)
            recs.nonbib_records.extend([rec._data, rec2._data])
            tasks.task_update_record(recs)
            self.assertFalse(next_task.called)

    def test_task_update_record_metrics(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            self.assertFalse(next_task.called)
            tasks.task_update_record(MetricsRecord(bibcode='2015ApJ...815..133S'))
            self.assertFalse(next_task.called)


    def test_task_update_record_metrics_list(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            recs = MetricsRecordList()
            metrics_data = {'bibcode': '2015ApJ...815..133S'}
            metrics_data2 = {'bibcode': '3015ApJ...815..133Z'}
            rec = MetricsRecord(**metrics_data)
            rec2 = MetricsRecord(**metrics_data2)
            recs.metrics_records.extend([rec._data, rec2._data])
            tasks.task_update_record(recs)
            self.assertFalse(next_task.called)
            

    def _reset_checksum(self, bibcode):
        with self.app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is None:
                r = Records(bibcode=bibcode)
                session.add(r)
            r.solr_checksum = None
            r.metrics_checksum = None
            r.datalinks_checksum = None
            session.commit()
    
    def _check_checksum(self, bibcode, solr=None, metrics=None, datalinks=None):
        with self.app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if solr is True:
                self.assertTrue(r.solr_checksum)
            else:
                self.assertEqual(r.solr_checksum, solr)
            if metrics is True:
                self.assertTrue(r.metrics_checksum)
            else:
                self.assertEqual(r.metrics_checksum, metrics)
            if datalinks is True:
                self.assertTrue(r.datalinks_checksum)
            else:
                self.assertEqual(r.datalinks_checksum, datalinks)

    def test_task_update_solr(self):
        # just make sure we have the entry in a database
        self._reset_checksum('foobar')
        
        with patch.object(self.app, 'mark_processed', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=[200]) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bibcode': 'foobar',
                                                               'bib_data_updated': get_date(),
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': get_date('2012'),}), \
            patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as task_index_records:

            self.assertFalse(update_solr.called)
            tasks.task_index_records('2015ApJ...815..133S')
            self.assertTrue(update_solr.called)
            self.assertTrue(update_timestamp.called)
        
        self._check_checksum('foobar', solr=True)
        self._reset_checksum('foobar')

        with patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=[200]) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bibcode': 'foobar',
                                                               'bib_data_updated': get_date(),
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': get_date('2025'),}), \
            patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as task_index_records:

            self.assertFalse(update_solr.called)
            tasks.task_index_records('2015ApJ...815..133S')
            self.assertFalse(update_solr.called)
            self.assertFalse(update_timestamp.called)
            
        self._check_checksum('foobar', solr=None)
        self._reset_checksum('foobar')



        with patch.object(self.app, 'mark_processed', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=[200]) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bibcode': 'foobar',
                                                               'bib_data_updated': get_date(),
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': get_date('2025'),}), \
            patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as task_index_records:

            self.assertFalse(update_solr.called)
            tasks.task_index_records('2015ApJ...815..133S', force=True)
            self.assertTrue(update_solr.called)
            self.assertTrue(update_timestamp.called)
            
        self._check_checksum('foobar', solr=True)
        self._reset_checksum('foobar')



        with patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=None) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bibcode': 'foobar',
                                                               'bib_data_updated': None,
                                                               'nonbib_data_updated': get_date(),
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': None,}), \
            patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as task_index_records:

            self.assertFalse(update_solr.called)
            tasks.task_index_records('2015ApJ...815..133S')
            self.assertFalse(update_solr.called)
            self.assertFalse(update_timestamp.called)
        
        self._check_checksum('foobar', solr=None)
        self._reset_checksum('foobar')


        with patch.object(self.app, 'mark_processed', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=[200]) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bibcode': 'foobar',
                                                               'bib_data_updated': get_date(),
                                                               'nonbib_data_updated': None,
                                                               'orcid_claims_updated': get_date(),
                                                               'processed': None,}), \
            patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as task_index_records:

            self.assertFalse(update_solr.called)
            tasks.task_index_records('2015ApJ...815..133S', force=True)
            self.assertTrue(update_solr.called)
            self.assertTrue(update_timestamp.called)
            self.assertFalse(task_index_records.called)
            


        with patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=[200]) as update_solr, \
            patch.object(self.app, 'get_record', return_value={'bibcode': 'foobar',
                                                               'bib_data_updated': None,
                                                               'nonbib_data_updated': None,
                                                               'orcid_claims_updated': None,
                                                               'fulltext_claims_updated': get_date(),
                                                               'processed': None,}), \
            patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as task_index_records:

            self.assertFalse(update_solr.called)
            tasks.task_index_records('2015ApJ...815..133S')
            self.assertFalse(update_solr.called)
            self.assertFalse(update_timestamp.called)
            

    def test_task_index_records(self):
        
        self.assertRaises(Exception, lambda : tasks.task_index_records(['foo', 'bar'], update_solr=False, update_metrics=False, update_links=False))
            
        with patch.object(tasks.logger, 'error', return_value=None) as logger:
            tasks.task_index_records(['non-existent'])
            logger.assert_called_with(u"The bibcode %s doesn't exist!", 'non-existent')


    def test_task_index_links(self):
        """verify data is sent to links microservice update endpoint"""
        r = Mock()
        r.status_code = 200
            
        # just make sure we have the entry in a database
        tasks.task_update_record(DenormalizedRecord(bibcode='linkstest'))
        
        with patch.object(self.app, 'get_record', return_value={'bibcode': 'linkstest',
                                                                'nonbib_data': {'data_links_rows': [{'baz': 0}]},
                                                                'bib_data_updated': get_date(),
                                                                'nonbib_data_updated': get_date(),
                                                                'processed': get_date('2025')}), \
             patch('requests.put', return_value = r, new_callable=CopyingMock) as p:
            tasks.task_index_records(['linkstest'], update_solr=False, update_metrics=False, update_links=True, force=True)
            p.assert_called_with('http://localhost:8080/update',
                                 data=json.dumps([{'bibcode': 'linkstest', 'data_links_rows': [{'baz': 0}]}]),
                                 headers={'Authorization': 'Bearer api_token'})
            
        
        rec = self.app.get_record(bibcode='linkstest')
        self.assertEquals(rec['datalinks_checksum'], '0x80e85169')
        self.assertEquals(rec['solr_checksum'], None)
        self.assertEquals(rec['metrics_checksum'], None)


    def test_task_index_links_no_data(self):
        """verify data links works when no data_links_rows is present"""
        with patch.object(self.app, 'get_record', return_value={'bibcode': 'linkstest',
                                                                'nonbib_data': {'boost': 1.2},
                                                                'bib_data_updated': get_date(),
                                                                'nonbib_data_updated': get_date(),
                                                                'processed': get_date('2025')}), \
                         patch('requests.put', new_callable=CopyingMock) as p:
            tasks.task_index_records(['linkstest'], update_solr=False, update_metrics=False, update_links=True, force=True)
            p.assert_not_called()


            
    def test_avoid_duplicates(self):
        
        # just make sure we have the entry in a database
        self._reset_checksum('foo')
        self._reset_checksum('bar')
        
        
        with patch.object(self.app, 'get_record') as getter, \
            patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=[200]) as update_solr:
            
            getter.return_value = {'bibcode': 'foo', 'bib_data_updated': get_date('1972-04-01')}
            tasks.task_index_records(['foo'], force=True)
            
            self.assertEquals(update_solr.call_count, 1)
            self._check_checksum('foo', solr='0xf2708ee8')
            
            # now change metrics (solr shouldn't be called)
            getter.return_value = {'bibcode': 'foo', 'metrics_updated': get_date('1972-04-02'),
                                   'bib_data_updated': get_date('1972-04-01'),
                                   'solr_checksum': '0xf2708ee8'}
            tasks.task_index_records(['foo'], force=True)
            self.assertEquals(update_solr.call_count, 1)


    def test_ignore_checksums_solr(self):
        """verify ingore_checksums works with solr updates"""
        self._reset_checksum('foo') # put bibcode in database
        with patch.object(self.app, 'get_record') as getter, \
            patch.object(self.app, 'update_processed_timestamp', return_value=None) as update_timestamp,\
            patch('adsmp.solr_updater.update_solr', return_value=[200]) as update_solr:
            getter.return_value = {'bibcode': 'foo', 'metrics_updated': get_date('1972-04-02'),
                                   'bib_data_updated': get_date('1972-04-01'),
                                   'solr_checksum': '0xf2708ee8'}

            # update with matching checksum and then update and ignore checksums
            tasks.task_index_records(['foo'], force=True, update_metrics=False, update_links=False, ignore_checksums=False)
            self.assertEquals(update_solr.call_count, 0)
            tasks.task_index_records(['foo'], force=True, update_metrics=False, update_links=False, ignore_checksums=True)
            self.assertEquals(update_solr.call_count, 1)

    def test_ignore_checksums_datalinks(self):
        """verify ingore_checksums works with datalinks updates"""        
        self._reset_checksum('linkstest')  # put bibcode in database
        r = Mock()
        r.status_code = 200
        with patch.object(self.app, 'get_record', return_value={'bibcode': 'linkstest',
                                                                'nonbib_data': {'data_links_rows': [{'baz': 0}]},
                                                                'bib_data_updated': get_date(),
                                                                'nonbib_data_updated': get_date(),
                                                                'processed': get_date('2025'),
                                                                'links_checksum': '0xb9ba6b3f'}), \
                                                        patch('requests.put', return_value = r, new_callable=CopyingMock) as p:
            # update with matching checksum and then update and ignore checksums
            tasks.task_index_records(['linkstest'], update_solr=False, update_metrics=False, update_links=True, force=True,
                                     ignore_checksums=False)
            self.assertEquals(p.call_count, 0)
            tasks.task_index_records(['linkstest'], update_solr=False, update_metrics=False, update_links=True, force=True,
                                     ignore_checksums=True)
            self.assertEquals(p.call_count, 1)

    def test_ignore_checksums_metrics(self):
        """verify ingore_checksums works with datalinks updates"""        
        self._reset_checksum('metricstest')  # put bibcode in database
        r = Mock()
        r.return_value = (['metricstest'], None)
        with patch.object(self.app, 'get_record', return_value={'bibcode': 'metricstest',
                                                                'bib_data_updated': get_date(),
                                                                'metrics': {'refereed': False, 'author_num': 2},
                                                                'processed': get_date('2025'),
                                                                'metrics_checksum': '0x424cb03e'}), \
                patch.object(self.app, 'update_metrics_db', return_value = (['metricstest'], None)) as u:
            # update with matching checksum and then update and ignore checksums
            tasks.task_index_records(['metricstest'], update_solr=False, update_metrics=True, update_links=False, force=True,
                                     ignore_checksums=False)
            self.assertEquals(u.call_count, 0)
            tasks.task_index_records(['metricstest'], update_solr=False, update_metrics=True, update_links=False, force=True,
                                     ignore_checksums=True)
            self.assertEquals(u.call_count, 1)
        
            
if __name__ == '__main__':
    unittest.main()
