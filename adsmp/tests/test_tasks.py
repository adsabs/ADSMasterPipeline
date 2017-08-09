import sys
import os
import json

from mock import patch
import unittest
from adsmp import app, tasks
from adsmp.models import Base
from adsputils import get_date
from adsmsg import DenormalizedRecord, FulltextUpdate, NonBibRecord, NonBibRecordList, MetricsRecord, MetricsRecordList

class TestWorkers(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = os.path.join(os.path.dirname(__file__), '../..')
        self._app = tasks.app
        self.app = app.ADSMasterPipelineCelery('test', local_config=\
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'SQLALCHEMY_ECHO': False,
            'SOLR_URLS': ['http://foo.bar.com/solr/v1']
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
            self.assertFalse(next_task.called)
            tasks.task_update_record(DenormalizedRecord(bibcode='2015ApJ...815..133S'))
            self.assertTrue(next_task.called)
            self.assertTrue(next_task.call_args[0], ('2015ApJ...815..133S',))
            
        
        with patch('adsmp.solr_updater.delete_by_bibcodes', return_value=[('2015ApJ...815..133S'), ()]) as solr_delete:
            tasks.task_update_record(DenormalizedRecord(bibcode='2015ApJ...815..133S', status='deleted'))
            self.assertTrue(next_task.call_args[0], ('2015ApJ...815..133S',))
            self.assertTrue(solr_delete.called)


    def test_task_update_record_fulltext(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            self.assertFalse(next_task.called)
            tasks.task_update_record(FulltextUpdate(bibcode='2015ApJ...815..133S', body='INTRODUCTION'))
            self.assertTrue(next_task.called)
            self.assertTrue(next_task.call_args[0], ('2015ApJ...815..133S',))

    def test_task_update_record_nonbib(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            self.assertFalse(next_task.called)
            tasks.task_update_record(NonBibRecord(bibcode='2015ApJ...815..133S'))
            self.assertTrue(next_task.called)
            self.assertTrue(next_task.call_args[0], ('2015ApJ...815..133S'))

    def test_task_update_record_nonbib_list(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            self.assertFalse(next_task.called)
            recs = NonBibRecordList()
            nonbib_data = {'bibcode': '2003ASPC..295..361M', 'refereed': False}
            nonbib_data2 = {'bibcode': '3003ASPC..295..361Z', 'refereed': True}
            rec = NonBibRecord(**nonbib_data)
            rec2 = NonBibRecord(**nonbib_data2)
            recs.nonbib_records.extend([rec._data, rec2._data])
            tasks.task_update_record(recs)
            self.assertTrue(next_task.called)
            self.assertTrue(next_task.call_args[0], ('2015ApJ...815..133S', '3003ASPC..295..361Z'))

    def test_task_update_record_metrics(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            self.assertFalse(next_task.called)
            tasks.task_update_record(MetricsRecord(bibcode='2015ApJ...815..133S'))
            self.assertTrue(next_task.called)
            self.assertTrue(next_task.call_args[0], ('2015ApJ...815..133S'))

    def test_task_update_record_metrics_list(self):
        with patch('adsmp.tasks.task_index_records.delay') as next_task:
            self.assertFalse(next_task.called)
            recs = MetricsRecordList()
            metrics_data = {'bibcode': '2015ApJ...815..133S'}
            metrics_data2 = {'bibcode': '3015ApJ...815..133Z'}
            rec = MetricsRecord(**metrics_data)
            rec2 = MetricsRecord(**metrics_data2)
            recs.metrics_records.extend([rec._data, rec2._data])
            tasks.task_update_record(recs)
            self.assertTrue(next_task.called)
            self.assertTrue(next_task.call_args[0], ('2015ApJ...815..133S', '3015ApJ...815..133Z'))


    def test_task_update_solr(self):
        with patch.object(self.app, '_mark_processed', return_value=None) as update_timestamp,\
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



        with patch.object(self.app, '_mark_processed', return_value=None) as update_timestamp,\
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
            #self.assertTrue(task_index_records.called)



        with patch.object(self.app, '_mark_processed', return_value=None) as update_timestamp,\
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
        self.assertRaises(Exception, lambda : tasks.task_index_records(['foo', 'bar'], update_solr=False, update_metrics=False))
            
        with patch.object(tasks.logger, 'error', return_value=None) as logger:
            tasks.task_index_records(['non-existent'])
            logger.assert_called_with(u"The bibcode %s doesn't exist!", 'non-existent')

        

if __name__ == '__main__':
    unittest.main()
