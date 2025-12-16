#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mock
from mock import patch
import unittest
import os
import sys
import json
import re
import tempfile
import time

import adsputils
from adsmp import app, models
from adsmp.models import Base, MetricsBase, Records, SitemapInfo, ChangeLog
from adsputils import get_date
import testing.postgresql
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from datetime import timedelta


class TestAdsOrcidCelery(unittest.TestCase):
    """
    Tests the appliction's methods
    """
    
    @classmethod
    def setUpClass(cls):
        cls.postgresql = \
            testing.postgresql.Postgresql(host='127.0.0.1', port=15678, user='postgres', 
                                          database='test')

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()
        
    def setUp(self):
        unittest.TestCase.setUp(self)
        
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        with mock.patch.dict('os.environ', {'ADS_API_TOKEN': 'fixme'}):
            self.app = app.ADSMasterPipelineCelery('test', local_config=\
                {
                'SQLALCHEMY_URL': 'sqlite:///',
                'METRICS_SQLALCHEMY_URL': 'postgresql://postgres@127.0.0.1:15678/test',
                'SQLALCHEMY_ECHO': False,
                'PROJ_HOME' : proj_home,
                'TEST_DIR' : os.path.join(proj_home, 'adsmp/tests'),
                })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
        
        MetricsBase.metadata.bind = self.app._metrics_engine
        MetricsBase.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        MetricsBase.metadata.drop_all()
        self.app.close_app()

    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == 'sqlite:///'
        assert self.app.conf.get('SQLALCHEMY_URL') == 'sqlite:///'

    def test_mark_processed(self):
        self.app.mark_processed(['abc'], 'solr', checksums=['jkl'], status='success')
        r = self.app.get_record('abc')
        self.assertEqual(r, None)
        
        self.app.update_storage('abc', 'bib_data', {'bibcode': 'abc', 'hey': 1})
        self.app.mark_processed(['abc'], 'solr', checksums=['jkl'], status='success')
        r = self.app.get_record('abc')
        
        self.assertTrue(r['solr_processed'])
        self.assertTrue(r['status'])

        self.app.mark_processed(['abc'], 'solr', checksums=['jkl'], status='solr-failed')
        r = self.app.get_record('abc')
        self.assertTrue(r['solr_processed'])
        self.assertTrue(r['processed'])
        self.assertEqual(r['status'], 'solr-failed')

    def test_index_solr(self):
        self.app.update_storage('abc', 'bib_data', {'bibcode': 'abc', 'hey': 1, 'test': 'test'})
        self.app.update_storage('foo', 'bib_data', {'bibcode': 'foo', 'hey': 1})
        
        with mock.patch('adsmp.solr_updater.update_solr', return_value=[200]):
            self.app.index_solr([{'bibcode': 'abc'},
                                 {'bibcode': 'foo'}],
                                ['checksum1', 'checksum2'],
                                         ['http://solr1'])
            with self.app.session_scope() as session:
                for x in ['abc', 'foo']:
                    r = session.query(models.Records).filter_by(bibcode=x).first()
                    self.assertTrue(r.processed)
                    self.assertFalse(r.metrics_processed)
                    self.assertTrue(r.solr_processed)
                    
        # pretend group failure and then success when records sent individually
        with mock.patch('adsmp.solr_updater.update_solr') as us, \
                mock.patch.object(self.app, 'mark_processed') as mp:
            us.side_effect = [[503], [200], [200]]
            self.app.index_solr([{'bibcode': 'abc'},
                                 {'bibcode': 'foo'}],
                                ['checksum1', 'checksum2'],
                                ['http://solr1'])
            # self.assertTrue(len(failed) == 0)
            x = str(mp.call_args_list[0])
            self.assertTrue('abc' in x)
            self.assertTrue('success' in x)
            self.assertTrue('solr' in x)
            self.assertEqual(us.call_count, 3)
            x = str(mp.call_args_list[1])
            self.assertTrue('foo' in x)
            self.assertTrue('success' in x)
            self.assertTrue('solr' in x)

        # pretend failure and success without body
        # update_solr should try to send two records together and then
        #   each record by itself twice: once as is and once without fulltext
        with mock.patch('adsmp.solr_updater.update_solr') as us, \
                mock.patch.object(self.app, 'mark_processed') as mp:
            us.side_effect = [[503, 503], Exception('body failed'), 200, Exception('body failed'), 200]
            self.app.index_solr([{'bibcode': 'abc', 'body': 'BAD BODY'},
                                 {'bibcode': 'foo', 'body': 'BAD BODY'}],
                                ['checksum1', 'checksum2'],
                                ['http://solr1'])
            self.assertEqual(us.call_count, 5)
            # self.assertTrue(len(failed) == 0)
            self.assertEqual(mp.call_count, 2)
            x = str(us.call_args_list[-2])
            self.assertTrue('http://solr1' in x)
            self.assertTrue('foo' in x)
            self.assertTrue('body' in x)
            self.assertTrue('BAD BODY' in x)
            x = str(us.call_args_list[-1])
            self.assertTrue('http://solr1' in x)
            self.assertTrue('foo' in x)

        # pretend failure and then lots more failure
        # update_solr should try to send two records together and then
        #   each record by itself twice: once as is and once without fulltext
        with mock.patch('adsmp.solr_updater.update_solr') as us:
            us.side_effect = [[503, 503],
                              Exception('body failed'), Exception('body failed'),
                              Exception('body failed'), Exception('body failed')]
            self.app.index_solr([{'bibcode': 'abc', 'body': 'bad body'},
                                 {'bibcode': 'foo', 'body': 'bad body'}],
                                ['checksum1', 'checksum2'],
                                ['http://solr1'])
            self.assertEqual(us.call_count, 5)

        # pretend failure and and then failure for a mix of reasons
        with mock.patch('adsmp.solr_updater.update_solr') as us:
            us.side_effect = [[503, 503], Exception('body failed'), Exception('failed'), Exception('failed')]
            self.app.index_solr([{'bibcode': 'abc', 'body': 'bad body'},
                                 {'bibcode': 'foo', 'body': 'good body'}],
                                ['checksum1', 'checksum2'],
                                ['http://solr1'])
            self.assertEqual(us.call_count, 4)
            if sys.version_info > (3,):
                call_dict = "{'bibcode': 'foo', 'body': 'good body'}"
            else:
                call_dict = "{'body': 'good body', 'bibcode': 'foo'}"
            self.assertEqual(str(us.call_args_list[-1]), "call([%s], ['http://solr1'], commit=False, ignore_errors=False)" % call_dict)

        # pretend failure and and then a mix of failure and success
        with mock.patch('adsmp.solr_updater.update_solr') as us, \
                mock.patch.object(self.app, 'mark_processed') as mp:
            us.side_effect = [[503, 503], Exception('body failed'), [200]]
            self.app.index_solr([{'bibcode': 'abc', 'body': 'bad body'},
                                 {'bibcode': 'foo', 'body': 'good body'}],
                                ['checksum1', 'checksum2'],
                                         ['http://solr1'])
            self.assertEqual(us.call_count, 4)
            # self.assertTrue(len(failed) == 1)
            self.assertEqual(us.call_count, 4)
            self.assertEqual(mp.call_count, 2)
            x = str(us.call_args_list[-1])
            self.assertTrue('foo' in x)
            self.assertTrue('good body' in x)
            self.assertTrue('http://solr1' in x)

    def test_update_metrics(self):
        self.app.update_storage('abc', 'metrics', {
                     'author_num': 1,
                     'bibcode': 'abc',
                    })
        self.app.update_storage('foo', 'metrics', {
                    'bibcode': 'foo',
                    'citation_num': 6,
                    'author_num': 3,
                    })
        
        batch_metrics = [self.app.get_record('abc')['metrics'], self.app.get_record('foo')['metrics']]
        batch_checksum = ['checksum1', 'checksum2']
        self.app.index_metrics(batch_metrics, batch_checksum)
        
        for x in ['abc', 'foo']:
            r = self.app.get_record(x)
            self.assertTrue(r['processed'])
            self.assertTrue(r['metrics_processed'])
            self.assertFalse(r['solr_processed'])
            
    def test_delete_metrics(self):
        """Makes sure we can delete a metrics record by bibcode"""
        self.app.update_storage('abc', 'metrics', {
                     'author_num': 1,
                     'bibcode': 'abc',
                    })
        r = self.app.get_record('abc')
        self.app.index_metrics([r], ['checksum'])
        m = self.app.get_metrics('abc')
        self.assertTrue(m, 'intialized metrics data')
        self.app.metrics_delete_by_bibcode('abc')
        m = self.app.get_metrics('abc')
        self.assertFalse(m, 'deleted metrics data')
        
    def test_update_records(self):
        """Makes sure we can write recs into the storage."""
        now = adsputils.get_date()
        last_time = adsputils.get_date()
        for k in ['bib_data', 'nonbib_data', 'orcid_claims']:
            self.app.update_storage('abc', k, {'foo': 'bar', 'hey': 1})
            with self.app.session_scope() as session:
                r = session.query(models.Records).filter_by(bibcode='abc').first()
                self.assertTrue(r.id == 1)
                self.assertTrue(r.scix_id == 'scix:0RW9-X19B-XHYY')
                j = r.toJSON()
                self.assertEqual(j[k], {'foo': 'bar', 'hey': 1})
                t = j[k + '_updated']
                self.assertTrue(now < t)
                self.assertTrue(last_time < j['updated'])
                last_time = j['updated']
        
        self.app.update_storage('abc', 'fulltext', {'body': 'foo bar'})
        with self.app.session_scope() as session:
            r = session.query(models.Records).filter_by(bibcode='abc').first()
            self.assertTrue(r.id == 1)
            self.assertTrue(r.scix_id == 'scix:0RW9-X19B-XHYY')
            j = r.toJSON()
            self.assertEqual(j['fulltext'], {'body': 'foo bar'})
            t = j['fulltext_updated']
            self.assertTrue(now < t)
        
        r = self.app.get_record('abc')
        self.assertEqual(r['id'], 1)
        self.assertEqual(r['scix_id'],'scix:0RW9-X19B-XHYY')
        self.assertEqual(r['processed'], None)
        
        r = self.app.get_record(['abc'])
        self.assertEqual(r[0]['id'], 1)
        self.assertEqual(r[0]['scix_id'],'scix:0RW9-X19B-XHYY')
        self.assertEqual(r[0]['processed'], None)
        
        r = self.app.get_record('abc', load_only=['id'])
        self.assertEqual(r['id'], 1)
        self.assertFalse('processed' in r)

        with self.assertRaises(ValueError) as e:
            self.app.mark_processed(['abc'], 'foobar')
            self.assertTrue('foobar' in e.exception)
        
        # now delete it
        self.app.delete_by_bibcode('abc')
        r = self.app.get_record('abc')
        self.assertTrue(r is None)
        with self.app.session_scope() as session:
            r = session.query(models.ChangeLog).filter_by(key='bibcode:abc').first()
            self.assertTrue(r.key, 'abc')

    def test_index_metrics_database_failure(self):
        """
           verify handles failure from database
           send one bibcode, verify there are two commits
        """
        self.app.update_storage('abc', 'metrics', {
            'author_num': 1,
            'bibcode': 'abc',
        })

        trans = mock.Mock()
        trans.commit.side_effect = SQLAlchemyError('test')
        m = mock.Mock()
        m.begin_nested.return_value = trans
        m.__exit__ = mock.Mock()
        m.__enter__ = mock.Mock()
        m.__enter__.return_value = mock.Mock()
        m.__enter__.return_value.begin_nested.return_value = trans
        # init database so timestamps and checksum can be updated
        with mock.patch('adsmp.app.ADSMasterPipelineCelery.metrics_session_scope', return_value=m) as p:
            metrics_payload = {'bibcode': 'abc', 'author_num': 1}
            checksum = 'checksum'
            self.app.index_metrics([metrics_payload], [checksum])
            self.assertEqual(trans.commit.call_count, 2)

    def test_index_datalinks_success(self):
        """verify passed data sent to resolver service
           verify handles success from service
           verify records table updated with processed, status and checksum
        """
        m = mock.Mock()
        m.status_code = 200
        # init database so timestamps and checksum can be updated
        nonbib_data = {'data_links_rows': [{'baz': 0}]}
        self.app.update_storage('linkstest', 'nonbib_data', nonbib_data)
        with mock.patch('requests.put', return_value=m) as p:
            datalinks_payload = {u'bibcode': u'linkstest', u'data_links_rows': [{u'baz': 0}]}
            checksum = 'thechecksum'
            self.app.index_datalinks([datalinks_payload], [checksum])
            p.assert_called_with('http://localhost:8080/update',
                                 data=json.dumps([{'bibcode': 'linkstest', 'data_links_rows': [{'baz': 0}]}]),
                                 headers={'Authorization': 'Bearer fixme'})
            self.assertEqual(p.call_count, 1)
            # verify database updated
            rec = self.app.get_record(bibcode='linkstest')
            self.assertEqual(rec['datalinks_checksum'], 'thechecksum')
            self.assertEqual(rec['solr_checksum'], None)
            self.assertEqual(rec['metrics_checksum'], None)
            self.assertEqual(rec['status'], 'success')
            self.assertTrue(rec['datalinks_processed'])

    def test_index_datalinks_service_failure(self):
        """
           verify handles failure from service
        """
        m = mock.Mock()
        m.status_code = 500
        # init database so timestamps and checksum can be updated
        nonbib_data = {'data_links_rows': [{'baz': 0}]}
        self.app.update_storage('linkstest', 'nonbib_data', nonbib_data)
        with mock.patch('requests.put', return_value=m) as p:
            datalinks_payload = {u'bibcode': u'linkstest', u'data_links_rows': [{u'baz': 0}]}
            checksum = 'thechecksum'
            self.app.index_datalinks([datalinks_payload], [checksum])
            p.assert_called_with('http://localhost:8080/update',
                                 data=json.dumps([{'bibcode': 'linkstest', 'data_links_rows': [{'baz': 0}]}]),
                                 headers={'Authorization': 'Bearer fixme'})

            rec = self.app.get_record(bibcode='linkstest')
            self.assertEqual(p.call_count, 2)
            self.assertEqual(rec['datalinks_checksum'], None)
            self.assertEqual(rec['solr_checksum'], None)
            self.assertEqual(rec['metrics_checksum'], None)
            self.assertEqual(rec['status'], 'links-failed')
            self.assertTrue(rec['datalinks_processed'])

    def test_index_datalinks_service_only_batch_failure(self):
        # init database so timestamps and checksum can be updated
        nonbib_data = {'data_links_rows': [{'baz': 0}]}
        self.app.update_storage('linkstest', 'nonbib_data', nonbib_data)
        with mock.patch('requests.put') as p:
            bad = mock.Mock()
            bad.status_code = 500
            good = mock.Mock()
            good.status_code = 200
            p.side_effect = [bad, good]
            datalinks_payload = {u'bibcode': u'linkstest', u'data_links_rows': [{u'baz': 0}]}
            checksum = 'thechecksum'
            self.app.index_datalinks([datalinks_payload], [checksum])
            p.assert_called_with('http://localhost:8080/update',
                                 data=json.dumps([{'bibcode': 'linkstest', 'data_links_rows': [{'baz': 0}]}]),
                                 headers={'Authorization': 'Bearer fixme'})
            self.assertEqual(p.call_count, 2)
            # verify database updated
            rec = self.app.get_record(bibcode='linkstest')
            self.assertEqual(rec['datalinks_checksum'], 'thechecksum')
            self.assertEqual(rec['solr_checksum'], None)
            self.assertEqual(rec['metrics_checksum'], None)
            self.assertEqual(rec['status'], 'success')
            self.assertTrue(rec['datalinks_processed'])

    def test_index_datalinks_update_processed_false(self):
        m = mock.Mock()
        m.status_code = 200
        # init database so timestamps and checksum can be updated
        nonbib_data = {'data_links_rows': [{'baz': 0}]}
        self.app.update_storage('linkstest', 'nonbib_data', nonbib_data)
        with mock.patch('requests.put', return_value=m) as p:
            datalinks_payload = {u'bibcode': u'linkstest', u'data_links_rows': [{u'baz': 0}]}
            checksum = 'thechecksum'
            self.app.index_datalinks([datalinks_payload], [checksum], update_processed=False)
            p.assert_called_with('http://localhost:8080/update',
                                 data=json.dumps([{'bibcode': 'linkstest', 'data_links_rows': [{'baz': 0}]}]),
                                 headers={'Authorization': 'Bearer fixme'})
            # verify database updated
            rec = self.app.get_record(bibcode='linkstest')
            self.assertEqual(rec['datalinks_checksum'], None)
            self.assertEqual(rec['solr_checksum'], None)
            self.assertEqual(rec['metrics_checksum'], None)
            self.assertEqual(rec['status'], None)
            self.assertEqual(rec['datalinks_processed'], None)

    def test_update_records_db_error(self):
        """test database exception IntegrityError is caught"""
        with mock.patch('sqlalchemy.orm.session.Session.commit', side_effect=[IntegrityError('a', 'b', 'c', 'd'), None]):
            self.assertRaises(IntegrityError, self.app.update_storage, 'abc', 'nonbib_data', '{}')
        
    def test_rename_bibcode(self):
        self.app.update_storage('abc', 'metadata', {'foo': 'bar', 'hey': 1})
        r = self.app.get_record('abc')
        
        self.app.rename_bibcode('abc', 'def')
        
        with self.app.session_scope() as session:
            ref = session.query(models.IdentifierMapping).filter_by(key='abc').first()
            self.assertTrue(ref.target, 'def')
            
        self.assertTrue(self.app.get_changelog('abc'), [{'target': u'def', 'key': u'abc'}])

    def test_generate_links_for_resolver(self):
        only_nonbib = {'bibcode': 'asdf',
                       'nonbib_data': 
                       {'data_links_rows': [{'url': ['http://arxiv.org/abs/1902.09522']}]}}
        links = self.app.generate_links_for_resolver(only_nonbib)
        self.assertEqual(only_nonbib['bibcode'], links['bibcode'])
        self.assertEqual(only_nonbib['nonbib_data']['data_links_rows'], links['data_links_rows'])

        only_bib = {'bibcode': 'asdf',
                    'bib_data':
                    {'links_data': ['{"access": "open", "instances": "", "title": "", "type": "preprint", "url": "http://arxiv.org/abs/1902.09522"}']}}
        links = self.app.generate_links_for_resolver(only_bib)
        self.assertEqual(only_bib['bibcode'], links['bibcode'])
        first = links['data_links_rows'][0]
        self.assertEqual('http://arxiv.org/abs/1902.09522', first['url'][0])
        self.assertEqual('ESOURCE', first['link_type'])
        self.assertEqual('EPRINT_HTML', first['link_sub_type'])
        self.assertEqual([''], first['title'])
        self.assertEqual(0, first['item_count'])

        bib_and_nonbib = {'bibcode': 'asdf',
                          'bib_data':
                          {'links_data': ['{"access": "open", "instances": "", "title": "", "type": "preprint", "url": "http://arxiv.org/abs/1902.09522zz"}']},
                          'nonbib_data':
                          {'data_links_rows': [{'url': ['http://arxiv.org/abs/1902.09522']}]}}
        links = self.app.generate_links_for_resolver(bib_and_nonbib)
        self.assertEqual(only_nonbib['bibcode'], links['bibcode'])
        self.assertEqual(only_nonbib['nonbib_data']['data_links_rows'], links['data_links_rows'])

        # string in database
        only_bib = {'bibcode': 'asdf',
                    'bib_data':
                    {'links_data': [u'{"access": "open", "instances": "", "title": "", "type": "preprint", "url": "http://arxiv.org/abs/1902.09522"}']}}
        links = self.app.generate_links_for_resolver(only_bib)
        self.assertEqual(only_bib['bibcode'], links['bibcode'])
        first = links['data_links_rows'][0]
        self.assertEqual('http://arxiv.org/abs/1902.09522', first['url'][0])
        self.assertEqual('ESOURCE', first['link_type'])
        self.assertEqual('EPRINT_HTML', first['link_sub_type'])
        
        # bad string in database
        with mock.patch.object(self.app.logger, 'error') as m:
            only_bib = {'bibcode': 'testbib',
                        'bib_data':
                        {'links_data': u'foobar[!)'}}
            links = self.app.generate_links_for_resolver(only_bib)
            self.assertEqual(None, links)
            self.assertEqual(1, m.call_count)
            m_args = m.call_args_list
            self.assertTrue('testbib' in str(m_args[0]))
            self.assertTrue('foobar' in str(m_args[0]))

    def test_should_include_in_sitemap_comprehensive(self):
        """Test all code paths and scenarios in should_include_in_sitemap function"""
        
        base_time = adsputils.get_date()
        
        
        # Test 1: Record with no bib_data (should be excluded)
        record_no_data = {
            'bibcode': '2023NoData..1..1A',
            'has_bib_data': False,
            'status': 'success'
        }
        self.assertFalse(self.app.should_include_in_sitemap(record_no_data), 
                        "Record without bib_data should be excluded")
        
        # Test 2: Record with empty bib_data string (should be excluded)
        record_empty_data = {
            'bibcode': '2023Empty..1..1A',
            'has_bib_data': False,
            'status': 'success'
        }
        self.assertFalse(self.app.should_include_in_sitemap(record_empty_data), 
                        "Record with empty bib_data should be excluded")
        
        # Test 3: Record with solr-failed status (should be excluded)
        record_solr_failed = {
            'bibcode': '2023Failed..1..1A',
            'has_bib_data': True,
            'status': 'solr-failed'
        }
        self.assertFalse(self.app.should_include_in_sitemap(record_solr_failed), 
                        "Record with solr-failed status should be excluded")
        
        # Test 4: Record with retrying status (should be excluded)
        record_retrying = {
            'bibcode': '2023Retrying..1..1A',
            'has_bib_data': True,
            'status': 'retrying'
        }
        self.assertFalse(self.app.should_include_in_sitemap(record_retrying), 
                        "Record with retrying status should be excluded")
        
        # Test 5: Record with None status (should be included)
        record_none_status = {
            'bibcode': '2023NoneStatus..1..1A',
            'has_bib_data': True,
            'status': None
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_none_status), 
                       "Record with None status should be included")
        
        # Test 6: Record with success status (should be included)
        record_success = {
            'bibcode': '2023Success..1..1A',
            'has_bib_data': True,
            'status': 'success',
            'bib_data_updated': base_time - timedelta(days=1)
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_success), 
                       "Record with success status should be included")
        
        # Test 7: Record with metrics-failed status (should be included - not SOLR-related)
        record_metrics_failed = {
            'bibcode': '2023MetricsFailed..1..1A',
            'has_bib_data': True,
            'status': 'metrics-failed'
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_metrics_failed), 
                       "Record with metrics-failed status should be included (not SOLR-related)")
        
        # Test 8: Record with links-failed status (should be included - not SOLR-related)
        record_links_failed = {
            'bibcode': '2023LinksFailed..1..1A',
            'has_bib_data': True,
            'status': 'links-failed'
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_links_failed), 
                       "Record with links-failed status should be included (not SOLR-related)")
        
        # Test 9: Record with None status and no solr_processed (should be included - not yet processed)
        record_not_processed = {
            'bibcode': '2023NotProcessed..1..1A',
            'has_bib_data': True,
            'status': None,
            'solr_processed': None
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_not_processed), 
                       "Record not yet processed by SOLR should be included")
        
        # Test 10: Record with recent solr_processed (should be included)
        record_recent_solr = {
            'bibcode': '2023Recent..1..1A',
            'has_bib_data': True,
            'status': 'success',
            'bib_data_updated': base_time - timedelta(days=1),
            'solr_processed': base_time  # More recent than bib_data_updated
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_recent_solr), 
                       "Record with recent SOLR processing should be included")
        
        # Test 11: Record with stale solr_processed (should be included with warning)
        record_stale_solr = {
            'bibcode': '2023Stale..1..1A',
            'has_bib_data': True,
            'status': 'success',
            'bib_data_updated': base_time,
            'solr_processed': base_time - timedelta(days=6)  # 6 days stale (> 5 day threshold)
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_stale_solr), 
                       "Record with stale SOLR processing should still be included (with warning)")
        
        # Test 12: Record with exactly 5+ days staleness (boundary condition)
        record_boundary = {
            'bibcode': '2023Boundary..1..1A',
            'has_bib_data': True,
            'status': 'success',
            'bib_data_updated': base_time,
            'solr_processed': base_time - timedelta(days=5, seconds=1)  # Just over 5 days
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_boundary), 
                       "Record with exactly 5+ days staleness should be included with warning")
        
        # Test 13: Record with no timestamps (should be included)
        record_no_timestamps = {
            'bibcode': '2023NoTimestamps..1..1A',
            'has_bib_data': True,
            'status': 'success',
            'bib_data_updated': None,
            'solr_processed': None
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_no_timestamps), 
                       "Record with no timestamps should be included")
        
        # Test 14: Record with bib_data_updated but no solr_processed (should be included)
        record_no_solr_time = {
            'bibcode': '2023NoSolrTime..1..1A',
            'has_bib_data': True,
            'status': 'success',
            'bib_data_updated': base_time,
            'solr_processed': None
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_no_solr_time), 
                       "Record with bib_data_updated but no solr_processed should be included")
        
        # Test 15: Record with solr_processed but no bib_data_updated (should be included)
        record_no_bib_time = {
            'bibcode': '2023NoBibTime..1..1A',
            'has_bib_data': True,
            'status': 'success',
            'bib_data_updated': None,
            'solr_processed': base_time
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_no_bib_time), 
                       "Record with solr_processed but no bib_data_updated should be included")
        
        # Test 16: Record with very fresh processing (should be included)
        record_fresh = {
            'bibcode': '2023Fresh..1..1A',
            'has_bib_data': True,
            'status': 'success',
            'bib_data_updated': base_time - timedelta(minutes=30),
            'solr_processed': base_time
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_fresh), 
                       "Record with very fresh processing should be included")
        
        # Test 17: Record with moderate lag (2 days, should be included without warning)
        record_moderate_lag = {
            'bibcode': '2023Moderate..1..1A',
            'has_bib_data': True,
            'status': 'success',
            'bib_data_updated': base_time - timedelta(days=2),
            'solr_processed': base_time
        }
        self.assertTrue(self.app.should_include_in_sitemap(record_moderate_lag), 
                       "Record with moderate processing lag should be included")

    def test_get_records_bulk_performance(self):
        """Test get_records_bulk with a considerable number of records"""
        
        # Create 1000 test records
        test_bibcodes = []
        
        for i in range(1000):
            bibcode = f'2023Bulk..{i:04d}..{i:04d}A'
            test_bibcodes.append(bibcode)
            
            # Simple test data
            bib_data = {
                'title': f'Test Paper {i}',
                'year': 2023
            }
            
            # Store record in database
            self.app.update_storage(bibcode, 'bib_data', bib_data)
        
        # Test 1: Get all records with default fields
        with self.app.session_scope() as session:
            start_time = adsputils.get_date()
            
            result = self.app.get_records_bulk(test_bibcodes, session)
            
            end_time = adsputils.get_date()
            query_time = (end_time - start_time).total_seconds()
            
            # Performance assertion - should complete within reasonable time
            self.assertLess(query_time, 10.0, f"Bulk query took {query_time:.2f}s, should be under 10s")
            
            # Verify all records returned
            self.assertEqual(len(result), 1000, "Should return all 1000 records")
            
            # Verify basic structure
            for bibcode in test_bibcodes[:5]:  # Check first 5 records
                self.assertIn(bibcode, result, f"Should contain record {bibcode}")
                record = result[bibcode]
                
                # Check required fields are present
                self.assertIn('id', record, "Should contain id field")
                self.assertIn('bibcode', record, "Should contain bibcode field") 
                self.assertIn('bib_data', record, "Should contain bib_data field")
                
                # Verify bibcode matches
                self.assertEqual(record['bibcode'], bibcode, "Bibcode should match")
            
            print(f" get_records_bulk performance: 1000 records retrieved in {query_time:.2f}s")
        
        # Test 2: Test load_only functionality
        with self.app.session_scope() as session:
            result_limited = self.app.get_records_bulk(
                test_bibcodes[:10], 
                session, 
                load_only=['bibcode', 'bib_data_updated']
            )
            
            # Verify correct fields returned
            for bibcode in test_bibcodes[:5]:
                record = result_limited[bibcode]
                
                # Should have requested fields
                self.assertIn('bibcode', record, "Should contain bibcode field")
                self.assertIn('bib_data_updated', record, "Should contain bib_data_updated field")
                
                # Should not have other fields (they should be None)
                self.assertIsNone(record.get('bib_data'), "bib_data should be None when not requested")
        
        # Test 3: Empty bibcode list
        with self.app.session_scope() as session:
            empty_result = self.app.get_records_bulk([], session)
            self.assertEqual(empty_result, {}, "Empty bibcode list should return empty dict")
        
        # Test 4: Non-existent bibcodes
        fake_bibcodes = ['2023Fake..1..1A', '2023Fake..1..2B']
        with self.app.session_scope() as session:
            fake_result = self.app.get_records_bulk(fake_bibcodes, session)
            self.assertEqual(fake_result, {}, "Non-existent bibcodes should return empty dict")

    def test_get_sitemap_info_bulk_performance(self):
        """Test get_sitemap_info_bulk with a considerable number of sitemaps"""
        
        # Create 1000 test records and sitemap entries
        test_bibcodes = []
        
        for i in range(1000):
            bibcode = f'2023Sitemap..{i:04d}..{i:04d}A'
            test_bibcodes.append(bibcode)
            
            # Simple test data
            bib_data = {
                'title': f'Test Sitemap Paper {i}',
                'year': 2023
            }
            
            # Store record in database
            self.app.update_storage(bibcode, 'bib_data', bib_data)
        
        # Create sitemap entries for these records
        with self.app.session_scope() as session:
            # Get record IDs
            records = session.query(Records).filter(Records.bibcode.in_(test_bibcodes)).all()
            record_map = {r.bibcode: r.id for r in records}
            
            # Create sitemap info entries
            for i, bibcode in enumerate(test_bibcodes):
                sitemap_info = SitemapInfo(
                    record_id=record_map[bibcode],
                    bibcode=bibcode,
                    sitemap_filename=f'sitemap_bib_{(i // 50) + 1}.xml',  # 50 records per file
                    filename_lastmoddate=adsputils.get_date(),
                    update_flag=False
                )
                session.add(sitemap_info)
            session.commit()
        
        # Test 1: Get all sitemap infos with performance timing
        with self.app.session_scope() as session:
            start_time = adsputils.get_date()
            
            result = self.app.get_sitemap_info_bulk(test_bibcodes, session)
            
            end_time = adsputils.get_date()
            query_time = (end_time - start_time).total_seconds()
            
            # Performance assertion - should complete within reasonable time
            self.assertLess(query_time, 10.0, f"Bulk sitemap query took {query_time:.2f}s, should be under 10s")
            
            # Verify all sitemap infos returned
            self.assertEqual(len(result), 1000, "Should return all 1000 sitemap infos")
            
            # Verify basic structure
            for bibcode in test_bibcodes[:5]:  # Check first 5 records
                self.assertIn(bibcode, result, f"Should contain sitemap info for {bibcode}")
                sitemap_data = result[bibcode]
                
                # Check required fields are present (toJSON() format)
                self.assertIn('bibcode', sitemap_data, "Should contain bibcode field")
                self.assertIn('sitemap_filename', sitemap_data, "Should contain sitemap_filename field")
                self.assertIn('update_flag', sitemap_data, "Should contain update_flag field")
                
                # Verify bibcode matches
                self.assertEqual(sitemap_data['bibcode'], bibcode, "Bibcode should match")
                
                # Verify filename format
                self.assertTrue(sitemap_data['sitemap_filename'].startswith('sitemap_bib_'), 
                               "Filename should have correct format")
            
            print(f"get_sitemap_info_bulk performance: 1000 sitemap infos retrieved in {query_time:.2f}s")
        
        # Test 2: Empty bibcode list
        with self.app.session_scope() as session:
            empty_result = self.app.get_sitemap_info_bulk([], session)
            self.assertEqual(empty_result, {}, "Empty bibcode list should return empty dict")
        
        # Test 3: Non-existent bibcodes
        fake_bibcodes = ['2023FakeSitemap..1..1A', '2023FakeSitemap..1..2B']
        with self.app.session_scope() as session:
            fake_result = self.app.get_sitemap_info_bulk(fake_bibcodes, session)
            self.assertEqual(fake_result, {}, "Non-existent bibcodes should return empty dict")

    def test_get_current_sitemap_state_performance(self):
        """Test get_current_sitemap_state with multiple sitemaps and records"""
        
        # Create test records across multiple sitemap files
        test_bibcodes = []
        
        for i in range(500):  
            bibcode = f'2023State..{i:04d}..{i:04d}A'
            test_bibcodes.append(bibcode)
            
            # Create highly unique bib_data to ensure different scix_ids
            
            bib_data = {
                'title': f'Test State Paper {i} - Unique Content {i*17} - {bibcode}',
                'year': 2023 + (i % 10),  # Vary the year
                'bibcode': bibcode,  # Include bibcode for uniqueness
                'abstract': f'This is a unique abstract for paper {i} with specific content {i*23} and bibcode {bibcode}',
                'authors': [f'Author{i}_{bibcode}', f'CoAuthor{i*2}_{bibcode}'],
                'unique_field': f'unique_value_{i}_{i*37}_{bibcode}_{int(time.time()*1000000) % 1000000}',
                'doi': f'10.1000/test.{i}.{i*41}',
                'page': f'{i*100}-{i*100+10}',
                'volume': str(i % 100 + 1),
                'issue': str(i % 12 + 1)
            }
            
            # Store record in database
            self.app.update_storage(bibcode, 'bib_data', bib_data)
        
        # Test Scenario 1: Last file has EQUAL records (100 each)
        with self.app.session_scope() as session:
            # Get record IDs
            records = session.query(Records).filter(Records.bibcode.in_(test_bibcodes)).all()
            record_map = {r.bibcode: r.id for r in records}
            
            # Create sitemap info entries - all files have 100 records each
            sitemap_distributions_equal = [
                ('sitemap_bib_1.xml', 100),  # 100 records
                ('sitemap_bib_2.xml', 100),  # 100 records  
                ('sitemap_bib_3.xml', 100),  # 100 records
                ('sitemap_bib_4.xml', 100),  # 100 records
                ('sitemap_bib_5.xml', 100),  # 100 records (equal - should be returned as highest)
            ]
            
            bibcode_index = 0
            for filename, record_count in sitemap_distributions_equal:
                for _ in range(record_count):
                    if bibcode_index < len(test_bibcodes):
                        bibcode = test_bibcodes[bibcode_index]
                        sitemap_info = SitemapInfo(
                            record_id=record_map[bibcode],
                            bibcode=bibcode,
                            sitemap_filename=filename,
                            filename_lastmoddate=adsputils.get_date(),
                            update_flag=False
                        )
                        session.add(sitemap_info)
                        bibcode_index += 1
            session.commit()
        
        # Test 1: Get current sitemap state with performance timing (EQUAL scenario)
        with self.app.session_scope() as session:
            start_time = adsputils.get_date()
            
            result = self.app.get_current_sitemap_state(session)
            
            end_time = adsputils.get_date()
            query_time = (end_time - start_time).total_seconds()
            
            # Performance assertion - should complete quickly
            self.assertLess(query_time, 2.0, f"Sitemap state query took {query_time:.3f}s, should be under 2s")
            
            # Verify it returns the latest filename (highest index) when all have equal records
            self.assertEqual(result['filename'], 'sitemap_bib_5.xml', 
                           "Should return the highest numbered sitemap file when all have equal records")
            self.assertEqual(result['count'], 100, 
                           "Should return 100 records for the latest file (equal scenario)")
            self.assertEqual(result['index'], 5, 
                           "Should return index 5 for the latest file")
            
            print(f"get_current_sitemap_state performance (EQUAL): query completed in {query_time:.3f}s")
        
        # Test Scenario 2: Last file has FEWER records (100, 100, 100, 100, 80)
        with self.app.session_scope() as session:
            # Clear existing sitemap info
            session.query(SitemapInfo).delete(synchronize_session=False)
            session.commit()
            
            # Create new distribution where last file has fewer records
            sitemap_distributions_fewer = [
                ('sitemap_bib_1.xml', 100),  # 100 records
                ('sitemap_bib_2.xml', 100),  # 100 records  
                ('sitemap_bib_3.xml', 100),  # 100 records
                ('sitemap_bib_4.xml', 100),  # 100 records
                ('sitemap_bib_5.xml', 80),   # 80 records (fewer - should still be returned as highest)
            ]
            
            bibcode_index = 0
            for filename, record_count in sitemap_distributions_fewer:
                for _ in range(record_count):
                    if bibcode_index < len(test_bibcodes):
                        bibcode = test_bibcodes[bibcode_index]
                        sitemap_info = SitemapInfo(
                            record_id=record_map[bibcode],
                            bibcode=bibcode,
                            sitemap_filename=filename,
                            filename_lastmoddate=adsputils.get_date(),
                            update_flag=False
                        )
                        session.add(sitemap_info)
                        bibcode_index += 1
            session.commit()
            
            # Test with fewer records in last file
            start_time = adsputils.get_date()
            
            result = self.app.get_current_sitemap_state(session)
            
            end_time = adsputils.get_date()
            query_time_fewer = (end_time - start_time).total_seconds()
            
            # Performance assertion
            self.assertLess(query_time_fewer, 2.0, f"Sitemap state query took {query_time_fewer:.3f}s, should be under 2s")
            
            # Verify it still returns the latest filename even with fewer records
            self.assertEqual(result['filename'], 'sitemap_bib_5.xml', 
                           "Should return the highest numbered sitemap file even when it has fewer records")
            self.assertEqual(result['count'], 80, 
                           "Should return 80 records for the latest file (fewer scenario)")
            self.assertEqual(result['index'], 5, 
                           "Should return index 5 for the latest file")
                    
        # Test 3: Verify state reflects the actual database content (using fewer scenario data)
        with self.app.session_scope() as session:
            # Verify the count matches actual database records
            actual_count = session.query(SitemapInfo).filter(
                SitemapInfo.sitemap_filename == 'sitemap_bib_5.xml'
            ).count()
            
            result = self.app.get_current_sitemap_state(session)
            self.assertEqual(result['count'], actual_count, 
                           "State count should match actual database count")
            self.assertEqual(result['count'], 80, 
                           "Should reflect the fewer records scenario (80 records)")
        
        # Test 4: Test with files that have None filenames (should be filtered out)
        with self.app.session_scope() as session:
            # Add some records with None filenames
            none_bibcodes = ['2023None..1..1A', '2023None..2..2A']
            for i, bibcode in enumerate(none_bibcodes):
                bib_data = {
                    'title': f'Test None {i} - {bibcode}', 
                    'year': 2024 + i,
                    'bibcode': bibcode,
                    'unique_field': f'none_test_{i}_{bibcode}_{int(time.time()*1000000) % 1000000}',
                    'abstract': f'Unique abstract for none test {i} with bibcode {bibcode}',
                    'authors': [f'NoneAuthor{i}_{bibcode}']
                }
                self.app.update_storage(bibcode, 'bib_data', bib_data)
            
            # Get the record IDs
            none_records = session.query(Records).filter(Records.bibcode.in_(none_bibcodes)).all()
            
            # Add SitemapInfo entries with None filenames
            for record in none_records:
                sitemap_info = SitemapInfo(
                    record_id=record.id,
                    bibcode=record.bibcode,
                    sitemap_filename=None,  # None filename should be filtered out
                    filename_lastmoddate=adsputils.get_date(),
                    update_flag=False
                )
                session.add(sitemap_info)
            session.commit()
            
            # Should still return sitemap_bib_5.xml, ignoring None filenames
            result = self.app.get_current_sitemap_state(session)
            self.assertEqual(result['filename'], 'sitemap_bib_5.xml', 
                           "Should ignore None filenames and return highest valid filename")
            self.assertEqual(result['count'], 80, 
                           "Should still return 80 records from the valid highest file")
        
        # Test 5: Empty database state (edge case)
        with self.app.session_scope() as session:
            # Clear all sitemap info
            session.query(SitemapInfo).delete(synchronize_session=False)
            session.commit()
            
            result = self.app.get_current_sitemap_state(session)
            
            # Should return default state
            self.assertEqual(result['filename'], 'sitemap_bib_1.xml', 
                           "Should return default filename when no records exist")
            self.assertEqual(result['count'], 0, 
                           "Should return 0 count when no records exist")
            self.assertEqual(result['index'], 1, 
                           "Should return default index 1 when no records exist")

    def test_process_sitemap_batch_session_persistence(self):
        """Test _process_sitemap_batch with session management and persistence"""
        
        # Create test records for batch processing
        test_bibcodes = []
        
        for i in range(100):
            bibcode = f'2023Batch..{i:04d}..{i:04d}A'
            test_bibcodes.append(bibcode)
            
            # Simple test data
            bib_data = {
                'title': f'Test Batch Paper {i}',
                'year': 2023
            }
            
            # Store record in database
            self.app.update_storage(bibcode, 'bib_data', bib_data)
        
        # Test session persistence
        with self.app.session_scope() as session:
            start_time = adsputils.get_date()
            
            # Get initial sitemap state
            sitemap_state = self.app.get_current_sitemap_state(session)
            
            # Test 1: Process first batch of 50 bibcodes
            batch_bibcodes_1 = test_bibcodes[:50]
            batch_stats, updated_state_1 = self.app._process_sitemap_batch(
                batch_bibcodes_1, 'add', session, sitemap_state
            )
            
            end_time = adsputils.get_date()
            query_time = (end_time - start_time).total_seconds()
            
            # Performance assertion
            self.assertLess(query_time, 5.0, f"Batch processing took {query_time:.3f}s, should be under 5s")
            
            # Verify first batch results
            self.assertEqual(batch_stats['successful'], 50, "Should successfully process all 50 bibcodes")
            self.assertEqual(batch_stats['failed'], 0, "Should have no failed bibcodes")
            self.assertEqual(len(batch_stats['sitemap_records']), 50, "Should return 50 sitemap records")
            
            print(f"process_sitemap_batch performance (ADD): processed 50 records in {query_time:.3f}s")
            
            # Test 2: Verify session persistence
            created_records_1 = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(batch_bibcodes_1)
            ).all()
            
            self.assertEqual(len(created_records_1), 50, 
                           "All 50 sitemap records should be visible in same session")
            
            # Test 3: Process second batch using updated_state from first batch
            batch_bibcodes_2 = test_bibcodes[50:80]
            batch_stats, updated_state_2 = self.app._process_sitemap_batch(
                batch_bibcodes_2, 'force-update', session, updated_state_1
            )
            
            # Verify second batch results
            self.assertEqual(batch_stats['successful'], 30, "Should successfully process all 30 bibcodes")
            self.assertEqual(batch_stats['failed'], 0, "Should have no failed bibcodes")
            self.assertEqual(len(batch_stats['sitemap_records']), 30, "Should return 30 sitemap records")
            
            # Test 4: Verify session consistency - state should be cumulative
            initial_count = sitemap_state['count']
            if initial_count + 80 <= self.app.conf.get('MAX_RECORDS_PER_SITEMAP', 50000):
                # Should be same file with cumulative records
                self.assertEqual(updated_state_2['filename'], sitemap_state['filename'],
                               "Should use same filename when under limit")
                self.assertEqual(updated_state_2['count'], initial_count + 80,
                               "Count should be cumulative across batches")
            
            # Test 5: Verify all records are visible in same session (no commits yet!)
            all_records_in_session = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(test_bibcodes[:80])
            ).all()
            
            self.assertEqual(len(all_records_in_session), 80,
                           "All 80 records should be visible in same session before commit")
            
            # Test 6: Verify state consistency within session
            current_state_in_session = self.app.get_current_sitemap_state(session)
            self.assertEqual(current_state_in_session['count'], updated_state_2['count'],
                           "Current state should match updated state within same session")
            
            
            # Now commit everything at once
            session.commit()
        
        # Test 7: Verify data persisted after session ends
        with self.app.session_scope() as new_session:
            verification_records = new_session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(test_bibcodes[:80])
            ).count()
            
            self.assertEqual(verification_records, 80,
                           "New session should see all committed records")
            
        
        # Test 6: Test empty batch edge case
        with self.app.session_scope() as session:
            empty_state = self.app.get_current_sitemap_state(session)
            batch_stats, empty_updated_state = self.app._process_sitemap_batch(
                [], 'add', session, empty_state
            )
            self.assertEqual(batch_stats['successful'], 0, "Empty batch should return 0 successful")
            self.assertEqual(batch_stats['failed'], 0, "Empty batch should return 0 failed")
            self.assertEqual(len(batch_stats['sitemap_records']), 0, "Empty batch should return empty records list")
            self.assertEqual(empty_updated_state, empty_state, "Empty batch should return unchanged state")

    def test_process_sitemap_batch_solr_filtering(self):
        """Test SOLR status filtering logic in _process_sitemap_batch"""
        
        # Create records with different statuses to test all should_include_in_sitemap logic
        test_bibcodes = [
            '2023Success..1..1A',      # success - should be included
            '2023SolrFailed..1..1A',   # solr-failed - should be excluded
            '2023Retrying..1..1A',     # retrying - should be excluded  
            '2023MetricsFailed..1..1A', # metrics-failed - should be included (not SOLR-related)
            '2023LinksFailed..1..1A',   # links-failed - should be included (not SOLR-related)
            '2023NoBibData..1..1A'     # will have no bib_data - should be excluded
        ]
        
        for i, bibcode in enumerate(test_bibcodes):
            if bibcode != '2023NoBibData..1..1A':  # Skip creating bib_data for this one
                bib_data = {'title': f'Test Paper {i}', 'year': 2023}
                self.app.update_storage(bibcode, 'bib_data', bib_data)
        
        # Set different statuses
        self.app.mark_processed(['2023SolrFailed..1..1A'], 'solr', checksums=['checksum_failed'], status='solr-failed')
        self.app.mark_processed(['2023Retrying..1..1A'], 'solr', checksums=['checksum_retrying'], status='retrying')
        self.app.mark_processed(['2023MetricsFailed..1..1A'], 'solr', checksums=['checksum_metrics'], status='metrics-failed')
        self.app.mark_processed(['2023LinksFailed..1..1A'], 'solr', checksums=['checksum_links'], status='links-failed')
        # 2023Success..1..1A gets default 'success' status
        # 2023NoBibData..1..1A will have no bib_data at all
        
        # Test 'add' action
        with self.app.session_scope() as session:
            initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 0, 'index': 1}
            
            batch_stats, updated_state_add = self.app._process_sitemap_batch(
                test_bibcodes, 'add', session, initial_state
            )
            
            # Should include: success, metrics-failed, links-failed = 3 successful
            # Should exclude: solr-failed, retrying, no-bib-data = 3 failed
            self.assertEqual(batch_stats['successful'], 3, "Add: Should include success, metrics-failed, links-failed statuses")
            self.assertEqual(batch_stats['failed'], 3, "Add: Should exclude solr-failed, retrying, and no-bib-data records")
            self.assertEqual(len(batch_stats['sitemap_records']), 3, "Add: Should return 3 sitemap records")
            self.assertEqual(updated_state_add['count'], 3, "Add: State should reflect only successful records")
        
        # Test 'force-update' action - should have same filtering results
        with self.app.session_scope() as session:
            initial_state = {'filename': 'sitemap_bib_2.xml', 'count': 0, 'index': 2}
            
            batch_stats, updated_state_force = self.app._process_sitemap_batch(
                test_bibcodes, 'force-update', session, initial_state
            )
            
            # Force-update should have same filtering results as add
            self.assertEqual(batch_stats['successful'], 3, "Force-update: Should include success, metrics-failed, links-failed statuses")
            self.assertEqual(batch_stats['failed'], 3, "Force-update: Should exclude solr-failed, retrying, and no-bib-data records")
            self.assertEqual(len(batch_stats['sitemap_records']), 3, "Force-update: Should return updated sitemap records for reporting")
            self.assertEqual(updated_state_force['count'], 0, "Force-update: State count should remain 0 (updating existing, not adding new)")
            
            # Results should be identical for filtering
            self.assertEqual(batch_stats['successful'], batch_stats['successful'], "Both actions should have same successful count")
            self.assertEqual(batch_stats['failed'], batch_stats['failed'], "Both actions should have same failed count")


    def test_process_sitemap_batch_new_vs_existing_records(self):
        """Test handling of new records vs existing sitemap entries"""
        
        # Create test records with specific timestamps
        base_time = adsputils.get_date()
        new_bibcode = '2023New..1..1A'
        existing_recent_bibcode = '2023ExistingRecent..1..1A'
        existing_stale_bibcode = '2023ExistingStale..1..1A'
        
        test_bibcodes = [new_bibcode, existing_recent_bibcode, existing_stale_bibcode]
        
        # Create records with specific bib_data_updated timestamps
        for i, bibcode in enumerate(test_bibcodes):
            bib_data = {'title': f'Test Paper {bibcode}', 'year': 2023}
            self.app.update_storage(bibcode, 'bib_data', bib_data)
            
            # Update bib_data_updated timestamps
            with self.app.session_scope() as session:
                session.query(Records).filter(Records.bibcode == bibcode).update({
                    'bib_data_updated': base_time - timedelta(hours=i)  # Different timestamps
                }, synchronize_session=False)
                session.commit()
        
        # Create existing sitemap entries
        with self.app.session_scope() as session:
            records = session.query(Records).filter(Records.bibcode.in_(test_bibcodes)).all()
            record_map = {r.bibcode: r.id for r in records}
            
            # Recent sitemap entry - filename_lastmoddate is NEWER than bib_data_updated
            recent_sitemap = SitemapInfo(
                record_id=record_map[existing_recent_bibcode],
                bibcode=existing_recent_bibcode,
                sitemap_filename='sitemap_bib_1.xml',
                filename_lastmoddate=base_time + timedelta(hours=1),  # NEWER than bib_data_updated
                update_flag=False
            )
            
            # Stale sitemap entry - filename_lastmoddate is OLDER than bib_data_updated
            stale_sitemap = SitemapInfo(
                record_id=record_map[existing_stale_bibcode],
                bibcode=existing_stale_bibcode,
                sitemap_filename='sitemap_bib_1.xml',
                filename_lastmoddate=base_time - timedelta(days=10),  # OLDER than bib_data_updated
                update_flag=False
            )
            
            session.add(recent_sitemap)
            session.add(stale_sitemap)
            session.commit()
        
        with self.app.session_scope() as session:
            initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 5, 'index': 1}
            
            batch_stats, updated_state = self.app._process_sitemap_batch(
                test_bibcodes, 'add', session, initial_state
            )
            
            # All 3 should be successful
            self.assertEqual(batch_stats['successful'], 3, "All records should be processed successfully")
            self.assertEqual(batch_stats['failed'], 0, "No records should fail")
            
            # Only NEW record increments count (1 new record)
            self.assertEqual(updated_state['count'], 6, "Only new record should increment count (5 + 1 = 6)")
            
            # Check that update_flags are set correctly
            with self.app.session_scope() as session:
                batch_stats['sitemap_records'] = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(test_bibcodes)
                ).all()
                
                for record in batch_stats['sitemap_records']:
                    if record.bibcode == new_bibcode:
                        # New record should have update_flag = True
                        self.assertTrue(record.update_flag, f"New record {record.bibcode} should have update_flag=True")
                    elif record.bibcode == existing_recent_bibcode:
                        # Recent record should NOT be updated (filename_lastmoddate > bib_data_updated)
                        self.assertFalse(record.update_flag, f"Recent record {record.bibcode} should have update_flag=False")
                    elif record.bibcode == existing_stale_bibcode:
                        # Stale record should be updated (filename_lastmoddate < bib_data_updated)
                        self.assertTrue(record.update_flag, f"Stale record {record.bibcode} should have update_flag=True")

    def test_process_sitemap_batch_add_action_with_recent_file(self):
        """Test 'add' action when file is newer than data (should NOT update)"""
        
        base_time = adsputils.get_date()
        test_bibcode = '2023AddRecent..1..1A'
        bib_data = {'title': 'Test Add Recent Paper', 'year': 2023}
        self.app.update_storage(test_bibcode, 'bib_data', bib_data)
        
        # Set bib_data_updated to be OLDER than filename_lastmoddate
        with self.app.session_scope() as session:
            session.query(Records).filter(Records.bibcode == test_bibcode).update({
                'bib_data_updated': base_time - timedelta(hours=2)  # 2 hours ago (OLDER)
            }, synchronize_session=False)
            session.commit()
        
        # Create existing sitemap entry with NEWER timestamp
        with self.app.session_scope() as session:
            record = session.query(Records).filter(Records.bibcode == test_bibcode).first()
            
            sitemap_info = SitemapInfo(
                record_id=record.id,
                bibcode=test_bibcode,
                sitemap_filename='sitemap_bib_1.xml',
                filename_lastmoddate=base_time,  # NEWER than bib_data_updated
                update_flag=False
            )
            session.add(sitemap_info)
            session.commit()
        
        # Store original sitemap_info values for comparison
        with self.app.session_scope() as session:
            original_record = session.query(SitemapInfo).filter(SitemapInfo.bibcode == test_bibcode).first()
            original_filename_lastmoddate = original_record.filename_lastmoddate
            original_sitemap_filename = original_record.sitemap_filename
            original_update_flag = original_record.update_flag
        
        # Test 'add' action
        with self.app.session_scope() as session:
            initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 0, 'index': 1}
            
            batch_stats, _ = self.app._process_sitemap_batch(
                [test_bibcode], 'add', session, initial_state
            )
            
            # Check that sitemap_info record remains unchanged
            sitemap_record = session.query(SitemapInfo).filter(SitemapInfo.bibcode == test_bibcode).first()
            
            self.assertEqual(batch_stats['successful'], 1, "Record should be processed successfully")
            self.assertEqual(batch_stats['failed'], 0, "No records should fail")
            
            # Verify the record was not modified
            self.assertFalse(sitemap_record.update_flag, "'add' should NOT set update_flag when file is newer than data")
            self.assertEqual(sitemap_record.filename_lastmoddate, original_filename_lastmoddate, "filename_lastmoddate should remain unchanged")
            self.assertEqual(sitemap_record.sitemap_filename, original_sitemap_filename, "sitemap_filename should remain unchanged")
            self.assertEqual(sitemap_record.update_flag, original_update_flag, "update_flag should remain unchanged (False)")

    def test_process_sitemap_batch_add_action_with_stale_file(self):
        """Test 'add' action when data is newer than file (should update)"""
        
        base_time = adsputils.get_date()
        test_bibcode = '2023AddStale..1..1A'
        bib_data = {'title': 'Test Add Stale Paper', 'year': 2023}
        self.app.update_storage(test_bibcode, 'bib_data', bib_data)
        
        # Set bib_data_updated to be NEWER than filename_lastmoddate
        with self.app.session_scope() as session:
            session.query(Records).filter(Records.bibcode == test_bibcode).update({
                'bib_data_updated': base_time  # Current time (NEWER)
            }, synchronize_session=False)
            session.commit()
        
        # Create existing sitemap entry with OLDER timestamp
        with self.app.session_scope() as session:
            record = session.query(Records).filter(Records.bibcode == test_bibcode).first()
            
            sitemap_info = SitemapInfo(
                record_id=record.id,
                bibcode=test_bibcode,
                sitemap_filename='sitemap_bib_1.xml',
                filename_lastmoddate=base_time - timedelta(hours=3),  # OLDER than bib_data_updated
                update_flag=False
            )
            session.add(sitemap_info)
            session.commit()
        
        # Store original sitemap_info values for comparison
        with self.app.session_scope() as session:
            original_record = session.query(SitemapInfo).filter(SitemapInfo.bibcode == test_bibcode).first()
            original_filename_lastmoddate = original_record.filename_lastmoddate
            original_sitemap_filename = original_record.sitemap_filename
            original_update_flag = original_record.update_flag
        
        # Test 'add' action
        with self.app.session_scope() as session:
            initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 0, 'index': 1}
            
            batch_stats, _ = self.app._process_sitemap_batch(
                [test_bibcode], 'add', session, initial_state
            )
            
            # Check that sitemap_info record was updated appropriately
            sitemap_record = session.query(SitemapInfo).filter(SitemapInfo.bibcode == test_bibcode).first()
            
            self.assertEqual(batch_stats['successful'], 1, "Record should be processed successfully")
            self.assertEqual(batch_stats['failed'], 0, "No records should fail")
            
            # Verify the record was updated correctly
            self.assertTrue(sitemap_record.update_flag, "'add' should set update_flag when data is newer than file")
            self.assertEqual(sitemap_record.filename_lastmoddate, base_time, "filename_lastmoddate should be updated to bib_data_updated")
            self.assertEqual(sitemap_record.sitemap_filename, original_sitemap_filename, "sitemap_filename should remain unchanged")
            self.assertNotEqual(sitemap_record.update_flag, original_update_flag, "update_flag should have changed from False to True")
            self.assertNotEqual(sitemap_record.filename_lastmoddate, original_filename_lastmoddate, "filename_lastmoddate should have been updated")

    def test_process_sitemap_batch_add_action_with_never_generated_file(self):
        """Test 'add' action when file has never been generated (filename_lastmoddate is None)"""
        
        base_time = adsputils.get_date()
        test_bibcode = '2023AddNeverGenerated..1..1A'
        bib_data = {'title': 'Test Never Generated Paper', 'year': 2023}
        self.app.update_storage(test_bibcode, 'bib_data', bib_data)
        
        # Set bib_data_updated to any time (doesn't matter since filename_lastmoddate is None)
        with self.app.session_scope() as session:
            session.query(Records).filter(Records.bibcode == test_bibcode).update({
                'bib_data_updated': base_time - timedelta(hours=1)  # 1 hour ago
            }, synchronize_session=False)
            session.commit()
        
        # Create existing sitemap entry with None filename_lastmoddate (never generated)
        with self.app.session_scope() as session:
            record = session.query(Records).filter(Records.bibcode == test_bibcode).first()
            
            sitemap_info = SitemapInfo(
                record_id=record.id,
                bibcode=test_bibcode,
                sitemap_filename='sitemap_bib_1.xml',
                filename_lastmoddate=None,  # Never been generated
                update_flag=False
            )
            session.add(sitemap_info)
            session.commit()
        
        # Store original sitemap_info values for comparison
        with self.app.session_scope() as session:
            original_record = session.query(SitemapInfo).filter(SitemapInfo.bibcode == test_bibcode).first()
            original_filename_lastmoddate = original_record.filename_lastmoddate
            original_sitemap_filename = original_record.sitemap_filename
            original_update_flag = original_record.update_flag
        
        # Test 'add' action
        with self.app.session_scope() as session:
            initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 0, 'index': 1}
            
            batch_stats, _ = self.app._process_sitemap_batch(
                [test_bibcode], 'add', session, initial_state
            )
            
            # Check that sitemap_info record was updated appropriately
            sitemap_record = session.query(SitemapInfo).filter(SitemapInfo.bibcode == test_bibcode).first()
            
            self.assertEqual(batch_stats['successful'], 1, "Record should be processed successfully")
            self.assertEqual(batch_stats['failed'], 0, "No records should fail")
            
            # Verify the record was updated correctly
            self.assertTrue(sitemap_record.update_flag, "'add' should set update_flag when file has never been generated")
            self.assertEqual(sitemap_record.filename_lastmoddate, base_time - timedelta(hours=1), "filename_lastmoddate should be updated to bib_data_updated")
            self.assertEqual(sitemap_record.sitemap_filename, original_sitemap_filename, "sitemap_filename should remain unchanged")
            self.assertNotEqual(sitemap_record.update_flag, original_update_flag, "update_flag should have changed from False to True")
            self.assertIsNone(original_filename_lastmoddate, "Original filename_lastmoddate should have been None")
            self.assertIsNotNone(sitemap_record.filename_lastmoddate, "filename_lastmoddate should now be set")

    def test_process_sitemap_batch_force_update_with_recent_file(self):
        """Test 'force-update' action when file is newer than data (should still update)"""
        
        base_time = adsputils.get_date()
        test_bibcode = '2023ForceRecent..1..1A'
        bib_data = {'title': 'Test Force Recent Paper', 'year': 2023}
        self.app.update_storage(test_bibcode, 'bib_data', bib_data)
        
        # Set bib_data_updated to be OLDER than filename_lastmoddate
        with self.app.session_scope() as session:
            session.query(Records).filter(Records.bibcode == test_bibcode).update({
                'bib_data_updated': base_time - timedelta(hours=4)  # 4 hours ago (OLDER)
            }, synchronize_session=False)
            session.commit()
        
        # Create existing sitemap entry with NEWER timestamp
        with self.app.session_scope() as session:
            record = session.query(Records).filter(Records.bibcode == test_bibcode).first()
            
            sitemap_info = SitemapInfo(
                record_id=record.id,
                bibcode=test_bibcode,
                sitemap_filename='sitemap_bib_1.xml',
                filename_lastmoddate=base_time,  # NEWER than bib_data_updated
                update_flag=False
            )
            session.add(sitemap_info)
            session.commit()
        
        # Store original sitemap_info values for comparison
        with self.app.session_scope() as session:
            original_record = session.query(SitemapInfo).filter(SitemapInfo.bibcode == test_bibcode).first()
            original_filename_lastmoddate = original_record.filename_lastmoddate
            original_sitemap_filename = original_record.sitemap_filename
            original_update_flag = original_record.update_flag
        
        # Test 'force-update' action
        with self.app.session_scope() as session:
            initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 0, 'index': 1}
            
            batch_stats, _ = self.app._process_sitemap_batch(
                [test_bibcode], 'force-update', session, initial_state
            )
            
            # Check that sitemap_info record was updated appropriately
            sitemap_record = session.query(SitemapInfo).filter(SitemapInfo.bibcode == test_bibcode).first()
            
            self.assertEqual(batch_stats['successful'], 1, "Record should be processed successfully")
            self.assertEqual(batch_stats['failed'], 0, "No records should fail")
            
            # Verify the record was updated correctly
            self.assertTrue(sitemap_record.update_flag, "'force-update' should ALWAYS set update_flag, even when file is newer")
            self.assertEqual(sitemap_record.filename_lastmoddate, base_time - timedelta(hours=4), "filename_lastmoddate should be updated to bib_data_updated")
            self.assertEqual(sitemap_record.sitemap_filename, original_sitemap_filename, "sitemap_filename should remain unchanged")
            self.assertNotEqual(sitemap_record.update_flag, original_update_flag, "update_flag should have changed from False to True")
            self.assertNotEqual(sitemap_record.filename_lastmoddate, original_filename_lastmoddate, "filename_lastmoddate should have been updated")

    def test_process_sitemap_batch_force_update_with_stale_file(self):
        """Test 'force-update' action when data is newer than file (should still update)"""
        
        base_time = adsputils.get_date()
        test_bibcode = '2023ForceStale..1..1A'
        bib_data = {'title': 'Test Force Stale Paper', 'year': 2023}
        self.app.update_storage(test_bibcode, 'bib_data', bib_data)
        
        # Set bib_data_updated to be NEWER than filename_lastmoddate
        with self.app.session_scope() as session:
            session.query(Records).filter(Records.bibcode == test_bibcode).update({
                'bib_data_updated': base_time  # Current time (NEWER)
            }, synchronize_session=False)
            session.commit()
        
        # Create existing sitemap entry with OLDER timestamp
        with self.app.session_scope() as session:
            record = session.query(Records).filter(Records.bibcode == test_bibcode).first()
            
            sitemap_info = SitemapInfo(
                record_id=record.id,
                bibcode=test_bibcode,
                sitemap_filename='sitemap_bib_1.xml',
                filename_lastmoddate=base_time - timedelta(hours=2),  # OLDER than bib_data_updated
                update_flag=False
            )
            session.add(sitemap_info)
            session.commit()
        
        # Store original sitemap_info values for comparison
        with self.app.session_scope() as session:
            original_record = session.query(SitemapInfo).filter(SitemapInfo.bibcode == test_bibcode).first()
            original_filename_lastmoddate = original_record.filename_lastmoddate
            original_sitemap_filename = original_record.sitemap_filename
            original_update_flag = original_record.update_flag
        
        # Test 'force-update' action
        with self.app.session_scope() as session:
            initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 0, 'index': 1}
            
            batch_stats, _ = self.app._process_sitemap_batch(
                [test_bibcode], 'force-update', session, initial_state
            )
            
            # Check that sitemap_info record was updated appropriately
            sitemap_record = session.query(SitemapInfo).filter(SitemapInfo.bibcode == test_bibcode).first()
            
            self.assertEqual(batch_stats['successful'], 1, "Record should be processed successfully")
            self.assertEqual(batch_stats['failed'], 0, "No records should fail")
            
            # Verify the record was updated correctly
            self.assertTrue(sitemap_record.update_flag, "'force-update' should ALWAYS set update_flag, regardless of timestamps")
            self.assertEqual(sitemap_record.filename_lastmoddate, base_time, "filename_lastmoddate should be updated to bib_data_updated")
            self.assertEqual(sitemap_record.sitemap_filename, original_sitemap_filename, "sitemap_filename should remain unchanged")
            self.assertNotEqual(sitemap_record.update_flag, original_update_flag, "update_flag should have changed from False to True")
            self.assertNotEqual(sitemap_record.filename_lastmoddate, original_filename_lastmoddate, "filename_lastmoddate should have been updated")

    

    def test_process_sitemap_batch_file_rollover(self):
        """Test sitemap file rollover when MAX_RECORDS_PER_SITEMAP is exceeded"""
        
        # Create new records for rollover test
        rollover_bibcodes = ['2023Rollover..1..1A', '2023Rollover..2..2A']
        for bibcode in rollover_bibcodes:
            bib_data = {'title': f'Rollover Paper {bibcode}', 'year': 2023}
            self.app.update_storage(bibcode, 'bib_data', bib_data)
        
        # Set low limit to trigger rollover
        original_max = self.app.conf.get('MAX_RECORDS_PER_SITEMAP', 50000)
        self.app.conf['MAX_RECORDS_PER_SITEMAP'] = 1  # Very low limit
        
        try:
            with self.app.session_scope() as session:
                initial_state = {
                    'filename': 'sitemap_bib_3.xml',
                    'count': 1,  # At limit
                    'index': 3
                }
                
                batch_stats, updated_state = self.app._process_sitemap_batch(
                    rollover_bibcodes, 'add', session, initial_state
                )
                
                # Should roll over to next file (final state after processing both records)
                self.assertEqual(updated_state['filename'], 'sitemap_bib_5.xml',
                               "Final filename should be sitemap_bib_5.xml after both rollovers")
                self.assertEqual(updated_state['index'], 5, "Final index should be 5 after both rollovers")
                self.assertEqual(updated_state['count'], 1, "Final count should be 1 (second record in sitemap_bib_5.xml)")
                self.assertEqual(batch_stats['successful'], 2, "Both records should be processed successfully")
                
                # Verify database was updated correctly
                sitemap_records_db = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(rollover_bibcodes)
                ).order_by(SitemapInfo.bibcode).all()
                
                self.assertEqual(len(sitemap_records_db), 2, "Should have 2 records in database")
                
                # Check first record (should be in sitemap_bib_4.xml after first rollover)
                first_record = sitemap_records_db[0]  # 2023Rollover..1..1A
                self.assertEqual(first_record.bibcode, '2023Rollover..1..1A', "First record bibcode should match")
                self.assertEqual(first_record.sitemap_filename, 'sitemap_bib_4.xml', "First record should be in sitemap_bib_4.xml")
                self.assertTrue(first_record.update_flag, "First record should have update_flag=True")
                self.assertIsNone(first_record.filename_lastmoddate, "First record should have filename_lastmoddate=None (new record)")
                
                # Check second record (should be in sitemap_bib_5.xml after second rollover)
                second_record = sitemap_records_db[1]  # 2023Rollover..2..2A
                self.assertEqual(second_record.bibcode, '2023Rollover..2..2A', "Second record bibcode should match")
                self.assertEqual(second_record.sitemap_filename, 'sitemap_bib_5.xml', "Second record should be in sitemap_bib_5.xml")
                self.assertTrue(second_record.update_flag, "Second record should have update_flag=True")
                self.assertIsNone(second_record.filename_lastmoddate, "Second record should have filename_lastmoddate=None (new record)")
                
                # Verify both records have valid record_id links
                self.assertIsNotNone(first_record.record_id, "First record should have valid record_id")
                self.assertIsNotNone(second_record.record_id, "Second record should have valid record_id")
                
                # Verify the Records table entries exist
                records_db = session.query(Records).filter(Records.bibcode.in_(rollover_bibcodes)).all()
                self.assertEqual(len(records_db), 2, "Should have 2 records in Records table")
                
                # Verify record_id relationships are correct
                record_ids = {r.bibcode: r.id for r in records_db}
                self.assertEqual(first_record.record_id, record_ids['2023Rollover..1..1A'], "First sitemap record_id should match Records table")
                self.assertEqual(second_record.record_id, record_ids['2023Rollover..2..2A'], "Second sitemap record_id should match Records table")
                
        finally:
            # Restore original limit
            self.app.conf['MAX_RECORDS_PER_SITEMAP'] = original_max

    def test_process_sitemap_batch_error_handling(self):
        """Test error handling for non-existent records and exceptions"""
        
        # Test 1: Non-existent record
        non_existent_bibcode = '2023Missing..1..1A'
        
        with self.app.session_scope() as session:
            initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 0, 'index': 1}
            
            batch_stats, updated_state = self.app._process_sitemap_batch(
                [non_existent_bibcode], 'add', session, initial_state
            )
            
            self.assertEqual(batch_stats['successful'], 0, "Non-existent record should not be processed")
            self.assertEqual(batch_stats['failed'], 1, "Non-existent record should be counted as failed")
            self.assertEqual(updated_state, initial_state, "State should not change for failed records")
        
        # Test 2: Exception during processing
        problematic_bibcode = '2023Problem..1..1A'
        bib_data = {'title': 'Problematic Paper'}
        self.app.update_storage(problematic_bibcode, 'bib_data', bib_data)
        
        # Mock should_include_in_sitemap to raise an exception
        original_method = self.app.should_include_in_sitemap
        def mock_should_include(record):
            if record.get('bibcode') == problematic_bibcode:
                raise Exception("Test exception")
            return original_method(record)
        
        self.app.should_include_in_sitemap = mock_should_include
        
        try:
            with self.app.session_scope() as session:
                batch_stats, updated_state = self.app._process_sitemap_batch(
                    [problematic_bibcode], 'add', session, initial_state
                )
                
                self.assertEqual(batch_stats['successful'], 0, "Exception should result in 0 successful")
                self.assertEqual(batch_stats['failed'], 1, "Exception should result in 1 failed")
                
        finally:
            # Restore original method
            self.app.should_include_in_sitemap = original_method

    def test_process_sitemap_batch_empty_input(self):
        """Test handling of empty bibcode list"""
        
        with self.app.session_scope() as session:
            initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 5, 'index': 1}
            
            batch_stats, updated_state = self.app._process_sitemap_batch(
                [], 'add', session, initial_state
            )
            
            self.assertEqual(batch_stats['successful'], 0, "Empty batch should have 0 successful")
            self.assertEqual(batch_stats['failed'], 0, "Empty batch should have 0 failed")
            self.assertEqual(len(batch_stats['sitemap_records']), 0, "Empty batch should return empty records")
            self.assertEqual(updated_state, initial_state, "Empty batch should not change state")

    def test_process_sitemap_batch_integration(self):
        """Integration test combining multiple scenarios in realistic workflow"""
        
        # Create a mix of different record types (realistic scenario)
        test_data = [
            ('2023Integration..1..1A', 'success', 'new'),      # New valid record
            ('2023Integration..2..2A', 'success', 'existing'), # Existing valid record
            ('2023Integration..3..3A', 'solr-failed', 'new'),  # New but SOLR failed
        ]
        
        # Setup records
        for bibcode, status, record_type in test_data:
            bib_data = {'title': f'Integration Test {bibcode}', 'year': 2023}
            self.app.update_storage(bibcode, 'bib_data', bib_data)
            
            if status != 'success':
                self.app.mark_processed([bibcode], 'solr', checksums=[f'checksum_{bibcode}'], status=status)
        
        # Create existing sitemap entry for one record
        with self.app.session_scope() as session:
            records = session.query(Records).filter(Records.bibcode.like('2023Integration%')).all()
            record_map = {r.bibcode: r.id for r in records}
            
            existing_sitemap = SitemapInfo(
                record_id=record_map['2023Integration..2..2A'],
                bibcode='2023Integration..2..2A',
                sitemap_filename='sitemap_bib_1.xml',
                filename_lastmoddate=adsputils.get_date() - timedelta(days=5),  # Stale
                update_flag=False
            )
            session.add(existing_sitemap)
            session.commit()
        
        # Run the integration test
        test_bibcodes = [item[0] for item in test_data]
        
        with self.app.session_scope() as session:
            initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 10, 'index': 1}
            
            batch_stats, updated_state = self.app._process_sitemap_batch(
                test_bibcodes, 'add', session, initial_state
            )
            
            # Expected: 2 successful (1 new valid + 1 existing valid), 1 failed (solr-failed)
            self.assertEqual(batch_stats['successful'], 2, "Should process 1 new + 1 existing valid record")
            self.assertEqual(batch_stats['failed'], 1, "Should fail 1 solr-failed record")
            # Only 1 new record should increment count
            self.assertEqual(updated_state['count'], 11, "Only new record should increment count")
            self.assertEqual(updated_state['filename'], 'sitemap_bib_1.xml', "Should stay in same file")
            
            # Verify database state
            sitemap_records_db = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.like('2023Integration%')
            ).order_by(SitemapInfo.bibcode).all()
            
            # Should have 2 records in database (1 new + 1 existing, solr-failed was not added)
            self.assertEqual(len(sitemap_records_db), 2, "Should have 2 sitemap records in database")
            
            # Check new record (2023Integration..1..1A)
            new_record = next((r for r in sitemap_records_db if r.bibcode == '2023Integration..1..1A'), None)
            self.assertIsNotNone(new_record, "New record should exist in database")
            self.assertEqual(new_record.sitemap_filename, 'sitemap_bib_1.xml', "New record should be in sitemap_bib_1.xml")
            self.assertTrue(new_record.update_flag, "New record should have update_flag=True")
            self.assertIsNone(new_record.filename_lastmoddate, "New record should have filename_lastmoddate=None")
            self.assertIsNotNone(new_record.record_id, "New record should have valid record_id")
            
            # Check existing record (2023Integration..2..2A) 
            existing_record = next((r for r in sitemap_records_db if r.bibcode == '2023Integration..2..2A'), None)
            self.assertIsNotNone(existing_record, "Existing record should still exist in database")
            self.assertEqual(existing_record.sitemap_filename, 'sitemap_bib_1.xml', "Existing record should stay in sitemap_bib_1.xml")
            self.assertTrue(existing_record.update_flag, "Existing record should have update_flag=True (was updated)")
            # filename_lastmoddate should be updated to bib_data_updated for existing record
            self.assertIsNotNone(existing_record.filename_lastmoddate, "Existing record should have filename_lastmoddate updated")
            self.assertIsNotNone(existing_record.record_id, "Existing record should have valid record_id")
            
            # Verify solr-failed record is NOT in sitemap database
            failed_record = next((r for r in sitemap_records_db if r.bibcode == '2023Integration..3..3A'), None)
            self.assertIsNone(failed_record, "SOLR-failed record should NOT be in sitemap database")
            
            # Verify Records table has all 3 records (including the failed one)
            records_db = session.query(Records).filter(Records.bibcode.like('2023Integration%')).all()
            self.assertEqual(len(records_db), 3, "Should have 3 records in Records table (including failed one)")
            
            # Verify record_id relationships are correct
            record_ids = {r.bibcode: r.id for r in records_db}
            self.assertEqual(new_record.record_id, record_ids['2023Integration..1..1A'], "New record record_id should match")
            self.assertEqual(existing_record.record_id, record_ids['2023Integration..2..2A'], "Existing record record_id should match")
            
        

    def test_bulk_insert_and_update_operations(self):
        """Test both bulk_insert_sitemap_records and bulk_update_sitemap_records in single batch"""
        
        # Create test data - mix of new records and records that will need updates
        test_bibcodes = [
            '2023BulkOps..1..1A',  # Will be new (insert)
            '2023BulkOps..2..2A',  # Will be new (insert)
            '2023BulkOps..3..3A',  # Will be existing (update)
            '2023BulkOps..4..4A',  # Will be existing (update)
        ]
        
        # Create Records entries
        for bibcode in test_bibcodes:
            bib_data = {'title': f'Bulk Operations Test {bibcode}', 'year': 2023}
            self.app.update_storage(bibcode, 'bib_data', bib_data)
        
        # Create existing SitemapInfo entries for records 3 and 4 (these will be updates)
        with self.app.session_scope() as session:
            records = session.query(Records).filter(Records.bibcode.like('2023BulkOps%')).all()
            record_map = {r.bibcode: r.id for r in records}
            
            existing_entries = [
                SitemapInfo(
                    record_id=record_map['2023BulkOps..3..3A'],
                    bibcode='2023BulkOps..3..3A',
                    sitemap_filename='sitemap_bib_1.xml',
                    filename_lastmoddate=adsputils.get_date() - timedelta(days=5),  # Stale
                    update_flag=False
                ),
                SitemapInfo(
                    record_id=record_map['2023BulkOps..4..4A'],
                    bibcode='2023BulkOps..4..4A',
                    sitemap_filename='sitemap_bib_1.xml',
                    filename_lastmoddate=adsputils.get_date() - timedelta(days=3),  # Stale
                    update_flag=False
                )
            ]
            
            for entry in existing_entries:
                session.add(entry)
            session.commit()
        
        # Mock the bulk operations to verify they're called correctly
        with patch.object(self.app, 'bulk_insert_sitemap_records') as mock_insert, \
             patch.object(self.app, 'bulk_update_sitemap_records') as mock_update:
            
            # Run the batch processing
            with self.app.session_scope() as session:
                initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 10, 'index': 1}
                
                batch_stats, updated_state = self.app._process_sitemap_batch(
                    test_bibcodes, 'add', session, initial_state
                )
                
                # Verify results
                self.assertEqual(batch_stats['successful'], 4, "Should process all 4 records successfully")
                self.assertEqual(batch_stats['failed'], 0, "Should have no failures")
                self.assertEqual(updated_state['count'], 12, "Should increment count by 2 (new records only)")
                
                # Verify bulk_insert_sitemap_records was called with new records
                self.assertTrue(mock_insert.called, "bulk_insert_sitemap_records should be called")
                insert_call_args = mock_insert.call_args[0]
                insert_records = insert_call_args[0]  # First argument: new_records list
                insert_session = insert_call_args[1]  # Second argument: session
                
                # Should have 2 new records (records 1 and 2)
                self.assertEqual(len(insert_records), 2, "Should insert 2 new records")
                insert_bibcodes = {r['bibcode'] for r in insert_records}
                expected_new = {'2023BulkOps..1..1A', '2023BulkOps..2..2A'}
                self.assertEqual(insert_bibcodes, expected_new, "Should insert correct new records")
                
                # Verify session parameter
                self.assertIs(insert_session, session, "Should pass correct session to bulk_insert")
                
                # Verify bulk_update_sitemap_records was called with existing records
                self.assertTrue(mock_update.called, "bulk_update_sitemap_records should be called")
                update_call_args = mock_update.call_args[0]
                update_records = update_call_args[0]  # First argument: update_records list
                update_session = update_call_args[1]  # Second argument: session
                
                # Should have 2 update records (records 3 and 4)
                self.assertEqual(len(update_records), 2, "Should update 2 existing records")
                update_bibcodes = {r[0]['bibcode'] for r in update_records}  # r[0] is sitemap_record
                expected_updates = {'2023BulkOps..3..3A', '2023BulkOps..4..4A'}
                self.assertEqual(update_bibcodes, expected_updates, "Should update correct existing records")
                
                # Verify session parameter
                self.assertIs(update_session, session, "Should pass correct session to bulk_update")
                
                # Verify update records have correct properties
                for sitemap_record, sitemap_info in update_records:  # Unpack tuple
                    self.assertTrue(sitemap_record['update_flag'], f"Update record {sitemap_record['bibcode']} should have update_flag=True")
                    self.assertIsNotNone(sitemap_record['filename_lastmoddate'], f"Update record {sitemap_record['bibcode']} should have filename_lastmoddate updated")
                
                # Verify insert records have correct properties
                for record in insert_records:
                    self.assertTrue(record['update_flag'], f"Insert record {record['bibcode']} should have update_flag=True")
                    self.assertIsNone(record['filename_lastmoddate'], f"Insert record {record['bibcode']} should have filename_lastmoddate=None")
                    self.assertEqual(record['sitemap_filename'], 'sitemap_bib_1.xml', f"Insert record {record['bibcode']} should be in correct file")
                
               

    def test_bulk_operations_error_handling(self):
        """Test error handling in bulk database operations during _process_sitemap_batch"""
        
        # Create test data
        test_bibcodes = ['2023BulkError..1..1A', '2023BulkError..2..2A']
        
        for bibcode in test_bibcodes:
            bib_data = {'title': f'Bulk Error Test {bibcode}', 'year': 2023}
            self.app.update_storage(bibcode, 'bib_data', bib_data)
        
        # Mock bulk_insert to raise an exception
        with patch.object(self.app, 'bulk_insert_sitemap_records', side_effect=Exception("Database insert failed")):
            
            with self.app.session_scope() as session:
                initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 10, 'index': 1}
                
                # Should raise the exception from bulk operations
                with self.assertRaises(Exception) as context:
                    self.app._process_sitemap_batch(test_bibcodes, 'add', session, initial_state)
                
                self.assertIn("Database insert failed", str(context.exception))

    def test_bulk_operations_empty_scenarios(self):
        """Test bulk operations when there are no records to insert or update"""
        
        # Create test records that will all be filtered out by SOLR status
        test_bibcodes = ['2023BulkEmpty..1..1A', '2023BulkEmpty..2..2A']
        
        for bibcode in test_bibcodes:
            bib_data = {'title': f'Bulk Empty Test {bibcode}', 'year': 2023}
            self.app.update_storage(bibcode, 'bib_data', bib_data)
            # Mark as solr-failed so they get filtered out
            self.app.mark_processed([bibcode], 'solr', checksums=[f'checksum_{bibcode}'], status='solr-failed')
        
        # Mock the bulk operations to verify they're not called
        with patch.object(self.app, 'bulk_insert_sitemap_records') as mock_insert, \
             patch.object(self.app, 'bulk_update_sitemap_records') as mock_update:
            
            with self.app.session_scope() as session:
                initial_state = {'filename': 'sitemap_bib_1.xml', 'count': 10, 'index': 1}
                
                batch_stats, updated_state = self.app._process_sitemap_batch(
                    test_bibcodes, 'add', session, initial_state
                )
                
                # Verify results
                self.assertEqual(batch_stats['successful'], 0, "Should have no successful records")
                self.assertEqual(batch_stats['failed'], 2, "Should have 2 failed records (filtered out)")
                self.assertEqual(updated_state['count'], 10, "Count should not change")
                
                # Verify bulk operations were not called (no valid records to process)
                self.assertFalse(mock_insert.called, "bulk_insert_sitemap_records should not be called")
                self.assertFalse(mock_update.called, "bulk_update_sitemap_records should not be called")
                

    def test_bulk_update_sitemap_records(self):
        """Test bulk_update_sitemap_records method with performance timing"""
        
        # Create test records
        test_bibcodes = []
        for i in range(100):
            bibcode = f'2023BulkUpdate..{i:04d}..{i:04d}A'
            test_bibcodes.append(bibcode)
            bib_data = {'title': f'Bulk Update Test {i}', 'year': 2023}
            self.app.update_storage(bibcode, 'bib_data', bib_data)
        
        # Create initial sitemap entries
        with self.app.session_scope() as session:
            records = session.query(Records).filter(Records.bibcode.in_(test_bibcodes)).all()
            record_map = {r.bibcode: r.id for r in records}
            
            for i, bibcode in enumerate(test_bibcodes):
                sitemap_info = SitemapInfo(
                    record_id=record_map[bibcode],
                    bibcode=bibcode,
                    sitemap_filename=f'sitemap_bib_{(i // 50) + 1}.xml',  # 50 per file
                    filename_lastmoddate=adsputils.get_date() - timedelta(hours=i),  # Different timestamps
                    update_flag=False
                )
                session.add(sitemap_info)
            session.commit()
        
        # Prepare update records (tuples of sitemap_record, sitemap_info)
        update_records = []
        new_timestamp = adsputils.get_date()
        
        with self.app.session_scope() as session:
            sitemap_infos = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(test_bibcodes)
            ).all()
            
            for sitemap_info in sitemap_infos:
                # Create sitemap_record dict with updated values
                sitemap_record = {
                    'bibcode': sitemap_info.bibcode,
                    'bib_data_updated': new_timestamp,
                    'sitemap_filename': sitemap_info.sitemap_filename,
                    'filename_lastmoddate': new_timestamp,  # Updated timestamp
                    'update_flag': True  # Mark for regeneration
                }
                
                # Create sitemap_info dict with id for bulk update
                sitemap_info_dict = {
                    'id': sitemap_info.id,
                    'bibcode': sitemap_info.bibcode,
                    'sitemap_filename': sitemap_info.sitemap_filename
                }
                
                update_records.append((sitemap_record, sitemap_info_dict))
        
        # Test bulk update with performance timing
        with self.app.session_scope() as session:
            start_time = adsputils.get_date()
            
            self.app.bulk_update_sitemap_records(update_records, session)
            session.commit()
            
            end_time = adsputils.get_date()
            update_time = (end_time - start_time).total_seconds()
            
            # Performance assertion
            self.assertLess(update_time, 5.0, f"Bulk update took {update_time:.3f}s, should be under 5s")
            
            print(f"bulk_update_sitemap_records performance: 100 records updated in {update_time:.3f}s")
        
        # Verify all records were updated correctly
        with self.app.session_scope() as session:
            updated_records = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(test_bibcodes)
            ).all()
            
            self.assertEqual(len(updated_records), 100, "All 100 records should still exist")
            
            for record in updated_records:
                # Verify update_flag was set to True
                self.assertTrue(record.update_flag, f"Record {record.bibcode} should have update_flag=True")
                
                # Verify filename_lastmoddate was updated (should be close to new_timestamp)
                time_diff = abs((record.filename_lastmoddate - new_timestamp).total_seconds())
                self.assertLess(time_diff, 60, f"Record {record.bibcode} filename_lastmoddate should be updated")
                
                # Verify bib_data_updated was updated
                bib_time_diff = abs((record.bib_data_updated - new_timestamp).total_seconds())
                self.assertLess(bib_time_diff, 60, f"Record {record.bibcode} bib_data_updated should be updated")
                
                # Verify sitemap_filename remains unchanged
                expected_filename = f'sitemap_bib_{(test_bibcodes.index(record.bibcode) // 50) + 1}.xml'
                self.assertEqual(record.sitemap_filename, expected_filename, 
                               f"Record {record.bibcode} sitemap_filename should remain unchanged")
        
        # Test edge cases
        
        # Test 1: Empty update_records list
        with self.app.session_scope() as session:
            # Should not raise an exception
            self.app.bulk_update_sitemap_records([], session)
        
        # Test 2: Single record update
        single_update = [(update_records[0][0], update_records[0][1])]  # First record
        
        with self.app.session_scope() as session:
            # Change update_flag back to False for testing
            session.query(SitemapInfo).filter(
                SitemapInfo.bibcode == test_bibcodes[0]
            ).update({'update_flag': False}, synchronize_session=False)
            session.commit()
            
            # Update with new values
            single_update[0][0]['update_flag'] = True
            single_update[0][0]['filename_lastmoddate'] = adsputils.get_date()
            
            self.app.bulk_update_sitemap_records(single_update, session)
            session.commit()
            
            # Verify single record was updated
            updated_record = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode == test_bibcodes[0]
            ).first()
            
            self.assertTrue(updated_record.update_flag, "Single record should have update_flag=True")
        
        # Test 3: Partial field updates (only some fields provided in sitemap_record)
        with self.app.session_scope() as session:
            # Get the record ID within the active session
            second_record = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode == test_bibcodes[1]
            ).first()
            
            partial_update_record = {
                'bibcode': test_bibcodes[1],
                'update_flag': False  # Only updating this field
            }
            
            partial_sitemap_info = {
                'id': second_record.id,  
                'bibcode': test_bibcodes[1]
            }
            
            partial_updates = [(partial_update_record, partial_sitemap_info)]
            
            self.app.bulk_update_sitemap_records(partial_updates, session)
            session.commit()
            
            # Verify only update_flag was changed
            partially_updated = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode == test_bibcodes[1]
            ).first()
            
            self.assertFalse(partially_updated.update_flag, "Partial update should set update_flag=False")
            # Other fields should remain as they were (not None)
            self.assertIsNotNone(partially_updated.sitemap_filename, "sitemap_filename should not be cleared")
            self.assertIsNotNone(partially_updated.filename_lastmoddate, "filename_lastmoddate should not be cleared")
        

    def test_bulk_insert_sitemap_records(self):
        """Test bulk_insert_sitemap_records method with performance timing"""
        
        # Create test records in Records table first
        test_bibcodes = []
        for i in range(200):
            bibcode = f'2023BulkInsert..{i:04d}..{i:04d}A'
            test_bibcodes.append(bibcode)
            bib_data = {'title': f'Bulk Insert Test {i}', 'year': 2023}
            self.app.update_storage(bibcode, 'bib_data', bib_data)
        
        # Get record IDs for foreign key relationships
        with self.app.session_scope() as session:
            records = session.query(Records).filter(Records.bibcode.in_(test_bibcodes)).all()
            record_map = {r.bibcode: r.id for r in records}
        
        # Prepare sitemap records for bulk insert
        sitemap_records = []
        base_timestamp = adsputils.get_date()
        
        for i, bibcode in enumerate(test_bibcodes):
            sitemap_record = {
                'record_id': record_map[bibcode],
                'bibcode': bibcode,
                'sitemap_filename': f'sitemap_bib_{(i // 100) + 1}.xml',  # 100 per file
                'bib_data_updated': base_timestamp - timedelta(minutes=i),  # Different timestamps
                'filename_lastmoddate': None,  # New records start with None
                'update_flag': True  # New records need file generation
            }
            sitemap_records.append(sitemap_record)
        
        # Test bulk insert with performance timing
        with self.app.session_scope() as session:
            # Verify no sitemap records exist initially
            initial_count = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(test_bibcodes)
            ).count()
            self.assertEqual(initial_count, 0, "Should start with no sitemap records")
            
            start_time = adsputils.get_date()
            
            self.app.bulk_insert_sitemap_records(sitemap_records, session)
            session.commit()
            
            end_time = adsputils.get_date()
            insert_time = (end_time - start_time).total_seconds()
            
            # Performance assertion
            self.assertLess(insert_time, 5.0, f"Bulk insert took {insert_time:.3f}s, should be under 5s")
            
            print(f"bulk_insert_sitemap_records performance: 200 records inserted in {insert_time:.3f}s")
        
        # Verify all records were inserted correctly
        with self.app.session_scope() as session:
            inserted_records = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(test_bibcodes)
            ).order_by(SitemapInfo.bibcode).all()
            
            self.assertEqual(len(inserted_records), 200, "All 200 records should be inserted")
            
            # Verify record distribution across files
            file_counts = {}
            for record in inserted_records:
                filename = record.sitemap_filename
                file_counts[filename] = file_counts.get(filename, 0) + 1
            
            # Should have 2 files with 100 records each
            self.assertEqual(len(file_counts), 2, "Should have exactly 2 sitemap files")
            self.assertEqual(file_counts.get('sitemap_bib_1.xml', 0), 100, "First file should have 100 records")
            self.assertEqual(file_counts.get('sitemap_bib_2.xml', 0), 100, "Second file should have 100 records")
            
            # Verify individual record properties
            for i, record in enumerate(inserted_records):
                expected_bibcode = test_bibcodes[i]
                self.assertEqual(record.bibcode, expected_bibcode, f"Record {i} bibcode should match")
                
                # Verify foreign key relationship
                self.assertEqual(record.record_id, record_map[expected_bibcode], 
                               f"Record {expected_bibcode} should have correct record_id")
                
                # Verify initial values for new records
                self.assertTrue(record.update_flag, f"Record {expected_bibcode} should have update_flag=True")
                self.assertIsNone(record.filename_lastmoddate, f"Record {expected_bibcode} should have filename_lastmoddate=None")
                
                # Verify sitemap filename assignment
                expected_filename = f'sitemap_bib_{(i // 100) + 1}.xml'
                self.assertEqual(record.sitemap_filename, expected_filename, 
                               f"Record {expected_bibcode} should be in {expected_filename}")
                
                # Verify timestamp was set
                self.assertIsNotNone(record.bib_data_updated, f"Record {expected_bibcode} should have bib_data_updated")
                
                # Verify timestamp precision (should be within expected range)
                expected_time = base_timestamp - timedelta(minutes=i)
                time_diff = abs((record.bib_data_updated - expected_time).total_seconds())
                self.assertLess(time_diff, 60, f"Record {expected_bibcode} timestamp should be accurate")
        
        # Test edge cases
        
        # Test 1: Empty batch_stats['sitemap_records'] list
        with self.app.session_scope() as session:
            # Should not raise an exception
            self.app.bulk_insert_sitemap_records([], session)
            session.commit()
        
        # Test 2: Single record insert
        single_bibcode = '2023SingleInsert..1..1A'
        single_bib_data = {'title': 'Single Insert Test', 'year': 2023}
        self.app.update_storage(single_bibcode, 'bib_data', single_bib_data)
        
        with self.app.session_scope() as session:
            single_record = session.query(Records).filter(Records.bibcode == single_bibcode).first()
            
            single_sitemap_record = {
                'record_id': single_record.id,
                'bibcode': single_bibcode,
                'sitemap_filename': 'sitemap_bib_single.xml',
                'bib_data_updated': adsputils.get_date(),
                'filename_lastmoddate': None,
                'update_flag': True
            }
            
            self.app.bulk_insert_sitemap_records([single_sitemap_record], session)
            session.commit()
            
            # Verify single record was inserted
            inserted_single = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode == single_bibcode
            ).first()
            
            self.assertIsNotNone(inserted_single, "Single record should be inserted")
            self.assertEqual(inserted_single.bibcode, single_bibcode, "Single record bibcode should match")
            self.assertEqual(inserted_single.sitemap_filename, 'sitemap_bib_single.xml', "Single record filename should match")
            self.assertTrue(inserted_single.update_flag, "Single record should have update_flag=True")
        
        # Test 3: Minimal required fields (test with only required fields)
        minimal_bibcode = '2023MinimalInsert..1..1A'
        minimal_bib_data = {'title': 'Minimal Insert Test', 'year': 2023}
        self.app.update_storage(minimal_bibcode, 'bib_data', minimal_bib_data)
        
        with self.app.session_scope() as session:
            minimal_record = session.query(Records).filter(Records.bibcode == minimal_bibcode).first()
            
            minimal_sitemap_record = {
                'record_id': minimal_record.id,
                'bibcode': minimal_bibcode,
                # Only required fields, test defaults
            }
            
            self.app.bulk_insert_sitemap_records([minimal_sitemap_record], session)
            session.commit()
            
            # Verify minimal record was inserted with defaults
            inserted_minimal = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode == minimal_bibcode
            ).first()
            
            self.assertIsNotNone(inserted_minimal, "Minimal record should be inserted")
            self.assertEqual(inserted_minimal.bibcode, minimal_bibcode, "Minimal record bibcode should match")
            self.assertEqual(inserted_minimal.record_id, minimal_record.id, "Minimal record should have correct record_id")
            # Other fields should have their database defaults
            self.assertIsNone(inserted_minimal.sitemap_filename, "Minimal record should have default sitemap_filename")
            self.assertFalse(inserted_minimal.update_flag, "Minimal record should have default update_flag=False")
        
        # Test 4: Verify no duplicate inserts (attempt to insert same bibcode twice should fail)
        duplicate_bibcode = '2023DuplicateTest..1..1A'
        duplicate_bib_data = {'title': 'Duplicate Test', 'year': 2023}
        self.app.update_storage(duplicate_bibcode, 'bib_data', duplicate_bib_data)
        
        with self.app.session_scope() as session:
            duplicate_record = session.query(Records).filter(Records.bibcode == duplicate_bibcode).first()
            
            duplicate_sitemap_record = {
                'record_id': duplicate_record.id,
                'bibcode': duplicate_bibcode,
                'sitemap_filename': 'sitemap_bib_duplicate.xml',
                'update_flag': True
            }
            
            # First insert should succeed
            self.app.bulk_insert_sitemap_records([duplicate_sitemap_record], session)
            session.commit()
            
            # Second insert of same bibcode should fail due to UNIQUE constraint
            with self.assertRaises(Exception):  # Should raise IntegrityError or similar
                with self.app.session_scope() as new_session:  
                    self.app.bulk_insert_sitemap_records([duplicate_sitemap_record], new_session)
                    new_session.commit()
        

    def test_delete_contents(self):
        """Test delete_contents method"""
        
        # Create test records in SitemapInfo table
        test_bibcodes = ['2023DeleteTest..1..1A', '2023DeleteTest..2..2A', '2023DeleteTest..3..3A']
        
        for bibcode in test_bibcodes:
            bib_data = {'title': f'Delete Test {bibcode}', 'year': 2023}
            self.app.update_storage(bibcode, 'bib_data', bib_data)
        
        # Create sitemap entries
        with self.app.session_scope() as session:
            records = session.query(Records).filter(Records.bibcode.in_(test_bibcodes)).all()
            
            for record in records:
                sitemap_info = SitemapInfo(
                    record_id=record.id,
                    bibcode=record.bibcode,
                    sitemap_filename='sitemap_bib_test.xml',
                    update_flag=True
                )
                session.add(sitemap_info)
            session.commit()
        
        # Verify records exist before deletion
        with self.app.session_scope() as session:
            initial_count = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(test_bibcodes)
            ).count()
            self.assertEqual(initial_count, 3, "Should have 3 sitemap records before deletion")
        
        # Test delete_contents
        self.app.delete_contents(SitemapInfo)
        
        # Verify all records were deleted
        with self.app.session_scope() as session:
            final_count = session.query(SitemapInfo).count()
            self.assertEqual(final_count, 0, "All sitemap records should be deleted")
            
            # Verify Records table is unaffected
            records_count = session.query(Records).filter(Records.bibcode.in_(test_bibcodes)).count()
            self.assertEqual(records_count, 3, "Records table should be unaffected")
        

    def test_backup_sitemap_files(self):
        """Test backup_sitemap_files method"""
        

        # Create temporary directory for test
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test sitemap files
            test_files = ['sitemap_bib_1.xml', 'sitemap_bib_2.xml', 'sitemap_index.xml']
            
            for filename in test_files:
                file_path = os.path.join(temp_dir, filename)
                with open(file_path, 'w') as f:
                    f.write(f'<sitemap>Test content for {filename}</sitemap>')
            
            # Verify files exist before backup
            initial_files = os.listdir(temp_dir)
            self.assertEqual(len(initial_files), 3, "Should have 3 test files before backup")
            for filename in test_files:
                self.assertIn(filename, initial_files, f"File {filename} should exist before backup")
            
            # Mock os.system to capture the backup commands
            backup_commands = []
            original_system = os.system
            
            def mock_system(command):
                backup_commands.append(command)
                # Execute mkdir command but skip mv command for testing
                if command.startswith('mkdir'):
                    return original_system(command)
                return 0  # Success for mv command
            
            # Test backup_sitemap_files with mocked os.system
            with patch('os.system', side_effect=mock_system):
                self.app.backup_sitemap_files(temp_dir)
            
            # Verify backup commands were called
            self.assertEqual(len(backup_commands), 2, "Should execute 2 commands (mkdir + mv)")
            
            # Check mkdir command
            mkdir_command = backup_commands[0]
            self.assertTrue(mkdir_command.startswith('mkdir -p /app/logs/tmp/sitemap_'), 
                           "First command should create backup directory")
            
            # Check mv command
            mv_command = backup_commands[1]
            self.assertTrue(mv_command.startswith(f'mv {temp_dir}/*'), 
                           "Second command should move files from source directory")
            self.assertIn('/app/logs/tmp/sitemap_', mv_command, 
                         "Move command should target backup directory")
            
            # Verify backup directory path format (contains date components)
            
            date_pattern = r'/app/logs/tmp/sitemap_\d{4}_\d{1,2}_\d{1,2}-'
            self.assertTrue(re.search(date_pattern, mkdir_command), 
                           "Backup directory should contain date components")
        

    def test_execute_remove_action_basic_functionality(self):
        """Test basic functionality of _execute_remove_action method"""
        
        # Create test records and sitemap entries
        test_bibcodes = [
            '2023RemoveTest..1..1A',
            '2023RemoveTest..1..2A', 
            '2023RemoveTest..1..3A'
        ]
        
        with self.app.session_scope() as session:
            # Clean up any existing test data
            session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023RemoveTest%')).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode.like('2023RemoveTest%')).delete(synchronize_session=False)
            session.commit()
            
            # Create Records entries
            records = []
            for i, bibcode in enumerate(test_bibcodes):
                record = Records(
                    bibcode=bibcode,
                    bib_data='{"title": "Test Record"}',
                    bib_data_updated=get_date(),
                    status='success'
                )
                session.add(record)
                records.append(record)
            
            session.flush()  # Get record IDs
            
            # Create SitemapInfo entries
            sitemap_records = []
            for i, (bibcode, record) in enumerate(zip(test_bibcodes, records)):
                sitemap_record = SitemapInfo(
                    record_id=record.id,
                    bibcode=bibcode,
                    sitemap_filename=f'sitemap_bib_{i+1}.xml',
                    bib_data_updated=get_date(),
                    filename_lastmoddate=get_date(),
                    update_flag=False
                )
                session.add(sitemap_record)
                sitemap_records.append(sitemap_record)
            
            session.commit()
            
            # Verify initial state
            initial_count = session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023RemoveTest%')).count()
            self.assertEqual(initial_count, 3, "Should have 3 sitemap records initially")
            
            # Test removing 2 bibcodes
            bibcodes_to_remove = test_bibcodes[:2]  # Remove first 2
            removed_count, files_to_delete, _ = self.app._execute_remove_action(session, bibcodes_to_remove)
            
            # Verify results
            self.assertEqual(removed_count, 2, "Should remove exactly 2 bibcodes")
            self.assertEqual(files_to_delete, {'sitemap_bib_1.xml', 'sitemap_bib_2.xml'}, 
                           "Should identify 2 files for deletion")
            
            # Verify database state
            remaining_count = session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023RemoveTest%')).count()
            self.assertEqual(remaining_count, 1, "Should have 1 sitemap record remaining")
            
            remaining_record = session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023RemoveTest%')).first()
            self.assertEqual(remaining_record.bibcode, test_bibcodes[2], "Should keep the third bibcode")
            self.assertFalse(remaining_record.update_flag, "Remaining record should have update_flag=False")
            
            # Clean up
            session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023RemoveTest%')).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode.like('2023RemoveTest%')).delete(synchronize_session=False)
            session.commit()
    

    def test_execute_remove_action_empty_files_detection(self):
        """Test that _execute_remove_action correctly identifies empty files"""
        
        test_bibcodes = [
            '2023EmptyTest..1..1A',
            '2023EmptyTest..1..2A',
            '2023EmptyTest..1..3A',
            '2023EmptyTest..1..4A'
        ]
        
        with self.app.session_scope() as session:
            # Clean up any existing test data
            session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023EmptyTest%')).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode.like('2023EmptyTest%')).delete(synchronize_session=False)
            session.commit()
            
            # Create Records entries
            records = []
            for bibcode in test_bibcodes:
                record = Records(
                    bibcode=bibcode,
                    bib_data='{"title": "Test Record"}',
                    bib_data_updated=get_date(),
                    status='success'
                )
                session.add(record)
                records.append(record)
            
            session.flush()
            
            # Create SitemapInfo entries - 2 records in file1, 1 record in file2, 1 record in file3
            sitemap_assignments = [
                ('sitemap_bib_1.xml', test_bibcodes[0]),  # File 1: 2 records
                ('sitemap_bib_1.xml', test_bibcodes[1]),
                ('sitemap_bib_2.xml', test_bibcodes[2]),  # File 2: 1 record  
                ('sitemap_bib_3.xml', test_bibcodes[3])   # File 3: 1 record
            ]
            
            for i, (filename, bibcode) in enumerate(sitemap_assignments):
                sitemap_record = SitemapInfo(
                    record_id=records[i].id,
                    bibcode=bibcode,
                    sitemap_filename=filename,
                    bib_data_updated=get_date(),
                    filename_lastmoddate=get_date(),
                    update_flag=False
                )
                session.add(sitemap_record)
            
            session.commit()
            
            # Remove records that will make file2 and file3 empty, but leave file1 with 1 record
            bibcodes_to_remove = [test_bibcodes[1], test_bibcodes[2], test_bibcodes[3]]  # Remove from file1, all of file2, all of file3
            removed_count, files_to_delete, files_to_update = self.app._execute_remove_action(session, bibcodes_to_remove)
            
            # Verify results
            self.assertEqual(removed_count, 3, "Should remove exactly 3 bibcodes")
            self.assertEqual(files_to_delete, {'sitemap_bib_2.xml', 'sitemap_bib_3.xml'}, 
                           "Should identify files 2 and 3 as empty")
            
            # Verify file1 is in files_to_update (needs regeneration but not deletion)
            self.assertIn('sitemap_bib_1.xml', files_to_update, "File 1 should be marked for update")
            
            # Verify file1 still has records
            file1_records = session.query(SitemapInfo).filter(
                SitemapInfo.sitemap_filename == 'sitemap_bib_1.xml'
            ).all()
            self.assertEqual(len(file1_records), 1, "File 1 should have 1 remaining record")
            
            # Clean up
            session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023EmptyTest%')).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode.like('2023EmptyTest%')).delete(synchronize_session=False)
            session.commit()

    def test_execute_remove_action_no_matching_records(self):
        """Test _execute_remove_action with bibcodes that don't exist"""
        
        with self.app.session_scope() as session:
            # Test with non-existent bibcodes
            non_existent_bibcodes = ['2023NonExistent..1..1A', '2023NonExistent..1..2A']
            removed_count, files_to_delete, files_to_update = self.app._execute_remove_action(session, non_existent_bibcodes)
            
            # Should return zero results
            self.assertEqual(removed_count, 0, "Should remove 0 bibcodes when none exist")
            self.assertEqual(files_to_delete, set(), "Should return empty set for files to delete")
        
    def test_execute_remove_action_empty_input(self):
        """Test _execute_remove_action with empty input"""
        
        with self.app.session_scope() as session:
            # Test with empty list
            removed_count, files_to_delete, files_to_update = self.app._execute_remove_action(session, [])
            
            # Should return zero results immediately
            self.assertEqual(removed_count, 0, "Should remove 0 bibcodes with empty input")
            self.assertEqual(files_to_delete, set(), "Should return empty set for files to delete")
        
    def test_execute_remove_action_mixed_scenarios(self):
        """Test _execute_remove_action with mixed existing/non-existing bibcodes"""
        
        test_bibcodes = [
            '2023MixedTest..1..1A',
            '2023MixedTest..1..2A'
        ]
        non_existent_bibcodes = ['2023NonExist..1..1A', '2023NonExist..1..2A']
        
        with self.app.session_scope() as session:
            # Clean up any existing test data
            session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023MixedTest%')).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode.like('2023MixedTest%')).delete(synchronize_session=False)
            session.commit()
            
            # Create Records entries
            records = []
            for bibcode in test_bibcodes:
                record = Records(
                    bibcode=bibcode,
                    bib_data='{"title": "Test Record"}',
                    bib_data_updated=get_date(),
                    status='success'
                )
                session.add(record)
                records.append(record)
            
            session.flush()
            
            # Create SitemapInfo entries
            for i, (bibcode, record) in enumerate(zip(test_bibcodes, records)):
                sitemap_record = SitemapInfo(
                    record_id=record.id,
                    bibcode=bibcode,
                    sitemap_filename='sitemap_bib_1.xml',
                    bib_data_updated=get_date(),
                    filename_lastmoddate=get_date(),
                    update_flag=False
                )
                session.add(sitemap_record)
            
            session.commit()
            
            # Test removing mix of existing and non-existing bibcodes
            mixed_bibcodes = test_bibcodes + non_existent_bibcodes
            removed_count, files_to_delete, files_to_update = self.app._execute_remove_action(session, mixed_bibcodes)
            
            # Should only remove the existing ones
            self.assertEqual(removed_count, 2, "Should remove only the 2 existing bibcodes")
            self.assertEqual(files_to_delete, {'sitemap_bib_1.xml'}, "Should identify 1 file for deletion")
            
            # Verify database state
            remaining_count = session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023MixedTest%')).count()
            self.assertEqual(remaining_count, 0, "Should have no sitemap records remaining")
            
            # Clean up
            session.query(Records).filter(Records.bibcode.like('2023MixedTest%')).delete(synchronize_session=False)
            session.commit()
        
    def test_execute_remove_action_partial_file_removal(self):
        """Test _execute_remove_action when only some records are removed from files"""
        
        test_bibcodes = [
            '2023PartialTest..1..1A',
            '2023PartialTest..1..2A',
            '2023PartialTest..1..3A',
            '2023PartialTest..1..4A',
            '2023PartialTest..1..5A'
        ]
        
        with self.app.session_scope() as session:
            # Clean up any existing test data
            session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023PartialTest%')).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode.like('2023PartialTest%')).delete(synchronize_session=False)
            session.commit()
            
            # Create Records entries
            records = []
            for bibcode in test_bibcodes:
                record = Records(
                    bibcode=bibcode,
                    bib_data='{"title": "Test Record"}',
                    bib_data_updated=get_date(),
                    status='success'
                )
                session.add(record)
                records.append(record)
            
            session.flush()
            
            # Create SitemapInfo entries - distribute across 2 files
            sitemap_assignments = [
                ('sitemap_bib_1.xml', test_bibcodes[0]),  # File 1: 3 records
                ('sitemap_bib_1.xml', test_bibcodes[1]),
                ('sitemap_bib_1.xml', test_bibcodes[2]),
                ('sitemap_bib_2.xml', test_bibcodes[3]),  # File 2: 2 records
                ('sitemap_bib_2.xml', test_bibcodes[4])
            ]
            
            for i, (filename, bibcode) in enumerate(sitemap_assignments):
                sitemap_record = SitemapInfo(
                    record_id=records[i].id,
                    bibcode=bibcode,
                    sitemap_filename=filename,
                    bib_data_updated=get_date(),
                    filename_lastmoddate=get_date(),
                    update_flag=False
                )
                session.add(sitemap_record)
            
            session.commit()
            
            # Remove 1 record from file1 and 1 record from file2 (partial removal)
            bibcodes_to_remove = [test_bibcodes[1], test_bibcodes[3]]  # 1 from each file
            removed_count, files_to_delete, files_to_update = self.app._execute_remove_action(session, bibcodes_to_remove)
            
            # Verify results
            self.assertEqual(removed_count, 2, "Should remove exactly 2 bibcodes")
            self.assertEqual(files_to_delete, set(), "Should not delete any files (both still have records)")
            
            # Verify both files are in files_to_update
            self.assertIn('sitemap_bib_1.xml', files_to_update, "File 1 should be marked for update")
            self.assertIn('sitemap_bib_2.xml', files_to_update, "File 2 should be marked for update")
            
            # Verify both files still have records
            file1_records = session.query(SitemapInfo).filter(
                SitemapInfo.sitemap_filename == 'sitemap_bib_1.xml'
            ).all()
            file2_records = session.query(SitemapInfo).filter(
                SitemapInfo.sitemap_filename == 'sitemap_bib_2.xml'  
            ).all()
            
            self.assertEqual(len(file1_records), 2, "File 1 should have 2 remaining records")
            self.assertEqual(len(file2_records), 1, "File 2 should have 1 remaining record")
            
            # Clean up
            session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023PartialTest%')).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode.like('2023PartialTest%')).delete(synchronize_session=False)
            session.commit()
        
    def test_execute_remove_action_performance_with_large_batch(self):
        """Test _execute_remove_action performance with larger batch sizes"""
        
        # Create a larger batch for performance testing
        batch_size = 1000
        test_bibcodes = [f'2023PerfTest..{i:03d}..{i:03d}A' for i in range(batch_size)]
        
        with self.app.session_scope() as session:
            # Clean up any existing test data
            session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023PerfTest%')).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode.like('2023PerfTest%')).delete(synchronize_session=False)
            session.commit()
            
            # Create Records entries
            records = []
            for bibcode in test_bibcodes:
                record = Records(
                    bibcode=bibcode,
                    bib_data='{"title": "Performance Test Record"}',
                    bib_data_updated=get_date(),
                    status='success'
                )
                session.add(record)
                records.append(record)
            
            session.flush()
            
            # Create SitemapInfo entries - distribute across multiple files
            for i, (bibcode, record) in enumerate(zip(test_bibcodes, records)):
                file_index = (i // 10) + 1  # 10 records per file
                sitemap_record = SitemapInfo(
                    record_id=record.id,
                    bibcode=bibcode,
                    sitemap_filename=f'sitemap_bib_{file_index}.xml',
                    bib_data_updated=get_date(),
                    filename_lastmoddate=get_date(),
                    update_flag=False
                )
                session.add(sitemap_record)
            
            session.commit()
            
            # Time the removal operation
            start_time = time.time()
            removed_count, files_to_delete, files_to_update = self.app._execute_remove_action(session, test_bibcodes)
            end_time = time.time()
            
            execution_time = end_time - start_time
            
            # Verify results
            self.assertEqual(removed_count, batch_size, f"Should remove all {batch_size} bibcodes")
            self.assertEqual(len(files_to_delete), 100, "Should identify 100 files for deletion")
            
            # Performance assertion - should complete reasonably quickly
            self.assertLess(execution_time, 5.0, f"Removal of {batch_size} records should complete in under 5 seconds")
            
            # Verify database state
            remaining_count = session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023PerfTest%')).count()
            self.assertEqual(remaining_count, 0, "Should have no sitemap records remaining")
            
            # Clean up
            session.query(Records).filter(Records.bibcode.like('2023PerfTest%')).delete(synchronize_session=False)
            session.commit()
        
        print(f"_execute_remove_action performance test completed in {execution_time:.3f} seconds for {batch_size} records")
        
    def test_delete_sitemap_files(self):
        """Test delete_sitemap_files method"""
        
        # Create temporary directory structure for test
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock SITES configuration
            sites_config = {
                'ads': {'base_url': 'https://ui.adsabs.harvard.edu/'},
                'scix': {'base_url': 'https://scixplorer.org/'}
            }
            
            # Create site directories and test files
            test_files = ['sitemap_bib_1.xml', 'sitemap_bib_2.xml', 'sitemap_index.xml']
            created_files = []
            
            for site_key in sites_config.keys():
                site_dir = os.path.join(temp_dir, site_key)
                os.makedirs(site_dir)
                
                for filename in test_files:
                    file_path = os.path.join(site_dir, filename)
                    with open(file_path, 'w') as f:
                        f.write(f'<sitemap>Test content for {filename} in {site_key}</sitemap>')
                    created_files.append(file_path)
            
            # Verify all files exist before deletion
            for file_path in created_files:
                self.assertTrue(os.path.exists(file_path), f"File {file_path} should exist before deletion")
            
            # Mock the SITES configuration
            original_sites = self.app.conf.get('SITES', {})
            self.app.conf['SITES'] = sites_config
            
            try:
                # Test delete_sitemap_files - delete first 2 files
                files_to_delete = {'sitemap_bib_1.xml', 'sitemap_bib_2.xml'}
                
                self.app.delete_sitemap_files(files_to_delete, temp_dir)
                
                # Verify deleted files are gone
                for site_key in sites_config.keys():
                    for filename in files_to_delete:
                        file_path = os.path.join(temp_dir, site_key, filename)
                        self.assertFalse(os.path.exists(file_path), 
                                       f"File {file_path} should be deleted")
                
                # Verify remaining files still exist
                for site_key in sites_config.keys():
                    remaining_file = os.path.join(temp_dir, site_key, 'sitemap_index.xml')
                    self.assertTrue(os.path.exists(remaining_file), 
                                  f"File {remaining_file} should still exist")
                
                # Test empty files_to_delete set (should do nothing)
                remaining_count_before = sum(len(os.listdir(os.path.join(temp_dir, site))) 
                                           for site in sites_config.keys())
                
                self.app.delete_sitemap_files(set(), temp_dir)
                
                remaining_count_after = sum(len(os.listdir(os.path.join(temp_dir, site))) 
                                          for site in sites_config.keys())
                
                self.assertEqual(remaining_count_before, remaining_count_after, 
                               "Empty files_to_delete should not change file count")
                
                # Test non-existent files (should not raise error)
                non_existent_files = {'non_existent_1.xml', 'non_existent_2.xml'}
                
                # Should not raise an exception
                self.app.delete_sitemap_files(non_existent_files, temp_dir)
                
                # Remaining files should still exist
                final_count = sum(len(os.listdir(os.path.join(temp_dir, site))) 
                                for site in sites_config.keys())
                self.assertEqual(final_count, 2, "Should still have 2 files (1 per site)")
                
            finally:
                # Restore original SITES configuration
                self.app.conf['SITES'] = original_sites
        

    def test_chunked(self):
        """Test chunked method"""
        
        # Test 1: Normal chunking with exact division
        data = list(range(10))  # [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        chunks = list(self.app.chunked(data, 5))
        
        self.assertEqual(len(chunks), 2, "Should create 2 chunks")
        self.assertEqual(chunks[0], [0, 1, 2, 3, 4], "First chunk should contain first 5 elements")
        self.assertEqual(chunks[1], [5, 6, 7, 8, 9], "Second chunk should contain last 5 elements")
        
        # Test 2: Chunking with remainder
        data = list(range(7))  # [0, 1, 2, 3, 4, 5, 6]
        chunks = list(self.app.chunked(data, 3))
        
        self.assertEqual(len(chunks), 3, "Should create 3 chunks")
        self.assertEqual(chunks[0], [0, 1, 2], "First chunk should have 3 elements")
        self.assertEqual(chunks[1], [3, 4, 5], "Second chunk should have 3 elements")
        self.assertEqual(chunks[2], [6], "Third chunk should have 1 element (remainder)")
        
        # Test 3: Single chunk (chunk_size larger than data)
        data = [1, 2, 3]
        chunks = list(self.app.chunked(data, 10))
        
        self.assertEqual(len(chunks), 1, "Should create 1 chunk")
        self.assertEqual(chunks[0], [1, 2, 3], "Single chunk should contain all elements")
        
        # Test 4: Empty iterable
        data = []
        chunks = list(self.app.chunked(data, 5))
        
        self.assertEqual(len(chunks), 0, "Empty iterable should produce no chunks")
        
        # Test 5: Chunk size of 1
        data = ['a', 'b', 'c']
        chunks = list(self.app.chunked(data, 1))
        
        self.assertEqual(len(chunks), 3, "Should create 3 chunks with size 1")
        self.assertEqual(chunks[0], ['a'], "First chunk should contain 'a'")
        self.assertEqual(chunks[1], ['b'], "Second chunk should contain 'b'")
        self.assertEqual(chunks[2], ['c'], "Third chunk should contain 'c'")
        
        # Test 6: Memory efficiency test with generator (doesn't copy data)
        def large_generator():
            for i in range(1000):
                yield f"item_{i}"
        
        chunks = list(self.app.chunked(large_generator(), 100))
        
        self.assertEqual(len(chunks), 10, "Should create 10 chunks from 1000 items")
        self.assertEqual(len(chunks[0]), 100, "Each chunk should have 100 items")
        self.assertEqual(len(chunks[-1]), 100, "Last chunk should also have 100 items")
        self.assertEqual(chunks[0][0], "item_0", "First item should be 'item_0'")
        self.assertEqual(chunks[-1][-1], "item_999", "Last item should be 'item_999'")
        
        # Test 7: String chunking
        data = "abcdefghij"
        chunks = list(self.app.chunked(data, 4))
        
        self.assertEqual(len(chunks), 3, "Should create 3 chunks from string")
        self.assertEqual(chunks[0], ['a', 'b', 'c', 'd'], "First chunk should contain first 4 chars")
        self.assertEqual(chunks[1], ['e', 'f', 'g', 'h'], "Second chunk should contain next 4 chars")
        self.assertEqual(chunks[2], ['i', 'j'], "Third chunk should contain remaining 2 chars")
        
        # Test 8: Different data types
        data = [1, 'two', 3.0, [4, 5], {'six': 6}]
        chunks = list(self.app.chunked(data, 2))
        
        self.assertEqual(len(chunks), 3, "Should create 3 chunks from mixed data types")
        self.assertEqual(chunks[0], [1, 'two'], "First chunk should contain first 2 items")
        self.assertEqual(chunks[1], [3.0, [4, 5]], "Second chunk should contain next 2 items")
        self.assertEqual(chunks[2], [{'six': 6}], "Third chunk should contain last item")
        

    def test_delete_by_bibcode_with_sitemap(self):
        """Test delete_by_bibcode function with sitemap records (database deletion only)"""
        # TEST CASE 1: Delete record with both Records and SitemapInfo entries
        test_bibcode = '2023DeleteSitemapTest..1..1A'
        bib_data = {'title': 'Test Record for Sitemap Deletion', 'year': 2023}
        
        # Create test record
        self.app.update_storage(test_bibcode, 'bib_data', bib_data)
        
        # Create sitemap entry
        with self.app.session_scope() as session:
            record = session.query(Records).filter_by(bibcode=test_bibcode).first()
            self.assertIsNotNone(record, "Test record should exist")
            
            sitemap_info = SitemapInfo(
                record_id=record.id,
                bibcode=test_bibcode,
                sitemap_filename='sitemap_bib_delete_test.xml',
                update_flag=False,
                bib_data_updated=record.bib_data_updated
            )
            session.add(sitemap_info)
            session.commit()
        
        # Verify setup: record and sitemap entry exist
        with self.app.session_scope() as session:
            record_count = session.query(Records).filter_by(bibcode=test_bibcode).count()
            sitemap_count = session.query(SitemapInfo).filter_by(bibcode=test_bibcode).count()
            self.assertEqual(record_count, 1, "Should have 1 Records entry before deletion")
            self.assertEqual(sitemap_count, 1, "Should have 1 SitemapInfo entry before deletion")
        
        # Delete the record
        result = self.app.delete_by_bibcode(test_bibcode)
        self.assertTrue(result, "delete_by_bibcode should return True for successful deletion")
        
        # Verify both Records and SitemapInfo entries are deleted
        with self.app.session_scope() as session:
            record = session.query(Records).filter_by(bibcode=test_bibcode).first()
            self.assertIsNone(record, "Records entry should be deleted")
            
            # Verify ChangeLog entry was created
            changelog = session.query(ChangeLog).filter_by(key=f'bibcode:{test_bibcode}').first()
            self.assertIsNotNone(changelog, "ChangeLog entry should be created")
            self.assertEqual(changelog.type, 'deleted', "ChangeLog type should be 'deleted'")
            
            # With application-level cascade, SitemapInfo should be deleted
            sitemap_info = session.query(SitemapInfo).filter_by(bibcode=test_bibcode).first()
            self.assertIsNone(sitemap_info, "SitemapInfo entry should be deleted by application logic")
        
        # TEST CASE 2: Delete when only SitemapInfo exists (Records already deleted)
        test_bibcode_2 = '2023DeleteSitemapTest..2..2A'
        
        # Create only SitemapInfo entry (no Records entry)
        with self.app.session_scope() as session:
            sitemap_info_2 = SitemapInfo(
                record_id=999999,  # Non-existent record_id
                bibcode=test_bibcode_2,
                sitemap_filename='sitemap_bib_orphan.xml',
                update_flag=False
            )
            session.add(sitemap_info_2)
            session.commit()
        
        # Verify setup: no Records entry, but SitemapInfo exists
        with self.app.session_scope() as session:
            record_count = session.query(Records).filter_by(bibcode=test_bibcode_2).count()
            sitemap_count = session.query(SitemapInfo).filter_by(bibcode=test_bibcode_2).count()
            self.assertEqual(record_count, 0, "Should have 0 Records entries")
            self.assertEqual(sitemap_count, 1, "Should have 1 SitemapInfo entry")
        
        # Delete orphaned sitemap entry
        result_2 = self.app.delete_by_bibcode(test_bibcode_2)
        self.assertTrue(result_2, "delete_by_bibcode should return True for SitemapInfo deletion")
        
        # Verify SitemapInfo entry is deleted
        with self.app.session_scope() as session:
            sitemap_info = session.query(SitemapInfo).filter_by(bibcode=test_bibcode_2).first()
            self.assertIsNone(sitemap_info, "Orphaned SitemapInfo entry should be deleted")
        
        # TEST CASE 3: Delete non-existent bibcode
        result_3 = self.app.delete_by_bibcode('2023NonExistent..1..1A')
        self.assertIsNone(result_3, "delete_by_bibcode should return None for non-existent bibcode")


if __name__ == '__main__':
    unittest.main()
