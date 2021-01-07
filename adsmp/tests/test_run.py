
import unittest
from mock import patch
import os
import testing.postgresql

from adsmp import app
from adsmp.models import Base, Records
from run import reindex_failed


class TestFixDbDuplicates(unittest.TestCase):

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

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def test_reindex_failed(self):
        # init database
        with self.app.session_scope() as session:
            session.add(Records(bibcode='bibcode1', status='success', bib_data='{}'))
            session.add(Records(bibcode='bibcode2', status='solr-failed', bib_data='{}'))
            session.add(Records(bibcode='bibcode3', status='links-failed', bib_data='{}'))
            session.add(Records(bibcode='bibcode4', status='retrying', bib_data='{}'))
            session.add(Records(bibcode='bibcode5', fulltext='foobar'))

        # execute reindex_failed from run.py
        with patch('adsmp.tasks.task_index_records.delay', return_value=None) as queue_bibcodes:
            reindex_failed(self.app)
            self.assertEqual(1, queue_bibcodes.call_count)
            queue_bibcodes.assert_called_with([u'bibcode2', u'bibcode3'],
                                              force=True, ignore_checksums=True,
                                              update_links=True, update_metrics=True,
                                              update_solr=True, update_processed=True)

        # verify database was updated propery
        with self.app.session_scope() as session:
            rec = session.query(Records).filter_by(bibcode='bibcode1').first()
            self.assertEqual(rec.status, 'success')
            rec = session.query(Records).filter_by(bibcode='bibcode2').first()
            self.assertEqual(rec.status, 'retrying')
            rec = session.query(Records).filter_by(bibcode='bibcode3').first()
            self.assertEqual(rec.status, 'retrying')
            rec = session.query(Records).filter_by(bibcode='bibcode4').first()
            self.assertEqual(rec.status, 'retrying')
            rec = session.query(Records).filter_by(bibcode='bibcode5').first()
            self.assertEqual(rec.status, None)

