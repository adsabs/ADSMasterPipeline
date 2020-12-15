#!/usr/bin/env python
# -*- coding: utf-8 -*-



import sys
import os

import unittest
from adsmp import app, models
from adsmp.models import Base, MetricsBase
from adsputils import load_config
import testing.postgresql

test1 = {
    "refereed": True,
    "bibcode": "bib1",
    "downloads": [],
    "reads": [],
    "citations": ["2006QJRMS.132..779R", "2008Sci...320.1622D", "1998PPGeo..22..553A"],
    "author_num": 1,
}

test2 = {
    "refereed": True,
    "bibcode": "bib2",
    "downloads": [],
    "reads": [],
    "citations": ["2006QJRMS.132..779R", "2008Sci...320.1622D", "1998PPGeo..22..553A"],
    "author_num": 2,
}

test3 = {"bibcode": "bib3",
         "downloads": [],
         "reads": [],
         "citations": ["2006QJRMS.132..779R", "2008Sci...320.1622D", "1998PPGeo..22..553A"],
         "author_num": 3,
}




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
        config = load_config()
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.ADSMasterPipelineCelery('test', local_config=\
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'METRICS_SQLALCHEMY_URL': 'postgresql://postgres@127.0.0.1:15678/test',
            'SQLALCHEMY_ECHO': True,
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

    def test_update_records(self):
        app = self.app
        t1 = test1.copy()
        app.update_metrics_db([t1])
        with app.metrics_session_scope() as session:
            r = session.query(models.MetricsModel).filter_by(bibcode='bib1').first()
            self.assertTrue(r.refereed)
            self.assertEqual(1, r.author_num)
            id = r.id

        t1['refereed'] = False
        t1['author_num'] = 5
        t2 = test2.copy()
        app.update_metrics_db([t1, t2])
        with app.metrics_session_scope() as session:
            r = session.query(models.MetricsModel).filter_by(bibcode='bib1').first()
            self.assertFalse(r.refereed)
            self.assertEqual(id, r.id)
            self.assertEqual(5, r.author_num)
            r2 = session.query(models.MetricsModel).filter_by(bibcode='bib2').first()
            self.assertTrue(r2.refereed)
            self.assertNotEqual(id, r2.id)
            self.assertEqual(2, r2.author_num)
            id2 = r2.id

        t2['refereed'] = False
        t2['author_num'] = 4
        app.update_metrics_db([t2, t1])
        with app.metrics_session_scope() as session:
            r = session.query(models.MetricsModel).filter_by(bibcode='bib1').first()
            self.assertFalse(r.refereed)
            self.assertEqual(id, r.id)
            self.assertEqual(5, r.author_num)
            r2 = session.query(models.MetricsModel).filter_by(bibcode='bib2').first()
            self.assertFalse(r2.refereed)
            self.assertEqual(id2, r2.id)
            self.assertEqual(4, r2.author_num)

        t2['author_num'] = 6
        app.update_metrics_db([t1, t2, test3])
        
        with app.metrics_session_scope() as session:
            b1 = session.query(models.MetricsModel).filter_by(bibcode='bib1').first()
            b2 = session.query(models.MetricsModel).filter_by(bibcode='bib2').first()
            self.assertEqual(5, b1.author_num)
            self.assertEqual(6, b2.author_num)
            b3 = session.query(models.MetricsModel).filter_by(bibcode='bib3').first()
            self.assertEqual(3, b3.author_num)
        
    def test_update_default_values(self):
        app = self.app
        app.update_metrics_db([{
         "bibcode": "bib9", 
         }])
        
        # test default values
        x = app.get_metrics('bib9')
        self.assertFalse(x['refereed'])
        
        app.update_metrics_db([{
         "bibcode": "bib9",
         "refereed": True 
         }])
        
        x = app.get_metrics('bib9')
        self.assertTrue(x['refereed'])
        
        # when updating without the values
        
        app.update_metrics_db([{
         "bibcode": "bib9",
         }, {"bibcode": "bib10", "refereed": True}])
        
        x = app.get_metrics('bib9')
        y = app.get_metrics('bib10')
        
        self.assertFalse(x['refereed'])
        self.assertTrue(y['refereed'])
        
        print(x)
        print(y)
        
if __name__ == '__main__':
    unittest.main()
