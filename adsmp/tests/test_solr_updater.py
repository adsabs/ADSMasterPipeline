#!/usr/bin/env python
# -*- coding: utf-8 -*-



import sys
import os

import unittest
import json
import re
import os
import math
import mock
import adsputils
from mock import patch
from io import BytesIO
from datetime import datetime
from adsmp import app, models, solr_updater
from adsmp.models import Base, Records
from adsputils import get_date

class TestSolrUpdater(unittest.TestCase):
    """
    Tests the appliction's methods
    """
    def setUp(self):
        unittest.TestCase.setUp(self)
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.ADSMasterPipelineCelery('test', local_config=\
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'METRICS_SQLALCHEMY_URL': 'sqlite:///',
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

    

    
    def test_solr_transformer(self):
        """Makes sure we can write recs into the storage."""
        
        self.app.update_storage('bibcode', 'metadata', {u'abstract': u'abstract text',
             u'aff': [u'-', u'-', u'-', u'-'],
             u'alternate_bibcode': [u'2003adass..12..283B'],
             u'author': [u'Blecksmith, E.', u'Paltani, S.', u'Rots, A.', u'Winkelman, S.'],
             u'author_count': 4,
             u'author_facet': [u'Blecksmith, E',
              u'Paltani, S',
              u'Rots, A',
              u'Winkelman, S'],
             u'author_facet_hier': [u'0/Blecksmith, E',
              u'1/Blecksmith, E/Blecksmith, E.',
              u'0/Paltani, S',
              u'1/Paltani, S/Paltani, S.',
              u'0/Rots, A',
              u'1/Rots, A/Rots, A.',
              u'0/Winkelman, S',
              u'1/Winkelman, S/Winkelman, S.'],
             u'author_norm': [u'Blecksmith, E',
              u'Paltani, S',
              u'Rots, A',
              u'Winkelman, S'],
             u'bibcode': u'2003ASPC..295..283B',
             u'bibgroup': [u'CXC', u'CfA'],
             u'bibgroup_facet': [u'CXC', u'CfA'],
             u'bibstem': [u'ASPC', u'ASPC..295'],
             u'bibstem_facet': u'ASPC',
             u'database': [u'astronomy'],
             u'date': u'2003-01-01T00:00:00.000000Z',
             u'doctype': u'inproceedings',
             u'doctype_facet_hier': [u'0/Article', u'1/Article/Proceedings Article'],
             u'email': [u'-', u'-', u'-', u'-'],
             u'first_author': u'Blecksmith, E.',
             u'first_author_facet_hier': [u'0/Blecksmith, E',
              u'1/Blecksmith, E/Blecksmith, E.'],
             u'first_author_norm': u'Blecksmith, E',
             u'id': u'1401492',
             u'identifier': [u'2003adass..12..283B'],
             u'links_data': u'',   ### TODO(rca): superconfusing string, but fortunately we are getting ridd of it
             u'orcid_pub': [u'-', u'-', u'-', u'-'],
             u'page': [u'283'],
             u'property': [u'OPENACCESS', u'ADS_OPENACCESS', u'ARTICLE', u'NOT REFEREED'],
             u'pub': u'Astronomical Data Analysis Software and Systems XII',
             u'pub_raw': u'Astronomical Data Analysis Software and Systems XII ASP Conference Series, Vol. 295, 2003 H. E. Payne, R. I. Jedrzejewski, and R. N. Hook, eds., p.283',
             u'pubdate': u'2003-00-00',
             u'title': [u'Chandra Data Archive Download and Usage Database'],
             u'volume': u'295',
             u'year': u'2003'})
        self.app.update_storage('bibcode', 'fulltext', 'fulltext')
        self.app.update_storage('bibcode', 'metrics', {"downloads": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 1, 0, 0, 1, 0, 0, 0, 1, 2], 
                                                       "bibcode": "2003ASPC..295..361M", 
                                                       "reads": [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4, 2, 5, 1, 0, 0, 1, 0, 0, 2, 4, 5], 
                                                       "author_num": 2})
        self.app.update_storage('bibcode', 'orcid_claims', {'authors': ['Blecksmith, E.', 'Paltani, S.', 'Rots, A.', 'Winkelman, S.'],
             'bibcode': '2003ASPC..295..283B',
             'unverified': ['-', '-', '0000-0003-2377-2356', '-']})
        self.app.update_storage('bibcode', 'metrics', {
             u'citation_num': 6,
             u'citations': [u'2007ApPhL..91g1118P',
              u'2010ApPhA..99..805K',
              u'2011TSF...520..610L',
              u'2012NatCo...3E1175B',
              u'2014IPTL...26..305A',
              u'2016ITED...63..197G']})
        self.app.update_storage('bibcode', 'nonbib_data', {u'authors': [u'Zaus, E',
              u'Tedde, S',
              u'Fuerst, J',
              u'Henseler, D',
              u'Doehler, G'],
             u'bibcode': u'2007JAP...101d4501Z',
             u'boost': 0.1899999976158142,
             u'downloads': [0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              1,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0],
             u'id': 7862455,
             u'norm_cites': 4225,
             u'reads': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 6, 2, 1, 0, 0, 1, 0, 1, 0, 0],
             u'refereed': True,
             u'reference': [u'1977JAP....48.4729M',
              u'1981psd..book.....S',
              u'1981wi...book.....S',
              u'1986PhRvB..33.5545M',
              u'1987ApPhL..51..913T',
              u'1992Sci...258.1474S',
              u'1994IJMPB...8..237S',
              u'1995Natur.376..498H',
              u'1995Sci...270.1789Y',
              u'1998TSF...331...76O',
              u'1999Natur.397..121F',
              u'2000JaJAP..39...94P',
              u'2002ApPhL..81.3885S',
              u'2004ApPhL..85.3890C',
              u'2004TSF...451..105S',
              u'2005PhRvB..72s5208M',
              u'2006ApPhL..89l3505L'],
             u'simbad_objects': [u'2419335 sim', u'3111723 sim*'],
             u'ned_objects': [u'2419335 HII', u'3111723 ned*'],
             u'grants': [u'2419335 g', u'3111723 g*'],
             })
        
        rec = self.app.get_record('bibcode')
        self.assertDictContainsSubset({u'abstract': u'abstract text',
             u'aff': [u'-', u'-', u'-', u'-'],
             u'alternate_bibcode': [u'2003adass..12..283B'],
             u'author': [u'Blecksmith, E.', u'Paltani, S.', u'Rots, A.', u'Winkelman, S.'],
             u'author_count': 4,
             u'author_facet': [u'Blecksmith, E',
              u'Paltani, S',
              u'Rots, A',
              u'Winkelman, S'],
             u'author_facet_hier': [u'0/Blecksmith, E',
              u'1/Blecksmith, E/Blecksmith, E.',
              u'0/Paltani, S',
              u'1/Paltani, S/Paltani, S.',
              u'0/Rots, A',
              u'1/Rots, A/Rots, A.',
              u'0/Winkelman, S',
              u'1/Winkelman, S/Winkelman, S.'],
             u'author_norm': [u'Blecksmith, E',
              u'Paltani, S',
              u'Rots, A',
              u'Winkelman, S'],
             'bibcode': u'2003ASPC..295..283B',
             u'bibgroup': [u'CXC', u'CfA'],
             u'bibgroup_facet': [u'CXC', u'CfA'],
             u'bibstem': [u'ASPC', u'ASPC..295'],
             u'bibstem_facet': u'ASPC',
             'body': u'fulltext',
             'citation': [u'2007ApPhL..91g1118P',
              u'2010ApPhA..99..805K',
              u'2011TSF...520..610L',
              u'2012NatCo...3E1175B',
              u'2014IPTL...26..305A',
              u'2016ITED...63..197G'],
             'citation_count': 6,
             'cite_read_boost': 0.1899999976158142,
             u'database': [u'astronomy'],
             u'date': u'2003-01-01T00:00:00.000000Z',
             u'doctype': u'inproceedings',
             u'doctype_facet_hier': [u'0/Article', u'1/Article/Proceedings Article'],
             u'email': [u'-', u'-', u'-', u'-'],
             u'first_author': u'Blecksmith, E.',
             u'first_author_facet_hier': [u'0/Blecksmith, E',
              u'1/Blecksmith, E/Blecksmith, E.'],
             u'first_author_norm': u'Blecksmith, E',
             'id': u'1401492',
             u'identifier': [u'2003adass..12..283B'],
             u'links_data': u'',
             'orcid_other' : [u'-', u'-', u'0000-0003-2377-2356', u'-'],
             u'orcid_pub': [u'-', u'-', u'-', u'-'],
             u'nedid': [u'2419335', u'3111723'],
             u'nedtype': [u'HII Region', u'Other'],
             u'ned_object_facet_hier': [u'0/HII Region', u'1/HII Region/2419335', u'0/Other', u'1/Other/3111723'],
             u'page': [u'283'],
             u'property': [u'OPENACCESS', u'ADS_OPENACCESS', u'ARTICLE', u'NOT REFEREED'],
             u'pub': u'Astronomical Data Analysis Software and Systems XII',
             u'pub_raw': u'Astronomical Data Analysis Software and Systems XII ASP Conference Series, Vol. 295, 2003 H. E. Payne, R. I. Jedrzejewski, and R. N. Hook, eds., p.283',
             u'pubdate': u'2003-00-00',
             u'read_count': 0,
             'reference': [u'1977JAP....48.4729M',
              u'1981psd..book.....S',
              u'1981wi...book.....S',
              u'1986PhRvB..33.5545M',
              u'1987ApPhL..51..913T',
              u'1992Sci...258.1474S',
              u'1994IJMPB...8..237S',
              u'1995Natur.376..498H',
              u'1995Sci...270.1789Y',
              u'1998TSF...331...76O',
              u'1999Natur.397..121F',
              u'2000JaJAP..39...94P',
              u'2002ApPhL..81.3885S',
              u'2004ApPhL..85.3890C',
              u'2004TSF...451..105S',
              u'2005PhRvB..72s5208M',
              u'2006ApPhL..89l3505L'],
             u'simbid': ['2419335', '3111723'],
             u'simbtype': [u'Other', u'Star'],
             u'simbad_object_facet_hier': [u'0/Other', u'1/Other/2419335', u'0/Star', u'1/Star/3111723'],
             u'title': [u'Chandra Data Archive Download and Usage Database'],
             u'volume': u'295',
             u'year': u'2003'},
        solr_updater.transform_json_record(rec))

        for x in Records._date_fields:
            if x in rec:
                rec[x] = get_date('2017-09-19T21:17:12.026474+00:00')
        
        x = solr_updater.transform_json_record(rec)
        for f in ('metadata_mtime', 'fulltext_mtime', 'orcid_mtime', 'nonbib_mtime', 'metrics_mtime', 'update_timestamp'):
            self.assertEquals(x[f], '2017-09-19T21:17:12.026474Z')
        
        rec['orcid_claims_updated'] = get_date('2017-09-20T21:17:12.026474+00:00')
        x = solr_updater.transform_json_record(rec)
        for f in ('metadata_mtime', 'fulltext_mtime', 'orcid_mtime', 'nonbib_mtime', 'metrics_mtime', 'update_timestamp'):
            if f == 'update_timestamp' or f == 'orcid_mtime':
                self.assertEquals(x[f], '2017-09-20T21:17:12.026474Z')
            else:
                self.assertEquals(x[f], '2017-09-19T21:17:12.026474Z')


if __name__ == '__main__':
    unittest.main()
