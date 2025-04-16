#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import os
import copy
import json
from mock import patch

from adsmp import app
from adsmp.models import Base
import testing.postgresql


class TestIdentifierExtraction(unittest.TestCase):
    """
    Tests the identifier extraction and processing methods in the application
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
        self.app = app.ADSMasterPipelineCelery('test', local_config=\
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'METRICS_SQLALCHEMY_URL': 'postgresql://postgres@127.0.0.1:15678/test',
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME': proj_home,
            'TEST_DIR': os.path.join(proj_home, 'adsmp/tests'),
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()
    
    def test_extract_data_components(self):
        """Test that _extract_data_components correctly extracts and validates data"""
        
        # Test with complete record
        complete_record = {
            'bibcode': '2008arXiv0802.0143H',
            'bib_data': {'title': 'Test Title'},
            'fulltext': {'body': 'Test body'},
            'metrics': {'citation_num': 5},
            'nonbib_data': {'property': ['ARTICLE']},
            'orcid_claims': {'authors': []}
        }
        
        bibcode, bib_data, fulltext, metrics, nonbib, orcid_claims = \
            self.app._extract_data_components(complete_record)
        
        self.assertEqual(bibcode, '2008arXiv0802.0143H')
        self.assertEqual(bib_data, {'title': 'Test Title'})
        self.assertEqual(fulltext, {'body': 'Test body'})
        self.assertEqual(metrics, {'citation_num': 5})
        self.assertEqual(nonbib, {'property': ['ARTICLE']})
        self.assertEqual(orcid_claims, {'authors': []})
        
        # Test with missing components
        incomplete_record = {
            'bibcode': '2008arXiv0802.0143H',
            'bib_data': None,  # This should be converted to {}
            # missing fulltext
            'metrics': 'invalid',  # Not a dict, should be converted to {}
            # missing nonbib
            'orcid_claims': []  # Not a dict, should be converted to {}
        }
        
        bibcode, bib_data, fulltext, metrics, nonbib, orcid_claims = \
            self.app._extract_data_components(incomplete_record)
        
        self.assertEqual(bibcode, '2008arXiv0802.0143H')
        self.assertEqual(bib_data, {})
        self.assertEqual(fulltext, {})
        self.assertEqual(metrics, {})
        self.assertEqual(nonbib, {})
        self.assertEqual(orcid_claims, {})
    
    def test_is_arxiv_id(self):
        """Test that _is_arxiv_id correctly identifies arXiv identifiers"""
        
        # Test standard arXiv format with prefix
        self.assertTrue(self.app.is_arxiv_id('arxiv:2301.12345'))
        self.assertTrue(self.app.is_arxiv_id('arXiv:2301.12345'))
        
        # Test URL format
        self.assertTrue(self.app.is_arxiv_id('10.48550/arXiv.2502.20510'))
        self.assertTrue(self.app.is_arxiv_id('10.48550/arXiv.astro-ph/0610305'))
        
        # Test edge cases
        self.assertTrue(self.app.is_arxiv_id('arXiv:2301.12345'))  # Original capitalization preserved
        self.assertTrue(self.app.is_arxiv_id('ARXIV:2301.12345'))  # Case preserved
        
        # Test with non-arXiv identifiers
        self.assertFalse(self.app.is_arxiv_id('doi:10.1234/test'))
        self.assertFalse(self.app.is_arxiv_id('2008arXiv0802.0143H'))  # Bibcode
        
        # Test with None or non-string input
        self.assertFalse(self.app.is_arxiv_id(123))
    
    def test_is_doi_id(self):
        """Test that _is_doi_id correctly identifies DOI identifiers"""
        
        # Test standard DOI format with prefix
        # self.assertTrue(self.app.is_doi_id('10.1234/test'))
        
        self.assertTrue(self.app.is_doi_id("10.48550/arXiv.2502.20510"))
        self.assertTrue(self.app.is_doi_id("10.1016/j.jtbi.2008.11.029"))
        
        # Test direct DOI format (no prefix)
        self.assertTrue(self.app.is_doi_id('10.1234/test'))
        
        # Test with non-DOI identifiers
        self.assertFalse(self.app.is_doi_id('arxiv:2301.12345'))
        self.assertFalse(self.app.is_doi_id('2008arXiv0802.0143H'))  # Bibcode
        
        # Test with None or non-string input
        self.assertFalse(self.app.is_doi_id(123))
    
    def test_collect_identifiers(self):
        """Test that _collect_identifiers correctly collects identifiers from all sources"""
        
        # Test with all sources having identifiers
        bibcode = '2008arXiv0802.0143H'
        bib_data = {
            'identifier': ["1988AnBot..61..393A",
                            "10.1093/oxfordjournals.aob.a087569",
                            "10.1016/j.physleta.2005.08.078",
                            "10.48550/arXiv.nlin/0510022",
                            "arXiv:nlin/0510022",
                            "2005nlin.....10022X",
                            "2006PhLA..349..128X",
                            "10.1134/S0036024419050133",
                            "2019RJPCA..93..993G",
                            "2005hep.ph...10301C"],
            'alternate_bibcode': [  "1942JGR....47..251E",
                                    "2004cond.mat.11661L",
                                    "1942QB51.B5........",
                                    "2005cond.mat..9445G",
                                    "2005cond.mat..3372D",
                                    "1977VeMFA..18...25K",
                                    "1942QB107.M3.......",
                                    "2004math.ph..12044E",
                                    "2005nlin.....10022X",
                                    None]
                                    }

        
        identifiers = self.app._collect_identifiers(bibcode, bib_data)
        
        expected = set([bibcode] + bib_data['identifier'] + [id for id in bib_data['alternate_bibcode'] if id is not None])

        self.assertEqual(sorted(identifiers), sorted(expected))
        
        # Test with missing data
        bibcode = '2008arXiv0802.0143H'
        bib_data = {}  # No identifiers
        
        identifiers = self.app._collect_identifiers(bibcode, bib_data)
        
        # Should only contain the bibcode
        self.assertEqual(set(identifiers), {'2008arXiv0802.0143H'})
    
    def test_populate_identifiers(self):
        """Test that _populate_identifiers correctly extracts and populates identifier information"""
        
        # Create a test record with a variety of identifiers
        test_record = {
            'bibcode': '2008arXiv0802.0143H',
            'bib_data': {
                'identifier': [
                    "10.48550/arXiv.2502.20510",
                    "arXiv:2502.20510",
                    "2025arXiv250220510L",
                    "10.48550/arXiv.2502.20407",
                    "2025arXiv250220407K",
                    "arXiv:2502.20407",
                    "10.48550/arXiv.2004.00015",
                    "arXiv:2004.00015",
                    "2020arXiv200400015S",
                    "2020arXiv200712475E",
                    "arXiv:2007.12475",
                    "10.48550/arXiv.2007.12475",
                    "arXiv:2502.20561" 
                ],
                'alternate_bibcode': ["1942JGR....47..251E",
                                    "2004cond.mat.11661L",
                                    "1942QB51.B5........",
                                    "2005cond.mat..9445G",
                                    "2005cond.mat..3372D",
                                    "1977VeMFA..18...25K",
                                    "1942QB107.M3.......",
                                    "2004math.ph..12044E",
                                    "2005nlin.....10022X"]
            }
        }
        
        # Create a resolver record and links structure
        resolver_record = {'bibcode': '2008arXiv0802.0143H'}
        links = {
            'ARXIV': [],
            'DOI': []
        }
        
        # Call the method
        updated_links = self.app._populate_identifiers(test_record, resolver_record, links)
        
        # Check that identifiers were extracted and consolidated
        self.assertIn('identifier', resolver_record)
        self.assertEqual(len(resolver_record['identifier']), len(set(test_record['bib_data']['identifier'] + test_record['bib_data']['alternate_bibcode'] + [test_record['bibcode']])))  # All unique identifiers
        
        # Check that ARXIV and DOI fields were populated correctly - now with full identifiers
        self.assertEqual(set(updated_links['ARXIV']), set(['10.48550/arXiv.2502.20510', 'arXiv:2502.20510', '10.48550/arXiv.2004.00015', 'arXiv:2502.20561', 'arXiv:2004.00015', '10.48550/arXiv.2007.12475', 'arXiv:2007.12475', 'arXiv:2502.20407', '10.48550/arXiv.2502.20407']))
        self.assertEqual(set(updated_links['DOI']), set(['10.48550/arXiv.2502.20407', '10.48550/arXiv.2502.20510', '10.48550/arXiv.2007.12475', '10.48550/arXiv.2004.00015']))
        
    
    def test_integration_with_populate_links_structure(self):
        """Test the integration between _populate_identifiers and _populate_links_structure"""
        
        # Create a comprehensive test record
        test_record = {
            'bibcode': '2008arXiv0802.0143H',
            'bib_data': {
                'abstract': 'This is a test abstract',
                'identifier': [
                    'arXiv:math/0406160',
                    'arXiv:2502.20510',
                    '10.48550/arXiv.2004.00015',
                    '10.1007/s10955-009-9793-2'
                ]
            },
            'fulltext': {
                'body': 'Test body'
            },
            'metrics': {
                'citation_num': 10,
                'reads': [20, 10, 5],
                'reference_num': 15
            },
            'nonbib_data': {
                'reference': ['ref1', 'ref2', 'ref3'],
                'property': ['ARTICLE', 'REFEREED', 'TOC']
            }
        }
        
        # Create a basic resolver record
        resolver_record = {
            'bibcode': '2008arXiv0802.0143H',
            'links': {
                'ARXIV': [],
                'DOI': [],
                'DATA': {},
                'ESOURCE': {},
                'ASSOCIATED': {'url': [], 'title': [], 'count': 0},
                'ABSTRACT': False,
                'CITATIONS': False,
                'GRAPHICS': False,
                'METRICS': False,
                'OPENURL': False,
                'REFERENCES': False,
                'TOC': False,
                'COREAD': False
            }
        }
        
        # Populate the links structure
        enriched_record = self.app._populate_links_structure(test_record, resolver_record)
        
        # Verify identifiers were collected
        self.assertIn('identifier', enriched_record)
        self.assertIn('2008arXiv0802.0143H', enriched_record['identifier'])
        self.assertIn('arXiv:math/0406160', enriched_record['identifier'])
        self.assertIn('10.48550/arXiv.2004.00015', enriched_record['identifier'])
        
        # Verify ARXIV and DOI fields were populated with original identifiers
        self.assertIn('arXiv:2502.20510', enriched_record['links']['ARXIV'])
        self.assertIn('10.48550/arXiv.2004.00015', enriched_record['links']['DOI'])
        self.assertIn('10.1007/s10955-009-9793-2', enriched_record['links']['DOI'])

        # Verify other fields were set correctly
        self.assertTrue(enriched_record['links']['ABSTRACT'])
        self.assertTrue(enriched_record['links']['CITATIONS'])
        self.assertTrue(enriched_record['links']['GRAPHICS'])
        self.assertTrue(enriched_record['links']['METRICS'])
        self.assertTrue(enriched_record['links']['REFERENCES'])
        self.assertTrue(enriched_record['links']['TOC'])


if __name__ == '__main__':
    unittest.main() 