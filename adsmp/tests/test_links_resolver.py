#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import os
import copy
import json

from adsmp import app
from adsmp.models import Base
from adsputils import get_date
import testing.postgresql

class TestLinksResolver(unittest.TestCase):
    """
    Tests for the links resolver data structure completeness
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
            'PROJ_HOME' : proj_home,
            'TEST_DIR' : os.path.join(proj_home, 'adsmp/tests'),
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def create_complete_test_record(self):
        """Create a test record with all fields needed for a complete resolver structure"""
        return {
            'bibcode': '2023MNRAS.518..529S',
            'bib_data': {
                'identifier': [
                    'arXiv:2301.12345',
                    '10.1093/mnras/stac3079', 
                    '10.48550/arXiv.astro-ph/0610305',
                    '2023MNRAS.518..529S',
                ],
                'alternate_bibcode': ['2023arXiv230112345S'],
                'abstract': 'This is a test abstract for a complete record',
                'pub': 'Monthly Notices of the Royal Astronomical Society',
                'volume': '518',
                'issue': '1',
                'page': '529-545',
            },
            'fulltext': {
                'body': 'This is the full text body.',
                'acknowledgements': 'The authors would like to thank everyone.'
            },
            'metrics': {
                'citation_num': 25,
                'reads': [50, 30, 20, 10],
                'downloads': [20, 15, 10, 5],
                'reference_num': 45,
                'citations': ['2023ApJ...934L...7B', '2023A&A...672A..34C'],
                'rn_citations_hist': {
                    '0': 10,  # Number of reads in the most recent period
                    '1': 8,
                    '2': 6
                }
            },
            'nonbib_data': {
                'citation': ['2023ApJ...934L...7B', '2023A&A...672A..34C'],
                'citation_count': 25,
                'reference': ['2021ApJ...908L..16F', '2022ApJ...930L..10K', '2022MNRAS.512.1317B'],
                'read_count': 110,
                'property': ['REFEREED', 'ARTICLE', 'TOC'],
                'data_links_rows': [
                    {
                        'link_type': 'ESOURCE',
                        'link_sub_type': 'PUB_HTML',
                        'url': ['https://academic.oup.com/mnras/article/518/1/529/6795543'],
                        'title': ['Publisher HTML'],
                        'item_count': 1
                    },
                    {
                        'link_type': 'DATA',
                        'link_sub_type': 'CDS',
                        'url': ['https://cdsarc.cds.unistra.fr/viz-bin/cat/J/MNRAS/518/529'],
                        'title': ['CDS Astronomical Database'],
                        'item_count': 1
                    },
                    {
                        'link_type': 'INSPIRE',
                        'url': ['https://inspirehep.net/literature/2023MNRAS.518..529S'],
                        'title': ['INSPIRE Entry'],
                        'item_count': 1
                    },
                    {
                        'link_type': 'ASSOCIATED',
                        'url': ['https://zenodo.org/record/12345'],
                        'title': ['Zenodo Dataset'],
                        'item_count': 1
                    },
                    {
                        'link_type': 'PRESENTATION',
                        'url': ['https://conference.org/2023/presentation123'],
                        'title': ['Conference Presentation'],
                        'item_count': 1
                    },
                    {
                        'link_type': 'LIBRARYCATALOG',
                        'url': ['https://library.institution.edu/catalog/2023MNRAS.518..529S'],
                        'title': ['Library Catalog Entry'],
                        'item_count': 1
                    }
                ]
            }
        }

    def test_links_resolver_completeness(self):
        """Test that the links resolver structure is complete with all expected fields"""
        
        # Create a complex test record with all required fields
        test_record = self.create_complete_test_record()
        
        # Generate resolver record
        resolver_record = self.app.generate_links_for_resolver(test_record)
        
        # Check that the resolver record is created and has the correct bibcode
        self.assertIsNotNone(resolver_record)
        self.assertEqual(test_record['bibcode'], resolver_record['bibcode'])
        
        # Check that links structure exists and is a dict
        self.assertIn('links', resolver_record)
        self.assertIsInstance(resolver_record['links'], dict)
        
        # Verify all identifiers were collected
        self.assertIn('identifier', resolver_record)
        
        # IDENTIFIERS
        identifiers = resolver_record['identifier']
        self.assertIsInstance(identifiers, list)
        # Check that all identifiers from the test record are included
        for identifier in test_record['bib_data']['identifier']:
            self.assertIn(identifier, identifiers)
        # Check that alternate bibcode is included
        self.assertIn(test_record['bib_data']['alternate_bibcode'][0], identifiers)
        
        # LINKS STRUCTURE COMPLETENESS
        links = resolver_record['links']
        
        # Check core fields are present
        self.assertIn('ARXIV', links)
        self.assertIn('DOI', links)
        self.assertIn('DATA', links)
        self.assertIn('ESOURCE', links)
        self.assertIn('ASSOCIATED', links)
        self.assertIn('INSPIRE', links)
        self.assertIn('LIBRARYCATALOG', links)
        self.assertIn('PRESENTATION', links)
        
        # Check boolean flags
        self.assertIn('ABSTRACT', links)
        self.assertIn('CITATIONS', links)
        self.assertIn('GRAPHICS', links)
        self.assertIn('METRICS', links)
        self.assertIn('OPENURL', links)
        self.assertIn('REFERENCES', links)
        self.assertIn('TOC', links)
        self.assertIn('COREAD', links)
        
        # Verify ARXIV and DOI fields contain the correct identifiers
        self.assertIsInstance(links['ARXIV'], list)
        self.assertIsInstance(links['DOI'], list)
        self.assertIn('arXiv:2301.12345', links['ARXIV'])
        self.assertIn('10.1093/mnras/stac3079', links['DOI'])
        self.assertIn('10.48550/arXiv.astro-ph/0610305', links['DOI'])
        # Verify DATA link types
        self.assertIsInstance(links['DATA'], dict)
        self.assertIn('CDS', links['DATA'])
        self.assertEqual(links['DATA']['CDS']['url'][0], 'https://cdsarc.cds.unistra.fr/viz-bin/cat/J/MNRAS/518/529')
        self.assertEqual(links['DATA']['CDS']['title'][0], 'CDS Astronomical Database')
        self.assertEqual(links['DATA']['CDS']['count'], 1)
        
        # Verify ESOURCE link types
        self.assertIsInstance(links['ESOURCE'], dict)
        self.assertIn('PUB_HTML', links['ESOURCE'])
        self.assertEqual(links['ESOURCE']['PUB_HTML']['url'][0], 'https://academic.oup.com/mnras/article/518/1/529/6795543')
        self.assertEqual(links['ESOURCE']['PUB_HTML']['title'][0], 'Publisher HTML')
        self.assertEqual(links['ESOURCE']['PUB_HTML']['count'], 1)
        
        # Verify ASSOCIATED link
        self.assertIsInstance(links['ASSOCIATED'], dict)
        self.assertIn('url', links['ASSOCIATED'])
        self.assertIn('title', links['ASSOCIATED'])
        self.assertIn('count', links['ASSOCIATED'])
        self.assertEqual(links['ASSOCIATED']['url'][0], 'https://zenodo.org/record/12345')
        self.assertEqual(links['ASSOCIATED']['title'][0], 'Zenodo Dataset')
        self.assertEqual(links['ASSOCIATED']['count'], 1)
        
        # Verify INSPIRE link
        self.assertIsInstance(links['INSPIRE'], dict)
        self.assertIn('url', links['INSPIRE'])
        self.assertIn('title', links['INSPIRE'])
        self.assertIn('count', links['INSPIRE'])
        self.assertEqual(links['INSPIRE']['url'][0], 'https://inspirehep.net/literature/2023MNRAS.518..529S')
        self.assertEqual(links['INSPIRE']['title'][0], 'INSPIRE Entry')
        self.assertEqual(links['INSPIRE']['count'], 1)
        
        # Verify PRESENTATION link
        self.assertIsInstance(links['PRESENTATION'], dict)
        self.assertIn('url', links['PRESENTATION'])
        self.assertIn('title', links['PRESENTATION'])
        self.assertIn('count', links['PRESENTATION'])
        self.assertEqual(links['PRESENTATION']['url'][0], 'https://conference.org/2023/presentation123')
        self.assertEqual(links['PRESENTATION']['title'][0], 'Conference Presentation')
        self.assertEqual(links['PRESENTATION']['count'], 1)
        
        # Verify LIBRARYCATALOG link
        self.assertIsInstance(links['LIBRARYCATALOG'], dict)
        self.assertIn('url', links['LIBRARYCATALOG'])
        self.assertIn('title', links['LIBRARYCATALOG'])
        self.assertIn('count', links['LIBRARYCATALOG'])
        self.assertEqual(links['LIBRARYCATALOG']['url'][0], 'https://library.institution.edu/catalog/2023MNRAS.518..529S')
        self.assertEqual(links['LIBRARYCATALOG']['title'][0], 'Library Catalog Entry')
        self.assertEqual(links['LIBRARYCATALOG']['count'], 1)
        
        # Verify boolean flags are all set correctly
        self.assertTrue(links['ABSTRACT'])
        self.assertTrue(links['CITATIONS'])
        self.assertTrue(links['GRAPHICS'])
        self.assertTrue(links['METRICS'])
        self.assertTrue(links['OPENURL'])
        self.assertTrue(links['REFERENCES'])
        self.assertTrue(links['TOC'])
        self.assertTrue(links['COREAD'])
        
        # Test record without links_data - should return None
        minimal_record = {
            'bibcode': 'minimal2023',
            'bib_data': {
                'identifier': ['minimal2023']
            }
        }
        
        minimal_resolver = self.app.generate_links_for_resolver(minimal_record)
        self.assertIsNone(minimal_resolver)

        # Test minimal record with links_data containing a URL
        minimal_record_with_links = {
            'bibcode': 'minimal2023',
            'bib_data': {
                'identifier': ['minimal2023'],
                'links_data': ['{"access": "open", "instances": "", "title": "", "type": "preprint", "url": "http://arxiv.org/abs/1902.09522"}']
            }
        }
        
        minimal_resolver = self.app.generate_links_for_resolver(minimal_record_with_links)
        
        # Check that resolver record is not None
        self.assertIsNotNone(minimal_resolver)
        self.assertEqual('minimal2023', minimal_resolver['bibcode'])
        
        # Check links structure
        minimal_links = minimal_resolver['links']
        
        # Verify structure is complete even with minimal data
        self.assertIn('ARXIV', minimal_links)
        self.assertIn('DOI', minimal_links)
        self.assertIn('DATA', minimal_links)
        self.assertIn('ESOURCE', minimal_links)
        self.assertIn('ASSOCIATED', minimal_links)
        self.assertIn('INSPIRE', minimal_links)
        self.assertIn('LIBRARYCATALOG', minimal_links)
        self.assertIn('PRESENTATION', minimal_links)
        self.assertIn('ABSTRACT', minimal_links)
        self.assertIn('CITATIONS', minimal_links)
        self.assertIn('GRAPHICS', minimal_links)
        self.assertIn('METRICS', minimal_links)
        self.assertIn('OPENURL', minimal_links)
        self.assertIn('REFERENCES', minimal_links)
        self.assertIn('TOC', minimal_links)
        self.assertIn('COREAD', minimal_links)
        
        # Check ESOURCE fields for the URL from links_data
        self.assertIn('EPRINT_HTML', minimal_links['ESOURCE'])
        self.assertEqual(minimal_links['ESOURCE']['EPRINT_HTML']['url'][0], 'http://arxiv.org/abs/1902.09522')
        
        # Check that a PDF link is also created
        self.assertIn('EPRINT_PDF', minimal_links['ESOURCE'])
        self.assertEqual(minimal_links['ESOURCE']['EPRINT_PDF']['url'][0], 'http://arxiv.org/pdf/1902.09522')
        
        # Test with non-bib data links
        nonbib_links_record = {
            'bibcode': 'nonbib2023',
            'nonbib_data': {
                'data_links_rows': [
                    {
                        'link_type': 'ESOURCE',
                        'link_sub_type': 'EPRINT_HTML',
                        'url': ['http://arxiv.org/abs/2301.00001'],
                        'title': ['arXiv Preprint'],
                        'item_count': 1
                    }
                ]
            }
        }
        
        nonbib_resolver = self.app.generate_links_for_resolver(nonbib_links_record)
        self.assertIsNotNone(nonbib_resolver)
        nonbib_links = nonbib_resolver['links']
        
        # Check that ESOURCE contains the expected data
        self.assertIn('EPRINT_HTML', nonbib_links['ESOURCE'])
        self.assertEqual(nonbib_links['ESOURCE']['EPRINT_HTML']['url'][0], 'http://arxiv.org/abs/2301.00001')
        self.assertEqual(nonbib_links['ESOURCE']['EPRINT_HTML']['title'][0], 'arXiv Preprint')

    def test_links_structure_with_edge_cases(self):
        """Test that the links resolver structure handles edge cases correctly"""
        
        # Test with empty fields
        empty_record = {
            'bibcode': 'empty2023',
            'bib_data': {},
            'fulltext': {},
            'metrics': {},
            'nonbib_data': {}
        }
        
        empty_resolver = self.app.generate_links_for_resolver(empty_record)
        self.assertIsNone(empty_resolver)
        
        # Test with None values
        none_record = {
            'bibcode': 'none2023',
            'bib_data': None,
            'fulltext': None,
            'metrics': None,
            'nonbib_data': None
        }
        
        none_resolver = self.app.generate_links_for_resolver(none_record)
        self.assertIsNone(none_resolver)

if __name__ == '__main__':
    unittest.main() 