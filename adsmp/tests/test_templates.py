#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import tempfile
import unittest

from adsmp import templates
import xml.etree.ElementTree as ET



class TestTemplates(unittest.TestCase):
    """
    Tests the template functionality for sitemap generation
    """
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        
    def test_robots_txt_template(self):
        """Test robots.txt template rendering"""
        # Test ADS robots.txt
        ads_robots = templates.render_robots_txt('https://ui.adsabs.harvard.edu/sitemap')
        self.assertIsInstance(ads_robots, str)
        self.assertGreater(len(ads_robots), 0)
        
        self.assertIn('Sitemap: https://ui.adsabs.harvard.edu/sitemap/sitemap_index.xml', ads_robots)
        
        # Check for major search engines
        self.assertIn('User-agent: Googlebot', ads_robots)
        self.assertIn('User-agent: msnbot', ads_robots)
        self.assertIn('User-agent: Slurp', ads_robots)
        self.assertIn('User-agent: Teoma', ads_robots)
        self.assertIn('User-agent: *', ads_robots)
        
        # Check common disallowed paths
        self.assertIn('Disallow: /v1/', ads_robots)
        self.assertIn('Disallow: /resources', ads_robots)
        self.assertIn('Disallow: /core', ads_robots)
        self.assertIn('Disallow: /tugboat', ads_robots)
        self.assertIn('Disallow: /link_gateway/', ads_robots)
        self.assertIn('Disallow: /search/', ads_robots)
        self.assertIn('Disallow: /execute-query/', ads_robots)
        self.assertIn('Disallow: /status', ads_robots)
        self.assertIn('Disallow: /cgi-bin/', ads_robots)
        
        # Check allowed paths
        self.assertIn('Allow: /help/', ads_robots)
        self.assertIn('Allow: /about/', ads_robots)
        self.assertIn('Allow: /blog/', ads_robots)
        self.assertIn('Allow: /abs/', ads_robots)
        self.assertIn('Allow: /full/', ads_robots)
        
        # Check specific disallowed sub-paths
        self.assertIn('Disallow: /abs/*/coreads', ads_robots)
        self.assertIn('Disallow: /abs/*/similar', ads_robots)
        self.assertIn('Disallow: /abs/*/toc', ads_robots)
        self.assertIn('Disallow: /abs/*/graphics', ads_robots)
        self.assertIn('Disallow: /abs/*/metrics', ads_robots)
        self.assertIn('Disallow: /abs/*/exportcitation', ads_robots)
        
        # Test SciX robots.txt
        scix_robots = templates.render_robots_txt('https://scixplorer.org/sitemap')
        self.assertIsInstance(scix_robots, str)
        self.assertGreater(len(scix_robots), 0)
        self.assertIn('Sitemap: https://scixplorer.org/sitemap/sitemap_index.xml', scix_robots)
        
        # Check that SciX also has the same structure as ADS
        self.assertIn('User-agent: Googlebot', scix_robots)
        self.assertIn('User-agent: msnbot', scix_robots)
        self.assertIn('User-agent: Slurp', scix_robots)
        self.assertIn('User-agent: Teoma', scix_robots)
        self.assertIn('User-agent: *', scix_robots)
        
        # Check common disallowed paths in SciX
        self.assertIn('Disallow: /v1/', scix_robots)
        self.assertIn('Disallow: /resources', scix_robots)
        self.assertIn('Disallow: /core', scix_robots)
        self.assertIn('Disallow: /tugboat', scix_robots)
        self.assertIn('Disallow: /link_gateway/', scix_robots)
        self.assertIn('Disallow: /search/', scix_robots)
        self.assertIn('Disallow: /execute-query/', scix_robots)
        self.assertIn('Disallow: /status', scix_robots)
        self.assertIn('Disallow: /cgi-bin/', scix_robots)
        
        # Check allowed paths in SciX
        self.assertIn('Allow: /help/', scix_robots)
        self.assertIn('Allow: /about/', scix_robots)
        self.assertIn('Allow: /blog/', scix_robots)
        self.assertIn('Allow: /abs/', scix_robots)
        
        # Check specific disallowed sub-paths in SciX
        self.assertIn('Disallow: /abs/*/coreads', scix_robots)
        self.assertIn('Disallow: /abs/*/similar', scix_robots)
        self.assertIn('Disallow: /abs/*/toc', scix_robots)
        self.assertIn('Disallow: /abs/*/graphics', scix_robots)
        self.assertIn('Disallow: /abs/*/metrics', scix_robots)
        self.assertIn('Disallow: /abs/*/exportcitation', scix_robots)
        
        
    def test_sitemap_file_template(self):
        """Test sitemap_file.xml template rendering"""
        # Test with default URL pattern (ADS)
        url_entry1 = templates.format_url_entry('2023ApJ...123..456A', '2024-01-15')
        self.assertIsInstance(url_entry1, str)
        self.assertIn('https://ui.adsabs.harvard.edu/abs/2023ApJ...123..456A/abstract', url_entry1)
        self.assertIn('<lastmod>2024-01-15</lastmod>', url_entry1)
        
        # Test with custom URL pattern (SciX)
        url_entry2 = templates.format_url_entry('2023ApJ...123..457B', '2024-01-16', 
                                               'https://scixplorer.org/abs/{bibcode}/abstract')
        self.assertIsInstance(url_entry2, str)
        self.assertIn('https://scixplorer.org/abs/2023ApJ...123..457B/abstract', url_entry2)
        self.assertIn('<lastmod>2024-01-16</lastmod>', url_entry2)
    
        # Test complete sitemap file rendering
        sitemap_content = templates.render_sitemap_file(url_entry1)
        self.assertIsInstance(sitemap_content, str)
        self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', sitemap_content)
        self.assertIn('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">', sitemap_content)
        self.assertIn('https://ui.adsabs.harvard.edu/abs/2023ApJ...123..456A/abstract', sitemap_content)
        self.assertIn('</urlset>', sitemap_content)

        sitemap_content = templates.render_sitemap_file(url_entry2)
        self.assertIsInstance(sitemap_content, str)
        self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', sitemap_content)
        self.assertIn('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">', sitemap_content)
        self.assertIn('https://scixplorer.org/abs/2023ApJ...123..457B/abstract', sitemap_content)
        self.assertIn('</urlset>', sitemap_content)
        
    def test_multiple_entries_ads_and_scix(self):
        """Test sitemap generation with multiple entries for both ADS and SciX"""
        # Test data with multiple bibcodes
        test_bibcodes = [
            ('2023ApJ...123..456A', '2024-01-15'),
            ('2023ApJ...123..457B', '2024-01-16'),
            ('2023A&A...789..123C', '2024-01-17'),
            ('2023MNRAS.456..789D', '2024-01-18'),
            ('2023Nature.567..890E', '2024-01-19')
        ]
        
        # ADS URL pattern
        ads_pattern = 'https://ui.adsabs.harvard.edu/abs/{bibcode}/abstract'
        ads_entries = []
        for bibcode, lastmod in test_bibcodes:
            entry = templates.format_url_entry(bibcode, lastmod, ads_pattern)
            ads_entries.append(entry)
            
        # SciX URL pattern  
        scix_pattern = 'https://scixplorer.org/abs/{bibcode}/abstract'
        scix_entries = []
        for bibcode, lastmod in test_bibcodes:
            entry = templates.format_url_entry(bibcode, lastmod, scix_pattern)
            scix_entries.append(entry)
            
        # Test ADS sitemap with multiple entries
        ads_sitemap_content = templates.render_sitemap_file(''.join(ads_entries))
        self.assertIsInstance(ads_sitemap_content, str)
        self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', ads_sitemap_content)
        self.assertIn('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">', ads_sitemap_content)
        self.assertIn('</urlset>', ads_sitemap_content)
        
        # Verify all ADS URLs are present
        for bibcode, lastmod in test_bibcodes:
            expected_ads_url = ads_pattern.format(bibcode=bibcode)
            self.assertIn(expected_ads_url, ads_sitemap_content, 
                         f"ADS URL for {bibcode} not found in sitemap")
            self.assertIn(f'<lastmod>{lastmod}</lastmod>', ads_sitemap_content,
                         f"Last modified date {lastmod} not found for {bibcode}")
            
        # Test SciX sitemap with multiple entries
        scix_sitemap_content = templates.render_sitemap_file(''.join(scix_entries))
        self.assertIsInstance(scix_sitemap_content, str)
        self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', scix_sitemap_content)
        self.assertIn('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">', scix_sitemap_content)
        self.assertIn('</urlset>', scix_sitemap_content)
        
        # Verify all SciX URLs are present
        for bibcode, lastmod in test_bibcodes:
            expected_scix_url = scix_pattern.format(bibcode=bibcode)
            self.assertIn(expected_scix_url, scix_sitemap_content,
                         f"SciX URL for {bibcode} not found in sitemap")
            self.assertIn(f'<lastmod>{lastmod}</lastmod>', scix_sitemap_content,
                         f"Last modified date {lastmod} not found for {bibcode}")
            
        # Verify that ADS and SciX sitemaps are different (contain different URLs)
        self.assertNotEqual(ads_sitemap_content, scix_sitemap_content,
                           "ADS and SciX sitemap content should be different")
        
        # Count URL entries to ensure we have the expected number
        ads_url_count = ads_sitemap_content.count('<url>')
        scix_url_count = scix_sitemap_content.count('<url>')
        expected_count = len(test_bibcodes)
        
        self.assertEqual(ads_url_count, expected_count,
                        f"Expected {expected_count} URL entries in ADS sitemap, found {ads_url_count}")
        self.assertEqual(scix_url_count, expected_count,
                        f"Expected {expected_count} URL entries in SciX sitemap, found {scix_url_count}")

    def test_sitemap_index_template(self):
        """Test sitemap_index.xml template rendering"""
        # Test sitemap entry formatting
        entry1 = templates.format_sitemap_entry('https://ui.adsabs.harvard.edu/sitemap', 
                                              'sitemap_bib_1.xml', '2024-01-15')
        self.assertIsInstance(entry1, str)
        self.assertIn('<sitemap>', entry1)
        self.assertIn('https://ui.adsabs.harvard.edu/sitemap/sitemap_bib_1.xml', entry1)
        self.assertIn('<lastmod>2024-01-15</lastmod>', entry1)
        self.assertIn('</sitemap>', entry1)
        
        entry2 = templates.format_sitemap_entry('https://ui.adsabs.harvard.edu/sitemap',
                                              'sitemap_bib_2.xml', '2024-01-16')
        self.assertIsInstance(entry2, str)
        
        # Test complete index rendering
        index_content = templates.render_sitemap_index(entry1 + entry2)
        self.assertIsInstance(index_content, str)
        self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', index_content)
        self.assertIn('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">', index_content)
        self.assertIn('https://ui.adsabs.harvard.edu/sitemap/sitemap_bib_1.xml', index_content)
        self.assertIn('https://ui.adsabs.harvard.edu/sitemap/sitemap_bib_2.xml', index_content)
        self.assertIn('</sitemapindex>', index_content)
        
    def test_template_files_exist(self):
        """Test that all template files exist and are readable"""
        template_files = [
            'robots.txt',
            'sitemap_file.xml', 
            'sitemap_index.xml'
        ]
        
        for template_name in template_files:
            with self.subTest(template=template_name):
                # Test template path resolution
                template_path = templates.get_template_path(template_name)
                self.assertIsInstance(template_path, str)
                self.assertTrue(os.path.exists(template_path), 
                              f"Template file not found: {template_path}")
                
                # Test template loading
                content = templates.load_template(template_name)
                self.assertIsInstance(content, str)
                self.assertGreater(len(content), 0)
                
                # Test template structure
                if template_name == 'robots.txt':
                    self.assertIn('{sitemap_url}', content)
                elif template_name == 'sitemap_file.xml':
                    self.assertIn('{url_entries}', content)
                    self.assertIn('urlset', content)
                elif template_name == 'sitemap_index.xml':
                    self.assertIn('{sitemap_entries}', content)
                    self.assertIn('sitemapindex', content)
                    
    def test_file_generation_integration(self):
        """Test complete file generation workflow
        
        Expected directory structure:
        /app/logs/sitemap/ads/robots.txt
        /app/logs/sitemap/ads/sitemap_bib_1.xml
        /app/logs/sitemap/ads/sitemap_index.xml
        /app/logs/sitemap/scix/robots.txt
        /app/logs/sitemap/scix/sitemap_bib_1.xml
        /app/logs/sitemap/scix/sitemap_index.xml
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create directory structure similar to production: /app/logs/sitemap/
            sitemap_base_dir = os.path.join(temp_dir, 'app', 'logs', 'sitemap')
            
            os.makedirs(sitemap_base_dir, exist_ok=True)
            
            # Test data
            sites = {
                'ads': {
                    'name': 'ADS',
                    'sitemap_url': 'https://ui.adsabs.harvard.edu/sitemap',
                    'abs_url_pattern': 'https://ui.adsabs.harvard.edu/abs/{bibcode}/abstract'
                },
                'scix': {
                    'name': 'SciX',
                    'sitemap_url': 'https://scixplorer.org/sitemap', 
                    'abs_url_pattern': 'https://scixplorer.org/abs/{bibcode}/abstract'
                }
            }
            
            test_bibcodes = [
                ('2023ApJ...123..456A', '2024-01-15'),
                ('2023ApJ...123..457B', '2024-01-16'), 
                ('2023ApJ...123..458C', '2024-01-17')
            ]
            
            for site_key, site_config in sites.items():
                with self.subTest(site=site_key):
                    # Create site directory under the sitemap base directory
                    site_dir = os.path.join(sitemap_base_dir, site_key)
                    os.makedirs(site_dir, exist_ok=True)
                    
                    # Verify the expected directory structure exists
                    self.assertTrue(os.path.exists(site_dir))
                    expected_path = os.path.join('app', 'logs', 'sitemap', site_key)
                    self.assertTrue(site_dir.endswith(expected_path),
                                  f"Site directory should end with '{expected_path}', got '{site_dir}'")
                    
                    # Generate robots.txt
                    robots_content = templates.render_robots_txt(site_config['sitemap_url'])
                    robots_path = os.path.join(site_dir, 'robots.txt')
                    with open(robots_path, 'w') as f:
                        f.write(robots_content)
                    
                    # Verify robots.txt was created
                    self.assertTrue(os.path.exists(robots_path))
                    with open(robots_path, 'r') as f:
                        content = f.read()
                        self.assertIn(site_config['sitemap_url'], content)
                    
                    # Generate sitemap file
                    url_entries = []
                    for bibcode, lastmod in test_bibcodes:
                        entry = templates.format_url_entry(bibcode, lastmod, site_config['abs_url_pattern'])
                        url_entries.append(entry)
                    
                    sitemap_content = templates.render_sitemap_file(''.join(url_entries))
                    sitemap_path = os.path.join(site_dir, 'sitemap_bib_1.xml')
                    with open(sitemap_path, 'w') as f:
                        f.write(sitemap_content)
                    
                    # Verify sitemap file was created
                    self.assertTrue(os.path.exists(sitemap_path))
                    with open(sitemap_path, 'r') as f:
                        content = f.read()
                        for bibcode, _ in test_bibcodes:
                            expected_url = site_config['abs_url_pattern'].format(bibcode=bibcode)
                            self.assertIn(expected_url, content)
                    
                    # Generate sitemap index
                    index_entry = templates.format_sitemap_entry(site_config['sitemap_url'], 
                                                               'sitemap_bib_1.xml', '2024-01-17')
                    index_content = templates.render_sitemap_index(index_entry)
                    index_path = os.path.join(site_dir, 'sitemap_index.xml')
                    with open(index_path, 'w') as f:
                        f.write(index_content)
                    
                    # Verify index file was created
                    self.assertTrue(os.path.exists(index_path))
                    with open(index_path, 'r') as f:
                        content = f.read()
                        expected_sitemap_url = f"{site_config['sitemap_url']}/sitemap_bib_1.xml"
                        self.assertIn(expected_sitemap_url, content)
                
                        
    def test_url_formatting_edge_cases(self):
        """Test URL formatting with various edge cases"""
        # Test with bibcode containing special characters
        bibcode = '2023A&A...123..456A'
        url_entry = templates.format_url_entry(bibcode, '2024-01-15')
        self.assertIn('2023A&A...123..456A', url_entry)
        
        # Test with different date formats
        url_entry_date = templates.format_url_entry('2023ApJ...123..456A', '2024-01-01')
        self.assertIn('<lastmod>2024-01-01</lastmod>', url_entry_date)
        
        
    def test_template_error_handling(self):
        """Test template error handling"""
        # Test with invalid template name
        with self.assertRaises(Exception):
            templates.load_template('nonexistent_template.xml')
            
        # Test with missing URL pattern placeholder
        invalid_entry = templates.format_url_entry('2023ApJ...123..456A', '2024-01-15', 'no-placeholder')
        # Should not raise exception but won't substitute properly
        self.assertIn('no-placeholder', invalid_entry)

    def test_multi_site_url_patterns(self):
        """Test URL pattern generation for different sites"""
        bibcode = '2023ApJ...123..456A'
        lastmod = '2024-01-15'
        
        # Test ADS URL pattern
        ads_pattern = 'https://ui.adsabs.harvard.edu/abs/{bibcode}'
        ads_entry = templates.format_url_entry(bibcode, lastmod, ads_pattern)
        
        self.assertIn('https://ui.adsabs.harvard.edu/abs/2023ApJ...123..456A', ads_entry)
        self.assertIn('<lastmod>2024-01-15</lastmod>', ads_entry)
        
        # Test SciX URL pattern  
        scix_pattern = 'https://scixplorer.org/abs/{bibcode}'
        scix_entry = templates.format_url_entry(bibcode, lastmod, scix_pattern)
        
        self.assertIn('https://scixplorer.org/abs/2023ApJ...123..456A', scix_entry)
        self.assertIn('<lastmod>2024-01-15</lastmod>', scix_entry)
        
        # Verify entries are different for different sites
        self.assertNotEqual(ads_entry, scix_entry)

    def test_robots_txt_multi_site_content(self):
        """Test robots.txt content for different sites"""
        
        # Test ADS robots.txt
        ads_sitemap_url = 'https://ui.adsabs.harvard.edu/sitemap_index.xml'
        ads_robots = templates.render_robots_txt(ads_sitemap_url)
        
        self.assertIn('User-agent: *', ads_robots)
        self.assertIn('https://ui.adsabs.harvard.edu/sitemap_index.xml', ads_robots)
        
        # Test SciX robots.txt
        scix_sitemap_url = 'https://scixplorer.org/sitemap_index.xml'
        scix_robots = templates.render_robots_txt(scix_sitemap_url)
        
        self.assertIn('User-agent: *', scix_robots)
        self.assertIn('https://scixplorer.org/sitemap_index.xml', scix_robots)
        
        # Verify content is different for different sites
        self.assertNotEqual(ads_robots, scix_robots)

    def test_sitemap_index_multi_site_entries(self):
        """Test sitemap index generation with different base URLs"""
        
        # Create sitemap entries for different sites
        ads_base_url = 'https://ui.adsabs.harvard.edu'
        scix_base_url = 'https://scixplorer.org'
        
        filename = 'sitemap_bib_1.xml'
        lastmod = '2024-01-15'
        
        ads_entry = templates.format_sitemap_entry(ads_base_url, filename, lastmod)
        scix_entry = templates.format_sitemap_entry(scix_base_url, filename, lastmod)
        
        self.assertIn(f'{ads_base_url}/{filename}', ads_entry)
        self.assertIn(f'{scix_base_url}/{filename}', scix_entry)
        self.assertIn('<lastmod>2024-01-15</lastmod>', ads_entry)
        self.assertIn('<lastmod>2024-01-15</lastmod>', scix_entry)
        
        # Generate index for each site
        ads_index = templates.render_sitemap_index(ads_entry)
        scix_index = templates.render_sitemap_index(scix_entry)
        
        self.assertIn('<sitemapindex', ads_index)
        self.assertIn('<sitemapindex', scix_index)
        self.assertIn(ads_base_url, ads_index)
        self.assertIn(scix_base_url, scix_index)

    def test_template_consistency_across_sites(self):
        """Test that template structure is consistent across different sites"""
        
        # Test with multiple entries
        entries_ads = []
        entries_scix = []
        
        bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B', '2023ApJ...123..458C']
        
        for bibcode in bibcodes:
            ads_entry = templates.format_url_entry(
                bibcode, '2024-01-15', 
                'https://ui.adsabs.harvard.edu/abs/{bibcode}'
            )
            scix_entry = templates.format_url_entry(
                bibcode, '2024-01-15',
                'https://scixplorer.org/abs/{bibcode}'
            )
            entries_ads.append(ads_entry)
            entries_scix.append(scix_entry)
        
        # Generate sitemap files
        ads_sitemap = templates.render_sitemap_file(''.join(entries_ads))
        scix_sitemap = templates.render_sitemap_file(''.join(entries_scix))
        
        # Both should have valid XML structure
        self.assertIn('<?xml version="1.0"', ads_sitemap)
        self.assertIn('<?xml version="1.0"', scix_sitemap)
        self.assertIn('<urlset', ads_sitemap)
        self.assertIn('<urlset', scix_sitemap)
        
        # Both should contain all bibcodes
        for bibcode in bibcodes:
            self.assertIn(bibcode, ads_sitemap)
            self.assertIn(bibcode, scix_sitemap)
        
        # Should contain correct number of URL entries
        ads_url_count = ads_sitemap.count('<url>')
        scix_url_count = scix_sitemap.count('<url>')
        self.assertEqual(ads_url_count, len(bibcodes))
        self.assertEqual(scix_url_count, len(bibcodes))

    def test_template_special_characters_handling(self):
        """Test template handling of bibcodes with special characters"""
        
        # Test with bibcode containing special characters
        special_bibcode = '2023A&A...123..456.'
        lastmod = '2024-01-15'
        
        # Should handle special characters in URL encoding
        ads_entry = templates.format_url_entry(
            special_bibcode, lastmod,
            'https://ui.adsabs.harvard.edu/abs/{bibcode}'
        )
        
        self.assertIn('2023A&A...123..456.', ads_entry)
        self.assertIn('<url>', ads_entry)
        self.assertIn('</url>', ads_entry)

    def test_xml_escaping_fix(self):
        """Test that XML characters are properly escaped to prevent malformed XML"""        
        # Test bibcode with ampersand (the bug we're fixing)
        problematic_bibcode = '1980Ap&SS..68..111M'
        lastmod = '2025-08-20'
        
        # Generate URL entry
        url_entry = templates.format_url_entry(problematic_bibcode, lastmod)
        
        # Should contain escaped ampersand in the URL
        self.assertIn('1980Ap&amp;SS..68..111M', url_entry)
        self.assertNotIn('1980Ap&SS..68..111M', url_entry)  # Should not contain unescaped &
        
        # Create a complete sitemap file to test XML parsing
        sitemap_content = templates.render_sitemap_file(url_entry)
        
        # Should be valid XML that can be parsed
        try:
            tree = ET.fromstring(sitemap_content)
            self.assertIsNotNone(tree)
        except ET.ParseError as e:
            self.fail(f"Generated sitemap XML is not well-formed: {e}")
        
        # Test other XML special characters
        test_cases = [
            ('2023Test<Tag>..123A', '2023Test&lt;Tag&gt;..123A'),  # < and >
            ('2023Test&Amp...123B', '2023Test&amp;Amp...123B'),   # &
            ('2023Test"Quote..123C', '2023Test&quot;Quote..123C'), # "
            ("2023Test'Apos...123D", "2023Test&#x27;Apos...123D")  # ' (html.escape uses &#x27; not &apos;)
        ]
        
        for input_bibcode, expected_escaped in test_cases:
            with self.subTest(bibcode=input_bibcode):
                url_entry = templates.format_url_entry(input_bibcode, lastmod)
                self.assertIn(expected_escaped, url_entry, 
                            f"Expected escaped version '{expected_escaped}' not found in: {url_entry}")
                
                # Ensure the complete sitemap is still valid XML
                sitemap_content = templates.render_sitemap_file(url_entry)
                try:
                    ET.fromstring(sitemap_content)
                except ET.ParseError as e:
                    self.fail(f"XML parsing failed for bibcode '{input_bibcode}': {e}")

    def test_sitemap_index_xml_escaping(self):
        """Test that sitemap index entries are also properly XML escaped"""        
        # Test sitemap URL with special characters
        base_url = 'https://example.com/sitemap?param=test&other=value'
        filename = 'sitemap_bib_1.xml'
        lastmod = '2025-08-20'
        
        # Generate sitemap entry
        sitemap_entry = templates.format_sitemap_entry(base_url, filename, lastmod)
        
        # Should contain escaped ampersand in the URL
        self.assertIn('param=test&amp;other=value', sitemap_entry)
        self.assertNotIn('param=test&other=value', sitemap_entry)  # Should not contain unescaped &
        
        # Create a complete sitemap index to test XML parsing
        index_content = templates.render_sitemap_index(sitemap_entry)
        
        # Should be valid XML that can be parsed
        try:
            tree = ET.fromstring(index_content)
            self.assertIsNotNone(tree)
        except ET.ParseError as e:
            self.fail(f"Generated sitemap index XML is not well-formed: {e}")


if __name__ == '__main__':
    unittest.main() 