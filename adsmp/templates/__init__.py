import os
from pathlib import Path
import html

# Tested
def get_template_path(template_name):
    """Get the full path to a template file"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, template_name)

# Tested
def load_template(template_name):
    """Load a template file and return its contents as a string"""
    template_path = get_template_path(template_name)
    with open(template_path, 'r', encoding='utf-8') as f:
        return f.read()

# Tested
def render_robots_txt(sitemap_url):
    """Render the robots.txt template with the given sitemap URL"""
    template = load_template('robots.txt')
    return template.format(sitemap_url=sitemap_url)

# Tested
def render_sitemap_index(sitemap_entries):
    """Render the sitemap index XML with the given sitemap entries"""
    template = load_template('sitemap_index.xml')
    return template.format(sitemap_entries=sitemap_entries)

# Tested
def render_sitemap_file(url_entries):
    """Render an individual sitemap file XML with the given URL entries"""
    template = load_template('sitemap_file.xml')
    return template.format(url_entries=url_entries)

# Tested
def format_sitemap_entry(sitemap_url, filename, lastmod_date):
    """Format a single sitemap entry for the sitemap index"""
    # Escape XML characters in URL and filename to prevent malformed XML
    escaped_url = html.escape(f"{sitemap_url}/{filename}")
    escaped_lastmod = html.escape(str(lastmod_date))
    return f"""
            <sitemap>
            <loc>{escaped_url}</loc>
            <lastmod>{escaped_lastmod}</lastmod>
            </sitemap>"""

# Tested 
def format_url_entry(bibcode, lastmod_date, abs_url_pattern='https://ui.adsabs.harvard.edu/abs/{bibcode}/abstract'):
    """Format a single URL entry for a sitemap file"""
    # Escape XML characters in bibcode to prevent malformed XML
    escaped_bibcode = html.escape(bibcode)
    url = abs_url_pattern.format(bibcode=escaped_bibcode)
    return f'\n<url><loc>{url}</loc><lastmod>{lastmod_date}</lastmod></url>' 