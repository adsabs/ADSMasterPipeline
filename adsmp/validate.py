import os
import time
import requests
from difflib import SequenceMatcher


class Validate():
    """Validates the output of a new pipeline by comparing its SOLR instance against
    that of a previous pipeline version"""

    def __init__(self,fields,ignore_fields,new_fields):
        self.fields = fields
        self.ignore_fields = ignore_fields
        self.new_fields = new_fields

        # - Use app logger:
        #import logging
        #self.logger = logging.getLogger('master-pipeline')
        # - Or individual logger for this file:
        from adsputils import setup_logging, load_config
        proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
        self.config = load_config(proj_home=proj_home)
        self.logger = setup_logging(__name__, proj_home=proj_home,
                                level=self.config.get('LOGGING_LEVEL', 'INFO'),
                                attach_stdout=self.config.get('LOG_STDOUT', False))

    def compare_solr(self, bibcodelist=None,filename=None):

        if (bibcodelist is None) and (filename is None):
            raise RuntimeError('Must pass in a list of bibcodes or a file of bibcodes')

        SOLR_OLD = self.config['SOLR_URL_OLD']
        SOLR_NEW = self.config['SOLR_URL_NEW']

        if bibcodelist is None:
            bibcodelist = []

        if filename is None:
            lines = []
        else:
            with open(filename) as f:
                lines = f.readlines()
            lines = map(str.strip, lines)

        bibcodes = bibcodelist + lines

        t1 = time.time()

        for bibcode in bibcodes:

            r1 = Validate.query_solr(self, SOLR_OLD, 'bibcode:"' + bibcode + '"', sort="bibcode desc", fl='*')
            r2 = Validate.query_solr(self, SOLR_NEW, 'bibcode:"' + bibcode + '"', sort="bibcode desc", fl='*')

            if (r1['response']['docs'] == []) and (r2['response']['docs'] == []):
                self.logger.error('Bibcode {} not in either SOLR'.format(bibcode))
            elif r1['response']['docs'] == []:
                self.logger.error('Bibcode {} missing from old SOLR'.format(bibcode))
            elif r2['response']['docs'] == []:
                self.logger.error('Bibcode {} missing from new SOLR'.format(bibcode))
            else:
                s1 = r1['response']['docs'][0]
                s2 = r2['response']['docs'][0]

                Validate.pipeline_mismatch(self, bibcode, s1, s2)

        tottime = time.time() - t1
        self.logger.info('Time elapsed to compare {} bibcodes: {} s'.format(len(bibcodes),tottime))

    def query_solr(self, endpoint, query, start=0, rows=200, sort="date desc", fl='bibcode'):
        d = {'q': query,
             'sort': sort,
             'start': start,
             'rows': rows,
             'wt': 'json',
             'indent': 'true',
             'hl': 'true',
             'hl.fl': 'abstract,ack,body',
             }
        if fl:
            d['fl'] = fl

        response = requests.get(endpoint, params=d)
        if response.status_code == 200:
            results = response.json()
            return results
        self.logger.warn('For query {}, there was a network problem: {0}\n'.format(query,response))
        return None

    def pipeline_mismatch(self, bibcode, s1, s2):

        mismatch = 0
        missing_required = 0
        missing = 0
        notins1 = 0
        notins2 = 0

        for field in self.fields:
            if field not in self.ignore_fields:
                match = Validate.fields_match(self, bibcode, s1, s2, field)
                if match != True:
                    if match == False:
                        mismatch += 1
                    if match == 'required new field not in bibcode':
                        missing_required += 1
                        missing += 1
                    if match == 'field not in bibcode':
                        missing += 1
                    if match == 'field not in s1':
                        notins1 += 1
                    if match == 'field not in s2':
                        notins2 += 1

        if mismatch == 0:
            self.logger.info('Bibcode {}: no mismatched fields'.format(bibcode))

        self.logger.info('The following fields are ignored: {}'.format(self.ignore_fields))
        self.logger.info('Mismatch stats for bibcode {}: {} mismatches, {} missing required new fields, '
                               '{} fields not in old database, {} fields not in new database, '
                               '{} fields not in either database'.format(bibcode,mismatch,missing_required,notins1,notins2,missing))


    def fields_match(self, bibcode, s1, s2, field):

        if (field in s1) and (field in s2):
            f1 = s1[field]
            f2 = s2[field]
        elif (field not in s1) and (field not in s2):
            if field in self.new_fields:
                self.logger.warn('Bibcode {}: required new field {} not present'.format(bibcode,field))
                return 'required new field not in bibcode'
            else:
                self.logger.info('Bibcode {}: field {} not present in either database'.format(bibcode,field))
                return 'field not in bibcode'
        elif field not in s1:
            self.logger.info('Bibcode {}: field {} not present in old database'.format(bibcode,field))
            return 'field not in s1'
        elif field not in s2:
            self.logger.info('Bibcode {}: field {} not present in new database'.format(bibcode,field))
            return 'field not in s2'

        # for citations, sort and compare the lists
        if field == 'citation':
            if sorted(f1) != sorted(f2):
                self.logger.warn('Bibcode {}: different numbers of citations present in each database'.format(bibcode))
                return False
            else:
                return True

        # allow citation_count to be different by up to 3
        if field == 'citation_count':
            if abs(f1 - f2) > 3:
                self.logger.warn(
                    'Bibcode {}: citation_count field is different between databases. Old: {} New: {}'.format(bibcode, f1, f2))
                return False
            else:
                return True

        # allow cite_read_boost to differ by up to 10%, unless one field is 0 and the other is non-zero
        if field == 'cite_read_boost':
            if (f1 ==0.) and (f2 == 0.):
                return True
            elif (f1 == 0. and f2 != 0.) or (f1 != 0. and f2 == 0.):
                self.logger.warn(
                    'Bibcode {}: cite_read_boost field is different between databases. Old: {} New: {}'.format(bibcode, f1,
                                                                                                               f2))
                return False
            elif (abs(f1-f2)/f1) > 0.1:
                self.logger.warn(
                    'Bibcode {}: cite_read_boost field is different between databases. Old: {} New: {}'.format(bibcode, f1, f2))
                return False
            else:
                return True

        # CDS has changed to SIMBAD in new pipeline; check for this. Then check the rest of the sorted list
        if field == 'data':
            if ('CDS' in f1) and ('SIMBAD' in f2):
                f1.remove('CDS')
                f2.remove('SIMBAD')
            if sorted(f1) != sorted(f2):
                self.logger.warn(
                    'Bibcode {}: data field is different between databases. Old: {} New: {}'.format(bibcode, f1, f2))
                return False
            else:
                return True

        # doctype intechreport has been changed to techreport
        if (field == 'doctype') and (f1 == 'intechreport') and (f2 == 'techreport'):
            return True

        # for identifier, sort first before comparing, since the order has changed
        if field == 'identifier':
            if sorted(f1) != sorted(f2):
                self.logger.warn('Bibcode {}: identifier field is different between databases. Old: {} New: {}'.format(bibcode,f1,f2))
                return False
            else:
                return True

        # for references, only check that the total number is the same (otherwise sorting
        # differences can confuse it)
        if field == 'reference':
            if len(f1) != len(f2):
                self.logger.warn(
                        'Bibcode {}: different numbers of references present in each database'.format(bibcode))
                return False
            else:
                return True

        if f1 != f2:
            # check how similar strings are
            if type(f1) is unicode:
                ratio = SequenceMatcher(None, f1, f2).ratio()
                if ratio < 0.8:
                    if field == 'body':
                        self.logger.warn(
                            'Bibcode %s: unicode field %s is different between databases.', bibcode, field,)
                    else:
                        self.logger.warn('Bibcode %s: unicode field %s is different between databases. Old: %r New: %r',bibcode,field,f1,f2)
                    return False
                else:
                    if field == 'body':
                        self.logger.info(
                            'Bibcode %s: unicode field %s is slightly different between databases.',
                            bibcode, field)
                    else:
                        self.logger.info('Bibcode %s: unicode field %s is slightly different between databases. Old: %r New: %r',bibcode,field,f1,f2)
            else:
                self.logger.warn('Bibcode {}: field {} is different between databases. Old: {} New: {}'.format(bibcode,field,f1,f2))
                return False

        return True
