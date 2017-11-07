import requests
import json
from adsputils import setup_logging, date2solrstamp
import time


logger = setup_logging('solr_updater')


def extract_metrics_pipeline(data, solrdoc):
    
    citation=data.get('citations', [])
    citation_count=len(citation)
    
    return dict(citation=citation, 
                citation_count=citation_count,
                )
    
def extract_data_pipeline(data, solrdoc):
    
    reader=data.get('readers', [])
    read_count=len(reader)
    
    grant = []
    grant_facet_hier = []
    for x in data.get('grants', []):
        agency, grant_no = x.split(' ', 1)
        grant.append(agency)
        grant.append(grant_no)
        grant_facet_hier.extend(generate_hier_facet(agency, grant_no))
        
    simbid = []
    simbtype = []
    simbad_object_facet_hier = []
    for x in data.get('simbad_objects', []):
        sid, stype = x.split(' ', 1)
        simbid.append(sid)
        simbtype.append(map_simbad_type(stype))
        simbad_object_facet_hier.extend(generate_hier_facet(map_simbad_type(stype), sid))
    
    nedid = []
    nedtype = []
    ned_object_facet_hier = []
    for x in data.get('ned_objects', []):
        nid, ntype = x.split(' ', 1)
        nedid.append(nid)
        nedtype.append(map_ned_type(ntype))
        ned_object_facet_hier.extend(generate_hier_facet(map_ned_type(ntype), nid))
    
    return dict(reader=reader, 
                read_count=read_count,
                cite_read_boost=data.get('boost', 0.0),
                classic_factor=data.get('norm_cites', 0.0),
                reference=data.get('reference', []),
                data=data.get('data', []),
                data_count=data.get('total_link_counts', 0),
                esources = data.get('esource', []),
                property = data.get('property', []),
                grant=grant,
                grant_facet_hier=grant_facet_hier,
                simbid=simbid,
                simbtype=simbtype,
                simbad_object_facet_hier=simbad_object_facet_hier,
                nedid=nedid,
                nedtype=nedtype,
                ned_object_facet_hier=ned_object_facet_hier
                )

def generate_hier_facet(*levels):
    levels = list(levels)
    out = []
    i = 0
    tmpl = u'{}/{}'
    j = len(levels)
    while i < j:
        out.append(tmpl.format(*[i] + levels[0:i+1]))
        tmpl += '/{}'
        i += 1
    return out
    
def get_orcid_claims(data, solrdoc):
    out = {}
    # TODO(rca): shall we check that list of authors corresponds?
    if 'verified' in data:
        out['orcid_user'] = data['verified']
    if 'unverified' in data:
        out['orcid_other'] = data['unverified']
    return out


#### TODO move to the data pipeline
def map_simbad_type(otype):
    """
    Maps a native SIMBAD object type to a subset of basic classes
    used for searching and faceting.  Based on Thomas Boch's mappings
    used in AladinLite
    """
    if otype.startswith('G') or otype.endswith('G'):
        return u'Galaxy'
    elif otype == 'Star' or otype.find('*') >= 0:
        return u'Star'
    elif otype == 'Neb' or otype.startswith('PN') or otype.startswith('SNR'):
        return u'Nebula'
    elif otype == 'HII':
        return u'HII Region'
    elif otype == 'X':
        return u'X-ray'
    elif otype.startswith('Radio') or otype == 'Maser' or otype == 'HI':
        return u'Radio'
    elif otype == 'IR' or otype.startswith('Red'):
        return u'Infrared'
    elif otype == 'UV':
        return u'UV'
    else:
        return u'Other'

_o_types = {}
[_o_types.__setitem__(x, u'Galaxy') for x in ["G","GClstr","GGroup","GPair","GTrpl","G_Lens","PofG"]]
[_o_types.__setitem__(x, u'Nebula') for x in ['Neb','PN','RfN']]
[_o_types.__setitem__(x, u'HII Region') for x in ['HII']]
[_o_types.__setitem__(x, u'X-ray') for x in ['X']]
[_o_types.__setitem__(x, u'Radio') for x in ['Maser', 'HI']]
[_o_types.__setitem__(x, u'Infrared') for x in ['IrS']]
[_o_types.__setitem__(x, u'Star') for x in ['Blue*','C*','exG*','Flare*','Nova','Psr','Red*','SN','SNR','V*','VisS','WD*','WR*']]
def map_ned_type(otype):
    """
    Maps a native NED object type to a subset of basic classes
    used for searching and faceting.
    """
    if otype.startswith('!'):
        return u'Galactic Object'
    elif otype.startswith('*'):
        return u'Star'
    elif otype.startswith('Uv'):
        return u'UV'
    elif otype.startswith('Radio'):
        return u'Radio'
    else:
        return _o_types.get(otype, u'Other')



# When building SOLR record, we grab data from the database and insert them
# into the dictionary with the following conventions:

# 'destination' (string) == insert the value into record[destination]
# '' (empty value) == extend the existing values with what you find under this key
# None == ignore the value completely
# function == receives the data (and solr doc as built already), should return dict 
fmap = dict(metadata_mtime='bib_data_updated',
           nonbib_mtime='nonbib_data_updated',
           fulltext_mtime='fulltext_updated',
           orcid_mtime='orcid_claims_updated',
           metrics_mtime='metrics_updated'
           )
def get_timestamps(db_record, out):
    out = {}
    last_update = None
    for k,v in fmap.items():
        if v in db_record and db_record[v]:
            t = db_record[v]
            out[k] = date2solrstamp(t)
            if last_update is None or t > last_update:
                last_update = t
    if last_update:
        out['update_timestamp'] = date2solrstamp(last_update)
    return out
     
DB_COLUMN_DESTINATIONS = {
    'bib_data': '', 
    'orcid_claims': get_orcid_claims, 
    'nonbib_data': extract_data_pipeline,
    'metrics': extract_metrics_pipeline,
    'id': 'id', 
    'fulltext': 'body',
    '#timestamps': get_timestamps, # use 'id' to be always called
    
    }


def delete_by_bibcodes(bibcodes, urls):
    '''Deletes records from SOLR, it returns the databstructure with 
    indicating which bibcodes were deleted.'''
  
    deleted = []
    failed = []
    headers = {"Content-Type":"application/json"}
    for bibcode in bibcodes:
        logger.info("Delete: %s" % bibcode)
        data = json.dumps({'delete':{"query":'bibcode:"%s"' % bibcode}})
        i = 0
        for url in urls:
            r = requests.post(url, headers=headers, data=data)
            if r.status_code == 200:
                i += 1
        if i == len(urls):
            deleted.append(bibcode)
        else:
            failed.append(bibcode)
    return (deleted, failed)
        


def update_solr(json_records, solr_urls, ignore_errors=False, commit=False):
    """ Sends data to solr
        :param: json_records - list of JSON formatted data (formatted in the way
                that SOLR expects)
        :param: solr_urls: list of urls where to post data to
        :param: ignore_errors: (True) if to generate an exception if a status 
                code as returned from SOLR is not 200
        :return:  list of status codes, one per each request
    """
    if not isinstance(json_records, list):
        json_records = [json_records]
    payload = json.dumps(json_records)
    out = []
    for url in solr_urls:
        if commit:
            if '?' in url:
                url = url + '&commit=true'
            else:
                url = url + '?commit=true'
        r = requests.post(url, data=payload, headers={'content-type': 'application/json'})
        if r.status_code != 200:
            logger.error("Error sending data to solr\nurl=%s\nresponse=%s\ndata=%s", url, payload, r.text)
            if ignore_errors == True:
                out.append(r.status_code)
            else:
                raise Exception('Error posting data to SOLR: %s (err code: %s)' % (url, r.status_code))
    return out
            
    

def transform_json_record(db_record):
    out = {'bibcode': db_record['bibcode']}
    
    # order timestamps (if any)
    timestamps = []
    for k, v in DB_COLUMN_DESTINATIONS.items():
        ts = db_record.get(k + '_updated', None)
        if ts:
            ts = time.mktime(ts.timetuple())
        else:
            ts = -1  
        timestamps.append((k, v, ts))
    timestamps.sort(key=lambda x: x[2])
    
    for field, target, _ in timestamps:
        if db_record.get(field, None):
            if target:
                if callable(target):
                    x = target(db_record.get(field), out) # in the interest of speed, don't create copy of out
                    if x:
                        out.update(x) 
                else:
                    out[target] = db_record.get(field)
            else:
                if target is None:
                    continue
                out.update(db_record.get(field))
        elif field.startswith('#'):
            if callable(target):
                x = target(db_record, out) # in the interest of speed, don't create copy of out
                if x:
                    out.update(x) 
    
    return out



