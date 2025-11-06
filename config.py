# Connection to the database where we save orcid-claims (this database
# serves as a running log of claims and storage of author-related
# information). It is not consumed by others (ie. we 'push' results)
# SQLALCHEMY_URL = 'postgres://docker:docker@localhost:6432/docker'
SQLALCHEMY_URL = "sqlite:///"
SQLALCHEMY_ECHO = False


# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = "INFO"
CELERY_INCLUDE = ["adsmp.tasks"]

FORWARD_MSG_DICT = [ \
    { \
    'OUTPUT_PIPELINE': 'affil', \
    'OUTPUT_CELERY_BROKER': "pyamqp://guest:guest@rabbitmq-broker-1:5672/test_augment_pipeline" , \
    'OUTPUT_TASKNAME': "ADSAffil.tasks.task_update_record" \
    }, \
    { \
    'OUTPUT_PIPELINE': 'boost' , \
    'OUTPUT_CELERY_BROKER': "pyamqp://guest:guest@rabbitmq-broker-1:5672/boost_pipeline", \
    'OUTPUT_TASKNAME': "adsboost.tasks.task_process_boost_request_message" \
    }] 

TESTING_MODE = True

# db connection to the db instance where we should send data; if not present
# the SOLR can still work but no metrics updates can be done
METRICS_SQLALCHEMY_URL = None  #'postgres://postgres@localhost:5432/metrics'

# db connection to the Boost Pipeline database where boost factors are stored
# if not present, boost factors will not be included in SOLR documents
BOOST_SQLALCHEMY_URL = None  #'postgresql://boost_user:boost_pass@localhost:5432/boost_db'


# Main Solr
# SOLR_URLS = ["http://localhost:9983/solr/collection1/update"]
SOLR_URLS = ["http://montysolr:9983/solr/collection1/update"]

# For the run's argument --validate_solr, which compares two Solr instances for
# the given bibcodes or file of bibcodes
SOLR_URL_NEW = "http://localhost:9983/solr/collection1/query"
SOLR_URL_OLD = "http://localhost:9984/solr/collection1/query"

# url and token for the update endpoint of the links resolver microservice
# new links data is sent to this url, the mircoservice updates its datastore
LINKS_RESOLVER_UPDATE_URL = "http://localhost:8080/update"
ADS_API_TOKEN = "fixme"

# Sitemap configuration
MAX_RECORDS_PER_SITEMAP = 50000
SITEMAP_BOOTSTRAP_BATCH_SIZE = 50000  
SITEMAP_DIR = '/app/logs/sitemap/'
SITEMAP_INDEX_GENERATION_DELAY = 15 # This is the delay between the generation of the sitemap and the indexing of the sitemap

# Sitemap index generation retry configuration
# Default 100 retries = ~90 minutes. Increase for larger databases (e.g., 300 for ~5.5 hours)
SITEMAP_INDEX_MAX_RETRIES = 100


# Site configurations for multi-site sitemap generation
SITES = {
    'ads': {
        'name': 'ADS',
        'base_url': 'https://ui.adsabs.harvard.edu',
        'sitemap_url': 'https://ui.adsabs.harvard.edu/sitemap',
        'abs_url_pattern': 'https://ui.adsabs.harvard.edu/abs/{bibcode}/abstract'
    },
    'scix': {
        'name': 'SciX Explorer', 
        'base_url': 'https://scixplorer.org',
        'sitemap_url': 'https://scixplorer.org/sitemap',
        'abs_url_pattern': 'https://scixplorer.org/abs/{bibcode}/abstract'
    }
}

# S3 Configuration for sitemap file sync
SITEMAP_S3_SYNC_ENABLED = True  # Set to True to enable S3 sync
SITEMAP_S3_BUCKET = 'sitemaps'  # S3 bucket for sitemap files
AWS_ACCESS_KEY_ID = 'AWS_ACCESS_KEY_ID'
AWS_SECRET_ACCESS_KEY = 'AWS_SECRET_ACCESS_KEY'
AWS_DEFAULT_REGION = 'us-east-1'

ENABLE_HAS = True

HAS_FIELDS = [
    "abstract",
    "ack",
    "aff",
    "aff_id",
    "author",
    "bibgroup",
    "body",
    "citation",
    "comment",
    "credit",
    "data",
    "database",
    "doctype",
    "doi",
    "first_author",
    "grant",
    "identifier",
    "institution",
    "issue",
    "keyword",
    "mention",
    "orcid_other",
    "orcid_pub",
    "orcid_user",
    "origin",
    "property",
    "pub",
    "pub_raw",
    "publisher",
    "reference",
    "title",
    "uat",
    "volume",
]

DOCTYPE_RANKING = {
    "article": 1,
    "eprint": 1,
    "inproceedings": 2,
    "inbook": 1,
    "abstract": 4,
    "book": 1,
    "bookreview": 4,
    "catalog": 2,
    "circular": 3,
    "erratum": 6,
    "mastersthesis": 3,
    "newsletter": 5,
    "obituary": 6,
    "phdthesis": 3,
    "pressrelease": 7,
    "proceedings": 3,
    "proposal": 4,
    "software": 2,
    "talk": 4,
    "techreport": 3,
    "misc": 8
}

SCIX_ID_GENERATION_FIELDS = [
    "author_norm",
    "doi",
    "abstract",
    "title",
    "doctype",
    "pub_raw"
]