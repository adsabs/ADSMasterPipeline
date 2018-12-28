# Connection to the database where we save orcid-claims (this database
# serves as a running log of claims and storage of author-related
# information). It is not consumed by others (ie. we 'push' results)
# SQLALCHEMY_URL = 'postgres://docker:docker@localhost:6432/docker'
SQLALCHEMY_URL = 'sqlite:///'
SQLALCHEMY_ECHO = False


# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = 'INFO'
CELERY_INCLUDE = ['adsmp.tasks']



# db connection to the db instance where we should send data; if not present
# the SOLR can still work but no metrics updates can be done
METRICS_SQLALCHEMY_URL = None #'postgres://postgres@localhost:5432/metrics'


# Main Solr
SOLR_URLS = ['http://localhost:9983/solr/collection1/update']

# For the run's argument --validate_solr, which compares two Solr instances for
# the given bibcodes or file of bibcodes
SOLR_URL_NEW = 'http://localhost:9983/solr/collection1/query'
SOLR_URL_OLD = 'http://localhost:9984/solr/collection1/query'

# url and token for the update endpoint of the links resolver microservice
# new links data is sent to this url, the mircoservice updates its datastore
LINKS_RESOLVER_UPDATE_URL = 'http://localhost:8080/update'
ADS_API_TOKEN = 'fixme'
