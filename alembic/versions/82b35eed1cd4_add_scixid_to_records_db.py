"""add scixid to records db

Revision ID: 82b35eed1cd4
Revises: 2d2af8a9c996
Create Date: 2024-10-15 22:07:29.923858

"""

# revision identifiers, used by Alembic.
revision = '82b35eed1cd4'
down_revision = '2d2af8a9c996'

from alembic import op
from sqlalchemy import Column, String, Integer, TIMESTAMP, DateTime, Text, Index, Boolean
from adsmp.models import Records
from adsmp import tasks
from sqlalchemy.orm import load_only, Session
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

                               
def populate_scix_id(session):
    sent = 0
    batch = []
    _tasks = []
    batch_size = 100
    # load all records from RecordsDB
    for rec in session.query(Records) \
                    .options(load_only(Records.bibcode)) \
                    .yield_per(batch_size):

        sent += 1

        batch.append(rec.bibcode)
        if len(batch) > batch_size:
            t = tasks.task_update_scixid(batch, 'update')
            _tasks.append(t)
            batch = []
    
    if len(batch) > 0:
        t = tasks.task_update_scixid(batch, 'update')
        _tasks.append(t)


def upgrade():
    op.add_column('records', Column('scix_id', String(19), nullable = True, default=None, unique=True))
    session = Session(bind = op.get_bind())
    start_time = datetime.now()
    logger.debug(start_time)
    populate_scix_id(session)
    end_time = datetime.now()
    logger.debug(end_time)
    logger.debug("Time taken for scix_id population: "+str(end_time-start_time))    

def downgrade():
    op.drop_column('records', 'scix_id')
