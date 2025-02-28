"""add_collections_column

Revision ID: 6e98dcc397e6
Revises: 2d2af8a9c996
Create Date: 2025-02-28 08:52:00.341542

"""

# revision identifiers, used by Alembic.
revision = '6e98dcc397e6'
down_revision = '2d2af8a9c996'

from alembic import op
import sqlalchemy as sa

                               

def upgrade():
    # sqlite doesn't have ALTER command
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.add_column(sa.Column('collections', sa.ARRAY(sa.String)))
            batch_op.add_column(sa.Column('collections_updated', sa.TIMESTAMP))
    else:
        op.add_column('records', sa.Column('collections', sa.ARRAY(sa.String)))
        op.add_column('records', sa.Column('collections_updated', sa.TIMESTAMP))


def downgrade():
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.drop_column('collections')
            batch_op.drop_column('collections_updated')
    else:
        op.drop_column('records', 'collections')
        op.drop_column('records', 'collections_updated')

