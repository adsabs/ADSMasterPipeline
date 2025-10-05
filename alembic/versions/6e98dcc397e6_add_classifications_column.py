"""add_classifications_column

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
            batch_op.add_column(sa.Column('classifications', sa.Text))
            batch_op.add_column(sa.Column('classifications_updated', sa.TIMESTAMP))
    else:
        op.add_column('records', sa.Column('classifications', sa.Text))
        op.add_column('records', sa.Column('classifications_updated', sa.TIMESTAMP))


def downgrade():
    cx = op.get_context()
    if 'sqlite' in cx.connection.engine.name:
        with op.batch_alter_table("records") as batch_op:
            batch_op.drop_column('classifications')
            batch_op.drop_column('classifications_updated')
    else:
        op.drop_column('records', 'classifications')
        op.drop_column('records', 'classifications_updated')

