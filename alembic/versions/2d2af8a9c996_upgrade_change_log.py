"""upgrade_change_log

Revision ID: 2d2af8a9c996
Revises: 0155d2dff74e
Create Date: 2023-11-13 14:56:21.410177

"""

# revision identifiers, used by Alembic.
revision = '2d2af8a9c996'
down_revision = '0155d2dff74e'

from alembic import op
import sqlalchemy as sa

                               


def upgrade():
    op.add_column('change_log', sa.Column('bigid', sa.BigInteger(), unique=True))
    op.execute('UPDATE change_log SET bigid=id')
    op.alter_column('change_log', 'bigid', nullable=False)
    op.drop_constraint('change_log_pkey', 'change_log', type_='primary')
    op.create_primary_key("change_log_pkey", "change_log", ["bigid", ])
    op.alter_column('change_log', 'id', nullable=False, new_column_name='old_id')
    op.alter_column('change_log', 'bigid', nullable=False, new_column_name='id')
    op.drop_column('change_log', 'old_id')
    # ### end Alembic commands ###


def downgrade():
    op.add_column('change_log', sa.Column('smallid', sa.Integer(), unique=True))
    op.execute('DELETE FROM change_log WHERE id > 2147483647')
    op.execute('UPDATE change_log SET smallid=id')
    op.alter_column('change_log', 'smallid', nullable=False)
    op.drop_constraint('change_log_pkey', 'change_log', type_='primary')
    op.create_primary_key("change_log_pkey", "change_log", ["smallid", ])
    op.alter_column('change_log', 'id', nullable=False, new_column_name='old_id')
    op.alter_column('change_log', 'smallid', nullable=False, new_column_name='id')
    op.drop_column('change_log', 'old_id')
