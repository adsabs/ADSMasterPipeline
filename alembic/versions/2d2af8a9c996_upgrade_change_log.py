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
    conn = op.get_bind()
    if conn.dialect.name == 'postgresql':
        op.execute('ALTER TABLE change_log ADD COLUMN big_id BIGINT;')
        op.execute('CREATE OR REPLACE FUNCTION set_new_id() RETURNS TRIGGER AS\n'\
                       '$BODY$\n'\
                       'BEGIN\n'\
                       '\t NEW.big_id := NEW.id;\n'\
                       '\t RETURN NEW;\n'\
                       'END\n'\
                       '$BODY$ LANGUAGE PLPGSQL;\n'\
                       'CREATE TRIGGER set_new_id_trigger BEFORE INSERT OR UPDATE ON {}\n'\
                       'FOR EACH ROW EXECUTE PROCEDURE set_new_id();\n'.format('change_log'))
        op.execute('UPDATE change_log SET big_id=id')
        op.execute('CREATE UNIQUE INDEX IF NOT EXISTS big_id_unique ON change_log(big_id);')
        op.execute('ALTER TABLE change_log ADD CONSTRAINT big_id_not_null CHECK (big_id IS NOT NULL) NOT VALID;')
        op.execute('ALTER TABLE change_log VALIDATE CONSTRAINT big_id_not_null;')
        op.execute('ALTER TABLE change_log DROP CONSTRAINT change_log_pkey, ADD CONSTRAINT change_log_pkey PRIMARY KEY USING INDEX big_id_unique;')
        op.execute('ALTER SEQUENCE change_log_id_seq OWNED BY change_log.big_id;')
        op.execute("ALTER TABLE change_log ALTER COLUMN big_id SET DEFAULT nextval('change_log_id_seq');")
        op.execute("ALTER TABLE change_log RENAME COLUMN id TO old_id;")
        op.execute("ALTER TABLE change_log RENAME COLUMN big_id TO id;")
        op.drop_column('change_log', 'old_id')
        op.execute('ALTER SEQUENCE change_log_id_seq as bigint MAXVALUE 9223372036854775807')
        op.execute('DROP TRIGGER IF EXISTS set_new_id_trigger ON change_log')
        # ### end Alembic commands ###


def downgrade():
    conn = op.get_bind()
    if conn.dialect.name == 'postgresql':
        op.add_column('change_log', sa.Column('small_id', sa.Integer(), unique=True))
        op.execute('DELETE FROM change_log WHERE id > 2147483647')
        op.execute('UPDATE change_log SET small_id=id')
        op.alter_column('change_log', 'small_id', nullable=False)
        op.drop_constraint('change_log_pkey', 'change_log', type_='primary')
        op.create_primary_key("change_log_pkey", "change_log", ["small_id", ])
        op.execute('ALTER SEQUENCE change_log_id_seq OWNED BY change_log.small_id;')
        op.execute("ALTER TABLE change_log ALTER COLUMN small_id SET DEFAULT nextval('change_log_id_seq');")
        op.alter_column('change_log', 'id', nullable=False, new_column_name='old_id')
        op.alter_column('change_log', 'small_id', nullable=False, new_column_name='id')
        op.drop_column('change_log', 'old_id')
        op.execute('ALTER SEQUENCE change_log_id_seq as int MAXVALUE 2147483647')

