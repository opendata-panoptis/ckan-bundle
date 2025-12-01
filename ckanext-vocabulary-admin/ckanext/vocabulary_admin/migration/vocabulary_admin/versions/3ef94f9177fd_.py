"""empty message

Revision ID: 3ef94f9177fd
Revises: 
Create Date: 2025-06-19 20:08:46.239943

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = '3ef94f9177fd'
down_revision = None
branch_labels = None
depends_on = None


def table_exists(table_name):
    """Check if a table exists in the database."""
    inspector = inspect(op.get_bind())
    return table_name in inspector.get_table_names()


def column_exists(table_name, column_name):
    """Check if a column exists in a table."""
    inspector = inspect(op.get_bind())
    columns = [c['name'] for c in inspector.get_columns(table_name)]
    return column_name in columns


def upgrade():
    # ### Create custom tables if they don't exist ###


    if not table_exists('vocabulary_description'):
        op.create_table('vocabulary_description',
            sa.Column('id', sa.types.UnicodeText, primary_key=True),
            sa.Column('vocabulary_id', sa.types.UnicodeText, sa.ForeignKey('vocabulary.id'), nullable=False),
            sa.Column('description_el', sa.types.UnicodeText),
            sa.Column('description_en', sa.types.UnicodeText)
        )

    if not table_exists('vocabulary_tag_metadata'):
        op.create_table('vocabulary_tag_metadata',
            sa.Column('id', sa.types.UnicodeText, primary_key=True),
            sa.Column('tag_id', sa.types.UnicodeText, sa.ForeignKey('tag.id'), nullable=False),
            sa.Column('value_uri', sa.types.UnicodeText),
            sa.Column('label_el', sa.types.UnicodeText),
            sa.Column('label_en', sa.types.UnicodeText),
            sa.Column('description_el', sa.types.UnicodeText),
            sa.Column('description_en', sa.types.UnicodeText),
            sa.Column('is_active', sa.types.Boolean, default=True)
        )



def downgrade():

    # ### Drop custom tables ###
    op.drop_table('vocabulary_tag_metadata')
    op.drop_table('vocabulary_description')