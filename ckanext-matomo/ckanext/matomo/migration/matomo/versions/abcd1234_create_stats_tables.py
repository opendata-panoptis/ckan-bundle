"""create stats tables

Revision ID: abcd1234
Revises:
Create Date: 2025-07-23 13:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'abcd1234'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # Create package_stats table
    op.create_table(
        'package_stats',
        sa.Column('package_id', sa.UnicodeText, nullable=False),
        sa.Column('visit_date', sa.DateTime, nullable=False, default=sa.func.now()),
        sa.Column('visits', sa.Integer, default=0),
        sa.Column('entrances', sa.Integer, default=0),
        sa.Column('downloads', sa.Integer, default=0),
        sa.Column('events', sa.Integer, default=0),
        sa.PrimaryKeyConstraint('package_id', 'visit_date')
    )

    # Create resource_stats table
    op.create_table(
        'resource_stats',
        sa.Column('resource_id', sa.UnicodeText, nullable=False),
        sa.Column('visit_date', sa.DateTime, nullable=False, default=sa.func.now()),
        sa.Column('visits', sa.Integer, default=0),
        sa.Column('downloads', sa.Integer, default=0),
        sa.Column('events', sa.Integer, default=0),
        sa.PrimaryKeyConstraint('resource_id', 'visit_date')
    )

    # Create search_terms table
    op.create_table(
        'search_terms',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True, unique=True),
        sa.Column('search_term', sa.UnicodeText, nullable=False),
        sa.Column('date', sa.DateTime, nullable=False, default=sa.func.now()),
        sa.Column('count', sa.Integer, default=0),
    )

    # Indexes for search_terms for better performance on search_term and date
    # op.create_index('ix_search_terms_search_term', 'search_terms', ['search_term'])
    # op.create_index('ix_search_terms_date', 'search_terms', ['date'])

    # Create audience_location table
    op.create_table(
        'audience_location',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True, unique=True),
        sa.Column('location_name', sa.UnicodeText, nullable=False)
    )

    op.create_table(
        'audience_location_date',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True, unique=True),
        sa.Column('date', sa.DateTime, primary_key=True, default=sa.func.now()),
        sa.Column('visits', sa.Integer, default=0),
        sa.Column('location_id', sa.Integer, sa.ForeignKey('audience_location.id')),
    )

def downgrade():
    op.drop_table('search_terms')
    op.drop_table('resource_stats')
    op.drop_table('package_stats')
    op.drop_table('audience_location_date')
    op.drop_table('audience_location')
