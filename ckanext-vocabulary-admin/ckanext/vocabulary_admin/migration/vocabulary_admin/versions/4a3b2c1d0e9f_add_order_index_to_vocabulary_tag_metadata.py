"""Add order_index column to vocabulary_tag_metadata

Revision ID: 4a3b2c1d0e9f
Revises: 215e7642708b
Create Date: 2025-11-20 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4a3b2c1d0e9f'
down_revision = '215e7642708b'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'vocabulary_tag_metadata',
        sa.Column('order_index', sa.Integer(), nullable=True)
    )


def downgrade():
    op.drop_column('vocabulary_tag_metadata', 'order_index')

