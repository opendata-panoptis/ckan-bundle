"""empty message

Revision ID: 215e7642708b
Revises: 3ef94f9177fd
Create Date: 2025-06-19 20:12:04.183254

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '215e7642708b'
down_revision = '3ef94f9177fd'
branch_labels = None
depends_on = None


def upgrade():

    # Προσθήκη UNIQUE constraint στη στήλη vocabulary_id του πίνακα vocabulary_description
    op.create_unique_constraint(
        'uq_vocabulary_description_vocabulary_id',
        'vocabulary_description',
        ['vocabulary_id']
    )

    # Προσθήκη UNIQUE constraint στη στήλη tag_id του πίνακα vocabulary_tag_metadata
    op.create_unique_constraint(
        'uq_vocabulary_tag_metadata_tag_id',
        'vocabulary_tag_metadata',
        ['tag_id']
    )

    # Αλλαγή της στήλης is_active ώστε να είναι NOT NULL
    op.alter_column(
        'vocabulary_tag_metadata',
        'is_active',
        existing_type=sa.BOOLEAN(),
        nullable=False,
        existing_server_default=sa.text('true')
    )
    # ### end Alembic commands ###


def downgrade():

    # Αφαίρεση του NOT NULL από τη στήλη is_active
    op.alter_column(
        'vocabulary_tag_metadata',
        'is_active',
        existing_type=sa.BOOLEAN(),
        nullable=True,
        existing_server_default=sa.text('true')
    )

    # Αφαίρεση του UNIQUE constraint από τον πίνακα vocabulary_tag_metadata
    op.drop_constraint(
        'uq_vocabulary_tag_metadata_tag_id',
        'vocabulary_tag_metadata',
        type_='unique'
    )

    # Αφαίρεση του UNIQUE constraint από τον πίνακα vocabulary_description
    op.drop_constraint(
        'uq_vocabulary_description_vocabulary_id',
        'vocabulary_description',
        type_='unique'
    )