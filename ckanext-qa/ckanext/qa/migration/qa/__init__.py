import os
def alembic_ini():
    """Return the absolute path στο alembic.ini αυτού του extension."""
    return os.path.join(os.path.dirname(__file__), 'alembic.ini')