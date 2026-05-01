import pytest
from sqlalchemy import create_engine
from src.config.settings import get_db_uri

@pytest.fixture
def db_engine():
    return create_engine(get_db_uri())