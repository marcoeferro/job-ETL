from setuptools import setup, find_packages

setup(
    name="job_market_etl",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "apache-airflow",
        "pandas",
        "psycopg2-binary",
        "sqlalchemy",
        "requests",
        "beautifulsoup4",
        "streamlit",
        "plotly",
        "fuzzywuzzy",
    ],
)