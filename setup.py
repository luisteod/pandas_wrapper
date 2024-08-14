from setuptools import setup

setup(
    name="pandas_wrapper",
    version="0.1",
    packages=["pandas_pg"],
    install_requires=[
        "pandas==2.2.2",
        "psycopg2-binary==2.9.9",
        "sqlalchemy==2.0.32",
    ],
    author="Luis Henrique",
)
