from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import logging
# Configure the logging system
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PandasPG:
    def __init__(self, host, port, db, user, pwd):
        self.__host = host
        self.__port = port
        self.__db = db
        self.__user = user
        self.__pwd = pwd

        with self.__get_psyco_conn() as conn:
            logging.info("Connection to PostgreSQL established successfully")

    def __get_alchemy_conn(self):
        conn_string = f"postgresql+psycopg2://{self.__user}:{self.__pwd}@{self.__host}:{self.__port}/{self.__db}"
        conn = create_engine(conn_string).connect()
        return conn

    def __get_psyco_conn(self):
        conn = psycopg2.connect(
            host=self.__host,
            port=self.__port,
            dbname=self.__db,
            user=self.__user,
            password=self.__pwd,
        )
        return conn

    def read_sql(self, query):

        with self.__get_psyco_conn() as conn:

            # Create a cursor object using the connection
            cursor = conn.cursor()

            # Execute the query
           
            cursor.execute(query)
            # Fetch all rows from the executed query
            rows = cursor.fetchall()
            # Fetch column names
            colnames = [desc[0] for desc in cursor.description]

            # Convert to a DataFrame
            df = pd.DataFrame(rows, columns=colnames)

            logging.info("Query executed and DataFrame created successfully")

            return df

    def read_sql_alchemy(self, query, col_types: dict = None):
        with self.__get_alchemy_conn() as conn:
            return pd.read_sql(query, conn, dtype=col_types)

    def to_sql(self, df, table_name, schema, if_exists="fail"):
        """
        Args:
            if_exists: {'fail', 'replace', 'append'}
        """

        with self.__get_psyco_conn() as conn:
            df.to_sql(table_name, conn, schema=schema, if_exists=if_exists, index=False)

    def to_sql_alchemy(self, df, table_name, schema, if_exists="fail"):
        """
        Args:
            if_exists: {'fail', 'replace', 'append'}
        """

        with self.__get_alchemy_conn() as conn:
            df.to_sql(table_name, conn, schema=schema, if_exists=if_exists, index=False)
