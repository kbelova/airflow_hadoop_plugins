from impala.dbapi import connect

from random import randint
from contextlib import closing
import unicodecsv as csv

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
import logging


class ImpalaHook(BaseHook):
    """
        Hook to impala over dbapi, with python3 support.

    """

    def __init__(self
                 , conn_id='impala'
                 , imp_vars=None
                 , auto_refresh=True
                 , verbose=False
                 ):
        self.conn_id = conn_id
        self.auto_refresh = auto_refresh
        self.imp_vars = imp_vars
        self.verbose = verbose

    def run(self, sql, schema='default'):
        with closing(self.get_conn(schema)) as conn:
            if isinstance(sql, str):
                sql = [sql]
            try:
                cur = conn.cursor()
                for q in sql:
                    cur.execute(q)
            except Exception as e:
                raise AirflowException(f"Running sql: {sql}. At query: {q},  ImapalaHook Error:", e)

    def get_conn(self, schema=None):
        db = self.get_connection(self.conn_id)
        return connect(
            host=db.host,
            port=db.port,
            database=schema or db.schema or 'default')

    def get_results(self, sql, schema='default', fetch_size=None):
        from impala.error import ProgrammingError

        with closing(self.get_conn(schema)) as conn:
            if isinstance(sql, str):
                sql = [sql]
            results = {
                'data': [],
                'header': [],
            }
            cur = conn.cursor()
            for statement in sql:
                logging.info(f"Impala query: {statement}")
                try:
                    cur.execute(statement)
                    records = []
                    try:
                        # impala Lib raises when no results are returned
                        # we're silencing here as some statements in the list
                        # may be `SET` or DDL
                        if fetch_size:
                            records = cur.fetchall()
                        else:
                            records = cur.fetchmany(size=fetch_size)
                    except ProgrammingError:
                        logging.debug("get_results returned no records")

                    if records:
                        results = {
                            'data': records,
                            'header': cur.description,
                        }
                except Exception as ex:
                    raise AirflowException(f"Running sql: {str(sql)}. At query: {statement},  ImapalaHook Error:", ex)
            return results

    def to_csv(
            self,
            sql: str,
            csv_filepath: str,
            schema='default',
            delimiter=',',
            lineterminator='\r\n',
            output_header=True,
            fetch_size=1000):
        schema_name = schema or 'default'
        with closing(self.get_conn(schema_name)) as conn:
            with conn.cursor() as cur:
                logging.info(f"Running query: {sql}")
                try:
                    cur.execute(sql)
                    schema = cur.description
                    with open(csv_filepath, 'wb') as f:
                        writer = csv.writer(f,
                                            delimiter=delimiter,
                                            lineterminator=lineterminator,
                                            encoding='utf-8'
                                            )
                        if output_header:
                            writer.writerow([c[0] for c in schema])
                        i = 0
                        while True:
                            rows = [row for row in cur.fetchmany(fetch_size) if row]
                            if not rows:
                                break

                            writer.writerows(rows)
                            i += len(rows)
                            logging.info(f"Written {str(i)} rows so far.")
                        logging.info(f"Done. Loaded a total of {str(i)} rows.")
                except Exception as ex:
                    raise AirflowException(f"Running sql: {str(sql)}. ImapalaHook Error:", ex)

    def get_records(self, sql, schema='default'):
        """
        Get a set of records from a Impala query.

        >>> hh = ImpalaHook()
        >>> sql = "SELECT * FROM airflow.static_babynames LIMIT 100"
        >>> len(hh.get_records(sql))
        100
        """
        return self.get_results(sql, schema=schema)['data']

    def get_pandas_df(self, sql, schema='default'):
        """
        Get a pandas dataframe from a Impala query

        >>> hook = ImpalaHook()
        >>> sql = "SELECT * FROM airflow.static_babynames LIMIT 100"
        >>> df = hook.get_pandas_df(sql)
        >>> len(df.index)
        100
        """
        import pandas as pd
        res = self.get_results(sql, schema=schema)
        df = pd.DataFrame(res['data'])
        df.columns = [c[0] for c in res['header']]
        return df

    def get_columns(self, table: dict) -> str:
        """
        Retrieve all columns for given table

        Parameters
        -----------
        table
            dict, describe table which columns we would like to get

        Returns
        --------
        str, columns list as a string of next template: "`col1`,`col2`,`col3`" etc
        """
        table = self.validate(**table)
        sql = f"SHOW COLUMN STATS {table['schema']}.{table['table_name']}"
        cols = self.get_records(sql, table['schema'])
        columns_str = ",".join([f"`{c[0]}`" for c in cols])
        logging.debug(f"Columns string: {columns_str}")
        return columns_str

    @staticmethod
    def validate(table_name=None, key_cols='*', schema='default', alias=None):
        if not table_name:
            raise AirflowException("Illegal Argument: table_name should be present in dict")

        if not alias:
            alias = f"{table_name[5]}{randint(0, 10999)}"
        return {'table_name': table_name, 'key_cols': key_cols, 'schema': schema, 'alias': alias}

    def invalidate_metadata(self, table_name: str = None, schema: str = 'default'):
        if table_name:
            # will mark on invalidate all tables in given schema - computationally heavy, use carefully
            q = f"INVALIDATE METADATA"
        else:
            # will mark on invalidate only particular table
            q = f"INVALIDATE METADATA {table_name}"
        self.run(sql=q, schema=schema)
