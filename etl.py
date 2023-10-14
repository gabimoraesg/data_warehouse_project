import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Function to load staging data into staging tables base on 'copy_table_queries' from sql_queries.py
    """
    for query in copy_table_queries:
        print('\n'.join(('\nExecuting load staging data:', query)))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Function to load data into star schema base on 'insert_table_queries' from sql_queries.py
    """
    for query in insert_table_queries:
        print('\n'.join(('\nExecuting load data:', query)))
        cur.execute(query)
        conn.commit()


def main():
    """
        Function load data staging table and star schema
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # ETL
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()