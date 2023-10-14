import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        Function to DROP tables if existed base on 'drop_table_queries' from sql_queries.py
    """
    for query in drop_table_queries:
        print('\n'.join(('\nExecuting DROP:', query)))
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Function to CREATE tables base on 'create_table_queries' from sql_queries.py
    """
    for query in create_table_queries:
        print('\n'.join(('\nExecuting CREATE:', query)))
        cur.execute(query)
        conn.commit()


def main():
    """
        Function to connect to redshift, DROP and CREATE tables on sql_queries.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to Redshift')
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()