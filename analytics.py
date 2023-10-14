import configparser
import psycopg2
from sql_queries import select_number_rows_queries, analytics_queries


def table_row_count(cur, conn):
    """
        Function count the number of rows inserted in each table
    """
    for query in select_number_rows_queries:
        print('\n'.join(('\nRunning:', query)))
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)

            
def analysis_queries(cur, conn):
    """
        Function with some analytics queries
    """
    for query in analytics_queries:
        print('\n'.join(('\nRunning:', query)))
        cur.execute(query)
        print(cur.fetchall())

def main():
    """
        Analytics Query Runner
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Analytics Query
    table_row_count(cur, conn)
    analysis_queries(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()