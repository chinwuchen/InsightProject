'''
Return dataframe for query with latitude/longitude
'''

import pandas as pd
import psycopg2

def db_connect():
    conn = psycopg2.connect(
                host = 'localhost',
                database = '',
                user = '',
                password = '')
    return conn

def get_rain_knn_df(year, latitude, longitude, n_neighbors):
    
    conn = db_connect()

    # Make query based on K Nearest Neighbor (KNN) searching
    query = '''
    SELECT SUM(dataval) as accu, ST_X(geogcol::geometry), ST_Y(geogcol::geometry)
    FROM rain_{0} GROUP BY rain_{0}.geogcol
    ORDER BY rain_{0}.geogcol <-> ST_MakePoint({2},{1})::geography
    LIMIT {3};
    '''.format(year, latitude, longitude, n_neighbors)
    
    rain_df = pd.read_sql(query, conn)
    conn.close()
    return rain_df


