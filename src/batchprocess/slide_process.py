'''
 Parsing landslide catalog
 Create yearly catalog table (month, latitude, longitude, countryname, countrycode) for later operations

'''

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

# Generate an array of years to process
def gen_year_array():
    beg_yr = 1988 
    end_yr = 2017
    year_arr = np.arange(beg_yr, end_yr+1)
    return year_arr

def db_connect():
    conn = psycopg2.connect(
                host = 'localhost',
                database = '',
                user = '',
                password = '')
    return conn

def read_df(file_path):
    df = pd.read_csv(file_path)
    return df

def execute_query(conn, query):
    returnval = 0
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    
    # If it is a select query, return the result
    if 'select' in query.lower():
        returnval = cursor.fetchall()
    cursor.close()
    return returnval


def df_to_postgres(year, df): 

    # Load selected columns to PostgreSQL 
 
    ydf = df[df.event_year == int(year)] 
#    print(ydf.head(5))

    cols = ['event_month','latitude','longitude','country_name','country_code']

    trimdf = ydf[cols]
    
    # Replace nan with blank string to fit varchar 
    trimdf = trimdf.replace(np.nan, ' ', regex=True)

    # Create a list of tupples from the df
    tuples = [tuple(x) for x in trimdf.to_numpy()]

    conn = db_connect() 
    cursor = conn.cursor()

    table_name = 'slide_{}'.format(year)

    create_table = ''' 
                    DROP TABLE IF EXISTS %s;
                    CREATE TABLE %s (
                        month INTEGER, 
                        latitude REAL,
                        longitude REAL,
                        countryname VARCHAR (50),
                        countrycode VARCHAR (2)
                    );
                    ''' % (table_name, table_name)

    cursor.execute(create_table)
    
    # Add df to table 
    new_cols = 'month,latitude,longitude,countryname,countrycode'
    insert_query = "INSERT INTO %s (%s) VALUES %%s"  % (table_name, new_cols)
    execute_values(cursor, insert_query, tuples, page_size=500)
    
    # Add column with postgis geography
    alter_query = 'ALTER TABLE %s ADD COLUMN geogcol geography(Point, 4326);' % table_name
    cursor.execute(alter_query)
    update_query = 'UPDATE %s SET geogcol = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);' % table_name
    cursor.execute(update_query)
    
    # Add index 
    add_index = 'CREATE INDEX %s_geo_index ON %s (geogcol) ;' % (table_name, table_name)
    cursor.execute(add_index)
    
    # Commit changes to database, close connection 
    conn.commit()
    cursor.close()
    conn.close()
    print('Parsed catalog year ' + year)

def process_slide(df): 

    year_arr = gen_year_array()
    
    for year in year_arr: 
        # check if event exists in the year        
        if len(df[df.event_year == int(year)]) >= 1:  

            df_to_postgres(year, df)        

            # check number of events read into table
#            conn = db_connect()
#            tname = 'slide_{}'.format(year)
#            query = "SELECT count(*) from %s;" % tname
#            n_rows = execute_query(conn, query)
#            print("Number of rows in the table = %s" % n_rows)
            conn.close()
        else:
            print('No event in year {}!'.format(year))
            continue

    print('Parsed catalog to database!')

def main():
    file_path = "s3a://global-landslide-data/global_landslide_catalog_ym.csv"
    df = read_df(file_path)
    process_slide(df)

if __name__ == "__main__":
    main()
