'''
 Batch process ghcn (weather) daily data from NOAA
 to get monthly average for each station-year
'''

import psycopg2
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import substring, col, avg, to_date
from pyspark.sql.types import (StructField, StringType, IntegerType, StructType, DoubleType, DateType)
from psycopg2.extras import execute_values

sc = SparkContext()

# return an array of years to loop over 
def gen_year_array(): 
    beg_yr = 2010
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


# pull the columns I'm storing from a noaa dataframe 
def select_noaa_cols(row): 
    return (row.month, row.dataval, row.latitude, row.longitude)

def get_noaa_simple_array(dataframe): 
    return dataframe.rdd.map(select_noaa_cols).collect()

# extract noaa precipitation data from s3 to postgres
def noaa_to_pg(year): 
    # first, handle the station info file 
    station_schema = StructType([StructField('station_id', StringType(), True),\
                         StructField('latitude', DoubleType(), True),\
                         StructField('longitude', DoubleType(), True),\
                         StructField('elevation', DoubleType(), True)])
    spark = SparkSession \
            .builder \
            .appName('Spark') \
            .config('spark.driver.extraClassPath', '/home/ubuntu') \
            .getOrCreate()

    station_file_path = 's3a://noaa-ghcnd-data/ghcnd-stations-clean.csv'
    station_data = spark.read.csv(station_file_path, sep=" ", header=False, schema=station_schema) 
    
    # Create schema for NOAA weather data
    data_schema = StructType([StructField('station_id', StringType(), True),\
                                  StructField('obsdate', StringType(), True),\
                                  StructField('element', StringType(), True),\
                                  StructField('dataval', StringType(), True),\
                                  StructField('mflag', StringType(), True),\
                                  StructField('qflag', StringType(), True),\
                                  StructField('sflag', StringType(), True),\
                                  StructField('obstime', StringType(), True)])
    
    # Load csv file from S3 
    file_path = 's3a://noaa-ghcnd-data/{}.csv.gz'.format(year)
    noaa_df = spark.read.csv(file_path, header=False, schema=data_schema)
    
    # Clean data, keep only the PRCP attribute with no quality flag (qflag) 
    rain_df = noaa_df.filter(noaa_data['element'].contains('PRCP'))
    rain_df = rain_df.filter(noaa_data['qflag'].isNull())

    # Add month column
    rain_df = rain_df.withColumn('month', substring('obsdate', 5, 2).cast(IntegerType()))
    
    # Join station data to get latitude/longitude
    rain_sta_df = rain_df.join(station_data, 'station_id', 'inner').drop('elevation', 'mflag', 'qflag', 'sflag', 'obstime')
    
    # Aggregate data by month, longitude, latitude
    rain_monthly_df = rain_sta_df.groupBy('month', 'longitude', 'latitude').agg(avg(col('dataval')).alias('dataval'))
    
    # Load the trimmed monthly dataframe to PostgreSQL     
    # Connect to database
    conn = db_connect() 
    cursor = conn.cursor()

    # Create rain data table
    table_name = 'rain_{}'.format(year)

    create_table = '''
                    DROP TABLE IF EXISTS %s;
                    CREATE TABLE %s (
                        month INTEGER, 
                        dataval INTEGER, 
                        latitude REAL,
                        longitude REAL
                    );
                    ''' % (table_name, table_name)
    cursor.execute(create_table)

    # Insert dataframe to table 
    new_cols = 'month,dataval,latitude,longitude'
    insert_query = 'INSERT INTO %s (%s) VALUES %%s' % (table_name, new_cols)
    rain_arr = rain_monthly_df.rdd.map(row.month,row.dataval,row.latitude,row.longitude).collect()
    execute_values(cursor, insert_query, rain_arr, page_size=500)

    # add column with postgis geography
    alter_query = 'ALTER TABLE %s ADD COLUMN geogcol geography(Point, 4326);' % table_name
    cursor.execute(alter_query)
    update_query = 'UPDATE %s SET geogcol = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);' %table_name
    cursor.execute(update_query)
    
    # Add index 
    add_index = 'CREATE INDEX %s_geo_index ON %s (geogcol) ;' % (table_name, table_name)
    cursor.execute(add_index)
    
    # Commit changes to database, close connection 
    conn.commit()
    cursor.close()
    conn.close()
    print('Processed NOAA data year {}'.format(year))

def proc_all_noaa(): 
    year_arr = gen_year_array()
    for year in year_arr: 
        noaa_to_pg(year)
    print('Processed all NOAA data at hand!')

def main(): 
    proc_all_noaa()

if __name__ == "__main__":
    main()
