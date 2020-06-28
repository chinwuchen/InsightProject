'''
Preprocess raw landslide catalog data 
Drop uninformative columns
Add respective month and year columns for later use
Save to new "clean-up" csv file for subsequent batch processing

'''


import pandas as pd
import numpy as np

raw_file = 'Global_Landslide_Catalog_Export.csv'
out_file = 'global_landslide_catalog_ym.csv'

# Read the raw catalog file
df = pd.read_csv(raw_file)

# Change event_date to pandas datetime object
# Move date and time in respective columns
df['event_date'] = pd.to_datetime(df['event_date'])
df['event_time'] = [d.time() for d in df['event_date']]
df['event_date'] = [d.date() for d in df['event_date']]

# Drop uninformative columns
drop_cols = ['event_id', 'created_date', 'last_edited_date', 'submitted_date', \
             'event_import_source','event_import_id', 'event_id', 'created_date', 'last_edited_date']
df.drop(drop_cols, axis=1, inplace=True)

# Add year and month columns
df['event_year'] = [d.year for d in df['event_date']]
df['event_month'] = [d.month for d in df['event_date']]
df.to_csv(outfile, index=None)
