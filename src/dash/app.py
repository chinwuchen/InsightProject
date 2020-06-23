'''
Dash front-end 

'''

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
import plotly.graph_objs as go
import folium
import pandas as pd
import numpy as np
from opencage.geocoder import OpenCageGeocode
from pprint import pprint
from ipyleaflet import *

from folium.plugins import MarkerCluster

import query_df

host='localhost'

# Years available
years_arr = np.arange(2010, 2018)

# Make a Mapbox scatter plot based on (lat, long, value)
def make_geo_mapbox(df, chart_title, lonname, latname, textname, user_lon, user_lat):
    fig = go.Figure()
    # Add database points
    fig.add_trace(
        go.Scattermapbox(
               lon = list(df[lonname]),
               lat = list(df[latname]),
               text = list(df[textname]),
               name = '',
               showlegend = False,
               mode='markers',
               marker = go.scattermapbox.Marker(
                   color = list(df[textname]),
                   size = 16,
                   showscale = True,
                   autocolorscale = False,
                   symbol = 'circle',
                   colorbar_title = ' precipitation (mm)'
                )
        )
    )

    # Add user point
    fig.add_trace(
        go.Scattermapbox(
               lon = [user_lon],
               lat = [user_lat],
               text = ['Input coord.'],
               name = '',
               showlegend = False,
               mode='markers',
               marker = go.scattermapbox.Marker(
                        showscale = False,
                        color = 'gold',
                        size = 15,
                        symbol = 'star',
                        colorbar_title = ''
                   )
        )
    )
    fig.update_layout(title=go.layout.Title(text=chart_title), hovermode='closest', mapbox = dict(
       accesstoken=mapbox_key,
       center = go.layout.mapbox.Center(lon = user_lon, lat = user_lat),
       zoom=5
         )
    )
    return fig

# Read event data
csvfile = 'global_landslide_catalog_ym.csv'
df = pd.read_csv(csvfile)
df = df[df['event_year']==2015]
print(len(df))

# Create folium map (global scale, centered around north Africa
map_center = [21.9354, 13.6261] #madama, niger
tile = 'Stamen Terrain'

testmap = folium.Map(location=map_center,tiles=tile,zoom_start=2)

# Create a marker cluster instance
mc = MarkerCluster()

#print(MarkerCluster())

for i, row in df.iterrows():
    mc.add_child(folium.Marker(location = [row['latitude'],row['longitude']], popup=[row['latitude'],row['longitude'],row['event_date'],row['event_description']]))

testmap.add_child(mc)

# Return map
global_map = testmap

# savae map for layout
global_map.save('global_map.html')


# dash app layout and setting

#external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

external_stylesheets = ['https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.4.1/semantic.min.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

colors = {
        'background': '#111111',
        'text': '#7FDBFF'
}

app.layout = html.Div([
    html.H2('SlideAware: Integrated Landslide and Weather database'),
    html.Iframe(id = 'map', srcDoc = open('global_map.html', 'r').read(),
        width='80%',height='600'),
    # Input boxes for latitude/longitude
    html.Div([
         html.Div( 
             [dcc.Input(
                id = 'input-lat',
                type = 'number',
                placeholder = 'latitude',
                debounce = True
              )],
             className = 'input'
          ),
          html.Div(
              [dcc.Input(
                id = 'input-lon',
                type = 'number',
                placeholder = 'longitude',
                debounce = True
              )],
            className = 'input'
           
          ),
          html.Div(
              [dcc.Dropdown(
                id = 'yaxis-col',
                options=[{'label':i, 'value':i} for i in year_arr],
                value='2015'              
              )],
                className = 'ui dropdown',
                id='drop-down'
             ),
          html.Div(
              [dcc.Graph(id='rain_graph')],
              className = 'graph'
        ),
    ], className = 'row'
    ),
    ],
    id = 'mainContainer',
    )

user_lat = 0
user_lon = 0

@app.callback(
        Output('rain_graph','figure'),
        [Input('yaxis-col', 'value'), Input('input-lat','value'), Input('input-lon','value')]
        )

# Generate figure according to query
def gen_figure(year, lat, lon):
    
    if year is None:
        raise PreventUpdate
    if lat is None:
        raise PreventUpdate
    if lon is None:
        raise PreventUpdate
    # If use address
#    geolocator = OpenCage(opencage_key, domain='api.opencagedata.com',schema=None, user_agent='cwchen1976', format_string=None, timout=4)
#    location = geolocator.geocode(address)
#    user_lat = location.latitude
#    user_lon = location.longitude
    user_lat = lat
    user_lon = lon

    rain_knn_df = query_df.get_rain_knn_df(year, lat, lon, 10)

    print(rain_knn_df.head(3))

    rain_graph = make_geo_mapbox(rain_knn_df, 'Accumulated Annual Rain', 'st_x', 'st_y', 'accu', user_lon, user_lat)

    return rain_graph

if __name__ == '__main__':
  app.run_server(host=host, debug=True)
