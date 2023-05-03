from lists_places import PRINCIPALS_PLACES, ALL_PLACES_TYPES
import numpy as np
import pandas as pd
import geopandas as gpd
from geopandas.tools import sjoin
import urllib.request as url_req
import configparser
import json
import time

config = configparser.ConfigParser()
config.read('config.ini')

SHAPE_PATH = config.get('ApiGoogle', 'SHAPE_PATH')
DATA_PATH = config.get('ApiGoogle', 'DATA_PATH')

def findCentroide(gpdf):
    """
    For a geopanda dataframe with polygons geometric,
    return centroids for each form

    Parameters
    ----------
    gpdf : 
        Geopandas dataframe
    
    Returns
    -------
    Geopandas dataframe with centroids column
    """
    if gpdf.crs != '6257':
        gpdf = gpdf.to_crs("EPSG:6257")
    gpdf['centroide'] = gpdf.centroid
    gpdf = gpdf.set_geometry("centroide")
    gpdf= gpdf.to_crs("EPSG:4326")
    gpdf['geometry']= gpdf['geometry'].to_crs("EPSG:4326")
    return gpdf


def urlResponse(url):
    """
    Function to capture reponse form url request

    Parameters
    ----------
    url : str
        Url for request
    
    Returns
    -------
    Json with url response
    """
    response = url_req.urlopen(url)
    json_raw = response.read()
    json_data = json.loads(json_raw)
    return json_data

def requestApi(url):
    """
    Make two request to url, where the second is realized if the first one fail.

    Parameters
    ----------
    url : str
        Url for request
    
    Returns
    -------
    Json with url response
    
    """
    json_data = urlResponse(url)
    if json_data['status'] != 'OK':
        time.sleep(2)
        json_data = urlResponse(url)
        if json_data['status'] != 'OK':
            if json_data['status'] == 'ZERO_RESULTS':
                print(f'#################### There arent this places near ####################')
            else:
                raise Exception("Api Response status is Not OK ({})".format(json_data['status']))
    return json_data


def getPlacesOftype(place_type, location, key):
    """
    Obtain information about nearby places from a location.

    Parameters
    ----------
    place_type : str
        Place to search nearby to the location
    location : list
        list with latitud and longitud pair.
    
    Returns
    -------
    Returns result and pagination

    """
    place_type = place_type.strip()
    if place_type not in ALL_PLACES_TYPES:
        raise Exception('Invalid place type {}'.format(place_type))
    location_str = '{},{}'.format(location[0],location[1])
    base_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?'
    request_url = '{}key={}&rankby=distance&location={}&type={}'.format(
        base_url, key, location_str, place_type
        )
    j=1
    response = requestApi(request_url)
    results = response['results']
    while 'next_page_token' in response:
        j+=1
        response = requestApi(
            '{}&pagetoken={}'.format(request_url, response['next_page_token']))
    results.extend(response['results'])
    return results,j


def pipelineProcess(place, location, key):
    """
    Execute the steps to find a kind of place nearby to a location

    Parameters
    ----------
    place : str
        Place to search nearby to the location
    location : list
        list with latitud and longitud pair.
    key : str
        Key necessary to use google maps api
    
    Returns
    -------
    Returns nearby places
    """
    num_req = 0
    df_vals = []
    result, r = getPlacesOftype(place, location, key)
    num_req+=r
    for values in result:
        geom = values['geometry']['location']
        struct_df = {
            'latitud' : geom['lat'],
            'longitud' : geom['lng'],
            'nombre' : values['name'],
            'direccion' : values['vicinity'],
            'tipo' : place
            }
        df_vals.append(struct_df)
        if num_req == 9000:
            raise Exception("ALCANZAMOS EL LIMITE")
    return df_vals, num_req

def findPlacesIterative(gpdf, principal_places):
    """
    Iterate over a locations for find places in a list
    """
    i = 0
    req = 0
    prueba = []
    time_one = time.time()
    for geom in gpdf.centroide:
        loc = [geom.y,geom.x]
        print(f'########## Start the process with latitude {loc[0]} and longitude {loc[1]} #########')
        i+=1
        for places in principal_places:
            print(f'##### Start the process with the place {places} --- NUMBER {i} #####')
            prueba2, reque = pipelineProcess(places, loc, KEY)
            prueba += prueba2
            req += reque
        print(f'############## NUMERO DE PETICIONES {req} #######################')
    time_two = time.time()
    print('THE FINAL PROCESS TIME IS  ',(time_two-time_one)/60)
    return prueba

if __name__ == '__main__':
    mede_shp = gpd.read_file(SHAPE_PATH)
    mede_shp = findCentroide(mede_shp)
    df = findPlacesIterative(mede_shp, PRINCIPALS_PLACES)
    df = pd.DataFrame(df)
    df.to_csv(DATA_PATH, sep='|')
    
