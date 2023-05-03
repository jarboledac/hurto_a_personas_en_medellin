import geopandas as gpd
import pandas as pd
import configparser


config = configparser.ConfigParser()
config.read('config.ini')

DATA_PATH = config.get('ApiGoogle', 'DATA_PATH')
DATA_GEO_PATH = config.get('ApiGoogle', 'DATA_GEO_PATH')



df = pd.read_csv(DATA_PATH, sep='|', index_col=0)
new_df = gpd.GeoDataFrame(
                    df, geometry=gpd.points_from_xy(df.longitud, df.latitud), crs=4326
                    )

new_df['duplicados'] = new_df[['latitud', 'longitud', 'nombre', 'direccion']].duplicated()
new_df = new_df[new_df.duplicados==False]
new_df.drop(columns=['duplicados'], inplace=True)
new_df.reset_index(drop=True, inplace=True)
new_df.to_file(DATA_GEO_PATH)