import sys
import datetime
import pytz
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from great_expectations.dataset import SparkDFDataset, PandasDataset
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import DataFrame, SparkSession
import awswrangler as wr
from squema import *
from utils import *
import pandas as pd
import numpy as np
from pyspark.sql.types import StringType, IntegerType, DecimalType
from pyspark.sql.functions import (
    to_date,
    col,
    lit,
    format_string,
    year,
    month,
    dayofmonth,
)

spark: SparkSession = (
    SparkSession.builder.appName('app_name')
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config(
        "spark.hadoop.hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    )
    .config("spark.sql.adaptive.enabled", True)
    .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", 120)
    .config("spark.sql.adaptive.coalescePartitions.enabled", True)
    .config("spark.sql.files.maxPartitonBytes", "3mb")
    .config("spark.sql.debug.maxToStringFields", 2000)
    # .config('spark.sql.hive.convertMetastoreParquet', 'false')
    .config(
        "spark.jars.packages",
        "org.apache.hudi:hudi-spark-bundle_2.12:0.7.0,org.apache.spark:spark-avro_2.12:3.0.2",
    )
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, [ 'BUCKET_ANALYTICS', 'BUCKET_RAW','PREFIX'])

prefix = args['PREFIX']
bucket_raw = args['BUCKET_RAW']
bucket = args['BUCKET_ANALYTICS']


#LOAD DATA
datasource0 = glueContext.create_dynamic_frame_from_options(
            connection_type = "s3", 
            connection_options = {"paths": [f"s3://{bucket_raw}/{prefix}.csv"]}, 
            format="csv", 
            format_options = {"withHeader": True,
            "separator": "|"})


#CONVERT DATA TYPES FROM DATAFRAME 
df = datasource0.toDF()

df = (
    df.select(col('comuna').cast(IntegerType()).alias('comuna'),
col('barrio').cast(IntegerType()).alias('barrio'),
col('shape__are').cast(DecimalType(38, 2)).alias('shape__are'),
col('nro_casos').cast(IntegerType()).alias('nro_casos'),
col('nro_venteros').cast(DecimalType(38, 2)).alias('nro_venteros'),
col('nro_sit_turisticos').cast(DecimalType(38, 2)).alias('nro_sit_turisticos'),
col('nro_centros_salud').cast(DecimalType(38, 2)).alias('nro_centros_salud'),
col('nro_postes_alumb').cast(DecimalType(38, 2)).alias('nro_postes_alumb'),
col('nro_paraderos_bus').cast(DecimalType(38, 2)).alias('nro_paraderos_bus'),
col('nro_inst_educ').cast(DecimalType(38, 2)).alias('nro_inst_educ'),
col('nro_hoteles').cast(DecimalType(38, 2)).alias('nro_hoteles'),
col('nro_esc_deportivos').cast(DecimalType(38, 2)).alias('nro_esc_deportivos'),
col('nro_centrocomercial').cast(DecimalType(38, 2)).alias('nro_centrocomercial'),
col('nro_camaras').cast(DecimalType(38, 2)).alias('nro_camaras'),
col('nro_bibliotecas').cast(DecimalType(38, 2)).alias('nro_bibliotecas'),
col('nro_acopios_taxi').cast(DecimalType(38, 2)).alias('nro_acopios_taxi'),
col('nro_arboles').cast(DecimalType(38, 2)).alias('nro_arboles'),
col('nro_cam_ars').cast(DecimalType(38, 2)).alias('nro_cam_ars'),
col('nro_cam_fotodeteccion').cast(DecimalType(38, 2)).alias('nro_cam_fotodeteccion'),
col('nro_semaforos').cast(DecimalType(38, 2)).alias('nro_semaforos'),
col('nro_estaciones_metro').cast(DecimalType(38, 2)).alias('nro_estaciones_metro'),
col('nro_hacienda_servicios').cast(DecimalType(38, 2)).alias('nro_hacienda_servicios'),
col('nro_hacienda_comercio').cast(DecimalType(38, 2)).alias('nro_hacienda_comercio'),
col('nro_hacienda_industria').cast(DecimalType(38, 2)).alias('nro_hacienda_industria'),
col('nro_hacienda_financiero').cast(DecimalType(38, 2)).alias('nro_hacienda_financiero'),
col('nro_hacienda_trat_especial').cast(DecimalType(38, 2)).alias('nro_hacienda_trat_especial'),
col('nro_paraderos_tpublico').cast(DecimalType(38, 2)).alias('nro_paraderos_tpublico'),
col('nro_centros_residuos').cast(DecimalType(38, 2)).alias('nro_centros_residuos'),
col('nro_predios').cast(IntegerType()).alias('nro_predios'),
col('nro_farmacias').cast(DecimalType(38, 2)).alias('nro_farmacias'),
col('nro_muebles_tienda').cast(DecimalType(38, 2)).alias('nro_muebles_tienda'),
col('nro_cent_policiales').cast(DecimalType(38, 2)).alias('nro_cent_policiales'),
col('nro_est_gasolina').cast(DecimalType(38, 2)).alias('nro_est_gasolina'),
col('nro_cajeros').cast(DecimalType(38, 2)).alias('nro_cajeros'),
col('nro_acond_fisico').cast(DecimalType(38, 2)).alias('nro_acond_fisico'),
col('nro_enti_bancarias').cast(DecimalType(38, 2)).alias('nro_enti_bancarias'),
col('nro_restaurantes').cast(DecimalType(38, 2)).alias('nro_restaurantes'),
col('nro_bares').cast(DecimalType(38, 2)).alias('nro_bares'),
col('nro_parqueaderos').cast(DecimalType(38, 2)).alias('nro_parqueaderos'),
col('nro_tiend_joyeria').cast(DecimalType(38, 2)).alias('nro_tiend_joyeria'),
col('nro_tiend_tenis').cast(DecimalType(38, 2)).alias('nro_tiend_tenis'),
col('nro_mall_comerciales').cast(DecimalType(38, 2)).alias('nro_mall_comerciales'),
col('nro_cafes').cast(DecimalType(38, 2)).alias('nro_cafes'),
col('nro_licorerias').cast(DecimalType(38, 2)).alias('nro_licorerias'),
col('nro_tiend_varias').cast(DecimalType(38, 2)).alias('nro_tiend_varias'),
col('nro_supermercados').cast(DecimalType(38, 2)).alias('nro_supermercados'),
col('nro_casinos').cast(DecimalType(38, 2)).alias('nro_casinos'),
col('nro_hoteles_hosta').cast(DecimalType(38, 2)).alias('nro_hoteles_hosta'),
col('nro_iglesias').cast(DecimalType(38, 2)).alias('nro_iglesias'),
col('nro_tiend_ropa').cast(DecimalType(38, 2)).alias('nro_tiend_ropa'),
col('nro_cent_mascotas').cast(DecimalType(38, 2)).alias('nro_cent_mascotas'),
col('nro_entre_nocturno').cast(DecimalType(38, 2)).alias('nro_entre_nocturno'),
col('nro_centros_medicos').cast(DecimalType(38, 2)).alias('nro_centros_medicos'),
col('nro_rutas_ciclismo').cast(DecimalType(38, 2)).alias('nro_rutas_ciclismo'),
col('nro_rutas_buses').cast(DecimalType(38, 2)).alias('nro_rutas_buses'),
col('nro_quebradas').cast(DecimalType(38, 2)).alias('nro_quebradas'),
col('geometry').cast(StringType()).alias('geometry'),
col('categorizacion').cast(IntegerType()).alias('categorizacion'),
to_date(col('ingest_time'), "dd-MM-yyyy").alias("ingest_time")))

df = df.toPandas()
#READING LIKE GX DATAFRAME
df_gx = PandasDataset(df)

#FIND NECESSARY FILES
df_cols = colValidations(df_gx, SQUEMA)
df_gen = generalValidations(df_gx, SQUEMA)

df_cols.loc[df_cols.colum_name == 'geometry','valor_max'] = 0
df_cols.loc[df_cols.colum_name == 'geometry','valor_min'] = 0


save_df = {
    'cantidad_general':df_cols,
    'general_features':df_gen,
    'hurtos_med_mod':df
}

#SAVING FILES IN S3
for val in save_df.keys():
    wr.s3.to_csv(
        df=save_df[val],
        path=f's3://{bucket}/observability/{val}.csv',
        sep='|',
        index=False
        )
