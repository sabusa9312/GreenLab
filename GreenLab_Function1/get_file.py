from azure.storage.blob import ContainerClient, BlobClient
import pandas as pd
from io import StringIO
from clean_file import *


def get_file(fname_blob1):

    #lista_blob_esistenti2=[blob.name for blob in container_client2.list_blobs()]
    #conn_str1='DefaultEndpointsProtocol=https;AccountName=greenlabits;AccountKey=NfnLp43FPKzqkUbUMNqSd46P1P54Aw9qIzccpqdiY037RoB1diFcHblRzI0O8Y0zrjY6Ux1o5DQg+AStGKiP7Q==;EndpointSuffix=core.windows.net'
    
    conn_str='DefaultEndpointsProtocol=https;AccountName=greenlabits;AccountKey=NfnLp43FPKzqkUbUMNqSd46P1P54Aw9qIzccpqdiY037RoB1diFcHblRzI0O8Y0zrjY6Ux1o5DQg+AStGKiP7Q==;EndpointSuffix=core.windows.net'
    container_client = ContainerClient.from_connection_string(conn_str=conn_str, container_name='file')
    lista_blob_esistenti=[blob.name for blob in container_client.list_blobs()]
    
    container_client2 = ContainerClient.from_connection_string(conn_str=conn_str, container_name='filepuliti')
    lista_blob_esistenti2=[blob.name for blob in container_client2.list_blobs()]
    
    if (fname_blob1 == 'import_2013_2016.csv') or (fname_blob1 == 'import_2017_2019.csv') or (fname_blob1 == 'import_fino_2013.csv'):
        name = "import.csv"
        downloaded_blob = container_client.download_blob('import_2013_2016.csv')
        downloaded_blob1 = container_client.download_blob('import_2017_2019.csv')
        downloaded_blob2 = container_client.download_blob('import_fino_2013.csv')
        df_import = importa(downloaded_blob.content_as_text(), downloaded_blob1.content_as_text(), downloaded_blob2.content_as_text())
        blob1=BlobClient.from_connection_string(conn_str=conn_str, container_name='prova',blob_name=name)

        blob1.upload_blob(df_import)
        return ('okappa')
     
     
        
    elif (fname_blob1 == 'percent'):
        
        if ('loss_states.csv' in lista_blob_esistenti2) and ('total_food.csv' in lista_blob_esistenti2):
            name = "percent.csv"
            downloaded_blob = container_client2.download_blob('loss_states.csv')
            downloaded_blob1 = container_client2.download_blob('total_food.csv')
            percent = percentu(downloaded_blob.content_as_text(), downloaded_blob1.content_as_text())
            blob1=BlobClient.from_connection_string(conn_str=conn_str, container_name='prova',blob_name=name)
            blob1.upload_blob(percent)
        else:
             return(f'Attenzione! Manca un file! \nI file presenti sono:\n{lista_blob_esistenti2}')
        return ('okappa')
    
    
    elif (fname_blob1 == 'export_2014_2016.csv') or (fname_blob1 == 'export_2017_2019.csv') or (fname_blob1 == 'export_fino_2013.csv'):
        
        name = "export.csv"
        downloaded_blob = container_client.download_blob('export_2014_2016.csv')
        downloaded_blob1 = container_client.download_blob('export_2017_2019.csv')
        downloaded_blob2 = container_client.download_blob('export_fino_2013.csv')
        df_export = exporta(downloaded_blob.content_as_text(), downloaded_blob1.content_as_text(), downloaded_blob2.content_as_text())
        blob1=BlobClient.from_connection_string(conn_str=conn_str, container_name='prova',blob_name=name)

        blob1.upload_blob(df_export)
        return 'okappa'
        
    elif (fname_blob1 == 'production_2014_2016.csv') or (fname_blob1 == 'production_2017_2019.csv') or (fname_blob1 == 'production_fino_2013.csv'):
        
        name = "production.csv"
        downloaded_blob = container_client.download_blob('production_2014_2016.csv')
        downloaded_blob1 = container_client.download_blob('production_2017_2019.csv')
        downloaded_blob2 = container_client.download_blob('production_fino_2013.csv')
        df_production = production(downloaded_blob.content_as_text(), downloaded_blob1.content_as_text(), downloaded_blob2.content_as_text())
        blob1=BlobClient.from_connection_string(conn_str=conn_str, container_name='prova',blob_name=name)
        blob1.upload_blob(df_production)
        return 'okappa'
        
    
    elif fname_blob1 == 'stock_variation.csv':
        name = 'stock_variation_EU.csv'
        downloaded_blob = container_client.download_blob(fname_blob1)    
        stock_variation_EU = stock_EU(downloaded_blob.content_as_text())
        blob1=BlobClient.from_connection_string(conn_str=conn_str, container_name='prova',blob_name=name)
        blob1.upload_blob(stock_variation_EU)
        return 'okappa'
    
    elif fname_blob1 == 'Trend_food_security.csv':
        
        name = 'Trend_food_security.csv'
        downloaded_blob = container_client.download_blob(fname_blob1)    
        Trend_food_security = security(downloaded_blob.content_as_text())
        blob1=BlobClient.from_connection_string(conn_str=conn_str, container_name='prova',blob_name=name)
        blob1.upload_blob(Trend_food_security)
        return 'okappa'
    
    elif fname_blob1 == 'loss_qty_area.csv':
        
        name = 'loss_food_ga.csv'
        downloaded_blob = container_client.download_blob(fname_blob1)    
        loss_food_ga = security(downloaded_blob.content_as_text())
        blob1=BlobClient.from_connection_string(conn_str=conn_str, container_name='prova',blob_name=name)
        blob1.upload_blob(loss_food_ga)
        return 'okappa'
        
        
    elif (fname_blob1 == 'land_use.csv') or (fname_blob1 == 'water_use.csv') or (fname_blob1 == 'carbon_footprints.csv'):
        # blob_file=container_client.get_blob_client(fname_blob).download_blob(fname_blob)
        name = "water_land_carbon.csv"
        downloaded_blob = container_client.download_blob('land_use.csv')
        #water1 = water_use(downloaded_blob.content_as_text())
        #blob=BlobClient.from_connection_string(conn_str=conn_str, container_name='prova',blob_name=fname_blob1)
        
        downloaded_blob1 = container_client.download_blob('water_use.csv')
        downloaded_blob2 = container_client.download_blob('carbon_footprints.csv')
        water_land_carbon = water_use(downloaded_blob.content_as_text(), downloaded_blob1.content_as_text(), downloaded_blob2.content_as_text())
        blob=BlobClient.from_connection_string(conn_str=conn_str, container_name='prova',blob_name=name)
        

        blob.upload_blob(water_land_carbon) 
        return 'okappa'
    
    elif (fname_blob1 == '_2010_11_12.csv') or (fname_blob1 == '_13_14_15.csv') or (fname_blob1 == '_16_17_18_19.csv'):
    
        name = "total_food.csv"
        downloaded_blob = container_client.download_blob('_2010_11_12.csv')
        downloaded_blob1 = container_client.download_blob('_13_14_15.csv')
        downloaded_blob2 = container_client.download_blob('_16_17_18_19.csv')
        total_food1 = total_food(downloaded_blob.content_as_text(), downloaded_blob1.content_as_text(), downloaded_blob2.content_as_text())
        blob1=BlobClient.from_connection_string(conn_str=conn_str, container_name='prova',blob_name=name)
        blob1.upload_blob(total_food1)

        # conn_str1='DefaultEndpointsProtocol=https;AccountName=greenlabits;AccountKey=NfnLp43FPKzqkUbUMNqSd46P1P54Aw9qIzccpqdiY037RoB1diFcHblRzI0O8Y0zrjY6Ux1o5DQg+AStGKiP7Q==;EndpointSuffix=core.windows.net'
        # container_client2 = ContainerClient.from_connection_string(conn_str=conn_str1, container_name='filepuliti')
        # lista_blob_esistenti2=[blob.name for blob in container_client2.list_blobs()]
        # if 'loss_states.csv' in lista_blob_esistenti2:
        #     name = 'percent.csv'
        #     downloaded_blob3 = container_client2.download_blob('loss_states.csv')
        #     downloaded_blob4 = container_client2.download_blob('total_food.csv.csv')
        #     percent = percentu(downloaded_blob3.content_as_text(), downloaded_blob4.content_as_text())
        #     blob2=BlobClient.from_connection_string(conn_str=conn_str1, container_name='prova',blob_name=name)
        #     blob2.upload_blob(percent)
        # else:
        #     return 'Manca un file'
        return 'okappa'
    
    # elif ('loss_states.csv' in container_client2) or ('total_food.csv' in container_client2):
    else:
        return(f'Attenzione! Nome non presente nella lista! Scrive percent per creare file della porcentuale\nI file presenti sono:\n{lista_blob_esistenti} ')