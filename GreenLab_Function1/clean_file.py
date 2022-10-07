from azure.storage.blob import ContainerClient, BlobClient
import pandas as pd
from io import StringIO

def water_use(name, name2, name3):
    water = pd.read_csv(StringIO(name))
    water.drop(['Code'],axis=1,inplace=True)
    water.rename(columns={'Freshwater withdrawals per kilogram (Poore & Nemecek, 2018)':'Freshwater withdrawals per kilogram'},inplace=True)
    
    carbon = pd.read_csv(StringIO(name2))
    carbon.drop(['Code'], axis=1, inplace=True)
    carbon.rename(columns={"GHG emissions per kilogram (Poore & Nemecek, 2018)": "GHG emissions per kilogram"},inplace = True)
    
    land = pd.read_csv(StringIO(name3))
    land.drop(['Code'], axis=1, inplace=True)
    land.rename(columns={"Land use per kilogram (Poore & Nemecek, 2018)": "Land use per kilogram"},inplace = True)
    
    water_land_carbon=pd.merge(water, land, on=('Entity','Year'), how='inner').merge(carbon, on=('Entity','Year'), how='inner')
    water_land_carbon.rename(columns={"Entity": "Foods"},inplace = True)
    water_land_carbon = water_land_carbon.to_csv(encoding='utf-8-sig')
    
    return water_land_carbon

def importa(name, name2, name3):
    
    import_13 = pd.read_csv(StringIO(name))
    import_14_16 = pd.read_csv(StringIO(name2))
    import_17_19 = pd.read_csv(StringIO(name3))
    df_import = pd.concat([import_13, import_14_16, import_17_19], axis=0)
    df_import.drop(['Domain Code','Domain','Area Code (FAO)','Element Code','Element','Item Code (CPC)','Year Code','Unit'],axis=1,inplace=True)
    df_import.rename(columns={"Value": "Import value (tonnes)"},inplace = True)
    df_import.dropna(inplace=True)
    df_import[df_import['Import value (tonnes)'].isnull()]
    delet_zero1 = df_import[df_import["Import value (tonnes)"]== 0].index
    df_import = df_import.drop(delet_zero1)
    df_import = df_import.to_csv(encoding='utf-8-sig')
    return df_import

def exporta(name, name2, name3):
    
    export_13 = pd.read_csv(StringIO(name))
    export_14_16 = pd.read_csv(StringIO(name2))
    export_17_19 = pd.read_csv(StringIO(name3))
    df_export = pd.concat([export_13, export_14_16, export_17_19], axis=0)
    df_export.drop(['Domain Code','Domain','Area Code (FAO)','Element Code','Element','Item Code (CPC)','Year Code','Unit'],axis=1,inplace=True)
    df_export.rename(columns={"Value": "Export value (tonnes)"},inplace = True)
    df_export.dropna(inplace=True)
    remove_negative= df_export[df_export["Export value (tonnes)"]< 0].index
    df_export = df_export.drop(remove_negative)
    delet_zero2 = df_export[df_export["Export value (tonnes)"]== 0].index
    df_export = df_export.drop(delet_zero2)
    df_export = df_export.to_csv(encoding='utf-8-sig')
    return df_export

def production(name, name2, name3):
    production_13 = pd.read_csv(StringIO(name))
    production_14_16 = pd.read_csv(StringIO(name2))
    production_17_19 = pd.read_csv(StringIO(name3))
    df_production = pd.concat([production_13, production_14_16, production_17_19], axis=0)
    df_production.drop(['Domain Code','Domain','Area Code (FAO)','Element Code','Element','Item Code (CPC)','Year Code','Unit'],axis=1,inplace=True)
    df_production.rename(columns={"Value": "Production value (tonnes)"},inplace = True)
    df_production.dropna(inplace=True)
    delet_zero3 = df_production[df_production["Production value (tonnes)"]== 0].index
    df_production = df_production.drop(delet_zero3)
    df_production["Production value (tonnes)"] = df_production["Production value (tonnes)"].astype(int)
    df_production = df_production.to_csv(encoding='utf-8-sig')
    return df_production

def stock_EU(name):
    stock_variation_EU = pd.read_csv(StringIO(name))
    stock_variation_EU.drop(['Domain Code','Domain','Area Code (FAO)','Element Code','Element','Item Code (CPC)','Year Code','Unit'],axis=1,inplace=True)
    stock_variation_EU.rename(columns={"Value": "Stock variation EU value (tonnes)"},inplace = True)
    stock_variation_EU.dropna(inplace=True)
    stock_variation_EU['Stock variation EU value (tonnes)'] = stock_variation_EU['Stock variation EU value (tonnes)'].astype(int)
    stock_variation_EU = stock_variation_EU.to_csv(encoding='utf-8-sig')
    return stock_variation_EU

def security(name):
    security = pd.read_csv(StringIO(name))
    table_1 = security[security['Unit']=='millions']
    table_2 = security[security['Unit']=='%']
    table_1 = table_1[table_1['Item']!='Number of severely food insecure people (million) (3-year average)']
    table_2 = table_2[table_2['Item']!='Prevalence of severe food insecurity in the total population (percent) (3-year average)']
    Trend_food_security = pd.merge(table_1, table_2, on=('Area','Domain','Year','Element'), how='inner')
    Trend_food_security.drop(['Domain','Element','Item_x','Item_y','Unit_x','Unit_y'],axis=1,inplace=True)
    Trend_food_security.rename(columns={"Area": "Geographic Area"},inplace = True)
    Trend_food_security.rename(columns={"Value_x": "Number of severely food insecure people (million)"},inplace = True)
    Trend_food_security.rename(columns={"Value_y": "Prevalence of severe food insecurity in the total population (%)"},inplace = True)
    Trend_food_security = Trend_food_security.to_csv(encoding='utf-8-sig')
    return Trend_food_security

def loss_food_ga(name):

    loss_food_ga = pd.read_csv(StringIO(name))
    loss_food_ga.drop(['Domain Code','Domain','Area Code (FAO)','Element Code','Element','Item Code (CPC)','Year Code','Unit'],axis=1,inplace=True)
    loss_food_ga.rename(columns={"Value": "Loss of food (tonnes)"},inplace = True)
    loss_food_ga.dropna(inplace=True)
    loss_food_ga['Loss of food (tonnes)'] = loss_food_ga['Loss of food (tonnes)'].astype(int)
    loss_food_ga = loss_food_ga.to_csv(encoding='utf-8-sig')
    return loss_food_ga

def total_food(name, name2, name3):
        totale_10_11_12 = pd.read_csv(StringIO(name))
        totale_13_14_15 = pd.read_csv(StringIO(name2))
        totale_16_17_18_19 = pd.read_csv(StringIO(name3))
        total_food = pd.concat([totale_10_11_12, totale_13_14_15, totale_16_17_18_19], axis=0)
        total_food.drop(['Domain Code','Domain','Area Code (FAO)','Element Code','Element','Item Code (CPC)','Year Code','Unit','Flag','Flag Description'],axis=1,inplace=True)
        total_food.rename(columns={"Value": "Total food by states (tonnes)"},inplace = True)
        total_food.dropna(inplace=True)
        total_food = total_food.to_csv(encoding='utf-8-sig')
        return total_food
    
def percentu(name, name2):
    loss_states = pd.read_csv(StringIO(name))
    total_food = pd.read_csv(StringIO(name2))
    total_loss_state = loss_states.groupby(['Area','Year'], as_index=False)['Loss of food by states (tonnes)'].sum()
    total_f = total_food.groupby(['Area','Year'], as_index=False)['Total food by states (tonnes)'].sum()
    percent = pd.merge(total_loss_state, total_f, on=('Area','Year'), how='inner')
    percent['Percent loss food by states'] = round(total_loss_state['Loss of food by states (tonnes)']/total_f['Total food by states (tonnes)']*100, 2)
    percent = percent.to_csv(encoding='utf-8-sig')
    return percent
    
def loss_states(name):
    loss_states = pd.read_csv(StringIO(name))
    loss_states.drop(['Domain','Element','Unit'],axis=1,inplace=True)
    loss_states.rename(columns={"Value": "Loss of food by states (tonnes)"},inplace = True)
    loss_states.dropna(inplace=True)
    loss_states['Loss of food by states (tonnes)'] = loss_states['Loss of food by states (tonnes)'].astype(int)
    loss_states = loss_states.to_csv(encoding='utf-8-sig')
    return loss_states

def popolazione(name, name1):
    population = pd.read_csv(StringIO(name))
    total_loss_state = pd.read_csv(StringIO(name1))
    population.drop(['Domain','Element','Item'],axis=1,inplace=True)
    population.rename(columns={"Value": "Population"},inplace = True)
    population = pd.merge(population, total_loss_state, on=('Year','Area'), how='inner')
    population['Population'] = (population['Population']*1000).astype(int)
    population['Lost food per person (Kg)'] = round((population['Loss of food by states (tonnes)']/population['Population'])*1000,2)