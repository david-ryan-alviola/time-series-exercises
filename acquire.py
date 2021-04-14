import pandas as pd
import requests as req

from io import StringIO

def _create_df_from_payloads(endpoint, max_pages, target_key_name):
    page_list = []
    
    for i in range(1, max_pages + 1):
        response = req.get(endpoint + "?page=" + str(i))
        data = response.json()
        page_items = data['payload'][target_key_name]
        page_list += page_items
        
    return pd.DataFrame(page_list)

def acquire_df_from_zach_api(endpoint, target_key_name):
    response = req.get(endpoint)
    response = response.json()

    max_pages = response['payload']['max_page']
    
    return _create_df_from_payloads(endpoint, max_pages, target_key_name)

def merge_zach_dataframes(items_df, stores_df, sales_df):
    merged_df = pd.DataFrame()
    
    sales_df.rename(columns={'item' : 'item_id'}, inplace=True)
    merged_df = pd.merge(items_df, sales_df, how="left", on="item_id")

    merged_df.rename(columns={'store' : 'store_id'}, inplace=True)
    merged_df = pd.merge(merged_df, stores_df, how="left", on="store_id")
    
    return merged_df

def acquire_germany():
    response = req.get("https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv")
    csv = StringIO(response.text)
    
    return pd.read_csv(csv)