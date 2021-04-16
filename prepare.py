import env
import pandas as pd

_col_dict = {'Consumption' : 'consumption', 'Date' : 'date', 'Wind' : 'wind', 'Solar' : 'solar', 'Wind+Solar' : 'wind_and_solar'}

def prepare_zach_df(df):
    """
    Takes in the complete dataframe (items, stores, sales) from the Zach API\
    and converts the sale_date column to a datetime, removes the timezone, then\
    sets the index to the sale_date and sorts the index. Returns a prepared dataframe\
    with additional features.
    """
    zach_df = df.copy()
    
    zach_df.sale_date = pd.to_datetime(zach_df.sale_date)
    
    zach_df = zach_df.set_index('sale_date').sort_index()
    zach_df.index = zach_df.index.tz_localize(None)
    
    return _add_zach_features(zach_df)
    
def _add_zach_features(df):
    """
    Helper function that adds month, day_of_week, and sales_total features to the complete Zach API dataframe.
    """
    zach_df = df.copy()
    
    zach_df['month'] = zach_df.index.month
    zach_df['day_of_week'] = zach_df.index.day_of_week
    zach_df['sales_total'] = zach_df.sale_amount * zach_df.item_price
    
    return zach_df

def prepare_germany_df(df):
    """
    Takes in the dataframe generated from the opsd_germany_daily.csv file and renames the columns,\
    converts the date column to a datetime, sets the index to date and sorts the index, then fills\
    NaN values with 0. Returns a prepared dataframe with additional features.
    """
    germany_df = df.copy()
    
    germany_df.rename(columns=_col_dict, inplace=True)
    
    germany_df.date = pd.to_datetime(germany_df.date)
    germany_df = germany_df.set_index('date').sort_index()
    
    germany_df = germany_df.fillna(value=0)
    
    return _add_germany_features(germany_df)
    
    
def _add_germany_features(df):
    """
    Helper function that adds month and year columns to the Germany dataframe.
    """
    germany_df = df.copy()
    
    germany_df['month'] = germany_df.index.month
    germany_df['year'] = germany_df.index.year
    
    return germany_df