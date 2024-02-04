import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    # Remove rows where the passenger count is equal to 0 or trip distance equal than 0
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    

    # Creates a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    data.rename(columns = {
        'VendorID':'vendor_id', 
        'RatecodeID':'rate_code_id',
        'PULocationID':'pu_location_id',
        'DOLocationID':'do_location_id'
        }, inplace = True)

    return data

@test
def test_output(output, *args) -> None:
    assert 'vendor_id' in output.columns, "Column 'vendor_id' is not present in the DataFrame."
    assert not (output['passenger_count'].isin([0])).any(), "Column 'passenger_count' contains at least one 0."
    assert not (output['trip_distance'].isin([0])).any(), "Column 'passenger_count' contains at least one 0."