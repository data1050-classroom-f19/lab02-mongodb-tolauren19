"""
(This is a file-level docstring.)
This file provides ETL (Extract Transform Load) functions for the NYC airbnb and NYC Taxi datasets.
"""
from pymongo import MongoClient, TEXT, GEOSPHERE
from datetime import datetime
import pandas as pds


def load_airbnb(file):
    """ Extracts the airbnb csv file into memory, Transforms certain fields, and Loads result into MongoDb.
        Creates TEXT index on 'name' and 'neighbourhood'.
        Creates GEOSPHERE index on 'location'. 
    Args:
        file: path to the airbnb csv file.
    """
    arr = []
    df = pds.read_csv(file)
    for i, row in df.iterrows():
        d = row.to_dict()
        d['location'] = {
            'type': 'Point',
            'coordinates': [
                d['longitude'], d['latitude']
            ]}
        d.pop('longitude')
        d.pop('latitude')
        arr.append(d.copy())
    print("Airbnb document fields: ", arr[0].keys())

    inserted_ids = db.airbnb.insert_many(arr).inserted_ids
    db.airbnb.create_index(
        [('name', TEXT), ('neighbourhood', TEXT)], default_language='english')
    db.airbnb.create_index([('location', GEOSPHERE)])

    print(len(inserted_ids), "Airbnb documents inserted")
    print("Text index created for airbnb")
    print("Geosphere index created for airbnb")


def load_taxi(file):
    """ Extracts the taxi csv file into memory, Transforms certain fields, and Loads result into MongoDb.
    Args:
        file: location of the taxi csv file.
    """
    arr = []
    df = pds.read_csv(file)
    for i, row in df.iterrows():
        d = row.to_dict()
        d['pickup_datetime'] = datetime.strptime(d['pickup_datetime'], '%Y-%m-%d %H:%M:%S %Z')
        arr.append(d.copy())

    # TODO: insert `arr` into `db.taxi` and print the number of records inserted.
    # Use load_airbnb as an example. This takes 2 lines of codes.
    arr = []
    df = pds.read_csv(file)
    for i, row in df.iterrows():
        d = row.to_dict()
        d['pickup'] = {
            'type': 'Point',
            'coordinates': [
                d['pickup_longitude'], d['pickup_latitude']
            ]}
        d['dropoff'] = {
            'type': 'Point',
            'coordinates': [
                d['dropoff_longitude'], d['dropoff_latitude']
            ]}
        d.pop('pickup_longitude')
        d.pop('pickup_latitude')
        d.pop('dropoff_longitude')
        d.pop('dropoff_latitude')
        arr.append(d.copy())
    print("Taxi document fields: ", arr[0].keys())

    inserted_ids = db.taxi.insert_many(arr).inserted_ids
    # db.taxi.create_index(
    #     [('name', TEXT), ('neighbourhood', TEXT)], default_language='english')
    db.taxi.create_index([('pickup', GEOSPHERE), ('dropoff', GEOSPHERE)])
    print(len(inserted_ids), "taxi documents inserted")


if __name__ == "__main__":
    db = MongoClient().test
    db.airbnb.drop()
    db.taxi.drop()
    load_airbnb('AB_NYC_2019.csv')
    load_taxi('TAXI_NYC_2019.csv')
