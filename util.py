import sys
import json
import csv
import yaml
import math
import copy
import pandas as pd
import numpy as np
import matplotlib as mpl
import time
from datetime import datetime
import pprint
import psycopg2
import util
import importlib
from sqlalchemy import create_engine, text as sql_text


def hello_world():
    print("Hello World")
    
def run_query_execute(query,db_eng):
 
    with db_eng.connect() as conn:
        result = conn.execute(sql_text(query))
    return result

def run_query_df(query,db_eng):
 
    with db_eng.connect() as conn:
        df = pd.read_sql(query, con=conn)
    return df

# Function to benchmark a simple query
def benchmark_query(query, time, benchmark_data, collection, index_name=None):
    if index_name:
        print(f"\nQuerying with index on {index_name}...")
    else:
        print("\nQuerying without index...")

    start_time = time.time()
    results = list(collection.find(query))
    end_time = time.time()

    duration = end_time - start_time
    result_count = len(results)

    print(f"Query took {duration:.6f} seconds")
    print(f"Number of results: {result_count}")

    benchmark_data.append({
        "query": query,
        "index_name": index_name,
        "duration": duration,
        "result_count": result_count
    })
    
    return benchmark_data


def build_query_left_join_listings_reviews_10():
    query = '''select *
    from listings l left join reviews r 
            on l.id = r.listing_id
      where left(l.id,2) = '10'
      '''
    return query



def build_query_left_join_listings_reviews():
    query = '''select *
    from listings l left join reviews r 
            on l.id = r.listing_id
            '''
    return query


def time_diff(time1,time2):
    return time2 - time1

def convert_date_str_to_datetime(dt):
    if dt is None:
        return None
    elif pd.isnull(dt):  # tests whether dt is the pandas value NaT ("not a time")
        # print('\nEntered the NaT case\n')
        return None
    elif dt != dt:
        return None        # could also use math.nan, I think
    elif dt == '':
        return None
    else:
        year = int(dt[0:4])
        month = int(dt[5:7])
        day = int(dt[8:10])
        # print(year, month, day)
        temp = datetime(year, month, day)
        ts = temp.timestamp()
        new_dt = datetime.fromtimestamp(ts)
        return new_dt
    
def convert_tf_to_boolean(val):
    if val == 't':
        return True
    elif val == 'f':
        return False
    else:
        return None

def convert_lwc_to_json(doc):
    doc_new = {}
    for key in ['_id', 'average_price']:
        doc_new[key] = doc[key]
    for key in ['first_available_date', 'last_available_date']:
        doc_new[key] = doc[key].strftime('%Y-%m-%d')
    dlist = []
    for d in doc['dates_list']:
        d_new = {}
        d_new['date'] = d['date'].strftime('%Y-%m-%d')
        for key in ['price', 'minimum_nights', 'maximum_nights', 'available']:
            d_new[key] = d[key]
        dlist.append(d_new)
    doc_new['dates_list'] = dlist
    return doc_new


def clean_listings_w_date(doc):
 
    doc['_id'] = str(doc['_id'])
    if 'price' in doc and doc['price'] not in [None, 'null', '']:
        try:
            doc['price'] = int(doc['price'])
        except ValueError:
            doc['price'] = None 
    else:
        doc['price'] = None
    
    if 'last_review' in doc and doc['last_review'] not in [None, 'null', '']:
        try:
            doc['last_review'] = pd.to_datetime(doc['last_review']).strftime('%Y-%m-%d')
        except Exception as e:
            doc['last_review'] = None
    else:
        doc['last_review'] = None
    
    if 'reviews_per_month' in doc and doc['reviews_per_month'] not in [None, 'null', '']:
        try:
            doc['reviews_per_month'] = int(float(doc['reviews_per_month']))
        except ValueError:
            doc['reviews_per_month'] = None 
    else:
        doc['reviews_per_month'] = None
    
    for review in doc.get('reviews', []):
        review['_id'] = str(review['_id'])
        if 'date' in review:
            review['date'] = review['date'].isoformat() if review['date'] else None

    for key, value in doc.items():
        if isinstance(value, float) and math.isnan(value):
            doc[key] = None
    
    return doc

def write_dict_to_dir_json(dict, dir, filename):
    with open(dir + '/' + filename, 'w') as fp:
        json.dump(dict, fp)
        
        
        
def clean_document(doc):

    if '_id' in doc:
        doc['_id'] = str(doc['_id'])
    
    if 'price' in doc and doc['price'] not in [None, 'null', '']:
        try:
            doc['price'] = int(doc['price'])
        except ValueError:
            doc['price'] = None 
    else:
        doc['price'] = None
    
    if 'last_review' in doc and doc['last_review'] not in [None, 'null', '']:
        try:
            doc['last_review'] = pd.to_datetime(doc['last_review']).strftime('%Y-%m-%d')
        except Exception as e:
            doc['last_review'] = None
    else:
        doc['last_review'] = None
    
    if 'reviews_per_month' in doc and doc['reviews_per_month'] not in [None, 'null', '']:
        try:
            doc['reviews_per_month'] = int(float(doc['reviews_per_month']))
        except ValueError:
            doc['reviews_per_month'] = None  
    else:
        doc['reviews_per_month'] = None
    
    for review in doc.get('reviews', []):
        review['_id'] = str(review['_id'])
        if 'date' in review and review['date'] not in [None, 'null', '']:
            try:
                review['date'] = review['date'].strftime('%Y-%m-%d')
            except Exception as e:
                review['date'] = None
    
    for key, value in doc.items():
        if isinstance(value, float) and math.isnan(value):
            doc[key] = None
        elif isinstance(value, datetime):
            doc[key] = value.strftime('%Y-%m-%d')

    return doc


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d')
        return json.JSONEncoder.default(self, obj)

def check_for_datetime(documents):
    for i, doc in enumerate(documents):
        for key, value in doc.items():
            if isinstance(value, datetime):
                print(f"Document {i}, Key {key} is datetime: {value}")
            elif key == 'reviews':
                for j, review in enumerate(value):
                    for review_key, review_value in review.items():
                        if isinstance(review_value, datetime):
                            print(f"Document {i}, Review {j}, Key {review_key} is datetime: {review_value}")


