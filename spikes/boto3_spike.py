#!/usr/bin/env python3
import boto3
import json
import decimal
import pprint
import string
import random
import datetime
import singer
import os

def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def clear_tables(dynamodb):
    for table in dynamodb.tables.all():
        print('Deleting table: {}'.format(table.name))
        table.delete()

def create_simple_table(dynamodb):
    print('\nCreating table: simple_table')
    table = dynamodb.create_table(
        TableName='simple_table',
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'  #Partition key
            },
            {
                'AttributeName': 'string_field',
                'KeyType': 'RANGE'  #Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'string_field',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'date_field',
                'AttributeType': 'S'
            },  

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 100,
            'WriteCapacityUnits': 100
        },
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'date_index',
                'KeySchema': [
                    {
                        'AttributeName': 'date_field',
                        'KeyType': 'HASH'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 100,
                    'WriteCapacityUnits': 100
                }
            }
        ]
    )
    print('Finished creating table: simple_table')

def populate_simple_table(dynamodb):
    print('\nPopulating table: simple_table')
    num_items = 50
    table = dynamodb.Table('simple_table')
    start_datetime = datetime.datetime(2018, 1, 1, 0, 0, 0, 0,
                                       tzinfo=datetime.timezone.utc)
    for int_value in range(num_items):
        item_dt = start_datetime + datetime.timedelta(days=(5*int_value))
        table.put_item(
            Item={
                "id": int_value,
                "string_field": random_string_generator(),
                "date_field": singer.strftime(item_dt)
            }
        )

    # wait for global secondary index to be backfilled
    while True:
        if not table.global_secondary_indexes or table.global_secondary_indexes[0]['IndexStatus'] != 'ACTIVE':
            print('Waiting for index to backfill...')
            time.sleep(5)
            table.reload()
        else:
            break

    print('Added {} items to table: simple_table'.format(num_items))

def query_simple_table(dynamodb):
    table = dynamodb.Table('simple_table')
    print('\nRetrieving items with date_field=1/1/2018')
    response = table.query(
        IndexName='date_index',
        KeyConditionExpression=boto3.dynamodb.conditions.Key('date_field').eq('2018-01-01T00:00:00.000000Z'))
    for simple_item in response.get('Items', []):
        print(' {}, {}, {} '.format(simple_item['id'],
                                    simple_item['string_field'],
                                    simple_item['date_field']))

def scan_simple_table(dynamodb):
    table = dynamodb.Table('simple_table')
    print('\nRetrieving items with date_field > 2018-03-01T00:00:00.000000Z')
    response = table.scan(
        IndexName='date_index',
        FilterExpression=boto3.dynamodb.conditions.Attr('date_field').gt('2018-03-01T00:00:00.000000Z'))
    for simple_item in response.get('Items', []):
        print(' {}, {}, {} '.format(simple_item['id'],
                                    simple_item['string_field'],
                                    simple_item['date_field']))
        
def create_movies(dynamodb):
    print('\nCreating table: movies')
    table = dynamodb.create_table(
        TableName='movies',
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH'  #Partition key
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE'  #Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            },  
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 100,
            'WriteCapacityUnits': 100
        }
    )
    print('Finished creating table: movies')
    
def populate_movies(dynamodb):
    print('\nPopulating table: movies')
    table = dynamodb.Table('movies')
    movie_count = 0
    with open("moviedata.json") as json_file:
        movies = json.load(json_file, parse_float = decimal.Decimal)
        for movie in movies:
            movie_count += 1
            year = int(movie['year'])
            title = movie['title']
            info = movie['info']

            table.put_item(
                Item={
                    'year': year,
                    'title': title,
                    'info': info,
                }
            )
    print('Added {} items to table: movies'.format(movie_count))

def query_movies(dynamodb):
    table = dynamodb.Table('movies')
    print('\nRetrieving Moby Dick from table...')
    response = table.get_item(Key={'year': 1956, 'title': 'Moby Dick'})
    if response.get('Item'):
        pp = pprint.PrettyPrinter(depth=3)
        pp.pprint(response['Item'])

    print('\nRetrieving movies made in 1985')
    response = table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('year').eq(1985))
    for mov in response.get('Items', []):
        print(' {}, {} '.format(mov['year'],
                                mov['title']))

def scan_movies(dynamodb):
    table = dynamodb.Table('movies')
    print('\nRetrieving movies made after 2015')
    response = table.scan(FilterExpression=boto3.dynamodb.conditions.Attr('year').gt(2015))
    for mov in response.get('Items', []):
        print(' {}, {} '.format(mov['year'],
                                mov['title']))


def main():
    dynamodb = boto3.resource('dynamodb',
                              endpoint_url='http://localhost:8000',
                              region_name='us-east-1')
    clear_tables(dynamodb)
    create_simple_table(dynamodb)
    populate_simple_table(dynamodb)
    query_simple_table(dynamodb)
    scan_simple_table(dynamodb)
    if  os.path.exists('moviedata.json'):
        create_movies(dynamodb)
        populate_movies(dynamodb)
        query_movies(dynamodb)
        scan_movies(dynamodb)
    else:
        print('\nError: did not find moviedata.json file to load, will not create movies table. Can download moviedata.json at https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/samples/moviedata.zip')

if __name__ == "__main__":
    main()
