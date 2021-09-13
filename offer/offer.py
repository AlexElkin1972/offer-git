# !/usr/bin/env python
# pylint: disable=C0116,W0613

"""
Price comparison DataBase
"""

import argparse
import decimal
import os
import logging
import time
from pprint import pprint

import pandas as pd
from peewee import chunked
from playhouse.db_url import connect

from offer.models import db, Supplier, Price

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)

logger = logging.getLogger(__name__)


def import_price(supplier_title: str, file_name: str) -> None:
    columns_dict = dict(partnumber='Part #',
                        description='Description',
                        description_ext='Russian Description',
                        price='Price',
                        date='Price Date',
                        origin='Origin',
                        weight='Weight',
                        weight_volume='V.Weight',
                        length='Length',
                        width='Width',
                        height='Height',
                        reserved='Reserved column')
    try:
        supplier = Supplier.get(Supplier.title == supplier_title.upper())
    except Supplier.DoesNotExist:
        supplier = Supplier.create(title=supplier_title.upper())

    ts_start = time.time()
    logger.log(logging.INFO, f'Reading content of "{file_name}"')
    df = pd.read_excel(file_name)
    ts_finish = time.time()
    logger.log(logging.INFO, f'Read of {len(df.index)} rows is completed in {round(ts_finish - ts_start, 3)} sec.')

    # Check columns title
    df_valid = True
    df_errors = []
    for column in df.columns:
        if not (column in columns_dict.values()):
            df_valid = False
            df_errors.append(f'Unexpected column {column}')

    # Store df in db
    if not df_valid:
        print(f'Invalid input file: {", ".join(df_errors)}')
    else:
        ts_start = time.time()

        # delete old data
        query = Price.delete().where(Price.supplier == supplier)
        with db.atomic():
            query.execute()
        ts_finish = time.time()
        logger.log(logging.INFO, f'Purging old data is completed in {round(ts_finish - ts_start, 3)} sec.')

        # add new data
        new_columns = []
        for column in df.columns:
            new_columns.append([x for x in columns_dict if columns_dict[x] == column][0])
        df.columns = new_columns
        df.insert(0, 'supplier', supplier, True)
        with db.atomic():
            for batch in chunked(df.to_dict(orient='records'), 100):
                Price.insert_many(batch).execute()

        ts_finish = time.time()
        logger.log(logging.INFO, f'Storing df to db is completed in {round(ts_finish - ts_start, 3)} sec.')


def query_price(query_file: str, output_file: str) -> None:
    ts_start = time.time()
    output = open(output_file, 'w')
    with open(query_file, 'r') as f:
        for query_pattern in f:
            query = Price.select().where(Price.partnumber == query_pattern.strip()).order_by(Price.price)
            for price in query:
                output.write(f'{price.partnumber}\t${cost(price.price, price.weight):.2f}'
                             f'\t{price.date}\t{price.supplier.title}\n')
                break
    output.close()
    ts_finish = time.time()
    logger.log(logging.INFO, f'Query is executed in {round(ts_finish - ts_start, 3)} sec.')


def cost(price, weight) -> str:
    return price * decimal.Decimal('1.03') + weight * decimal.Decimal('9.8')


def main() -> None:
    parser = argparse.ArgumentParser(
        description=f'Price comparison DataBase')
    parser.add_argument('-s', '--supplier', help="title of supplier, e.g. RA-TY-1")
    parser.add_argument('-f', '--file', help="path to file for import")
    parser.add_argument('-q', '--query', help="path to file for query")
    parser.add_argument('-o', '--output', help="path to file for result")

    args = parser.parse_args()

    database = connect('sqlite:///default.db')
    db.initialize(database)

    # Create the tables.
    db.create_tables([Supplier, Price])

    if args.query is None:
        if args.supplier is None:
            parser.error("Please provide --supplier option")
        if args.file is None:
            parser.error("Please provide --file option")
        import_price(args.supplier, args.file)
    else:
        if args.output is None:
            parser.error("Please provide --output option")
        query_price(args.query, args.output)
