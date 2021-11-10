# !/usr/bin/env python
# pylint: disable=C0116,W0613

"""
Price comparison DataBase
"""

import argparse
import decimal
import logging
import time
import datetime

import pandas as pd
from suds.client import Client
'''
Dirty hack is required to venv/Lib/site-packages/suds/sax/date.py, line 142
        match_result = _RE_DATETIME.match(value + ' 0:00:00.0')
'''
from tqdm import tqdm

from peewee import chunked
from playhouse.db_url import connect

from offer.models import db, Supplier, Price

WSDL_IP = 'https://ra.ae/webservice/customers/RaPriceOnlineWebservice.php?wsdl'
MULTIPLIER_COST = '1.03'
MULTIPLIER_WEIGHT = '9.8'

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
    output.write(f'{"Part number"};{"Cost, $"};{"W/Delivery"}'
                 f';{"Date"};{"Supplier"}\n')
    with open(query_file, 'r') as f:
        for query_pattern in f:
            query = Price.select().where(Price.partnumber == query_pattern.strip().replace('-', '')) \
                .order_by(Price.price)
            for price in query:
                output.write(f'{query_pattern.strip()};{cost(price.price)}'
                             f';{cost_ext(price.price, price.weight)};{price.date};{price.supplier.title}\n')
                break
    output.close()
    ts_finish = time.time()
    logger.log(logging.INFO, f'Query is executed in {round(ts_finish - ts_start, 3)} sec.')


def cost_ext(price, weight) -> str:
    if weight is None:
        weight = 0
    if price is None:
        price = 0
    return f'{price * decimal.Decimal(MULTIPLIER_COST) + weight * decimal.Decimal(MULTIPLIER_WEIGHT):.2f}' \
        .replace('.', ',')


def cost(price) -> str:
    if price is None:
        price = 0
    return f'{price * decimal.Decimal(MULTIPLIER_COST):.2f}'.replace('.', ',')


def header(items, group: bool) -> str:
    if group:
        items.pop('AverageSupplyTimeCorrected', None)
        items.pop('UpdateDate', None)
        items.pop('IsWeightChecked', None)
        items.pop('Available', None)
        items.pop('AvailabilityTS', None)
        items.pop('SupplierOnlineCode', None)
    return ';'.join(list(items.keys()))


def row(items) -> str:
    _header = list(items.keys())
    values = [str(items[x]).replace('.', ',') for x in _header]
    return ';'.join(values)


def group_row(group_items) -> str:
    price = 0
    priceincludingshipment = 0
    weight = 0
    weight_cnt = 0
    weightwithpackaging = 0
    weightwithpackaging_cnt = 0
    for g in group_items:
        price += g['Price']
        priceincludingshipment += g['PriceIncludingShipment']
        if g['Weight']:
            weight += g['Weight']
            weight_cnt += 1
        if g['WeightWithPackaging']:
            weightwithpackaging += g['WeightWithPackaging']
            weightwithpackaging_cnt += 1
    price = round(price / len(group_items) * 100) / 100
    priceincludingshipment = round(priceincludingshipment / len(group_items) * 100) / 100
    if weight_cnt > 0:
        weight = round(weight / weight_cnt * 1000) / 1000
    if weightwithpackaging_cnt > 0:
        weightwithpackaging = round(weightwithpackaging / weightwithpackaging_cnt * 1000) / 1000

    _header = list(group_items[0].keys())
    try:
        _header.remove('AverageSupplyTimeCorrected')
        _header.remove('UpdateDate')
        _header.remove('IsWeightChecked')
        _header.remove('Available')
        _header.remove('AvailabilityTS')
        _header.remove('SupplierOnlineCode')
    except ValueError:
        pass

    values = []
    for x in _header:
        if x == 'Price':
            values.append(str(price).replace('.', ','))
            continue
        if x == 'PriceIncludingShipment':
            values.append(str(priceincludingshipment).replace('.', ','))
            continue
        if x == 'Weight':
            values.append(str(weight).replace('.', ','))
            continue
        if x == 'WeightWithPackaging':
            values.append(str(weightwithpackaging).replace('.', ','))
            continue
        values.append(str(group_items[0][x]))
    return ';'.join(values)


def webservice_price(query_file: str,
                     login: str,
                     password: str,
                     output_file: str,
                     titles_file: str,
                     age: int,
                     group: bool) -> None:
    ts_start = time.time()
    print('Ping the web service...')
    wsdl = WSDL_IP
    client = Client(wsdl)
    print('...pass')

    titles = []
    if titles_file is not None:
        with open(titles_file, 'r') as f:
            for title in f:
                titles.append(title.strip())

    output = open(output_file, 'w')
    is_header_printed = False
    today = datetime.date.today()
    with open(query_file, 'r') as f:
        for query_pattern in tqdm(f, desc="Polling the web service with part numbers"):
            resp = client.service.GetPartInfoItems(
                login,
                password,
                query_pattern.strip(),
                False,
                'E',
                0.0)
            resp_group = []
            for r in resp:
                r_dict = Client.dict(r)
                # Check if price offer from desired title
                if titles:
                    offer_title = f"RA-{r_dict['ManufacturerShortName']}-{r_dict['SupplierOnlineCode']}"
                    if offer_title not in titles:
                        continue
                if age:
                    offer_age = today - r_dict['UpdateDate'].date()
                    if offer_age.days > int(age):
                        continue

                if not is_header_printed:
                    output.write(f'{header(r_dict, group)}\n')
                    is_header_printed = True
                if group:
                    resp_group.append(r_dict)
                else:
                    output.write(f'{row(r_dict)}\n')
            if len(resp_group) > 0:
                output.write(f'{group_row(resp_group)}\n')
    output.close()
    ts_finish = time.time()
    logger.log(logging.INFO, f'Query is executed in {round(ts_finish - ts_start, 3)} sec.')


def main() -> None:
    parser = argparse.ArgumentParser(
        description=f'Price comparison DataBase')
    parser.add_argument('-s', '--supplier', help='title of supplier, e.g. RA-TY-1')
    parser.add_argument('-f', '--file', help='path to file for import, e.g. '
                                             '"EXCLUSIVE PLUS_TY_SUPPLIER_1_2021-09-09.xlsx"')
    parser.add_argument('-q', '--query', help='path to file for query from database, e.g. query.csv')
    parser.add_argument('-w', '--webservice', help='path to file for query from webservice, e.g. query.csv')
    parser.add_argument('-l', '--login', help='login to access webservice')
    parser.add_argument('-p', '--password', help='password to access webservice')
    parser.add_argument('-t', '--titles', help='path to file with warehouses titles, e.g. titles.txt, optional')
    parser.add_argument('-a', '--age', help='maximum age in days to be reported, e.g. 30, optional')
    parser.add_argument('-g', '--group', action='store_true', help='group offers with average price')

    parser.add_argument('-o', '--output', help='path to file for result, e.g. output.csv')

    args = parser.parse_args()

    database = connect('sqlite:///default.db')
    db.initialize(database)

    # Create the tables.
    db.create_tables([Supplier, Price])

    if args.query is None and args.webservice is None:
        if args.supplier is None:
            parser.error("Please provide --supplier option")
        if args.file is None:
            parser.error("Please provide --file option")
        import_price(args.supplier, args.file)
    else:
        if args.output is None:
            parser.error("Please provide --output option")
        if args.query and args.webservice:
            parser.error("Please provide just one option of query and webservice")
        if args.query:
            query_price(args.query, args.output)
        if args.webservice:
            webservice_price(args.webservice, args.login, args.password, args.output, args.titles, args.age, args.group)
