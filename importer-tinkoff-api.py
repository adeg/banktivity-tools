#!/usr/bin/env python3
import argparse
import configparser
import dateutil
import getpass
import io
import keyring
import logging
import pprint
import pytz
from libs import Banktivity
# Awethon/open-api-python-client
from datetime import datetime, timedelta
from openapi_client import openapi
from pytz import timezone


# Read configuration stored in settings.ini
config = configparser.ConfigParser()
config.read('settings.ini')
importer_config = config['importer-tinkoff-api']
debug = importer_config.getboolean('Debug')
dryrun = importer_config.getboolean('DryRun')
our_timezone = importer_config['Timezone']
default_banktivity_document = importer_config['DefaultBanktivityDocument']
# This dict resolves Tinkoff.Investments accounts into Banktivity account names
account_type_to_names = {
    'Tinkoff': importer_config['BanktivityInvestmentAccountName'],
    'TinkoffIis': importer_config['BanktivityInvestmentIISAccountName']
}


# Constants
OPENAPI_TOKEN_LENGTH = 87


loggingLevel = logging.INFO
if debug:
    loggingLevel = logging.DEBUG

# Get Tinkoff Investments OpenAPI token
token = keyring.get_password('adeg/banktivity-importer', 'tinkoff-api')
if token is None or len(token) < OPENAPI_TOKEN_LENGTH:
    token = getpass.getpass(prompt='Enter Tinkoff Investments OpenAPI token: ')
    if len(token) < OPENAPI_TOKEN_LENGTH:
        print("Unable to obtain a valid Tinkoff Investments OpenAPI token")
        exit(1)


# Global variables and objects
banktivity = None
broker_accounts = {}
broker_portfolio = {}
broker_operations = {}
client = openapi.api_client(token)
del token # if I can remove sensitive info from some part of the memory - I go for it
logging.basicConfig(filename='importer-tinkoff-api.log', level=loggingLevel, format="[%(levelname)s] %(funcName)s(): %(message)s")


def main():
    global banktivity, default_banktivity_document

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', help="either 'print' or 'import'")
    parser.add_argument('collection',
                        help="Tinkoff Broker Data collections: <all|accounts|portfolio|operations>")
    parser.add_argument(
        'period_start',
        nargs='?',
        help="Начало временного промежутка, желательно в ISO-формате",
        type=lambda s: timezone(our_timezone).localize(dateutil.parser.parse(s)),
        default=datetime.now(tz=timezone(our_timezone)) - timedelta(days=90)
    )
    parser.add_argument(
        'period_end',
        nargs='?',
        help="Конец временного промежутка, желательно в ISO-формате. Если не указано, то будет now()",
        type=lambda s: timezone(our_timezone).localize(dateutil.parser.parse(s)),
        default=datetime.now(tz=timezone(our_timezone))
    )
    parser.add_argument(
        'banktivity_document',
        nargs='?',
        default=default_banktivity_document
    )
    parser.add_argument('--log', dest='loglevel', help="")
    args = parser.parse_args()

    if args.command and args.collection:
        fetch_accounts()
        fetch_portfolio()
        if args.command == 'print':
            if args.collection == 'accounts':
                print("Accounts")
                pprint.pprint(broker_accounts)
            elif args.collection == 'portfolio':
                print("Portfolio at broker")
                pprint.pprint(broker_portfolio)
            elif args.collection == 'operations':
                print("Operations")
                for item in broker_accounts:
                    broker_account_id = item.broker_account_id
                    broker_account_type = item.broker_account_type
                    print("Account type " + broker_account_type + " with ID " + broker_account_id)
                    response = client.operations.operations_get(
                        _from=args.period_start.isoformat()
                        , to=args.period_end.isoformat()
                        , broker_account_id=broker_account_id
                    )
                    print("Fetching operations for " + broker_account_id)
                    for op in response.payload.operations:
                        pprint.pprint(op)
        elif args.command == 'import' and args.collection == 'all':
            banktivity = Banktivity.Banktivity(args.banktivity_document)
            import_operations(args)

            if not dryrun:
                banktivity.commit()
        else:
            print("I don't know what to do. Probably unexpected combination of command line arguments given.")


def get_portfolio_security_by_figi(figi):
    elem_index = None
    for i, dic in enumerate(broker_portfolio):
        if dic.figi == figi:
            elem_index = i
            break

    if elem_index is not None:
        return broker_portfolio[elem_index]
    else:
        return None


def fetch_accounts():
    global broker_accounts
    response = client.user.user_accounts_get()
    broker_accounts = response.payload.accounts.copy()
    logging.debug(f"Raw Tinkoff Investments account data:\n{pprint.pformat(broker_accounts)}")


def fetch_portfolio():
    global broker_portfolio
    response = client.portfolio.portfolio_get()
    broker_portfolio = response.payload.positions
    '''
    {'average_position_price': {'currency': 'RUB', 'value': 0.0343},
     'average_position_price_no_nkd': None,
     'balance': 6290000.0,
     'blocked': None,
     'expected_yield': {'currency': 'RUB', 'value': 2327.3},
     'figi': 'BBG004730ZJ9',
     'instrument_type': 'Stock',
     'isin': 'RU000A0JP5V6',
     'lots': 629,
     'name': 'Банк ВТБ',
     'ticker': 'VTBR'}
    '''
    logging.debug(f"Broker Portfolio raw data:\n{pprint.pformat(broker_portfolio)}")


def get_broker_security_by_figi(figi):
    broker_security = get_portfolio_security_by_figi(figi)
    if broker_security is None:
        broker_security = search_by_figi(figi)
        if broker_security.isin is None:
            print(f"ERROR: Couldn't find broker security by figi {figi}.")
            return None

    return broker_security


def search_by_figi(figi):
    response = client.market.market_search_by_figi_get(figi)
    '''
    {
      "trackingId": "string",
      "status": "string",
      "payload": {
        "figi": "string",
        "ticker": "string",
        "isin": "string",
        "minPriceIncrement": 0,
        "lot": 0,
        "currency": "RUB",
        "name": "string",
        "type": "Stock"
      }
    }
    '''
    logging.debug(f"Search by FIGI {figi}:\n{pprint.pformat(response)}")

    return response.payload


def get_market_candle_by_figi_and_day(figi, datetime):
    datetimefrom = datetime.replace(hour=0, minute=0, second=0)
    datetimeto = datetime.replace(hour=23, minute=59, second=59)
    response = client.market.market_candles_get(
        figi=figi, _from=datetimefrom.isoformat(), to=datetimeto.isoformat(), interval='day'
    )
    candles = response.payload.candles
    logging.debug(f"Fetching candles for {figi}:\n{pprint.pformat(response)}")
    if len(candles) > 1:
        print(
            f"WARNING: More than 1 candle ({len(candles)}) returned for FIGI {figi} in the period from {datetimefrom} to {datetimeto}")

    return response.payload.candles[0]


def get_zsecurity_by_figi(figi):
    global broker_portfolio

    broker_security = get_portfolio_security_by_figi(figi)
    # If security couldn't be found in portfolio, search online
    if broker_security is None:
        broker_security = search_by_figi(figi)
        if broker_security.isin is None:
            print(f"ERROR: Couldn't find broker security by figi {figi}. Aborting.")
            return None

    return banktivity.get_zsecurity_by_symbol(broker_security.isin)


def prepare_account_operation_data(broker_operation_data, account_transaction_data):
    if broker_operation_data.operation_type == 'PayIn':
        account_transaction_data.update({
            'transaction_type': 'Deposit',
            'transaction_category_name': None,  # Used in ZLINEITEM. No category for Deposits
            'zpnote': f"[Переводы/иб] Пополнение счета Тинькофф Брокер ({str(broker_operation_data.payment)} {broker_operation_data.currency})",
        })
    elif broker_operation_data.operation_type == 'ServiceCommission':
        account_transaction_data.update({
            'transaction_category_name': "Банк:Оплата за услуги",
            'transaction_type': 'Withdrawal',
            'zpnote': f"{broker_operation_data.operation_type}"
        })

    else:
        print("ERROR: Unknown broker operation type " + broker_operation_data.operation_type + ". Aborting.")
        pprint.pprint(broker_operation_data)
        exit(1)

    account_transaction_data.update({
        'transaction_currency_code': broker_operation_data.currency,
        'zpadjustment': None,
        'zpchecknumber': 0,  # used in ZTRANSACTION
        'zpdate': broker_operation_data.date.isoformat(),  # used in ZTRANSACTION
        'zptitle': None,  # used in ZTRANSACTION
        'zptransactionamount': broker_operation_data.payment,
        # used in ZLINEITEM: this field is zero for security transactions
    })

    return True


# end of  prepare_account_operation_data()


def prepare_security_operation_data(broker_operation_data, security_transaction_data):
    # Resolve figi into ticker symbol
    # Tinkoff broker operations references securities by figi, but Banktivity uses ticker symbols
    zsecurity = get_zsecurity_by_figi(broker_operation_data.figi)

    # if security not found in Banktivity — add it
    if zsecurity is None:
        broker_security = get_broker_security_by_figi(broker_operation_data.figi)
        logging.debug(f"broker_security object:\n{broker_security}")
        if broker_security is None:
            print(
                f"ERROR: Couldn't find broker security by FIGI {broker_operation_data.figi}. Can't continue. Aborting.")
            exit(1)

        new_zsecurity_data = {
            'zptype': banktivity.get_zsecurity_zptype_by_name(broker_operation_data.instrument_type),
            'currency': broker_security.average_position_price.currency if hasattr(broker_security, 'average_position_price') else broker_security.currency,
            # will get converted into zpcurrency.z_pk by add_zsecurity()
            'zpdate': broker_operation_data.date.isoformat(),
            # important to pass full datetime w/ TZ as Banktivity stores unixepoch/UTC
            # not exactly right value to use, but Tinkoff broker does not seem to show original bond par value
            'zpparvalue': None,
            'zpname': f"{broker_security.name} ({broker_security.ticker})",
            'zpnote': f"{broker_security.name}: тикер {broker_security.ticker}, ISIN {broker_security.isin}, FIGI {broker_operation_data.figi}",
            'zpsymbol': broker_security.isin
        }
        if broker_operation_data.instrument_type == 'Bond':
            new_zsecurity_data.update({
                # 1000 Par Value seems to be a safe assumption for pretty much all the bonds
                # TODO: find out how it's possible to get Bond par value from Tinkoff broker
                'zpparvalue': 1000
            })
        banktivity.add_zsecurity(new_zsecurity_data)
        zsecurity = get_zsecurity_by_figi(broker_operation_data.figi)

    # commission_amount: used in ZSECURITYLINEITEM
    # zpchecknumber: used in ZTRANSACTION
    # zpdate: used in ZTRANSACTION & ZSECURITYPRICE. IMPORTANT to pass full
    #           datetime w/ TZ as Banktivity stores unixepoch/UTC
    # zptitle: used in ZTRANSACTION
    # zpintradaysortindex:
    #   Used in ZLINEITEM. Checking accounts could be wrongly showing
    #   overdraft just because expense transactions got imported before
    #   deposits. Deprioritise expense transaction LineItems.
    # zpsecurity: used in ZSECURITYLINEITEM, references ZSECURITY.Z_PK
    security_transaction_data.update({
        'transaction_currency_code': broker_operation_data.currency,
        'zpadjustment': None,
        'commission_amount': broker_operation_data.commission.value if broker_operation_data.commission else 0,
        'zpchecknumber': 0,
        'zpdate': broker_operation_data.date.isoformat(),
        'zptitle': None,
        'zpintradaysortindex': 2,
        'zpsecurity': zsecurity['Z_PK'],
    })

    if broker_operation_data.operation_type == 'Buy' or broker_operation_data.operation_type == 'BuyCard' or broker_operation_data.operation_type == 'Sell':
        banktivity_zpshares = broker_operation_data.quantity
        if broker_operation_data.operation_type == 'Buy' or broker_operation_data.operation_type == 'BuyCard':
            transaction_type = 'Buy'
        elif broker_operation_data.operation_type == 'Sell':
            transaction_type = 'Sell'
            banktivity_zpshares *= -1
        else:
            print(f"ERROR: Unsupported Tinkoff broker operation type {broker_operation_data.operation_type}. Aborting.")
            exit(1)

        # Tinkoff broker uses bond market prices in operations but Banktivity expects percentage of par value
        banktivity_zpamount = (-1 * broker_operation_data.price * banktivity_zpshares) + broker_operation_data.commission.value
        if broker_operation_data.instrument_type == 'Bond':
            security_transaction_data['zppricemultiplier'] = zsecurity['ZPPARVALUE']
            banktivity_price_per_share = broker_operation_data.price / zsecurity['ZPPARVALUE']
        else:
            banktivity_price_per_share = broker_operation_data.price

        security_transaction_data.update({
            'transaction_category_name': None,  # no category for Buy or Sell
            'transaction_type': transaction_type,
            'zpnote': f"{transaction_type} {broker_operation_data.quantity} of {broker_operation_data.instrument_type} {zsecurity['ZPNAME']} @ {broker_operation_data.price}",
            'zptransactionamount': 0,  # used in ZLINEITEM: this field is zero for security Buy/Sell transactions
            # used in ZSECURITYLINEITEM. Note that Tinkoff broker reports payment as the cost of the stock/bond transaction w/o commission. Banktivity records commission in ZPAMOUNT
            'zpamount': banktivity_zpamount,
            'zppricepershare': banktivity_price_per_share,  # used in ZSECURITYLINEITEM
            'zpshares': banktivity_zpshares
        })

        security_dayprices = get_market_candle_by_figi_and_day(broker_operation_data.figi, broker_operation_data.date)
        # Create or update ZSECURITYPRICE entry for the day of the transaction for transactions on the market
        zsecurity_par_value = zsecurity['ZPPARVALUE'] if zsecurity['ZPPARVALUE'] is not None else 1
        zsecurity_dayprices = {
            'zpdate': security_transaction_data['zpdate'],
            'zpsecurity_pk': zsecurity['Z_PK'],  # needed to look up ZSECURITYPRICEITEM.Z_PK
            'c': security_dayprices.c / zsecurity_par_value,
            'h': security_dayprices.h / zsecurity_par_value,
            'l': security_dayprices.l / zsecurity_par_value,
            'o': security_dayprices.o / zsecurity_par_value,
            'v': security_dayprices.v
        }
        banktivity.add_zsecurityprice(zsecurity_dayprices)

    elif broker_operation_data.operation_type == 'BrokerCommission':
        transaction_type = 'Interest Inc.'
        security_transaction_data.update({
            'transaction_category_name': "Банк:Оплата за услуги",
            'transaction_type': transaction_type,
            'zpnote': f"{broker_operation_data.operation_type} for {broker_operation_data.instrument_type} {zsecurity['ZPNAME']}",
            # used in ZLINEITEM. zptransactionamount is zero for Buy/Sell transactions
            'zptransactionamount': broker_operation_data.payment,
        })
    elif broker_operation_data.operation_type == 'TaxCoupon':
        transaction_type = 'Interest Inc.'
        security_transaction_data.update({
            'transaction_category_name': "Налоги",
            'transaction_type': transaction_type,
            'zpnote': f"{broker_operation_data.operation_type} on revenue for {broker_operation_data.instrument_type} {zsecurity['ZPNAME']}",
            # used in ZLINEITEM. zptransactionamount is zero for Buy/Sell transactions
            'zptransactionamount': broker_operation_data.payment,
            'zpincome': broker_operation_data.payment,
        })
    elif broker_operation_data.operation_type == 'Coupon':
        transaction_type = 'Interest Inc.'
        security_transaction_data.update({
            'transaction_category_name': "Инвестиции:Проценты",
            'transaction_type': transaction_type,
            'zpnote': f"{broker_operation_data.operation_type} on {broker_operation_data.instrument_type} {zsecurity['ZPNAME']}",
            # used in ZLINEITEM. zptransactionamount is zero for Buy/Sell transactions
            'zptransactionamount': broker_operation_data.payment,
            'zpincome': broker_operation_data.payment,
        })
    elif broker_operation_data.operation_type == 'Dividend':
        '''
        Note that the Banktivity Dividend is meant to be used to account
        cash received from the profit made on an investment. The way I understand
        their document, Dividend reflects income after a security had been sold
        and profit had been made. So, dividends on stocks should be filed as
        Investment Inc. in Banktivity
        '''
        transaction_type = 'Investment Inc.'
        security_transaction_data.update({
            'transaction_category_name': "Инвестиции:Дивиденды",
            'transaction_type': transaction_type,
            'zpnote': f"Profit on {broker_operation_data.instrument_type} {zsecurity['ZPNAME']}",
            'zptransactionamount': 0,  # used in ZLINEITEM: this field is zero for security transactions
            'zpincome': broker_operation_data.payment, # used in ZSECURITYLINEITEM
        })
    else:
        print("ERROR: Unknown broker operation type " + broker_operation_data.operation_type + ". Aborting.")
        pprint.pprint(broker_operation_data)
        return None

    return True
# end of  prepare_security_operation_data()

def import_operations(args):
    global broker_accounts, our_timezone
    broker_account_id = ""
    for item in broker_accounts:
        #pprint.pprint(item)
        broker_account_id = item.broker_account_id
        broker_account_type = item.broker_account_type
        print("Account type " + broker_account_type + " with ID " + broker_account_id)

        fmt = '%Y-%m-%d %H:%M:%S%z'
        print(f"Fetching operations for Tinkoff.Investments account ID {broker_account_id} for the period from {args.period_start.strftime(fmt)} to {args.period_end.strftime(fmt)}")
        response = client.operations.operations_get(
            _from=args.period_start.isoformat()
            , to=args.period_end.isoformat()
            , broker_account_id=broker_account_id
        )

        for op in response.payload.operations:
            """
            {'commission': None,
             'currency': 'USD',
             'date': datetime.datetime(2020, 5, 21, 17, 35, 58, tzinfo=tzoffset(None, 10800)),
             'figi': None,
             'id': '-1',
             'instrument_type': None,
             'is_margin_call': False,
             'operation_type': 'PayIn',
             'payment': 10000.0,
             'price': None,
             'quantity': None,
             'status': 'Done',
             'trades': None},
            {'commission': {'currency': 'USD', 'value': -2.59},
             'currency': 'USD',
             'date': datetime.datetime(2020, 5, 26, 12, 27, 23, 165715, tzinfo=tzoffset(None, 10800)),
             'figi': 'BBG000DWG505',
             'id': '130321407480',
             'instrument_type': 'Stock',
             'is_margin_call': False,
             'operation_type': 'Buy',
             'payment': -10394.86,
             'price': 179.221724,
             'quantity': 58,
             'status': 'Done',
             'trades': [{'date': datetime.datetime(2020, 5, 26, 12, 27, 23, 166155, tzinfo=tzoffset(None, 10800)),
                         'price': 179.53,
                         'quantity': 15,
                         'trade_id': '734866040'},
                        {'date': datetime.datetime(2020, 5, 26, 12, 27, 23, 165715, tzinfo=tzoffset(None, 10800)),
                         'price': 179.0,
                         'quantity': 2,
                         'trade_id': '734866020'},
                        {'date': datetime.datetime(2020, 5, 26, 12, 27, 23, 165715, tzinfo=tzoffset(None, 10800)),
                         'price': 178.99,
                         'quantity': 1,
                         'trade_id': '734866000'},
                        {'date': datetime.datetime(2020, 5, 26, 12, 27, 23, 165715, tzinfo=tzoffset(None, 10800)),
                         'price': 178.99,
                         'quantity': 2,
                         'trade_id': '734866010'},
                        {'date': datetime.datetime(2020, 5, 26, 12, 27, 23, 165715, tzinfo=tzoffset(None, 10800)),
                         'price': 179.13,
                         'quantity': 38,
                         'trade_id': '734866030'}]}
             """

            # We want to work with completed operations only
            if op.status == 'Done':
                pass
            elif op.status == 'Decline':
                continue
            else:
                print("NOTICE: Unsupported operation status " + op.status)
                pprint.pprint(op)
                return None

            # No need to add transactions for BrokerCommission as these are accounted in Buy/Sell transactions
            # TODO: check if there are non-buy/sell broker commission operations
            if op.operation_type == 'BrokerCommission':
                continue

            # Determine Banktivity target account name
            banktivity_target_account_name = ""
            if broker_account_type == 'Tinkoff':
                banktivity_target_account_name = account_type_to_names[broker_account_type] + ' ' + op.currency
            elif broker_account_type == 'TinkoffIis':
                banktivity_target_account_name = account_type_to_names[broker_account_type]
            else:
                print("ERROR: Unknown broker account type " + broker_account_type + ". Aborting.")
                return None
            banktivity_target_account_pk = banktivity.get_zaccount_pk(banktivity_target_account_name)

            # Set up generic data for Banktivity transaction
            banktivity_transaction_data = {
                'transaction_account_name': banktivity_target_account_name,
                'primaryaccount_zaccount_pk': banktivity_target_account_pk
            }

            logging.debug(f"Processing Tinkoff Investments OpenAPI operation:\n{pprint.pformat(op)}")

            # PayIn Tinkoff broker operation should be handled as a regular Banktivity transaction
            # ServiceCommission Tinkoff broker operations do not have references to security (via FIGI), so should be filed as Withdrawals
            if op.operation_type == 'PayIn' or op.operation_type == 'ServiceCommission':
                prepare_account_operation_data(op, banktivity_transaction_data)
            else:
                prepare_security_operation_data(op, banktivity_transaction_data)

            logging.debug(
                f"banktivity_transaction_data before add_transaction():\n{pprint.pformat(banktivity_transaction_data)}")

            if op.operation_type == 'PayIn' or op.operation_type == 'ServiceCommission':
                duplicate_found = banktivity.find_primaryaccount_transaction_duplicate(banktivity_transaction_data)
                if duplicate_found:
                    pass
                else:
                    print(
                        f"Adding broker {op.operation_type} operation to Banktivity account '{banktivity_target_account_name}' (Z_PK {str(banktivity_target_account_pk)}) as PrimaryAccount transaction {banktivity_transaction_data['transaction_type']}")
                    banktivity.add_transaction(banktivity_transaction_data)
            else:
                duplicate_found = banktivity.find_security_transaction_duplicate(banktivity_transaction_data)
                if duplicate_found:
                    pass
                else:
                    print(
                        f"Adding broker {op.operation_type} transaction to Banktivity account '{banktivity_target_account_name}' (Z_PK {str(banktivity_target_account_pk)}) as Security transaction {banktivity_transaction_data['transaction_type']}")
                    banktivity.add_security_transaction(banktivity_transaction_data)

            if duplicate_found:
                duplicate_notice_text = f"Possible duplicate found in Banktivity for broker operation id {op.id} dated {banktivity_transaction_data['zpdate']}, amount {banktivity_transaction_data['zptransactionamount']}, note {banktivity_transaction_data['zpnote']}. Skipping."
                print(duplicate_notice_text)
                duplicate_notice_detail = f"""{duplicate_notice_text}Details below:
Broker operation data:
{pprint.pformat(op)}
Banktivity transaction prepared data:
{pprint.pformat(banktivity_transaction_data)}
"""
                logging.warn(duplicate_notice_detail)
# End of import_operations()

if __name__ == "__main__":
    main()
