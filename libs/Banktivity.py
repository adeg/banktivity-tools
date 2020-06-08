#!/usr/bin/env python3
from datetime import datetime
from os.path import expanduser
import pprint
import sqlite3
import sys
import uuid


class Banktivity():
    con = None
    cur = None

    def __init__(self, banktivity_file):
        self.con = sqlite3.connect(expanduser(f"{banktivity_file}/StoreContent/core.sql"))
        self.con.row_factory = self.dict_factory
        #self.con.set_trace_callback(print)
        self.cur = self.con.cursor()

    def dict_factory(self, cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def update_z_max(self, table_name, record_name):
        cur = self.cur
        cur.execute(f"UPDATE Z_PRIMARYKEY SET Z_MAX = COALESCE((SELECT MAX(Z_PK)+1 FROM {table_name}), 0) WHERE Z_NAME = ?",
                    (record_name,))
        return cur.rowcount == 1

    def commit(self):
        updated_tables = {
            'ZACCOUNT': 'Account'
            , 'ZTRANSACTION': 'Transaction'
            , 'ZLINEITEM': 'LineItem'
            , 'ZSECURITY': 'Security'
            , 'ZSECURITYLINEITEM': 'SecurityLineItem'
            , 'ZSECURITYLOT': 'SecurityLot'
            , 'ZSECURITYPRICE': 'SecurityPrice'
            , 'ZSECURITYPRICEITEM': 'SecurityPriceItem'
        }
        for table_name, record_name in updated_tables.items():
            self.update_z_max(table_name, record_name)

        return self.con.commit()

    def get_zcurrency_pk(self, zpcode):
        cur = self.cur
        cur.execute("SELECT Z_PK FROM ZCURRENCY WHERE ZPCODE = ?", (zpcode,))
        res = cur.fetchone()
        if res is None:
            return None

        return res['Z_PK']

    def get_zaccount_pk(self, zaccount_name):
        cur = self.cur
        cur.execute("SELECT Z_PK FROM ZACCOUNT WHERE ZPFULLNAME = ?", (zaccount_name,))
        res = cur.fetchone()

        if res is None:
            return None

        return res['Z_PK']

    def get_zsecuritypriceitem_pk_by_zsecurity(self, zsecurity_pk: int) -> int:
        cur = self.cur
        cur.execute(
            "SELECT spi.Z_PK FROM ZSECURITYPRICEITEM spi JOIN ZSECURITY s ON (spi.ZPSECURITYID = s.ZPUNIQUEID) WHERE s.Z_PK = ?",
            (zsecurity_pk,))
        res = cur.fetchone()

        if res is None:
            return None

        return res['Z_PK']

    def get_zsecurity_zptype_by_name(self, name):
        security_types = {
            'Stock': 1,
            'Bond': 3,
            'Mutual Fund': 4,
            'Index': 7,
        }
        return security_types.get(name, None)

    def get_zptransactiontype_by_name(self, name):
        transaction_types = {
            'Buy': 5
        }
        return transaction_types.get(name, None)

    def add_zaccount(self, z_ent, account_data):
        SQL_ZACCOUNT_ADD = """
    INSERT INTO
        ZACCOUNT
    VALUES (
          :Z_PK
        , :Z_ENT
        , :Z_OPT
        , :ZPACCOUNTCLASS
        , :ZPDEBIT
        , :ZPHIDDEN
        , :ZPTAXABLE
        , :ZPPARENTACCOUNT
        , :Z1_PPARENTACCOUNT
        , :ZTYPE
        , :ZCURRENCY
        , :ZORGANIZATION
        , strftime('%s', :ZPCREATIONTIME)-978307200
        , strftime('%s', :ZPMODIFICATIONDATE)-978307200
        , :ZPINTERESTRATE
        , :ZPTHRESHOLDBALANCE
        , :ZPFULLNAME
        , :ZPNAME
        , :ZPNOTE
        , :ZPUNIQUEID
        , :ZPIMAGEID
        , :ZPTAXCODE
        , :ZPBANKACCOUNTNUMBER
        , :ZPBANKROUTINGNUMBER
        , :ZPCOLORDATA
    )
    """
        zcurrency = None
        if z_ent == 3:
            zcurrency = self.get_zcurrency_pk(account_data['currency_code'])

        self.cur.execute(SQL_ZACCOUNT_ADD, {
            "Z_PK": None,
            "Z_ENT": z_ent,  # entry type (see catalog in Z_PRIMARYKEY)
            "Z_OPT": 1,  # (looks like how many times the entry was edited?)
            "ZPACCOUNTCLASS": account_data['zpaccountclass'],
            "ZPDEBIT": None,  # seems to always be NULL
            "ZPHIDDEN": 0,  # don't hide any imported accounts by default
            "ZPTAXABLE": None,  # don't highlight any accounts as Taxable
            "ZPPARENTACCOUNT": account_data['zpparentaccount'] if 'zpparentaccount' in account_data else None,
            "Z1_PPARENTACCOUNT": 2 if 'zpparentaccount' in account_data else None,
            # looks like when ZPPARENTACCOUNT!=NULL, then this is always == 2
            "ZTYPE": None if z_ent == 2 else 0,  # this is always NULL for category accounts
            "ZCURRENCY": zcurrency,  # NULL for category accounts, otherwise references ZCURRENCY.Z_PK
            "ZORGANIZATION": None if z_ent == 2 else 1,
            # NULL for category  accounts, otherwise references ZORGANIZATION.Z_PK [TODO TODO TODO]
            "ZPCREATIONTIME": account_data['zpcreationtime'] if 'zpcreationtime' in account_data else 'now',
            "ZPMODIFICATIONDATE": account_data['zpcreationtime'] if 'zpcreationtime' in account_data else 'now',
            "ZPINTERESTRATE": None if z_ent == 2 else 0,  # this is always NULL for category accounts
            "ZPTHRESHOLDBALANCE": None if z_ent == 2 else 0,  # this is always NULL for category accounts
            "ZPFULLNAME": account_data['zpfullname'],
            "ZPNAME": account_data['zpname'] if 'zpname' in account_data else account_data['zpfullname'],
            "ZPNOTE": account_data['zpnote'] if 'zpnote' in account_data else None,
            "ZPUNIQUEID": account_data['zpuniqueid'] if 'zpuniqueid' in account_data else str(uuid.uuid4()),
            "ZPIMAGEID": 'category-misc-expenses',
            "ZPTAXCODE": None,
            "ZPBANKACCOUNTNUMBER": account_data[
                'zpbankaccountnumber'] if 'zpbankaccountnumber' in account_data else None,
            "ZPBANKROUTINGNUMBER": None,
            "ZPCOLORDATA": None})

        zaccount_pk = self.cur.lastrowid
        return zaccount_pk

    def add_account(self, account_data):
        account_name = account_data['zpfullname']
        account_pk = self.get_zaccount_pk(account_name)
        if account_pk is not None:
            print("Account " + account_name + " already exists, skipping.")
            return account_pk

        return self.add_zaccount(3, account_data)  # 3 for Account

    def add_category(self, category_data):
        category_name = category_data['zpfullname']
        category_pk = self.get_zaccount_pk(category_name)
        if category_pk is not None:
            print("Category " + category_name + " already exists, skipping.")
            return category_pk

        if ':' in category_name:
            parent_category, subcategory = category_name.split(':')
            category_data['zpparentaccount'] = self.get_zaccount_pk(parent_category)
            category_data['zpname'] = subcategory

        return self.add_zaccount(2, category_data)  # 2 for Category

    def add_ztransaction(self, transaction_data):
        cur = self.cur

        SQL_ZTRANSACTION = """
        INSERT INTO
            ZTRANSACTION
        VALUES (
              :Z_PK
            , (SELECT Z_ENT FROM Z_PRIMARYKEY WHERE Z_NAME = :Z_ENT)
            , :Z_OPT
            , :ZPADJUSTMENT
            , :ZPCHECKNUMBER
            , :ZPCLEARED
            , :ZPVOID
            , :ZPCURRENCY
            , :ZPFILEATTACHMENT
            , (SELECT Z_PK FROM ZTRANSACTIONTYPE WHERE ZPNAME = :ZPTRANSACTIONTYPE_ZPNAME)
            , strftime('%s', :ZPCREATIONTIME)-978307200
            , strftime('%s', :ZPDATE)-978307200
            , strftime('%s', :ZPMODIFICATIONDATE)-978307200
            , :ZPNOTE
            , :ZPTITLE
            , :ZPUNIQUEID
        )
        """

        zpcurrency = self.get_zcurrency_pk(transaction_data['transaction_currency_code'])

        cur.execute(SQL_ZTRANSACTION, {
            "Z_PK": None,
            "Z_ENT": 'Transaction',  # entry type (see catalog in Z_PRIMARYKEY)
            "Z_OPT": 1,  # (looks like how many times the entry was edited?)
            "ZPADJUSTMENT": transaction_data['zpadjustment'],  # 1 for Balance Adjustment entries, otherwise NULL
            "ZPCHECKNUMBER": transaction_data['zpchecknumber'],
            "ZPCLEARED": 0,
            "ZPVOID": 0,  # seems to be always zero
            "ZPCURRENCY": zpcurrency,
            "ZPFILEATTACHMENT": None,
            "ZPTRANSACTIONTYPE_ZPNAME": transaction_data['transaction_type'],
            "ZPCREATIONTIME": transaction_data['zpcreationtime'] if 'zpcreationtime' in transaction_data else 'now',
            "ZPDATE": transaction_data['zpdate'],
            "ZPMODIFICATIONDATE": transaction_data['zpmodificationdate'] if 'zpmodificationdate' in transaction_data else 'now',
            "ZPNOTE": transaction_data['zpnote'],
            "ZPTITLE": transaction_data['zptitle'],
            "ZPUNIQUEID": transaction_data['zpuniqueid'] if 'zpuniqueid' in transaction_data else str(
                uuid.uuid5(uuid.NAMESPACE_DNS,
                           "ztransaction_" + transaction_data['zpdate'] + transaction_data['zpnote']))})

        ztransaction_pk = self.cur.lastrowid
        return ztransaction_pk

    def add_transaction(self, transaction_data):
        cur = self.cur

        ztransaction_pk = self.add_ztransaction(transaction_data)

        SQL_ZLINEITEM = """
        INSERT INTO
            ZLINEITEM
        VALUES (
              :Z_PK
            , (SELECT Z_ENT FROM Z_PRIMARYKEY WHERE Z_NAME = :Z_ENT)
            , :Z_OPT
            , :ZPCLEARED
            , :ZPINTRADAYSORTINDEX
            , :ZPACCOUNT
            , (SELECT Z_ENT FROM Z_PRIMARYKEY WHERE Z_NAME = :Z1_PACCOUNT)
            , :ZPSECURITYLINEITEM
            , :ZPSTATEMENT
            , :ZPTRANSACTION
            , strftime('%s', :ZPCREATIONTIME)-978307200
            , :ZPEXCHANGERATE
            , :ZPRUNNINGBALANCE
            , :ZPTRANSACTIONAMOUNT
            , :ZPMEMO
            , :ZPUNIQUEID
        )
        """

        cur.execute("SELECT Z_ENT FROM Z_PRIMARYKEY WHERE Z_NAME = ?", ('LineItemSource',))
        z_primarykey_lineitemsource = cur.fetchone()['Z_ENT']

        z_lineitem_zpaccount = self.get_zaccount_pk(transaction_data['transaction_account_name'])
        if 'transaction_dest_account_name' in transaction_data:
            z_lineitem_zpaccount_dest = self.get_zaccount_pk(transaction_data['transaction_dest_account_name'])

        # ZPACCOUNT for category ZLINEITEM should be NULL for security Buy/Sell transactions
        if transaction_data['transaction_category_name'] is None:
            z_lineitem_cat_zpaccount = None
        else:
            z_lineitem_cat_zpaccount = self.get_zaccount_pk(transaction_data['transaction_category_name'])

        SQL_ZLINEITEM1_VALUES = {
            "Z_PK": None,
            "Z_ENT": 'LineItem',  # resolved into integer in the query
            "Z_OPT": 1,  # (looks like how many times the entry was edited?)
            "ZPCLEARED": 1,  # for 1 entry it's 1, for memo entry it's NULL
            "ZPINTRADAYSORTINDEX": transaction_data[
                'zpintradaysortindex'] if 'zpintradaysortindex' in transaction_data else 0,
            # default is 0 (the earliest), set by GUI
            "ZPACCOUNT": z_lineitem_zpaccount,
            "Z1_PACCOUNT": 'PrimaryAccount',  # resolved into integer in the query
            "ZPSECURITYLINEITEM": None,
            # references ZSECURITYLINEITEM.Z_PK for transactions in investment accounts. NULL for regular transactions.
            "ZPSTATEMENT": None,
            "ZPTRANSACTION": ztransaction_pk,  # Transaction Z_PK
            "ZPCREATIONTIME": transaction_data['zpcreationtime'] if 'zpcreationtime' in transaction_data else 'now',
            "ZPEXCHANGERATE": 1,  # this is always '1' for all non-transfer or transfer-source LineItem entries
            "ZPRUNNINGBALANCE": None,  # looks like this is auto-generated by the GUI
            "ZPTRANSACTIONAMOUNT": transaction_data['zptransactionamount'],
            "ZPMEMO": None,
            "ZPUNIQUEID": str(uuid.uuid4())
        }

        # [1/2] add LineItem in source account
        cur.execute(SQL_ZLINEITEM, SQL_ZLINEITEM1_VALUES)

        if transaction_data['transaction_type'] == 'Transfer':
            # [2/2B] add LineItem in destination account
            cur.execute(SQL_ZLINEITEM, {
                "Z_PK": None,
                "Z_ENT": 'LineItem',
                "Z_OPT": 1,  # (looks like how many times the entry was edited?)
                "ZPCLEARED": 1,  # for 1 entry it's 1, for memo entry it's NULL
                "ZPINTRADAYSORTINDEX": transaction_data[
                    'zpintradaysortindex_dest'] if 'zpintradaysortindex_dest' in transaction_data else 0,
                # default is 0 (the earliest), set by GUI
                "ZPACCOUNT": z_lineitem_zpaccount_dest,
                "Z1_PACCOUNT": 'PrimaryAccount',  # resolved into integer in the query
                "ZPSECURITYLINEITEM": None,
                # references ZSECURITYLINEITEM.Z_PK for transactions in investment accounts. NULL for regular transactions.
                "ZPSTATEMENT": None,
                "ZPTRANSACTION": ztransaction_pk,  # Transaction Z_PK
                "ZPCREATIONTIME": transaction_data['zpcreationtime'] if 'zpcreationtime' in transaction_data else 'now',
                "ZPEXCHANGERATE": transaction_data['zpexchangerate_dest'],
                "ZPRUNNINGBALANCE": None,  # looks like this is auto-generated by the GUI
                "ZPTRANSACTIONAMOUNT": transaction_data['zptransactionamount_dest'],
                "ZPMEMO": None,
                "ZPUNIQUEID": str(uuid.uuid4())})
        else:
            # [2/2C] add the category expense for
            cur.execute(SQL_ZLINEITEM, {
                "Z_PK": None,
                "Z_ENT": 'LineItem',
                "Z_OPT": 1,  # (looks like how many times the entry was edited?)
                "ZPCLEARED": None,  # for 1 entry it's 1, for memo entry it's NULL
                "ZPINTRADAYSORTINDEX": 0,  # always 0 unless (I guess) modified by GUI
                "ZPACCOUNT": z_lineitem_cat_zpaccount,
                "Z1_PACCOUNT": 'Category' if z_lineitem_cat_zpaccount else None,  # resolved into integer in the query
                "ZPSECURITYLINEITEM": None,
                # references ZSECURITYLINEITEM.Z_PK for transactions in investment accounts. NULL for regular transactions.
                "ZPSTATEMENT": None,
                "ZPTRANSACTION": ztransaction_pk,  # Transaction Z_PK
                "ZPCREATIONTIME": transaction_data['zpcreationtime'] if 'zpcreationtime' in transaction_data else 'now',
                "ZPEXCHANGERATE": 1,
                "ZPRUNNINGBALANCE": None,  # looks like this is auto-generated by the GUI
                "ZPTRANSACTIONAMOUNT": -1 * transaction_data['zptransactionamount'],
                # category LineItem's amount is always the opposite of the transaction LineItem's amount
                "ZPMEMO": None,
                "ZPUNIQUEID": str(uuid.uuid4())})
        # [-] for

        return ztransaction_pk

    # end add_transaction()

    def add_zsecurity(self, security_data):
        cur = self.cur

        SQL_ZSECURITY = """
        INSERT INTO
            ZSECURITY
        VALUES (
              :Z_PK
            , (SELECT Z_ENT FROM Z_PRIMARYKEY WHERE Z_NAME = :Z_ENT)
            , :Z_OPT
            , :ZPEXCLUDEFROMQUOTEUPDATES
            , :ZPISINDEX
            , :ZPRISKTYPE
            , :ZPTRADESINPENCE
            , :ZPTYPE
            , :ZPCURRENCY
            , strftime('%s', :ZPCREATIONTIME)-978307200
            , strftime('%s', :ZPMODIFICATIONDATE)-978307200
            , :ZPCONTRACTSIZE
            , :ZPPARVALUE
            , :ZPCUSIP
            , :ZPNAME
            , :ZPNOTE
            , :ZPSYMBOL
            , :ZPUNIQUEID
        )
        """

        cur.execute(SQL_ZSECURITY, {
            "Z_PK": None,
            "Z_ENT": 'Security',  # entry type (see catalog in Z_PRIMARYKEY)
            "Z_OPT": 1,  # (looks like how many times the entry was edited?)
            "ZPEXCLUDEFROMQUOTEUPDATES": None,
            "ZPISINDEX": 0,  # don't know what this for, seems to always be 0
            "ZPRISKTYPE": 2,  # Looks like values for this are hardcoded (2=Growth)
            "ZPTRADESINPENCE": None,  # stocks sometimes traded in pence rather than pounds.
            "ZPTYPE": security_data['zptype'] if 'zptype' in security_data else self.get_zsecurity_zptype_by_name(
                security_data['type']),
            # Looks like values for this are hardcoded (1=Stock, 3=Bond, 4=Mutual Fund,7=Index)
            "ZPCURRENCY": security_data['zpcurrency'] if 'zpcurrency' in security_data else self.get_zcurrency_pk(
                security_data['currency']),
            "ZPCREATIONTIME": security_data['zpcreationtime'] if 'zpcreationtime' in security_data else 'now',
            "ZPMODIFICATIONDATE": security_data['zpmodificationdate'] if 'zpmodificationdate' in security_data else 'now',
            "ZPCONTRACTSIZE": 100,  # Seems to always be 100
            "ZPPARVALUE": security_data['zpparvalue'] if 'zpparvalue' in security_data else 1000,
            # Seems to be 1000 by default when not applicable (i.e. for stocks)
            "ZPCUSIP": None,  # Seems to always be NULL
            "ZPNAME": security_data['zpname'],
            "ZPNOTE": security_data['zpnote'] if 'zpnote' in security_data else None,
            "ZPSYMBOL": security_data['zpsymbol'],
            "ZPUNIQUEID": str(
                uuid.uuid5(uuid.NAMESPACE_DNS, "ztransaction_" + security_data['zpdate'] + security_data['zpsymbol']))
        })
        ztransaction_pk = self.cur.lastrowid

        zsecurity = self.get_zsecurity_by_symbol(security_data['zpsymbol'])

        # Add ZSECURITYPRICEITEM. Gets created for every security in Banktivity the moment the price for it
        # learned the first time. Did not research how this is used, but probably by the quote fetch mechanism
        # in the GUI. Create this right after adding the security to save the trouble later.
        SQL_ZSECURITYPRICEITEM = """
        INSERT INTO
            ZSECURITYPRICEITEM
        VALUES (
              :Z_PK
            , (SELECT Z_ENT FROM Z_PRIMARYKEY WHERE Z_NAME = :Z_ENT)
            , :Z_OPT=1
            , :ZPKNOWNDATERANGEBEGIN
            , :ZPKNOWNDATERANGEEND
            , :ZPLATESTIMPORTDATE
            , :ZPSECURITYID
        )
        """

        cur.execute(SQL_ZSECURITYPRICEITEM, {
            "Z_PK": None,
            "Z_ENT": 'SecurityPriceItem',
            "Z_OPT": 1,
            "ZPKNOWNDATERANGEBEGIN": None,
            "ZPKNOWNDATERANGEEND": None,
            "ZPLATESTIMPORTDATE": None,
            "ZPSECURITYID": zsecurity['ZPUNIQUEID']  # reference to ZSECURITY.ZPUNIQUEID
        })

        return ztransaction_pk
    # end add_zsecurity()

    def add_zsecurityprice(self, data):
        cur = self.cur

        zpsecuritypriceitem_pk = self.get_zsecuritypriceitem_pk_by_zsecurity(data['zpsecurity_pk'])

        # Check if ZSECURITYPRICE for the specified date already exists.
        cur.execute("SELECT * FROM ZSECURITYPRICE WHERE ZPSECURITYPRICEITEM = ? AND ZPDATE = strftime('%s', ?)/(60*60*24)", (zpsecuritypriceitem_pk, data['zpdate']))
        zsecurityprices = cur.fetchall()
        rowcount_zsecurityprice = len(zsecurityprices)
        if rowcount_zsecurityprice == 0:
            # ZSECURITYPRICE.ZPDATE is an exception to ZPDATEs in other tables like ZTRANSACTION, ZLINEITEM, ZSECURITY.
            # Values stored in ZPDATE in ZSECURITYPRICE are unixepoch/(60*60*24).
            SQL_ZSECURITYPRICE = """
            INSERT INTO
                ZSECURITYPRICE
            VALUES (
                  :Z_PK
                , (SELECT Z_ENT FROM Z_PRIMARYKEY WHERE Z_NAME = :Z_ENT)
                , :Z_OPT
                , :ZPDATASOURCE
                , strftime('%s', :ZPDATE)/(60*60*24)
                , :ZPSECURITYPRICEITEM
                , :ZPADJUSTEDCLOSEPRICE
                , :ZPCLOSEPRICE
                , :ZPHIGHPRICE
                , :ZPLOWPRICE
                , :ZPOPENPRICE
                , :ZPPREVIOUSCLOSEPRICE
                , :ZPVOLUME
            )
            """

            cur.execute(SQL_ZSECURITYPRICE, {
                "Z_PK": None,
                "Z_ENT": 'SecurityPrice',  # entry type (see catalog in Z_PRIMARYKEY)
                "Z_OPT": 1,  # (looks like how many times the entry was edited?)
                # Not sure about this one, observed 0s and 3s, so default at 0
                "ZPDATASOURCE": 0,
                # Here we expect string datetime, Banktivity expects unixepoch/(60*60*24). The "adapter" is in SQL.
                "ZPDATE": data['zpdate'],
                # ZSECURITYPRICE.ZPSECURITYPRICEITEM is the reference to ZSECURITYPRICEITEM.Z_PK
                "ZPSECURITYPRICEITEM": zpsecuritypriceitem_pk,
                "ZPADJUSTEDCLOSEPRICE": 0,
                # ZPCLOSEPRICE matters the most as it affects the portfolio value. This is what changes when the security price is updated in the Portfolio section
                "ZPCLOSEPRICE": data['c'],
                "ZPHIGHPRICE": data['h'],
                "ZPLOWPRICE": data['l'],
                "ZPOPENPRICE": data['o'],
                "ZPPREVIOUSCLOSEPRICE": 0,
                "ZPVOLUME": data['v']
            })
            pk = self.cur.lastrowid
        # end of if cur.rowcount == 0:
        elif rowcount_zsecurityprice == 1:
            pk = zsecurityprices[0]['Z_PK']
            SQL_ZSECURITYPRICE = """
            UPDATE
                ZSECURITYPRICE
            SET
                  ZPADJUSTEDCLOSEPRICE = ?
                , ZPCLOSEPRICE = ?
                , ZPHIGHPRICE = ?
                , ZPLOWPRICE = ?
                , ZPOPENPRICE = ?
                , ZPPREVIOUSCLOSEPRICE = ?
                , ZPVOLUME = ?
            WHERE
                Z_PK = ?
            """
            cur.execute(SQL_ZSECURITYPRICE, (0, data['c'], data['h'], data['l'], data['o'], 0, data['v'], pk))
            rows_affected = cur.rowcount
            if rows_affected != 1:
                print(f"UNEXPECTED ERROR: UPDATE ZSECURITYPRICE affected {rows_affected} rows instead of 1. Aborting.")
                exit(1)
        # end of update existing ZSECURITYPRICE
        else:
            print(f"ERROR: Found {rowcount_zsecurityprice} ZSECURITYPRICE records for ZPSECURITY_PK {data['zpsecurity_pk']} for date {data['zpdate']}. Expected either 1 or 0. Aborting.")
            exit(1)

        return pk
    # end add_zsecurityprice()

    def get_zsecurity_by_symbol(self, zpsymbol):
        cur = self.cur
        cur.execute(
            "SELECT * FROM ZSECURITY WHERE ZPSYMBOL = ?",
            (zpsymbol,)
        )
        return cur.fetchone()

    def find_primaryaccount_transaction_duplicate(self, transaction_data):
        """Find the PrimaryAccount transaction matching supplied data by some criteria.

        Caveats:
        Note that the current algo is a bit unsafe as it fails in situations when multiple transactions are
        present in the same date and with the same amount. For example, if there are 3 deposits in the same
        day for 1000 ₽, only the first one will be added.

        For reference — Banktivity transaction prepared data:
        {'primaryaccount_zaccount_pk': 248,
         'transaction_account_name': 'Тинькофф - Брокер USD',
         'transaction_category_name': None,
         'transaction_currency_code': 'USD',
         'transaction_type': 'Deposit',
         'zpadjustment': None,
         'zpchecknumber': 0,
         'zpdate': '2020-05-21',
         'zpnote': '[Переводы/иб] Пополнение счета Тинькофф Брокер (10000.0 USD)',
         'zptitle': None,
         'zptransactionamount': 10000.0}
        """
        __method__ = "find_primaryaccount_transaction_duplicate()"
        cur = self.cur

        SQL_QUERY = """
        SELECT
            *
        FROM
            ZLINEITEM li
        JOIN
            ZTRANSACTION t
        ON (li.ZPTRANSACTION = t.Z_PK)
        WHERE
                ZPACCOUNT = :ZACCOUNT_PK
            AND DATE(978307200+ZPDATE, 'unixepoch', 'localtime') = strftime('%Y-%m-%d', :ZPDATE)
            AND ZPTRANSACTIONAMOUNT  = :ZPTRANSACTIONAMOUNT
        """
        SQL_VALUES = {
            "ZACCOUNT_PK": transaction_data['primaryaccount_zaccount_pk'],
            "ZPDATE": transaction_data['zpdate'],
            "ZPTRANSACTIONAMOUNT": transaction_data['zptransactionamount']
        }
#        print(f"{__method__} [debug] prepared SQL query: {SQL_QUERY}")
#        pprint.pprint(SQL_VALUES)
        cur.execute(SQL_QUERY, SQL_VALUES)
        existing_data = cur.fetchall()
        rowcount = len(existing_data)
        if rowcount == 0:
            return False
        elif rowcount == 1:
#            print(f"NOTICE: Existing Banktivity transaction found matching key parameters.")
#            pprint.pprint(existing_data[0])
#            print()
            return True
        else:
            print(
                f"ERROR: Found {rowcount} ZSECURITYPRICE+ZLINEITEM+ZTRANSACTION joined records for data specified. Expected either 1 or 0. Aborting.")
            print("Input transaction data:")
            pprint.pprint(transaction_data)
            print("Data found in Banktivity DB:")
            pprint.pprint(existing_data)
            exit(1)

    def find_security_transaction_duplicate(self, transaction_data):
        """Find the security transaction matching supplied data by some criteria.

        For reference — Banktivity transaction prepared data:
            transaction_data = {'zpamount': -49663.68,
             'commission_amount': -146.42,
             'primaryaccount_zaccount_pk': 244,
             'transaction_account_name': 'Тинькофф - ИИС',
             'transaction_category_name': None,
             'transaction_currency_code': 'RUB',
             'transaction_type': 'Buy',
             'zpadjustment': None,
             'zpchecknumber': 0,
             'zpdate': '2020-03-06',
             'zpnote': 'Buy 48 of Bond ПИК-Корпорация выпуск 2 (RU000A1016Z3) @ 1016.8',
             'zppricepershare': 1016.8,
             'zpsecurity': 23,
             'zpshares': 48,
             'zptitle': None,
             'zptransactionamount': 0}
        """
        __method__ = "find_security_transaction_duplicate()"
        cur = self.cur

        SQL_QUERY = """
        SELECT
            *
        FROM
            ZSECURITYLINEITEM sli
        JOIN
            ZLINEITEM li
        ON (sli.ZPLINEITEM = li.Z_PK)
        JOIN
            ZTRANSACTION t
        ON (li.ZPTRANSACTION = t.Z_PK)
        WHERE
                ZPSECURITY = :ZSECURITY_PK
            AND ZPACCOUNT = :ZACCOUNT_PK
            AND DATE(978307200+ZPDATE, 'unixepoch', 'localtime') = strftime('%Y-%m-%d', :ZPDATE)
            AND ZPAMOUNT = :ZPAMOUNT
            AND ZPCOMMISSION = :ZPCOMMISSION
            AND ZPINCOME = :ZPINCOME
            AND ZPPRICEPERSHARE = :ZPPRICEPERSHARE
            AND ZPSHARES = :ZPSHARES
        """
        SQL_VALUES = {
            "ZSECURITY_PK": transaction_data['zpsecurity'],
            "ZACCOUNT_PK": transaction_data['primaryaccount_zaccount_pk'],
            "ZPDATE": transaction_data['zpdate']
        }
        if transaction_data['transaction_type'] == 'Buy' or transaction_data['transaction_type'] == 'Sell':
            SQL_VALUES.update({
                # negative for Buy, positive for Sell, else NULL. Also: Banktivity accounts share buy/sell amount PLUS commission amount in ZPAMOUNT
                # WARNING: the formula below depends on the correct sign (+/-) given in transaction_data['zpshares']!!!
                "ZPAMOUNT": transaction_data['zpamount'],
                "ZPCOMMISSION": transaction_data['commission_amount'],  # non-null negative or NULL
                "ZPINCOME": 0,
                "ZPPRICEPERSHARE": transaction_data['zppricepershare'],  # NULL for dividends
                "ZPSHARES": transaction_data['zpshares'],  # quantity of notes purchased/sold
            })
        elif transaction_data['transaction_type'] == 'Investment Inc.' or transaction_data[
            'transaction_type'] == 'Interest Inc.' or transaction_data['transaction_type'] == 'Dividend':
                SQL_QUERY = SQL_QUERY.replace('= :ZPAMOUNT', 'IS NULL')
                SQL_QUERY = SQL_QUERY.replace('= :ZPCOMMISSION', 'IS NULL')
                SQL_QUERY = SQL_QUERY.replace('= :ZPPRICEPERSHARE', 'IS NULL')
                SQL_QUERY = SQL_QUERY.replace('= :ZPSHARES', 'IS NULL')
                SQL_VALUES.update({
                    "ZPINCOME": transaction_data['zpincome']  # positive for dividends etc. or 0
                })
        else:
            print(f"ERROR: Unsupported transaction_type ({transaction_data['transaction_type']}). Abort.")
            exit(1)

#        print(f"{__method__} [debug] prepared SQL query: {SQL_QUERY}")
#        pprint.pprint(SQL_VALUES)
        cur.execute(SQL_QUERY, SQL_VALUES)
        existing_data = cur.fetchall()
        rowcount = len(existing_data)
        if rowcount == 0:
            return False
        elif rowcount == 1:
#            print(f"NOTICE: Existing Banktivity transaction found matching key parameters.")
#            pprint.pprint(existing_data[0])
#            print()
            return True
        else:
            print(f"ERROR: Found {rowcount} ZSECURITYPRICE+ZLINEITEM+ZTRANSACTION joined records for data specified. Expected either 1 or 0. Aborting.")
            print("Input transaction data:")
            pprint.pprint(transaction_data)
            print("Data found in Banktivity DB:")
            pprint.pprint(existing_data)
            exit(1)

    def add_security_transaction(self, transaction_data):
        """The do-it-all method for adding security-related transactions.

        All data required for add_transaction() should be provided in transaction_data along with data
        used by this method.
        o Adds ZTRANSACTION
        o Add ZLINEITEM for the account
        o Add ZLINEITEM for the category (even if it's not specified)
        o Add ZSECURITYLINEITEM with some data of the whole security transaction. References ZLINEITEM.Z_PK.
        """
        cur = self.cur
        __method__ = "add_security_transaction()"

        # [1/4] This adds ZTRANSACTION and the corresponding ZLINEITEM entries for account and category
        ztransaction_pk = self.add_transaction(transaction_data)

        # [2/4] Add ZSECURITYLINEITEM
        # Get Z_PK of PrimaryAccount ZLINEITEM
        cur.execute(
            "SELECT Z_PK FROM ZLINEITEM WHERE ZPTRANSACTION = ? AND ZPACCOUNT = ?",
            (ztransaction_pk, self.get_zaccount_pk(transaction_data['transaction_account_name']))
        )
        primaryaccount_zlineitem_z_pk = cur.fetchone()['Z_PK']
        # Get ZPUNIQUEID of Category ZLINEITEM
        categoryaccount_zlineitem_zpuniqueid = None
        if transaction_data['transaction_type'] == 'Investment Inc.' or transaction_data[
            'transaction_type'] == 'Interest Inc.' or transaction_data['transaction_type'] == 'Dividend':
            cur.execute(
                "SELECT ZPUNIQUEID FROM ZLINEITEM WHERE ZPTRANSACTION = ? AND Z_PK != ?",
                (ztransaction_pk, primaryaccount_zlineitem_z_pk)
            )
            categoryaccount_zlineitem_zpuniqueid = cur.fetchone()['ZPUNIQUEID']

        SQL_ZSECURITYLINEITEM = """
        INSERT INTO
            ZSECURITYLINEITEM
        VALUES (
              :Z_PK
            , (SELECT Z_ENT FROM Z_PRIMARYKEY WHERE Z_NAME = :Z_ENT)
            , :Z_OPT
            , :ZPCOSTBASISMETHOD
            , :ZPDISTRIBUTIONTYPE
            , :ZPLINEITEM
            , :ZPSECURITY
            , :ZPAMOUNT
            , :ZPCOMMISSION
            , :ZPINCOME
            , :ZPPRICEMULTIPLIER
            , :ZPPRICEPERSHARE
            , :ZPSHARES
            , :ZPINCOMECATEGORYLINEITEMID
        )
        """

        SQL_VALUES = {
            "Z_PK": None,
            "Z_ENT": 'SecurityLineItem',  # entry type (see catalog in Z_PRIMARYKEY)
            "Z_OPT": 1,  # (looks like how many times the entry was edited?)
            "ZPCOSTBASISMETHOD": None,  # used for Sell transactions
            "ZPDISTRIBUTIONTYPE": 1,  # Seems to always be 1
            "ZPLINEITEM": primaryaccount_zlineitem_z_pk,  # points to the PrimaryAccount ZLINEITEM.Z_PK
            "ZPSECURITY": transaction_data['zpsecurity'],  # points to ZPSECURITY.Z_PK
            "ZPPRICEMULTIPLIER": transaction_data['zppricemultiplier'] if 'zppricemultiplier' in transaction_data else 1,  # used for Bonds
            "ZPINCOMECATEGORYLINEITEMID": categoryaccount_zlineitem_zpuniqueid
            # Usually NULL. For Investment Inc., Interest Inc., Dividend transactions points to ZLINEITEM.ZPUNIQUEID of ZLINEITEM of the category (not transaction!)
        }

        if transaction_data['transaction_type'] == 'Investment Inc.' or transaction_data[
            'transaction_type'] == 'Interest Inc.' or transaction_data['transaction_type'] == 'Dividend':
            SQL_VALUES.update({
                "ZPAMOUNT": None,  # negative for Buy, positive for Sell, else NULL.
                "ZPCOMMISSION": None,
                "ZPINCOME": transaction_data['zpincome'],  # positive for dividends etc. or 0
                "ZPPRICEPERSHARE": None,  # NULL for dividends
                "ZPSHARES": None,  # quantity of notes purchased/sold. NULL for dividends
            })
        else:
            if transaction_data['transaction_type'] == 'Sell':
                SQL_VALUES['ZPCOSTBASISMETHOD'] = 1 # 1 = First In First Out, 2 = Last In First Out
                if transaction_data['zpshares'] >= 0:
                    print(f"{__method__} [error] quantitative securities change to security account was positive while Banktivity expects negative. Aborting as calculations will likely come up wrong if financial data is supplied with incorrect signs!")
                    exit(1)
            SQL_VALUES.update({
                # negative for Buy, positive for Sell, else NULL. Also: Banktivity accounts share buy/sell amount PLUS commission amount in ZPAMOUNT
                # WARNING: the formula below depends on the correct sign (+/-) given in transaction_data['zpshares']!!!
                "ZPAMOUNT": transaction_data['zpamount'] if 'zpamount' in transaction_data else -1 * (transaction_data['zppricepershare'] * transaction_data['zpshares']) + transaction_data['commission_amount'],
                "ZPCOMMISSION": transaction_data['commission_amount'],  # negative value if commission was paid or NULL
                "ZPINCOME": 0,  # positive for dividends etc. or 0
                "ZPPRICEPERSHARE": transaction_data['zppricepershare'],  # NULL for dividends
                "ZPSHARES": transaction_data['zpshares'],  # quantity of notes purchased/sold. Must be negative for Sell transactions
            })

        cur.execute(SQL_ZSECURITYLINEITEM, SQL_VALUES)
        zsecuritylineitem_z_pk = self.cur.lastrowid
        # End of [2/4] Add ZSECURITYLINEITEM

        # [3/4] Update PrimaryAccount ZLINEITEM.ZPSECURITYLINEITEM to reference to Z_PK of ZSECURITYITEM just inserted
        cur.execute(
            "UPDATE ZLINEITEM SET ZPSECURITYLINEITEM = ? WHERE Z_PK = ?",
            (zsecuritylineitem_z_pk, primaryaccount_zlineitem_z_pk)
        )
    # end add_security_transaction()
# end class Banktivity()
