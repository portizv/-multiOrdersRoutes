import unittest
import pandas as pd
from utils import group_orders, BigQueryManager, get_OMS_query, norm_address, from_ordinal
from configs import PATH_TESTS_INPUTS, IDX_COL_IN, PATH_MAIN, DATE_COL
import json

f = open(PATH_MAIN / 'tc-sc-bi-bigdata-corp-tsod-dev-d72f9644a685.json')
cred_json = json.load(f)


class utils(unittest.TestCase):

    def test_from_ordinal(self):
        test = pd.read_excel(PATH_TESTS_INPUTS / "testRegions01.xlsx")
        res = test[DATE_COL].apply(lambda x: from_ordinal(x).date())
        self.assertEqual(str(res.max()), "2022-05-28")
        self.assertEqual(str(res.min()), "2022-05-09")

    def test_group_orders(self):
        df_orders = pd.read_excel(PATH_TESTS_INPUTS / "testRegions01.xlsx")
        gb_orders = group_orders(df_orders=df_orders, idx_col=IDX_COL_IN, cred_json=cred_json)
        self.assertEqual(len(df_orders), len(gb_orders))
        self.assertEqual(len(gb_orders[gb_orders["is_multi"] > 0]) > 1, True)

    def test_BigQueryManager(self):
        qry = """SELECT
                  PURCHASE_ORDERS_ID
                FROM
                  `tc-sc-bi-bigdata-corp-tsod-dev.sandbox_building_type_recognition.oms_raw_data`
                WHERE
                  DATE(F_CREACION) = "2022-05-01"
                LIMIT
                  10"""
        bqm = BigQueryManager(cred_json=cred_json)
        res = bqm.read_data_gbq(query=qry)
        self.assertEqual(len(res), 10)

    def test_get_OMS_query(self):
        query = get_OMS_query(dti="2022-05-01", dtf="2022-05-31", idxs=("118753577", "118777716"))
        print(query)
        self.assertEqual(1, 1)

    def test_norm_address(self):
        test_address = [("Av.  providencia #123", "av providencia 123"),
                        ("PERÉZ ÑAÑÉZ      N11", "perez nanez n11")]
        for ta in test_address:
            val = ta[0]
            exp = ta[1]
            self.assertEqual(norm_address(address=val), exp)


if __name__ == '__main__':
    unittest.main()
