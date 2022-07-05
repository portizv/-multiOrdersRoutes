import unittest
import pandas as pd
from utils import group_orders, BigQueryManager, get_OMS_query, norm_address, from_ordinal
from configs import PATH_TESTS_INPUTS, IDX_COL_IN, PATH_MAIN, DATE_COL, JSON_NAME
import json

f = open(PATH_MAIN / JSON_NAME)
cred_json = json.load(f)


class utils(unittest.TestCase):

    def test_from_ordinal(self):
        test = pd.read_excel(PATH_TESTS_INPUTS / "testRegions01.xlsx")
        res = test['RANGOFECHAPACTADA'].apply(lambda x: from_ordinal(x))
        self.assertEqual(str(res.max()), "2022-05-28")
        self.assertEqual(str(res.min()), "2022-05-09")

    def test_group_orders(self):
        paths = [("testRegions01.xlsx", "SOC", "RANGOFECHAPACTADA"),
                 ("testRegions02.xlsx", "SUBORDEN", "FECHA"),
                 ("testRegions03.xlsx", "SUBORDEN", "FECHA")]

        for i, p in enumerate(paths):
            if i != 2:
                continue

            pth = p[0]
            sub_col = p[1]
            dt_col = p[2]
            print("Testing {}: {}".format(i, pth))
            df_orders = pd.read_excel(PATH_TESTS_INPUTS / pth)
            gb_orders = group_orders(df_orders=df_orders, idx_col=sub_col, cred_json=cred_json,
                                     date_col=dt_col)
            self.assertEqual(len(df_orders), len(gb_orders))
            self.assertEqual(len(gb_orders[gb_orders["is_multi"] > 0]) > 1, True)
            for c in df_orders.columns:
                if c not in gb_orders.columns:
                    print(c)
                self.assertEqual(c in gb_orders.columns, True)
            print("DONE")

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
        query = get_OMS_query(dti="2022-05-01", dtf="2022-05-31", idxs=('149055431437', '149055374742'))
        print(query)
        bqm = BigQueryManager(cred_json=cred_json)
        res = bqm.read_data_gbq(query=query)
        self.assertEqual(len(res), 2)

    def test_norm_address(self):
        test_address = [("Av.  providencia #123", "av providencia 123"),
                        ("PERÉZ ÑAÑÉZ      N11", "perez nanez n11"),
                        ("Hamlet #4340 dpto 706", "hamlet 4340"),
                        ("avda. los Trapenses 155 bloque B3", "avda los trapenses 155")]
        for ta in test_address:
            val = ta[0]
            exp = ta[1]
            res = norm_address(address=val)
            self.assertEqual(res, exp)


if __name__ == '__main__':
    unittest.main()
