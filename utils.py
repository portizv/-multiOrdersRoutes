import base64
import datetime
import time
from io import BytesIO

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from tabulate import tabulate

from configs import IDX_COL_IN, IND_COL_QRY, SPANISH_SPECIAL, ADDRESS_COL


def show_data_frame_as_tabulate(data_frame, show_first=25, float_decimals=-1):
    """
    Show a pd.DataFrame in a nice way
    :param float_decimals: number of decimal to print
    :type  float_decimals: int
    :param data_frame: table to be showed
    :type data_frame: pd.DataFrame
    :param show_first: number of first rows to be showed
    :type show_first: int
    """
    max_rows = min(len(data_frame), show_first)
    n_rows, n_cols = data_frame.shape

    if float_decimals >= 0:
        tabulate_table = tabulate(data_frame.iloc[0:max_rows], headers="keys", tablefmt="psql",
                                  floatfmt=".{}f".format(float_decimals), missingval="?", numalign="right")
    else:
        tabulate_table = tabulate(data_frame.iloc[0:max_rows], headers="keys", tablefmt="psql", missingval="?",
                                  numalign="right")
    print("Showing {}/{} rows and {} columns".format(max_rows, n_rows, n_cols))
    print(tabulate_table)


class BigQueryManager:
    def __init__(self, cred_json=None, verbose=0):
        credentials = service_account.Credentials.from_service_account_info(cred_json)
        self.client = bigquery.Client(location="US", credentials=credentials, project=cred_json["project_id"],
                                      default_query_job_config={})
        self.verbose = verbose

    def load_data_gbq(self, data, table_name, data_set, replace=False, show_table=False):
        n = len(data)
        dataset = self.client.get_dataset(data_set)
        table_ref = dataset.table(table_name)
        job_config = bigquery.job.LoadJobConfig()
        if replace:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job = self.client.load_table_from_dataframe(data, table_ref, location="US", job_config=job_config)
        job.result()
        if self.verbose > 0:
            print("Loaded dataframe with {} rows to {}".format(n, table_ref.path))
        if show_table:
            show_data_frame_as_tabulate(data_frame=data, show_first=5)

    def read_data_gbq(self, query, show_table=False, as_json=False, idx_col=None):
        query_fmt = "".join([c for c in query if c.isnumeric() or c.isalpha()])
        query_fmt_lmts = min(15, int(len(query_fmt) * 0.25))
        if self.verbose > 2:
            print("Executing query {} ... {}".format(query[:query_fmt_lmts], query[-query_fmt_lmts:]))
        query_job = self.client.query(query, location="US")
        data = query_job.to_dataframe()
        if self.verbose > 0:
            if data.empty:
                print("No data fetched")
            print("Total rows fetched:", len(data))

        if idx_col:
            data.set_index(idx_col, inplace=True)

        if show_table:
            show_data_frame_as_tabulate(data_frame=data, show_first=5)

        if as_json:
            data = data.to_dict(orient="index")

        return data


def get_OMS_query(dti, dtf, idxs, idx_qry=IND_COL_QRY):
    idxs_fmmt_inside = ""
    for i, idx in enumerate(idxs):
        if i + 1 == len(idxs):
            idxs_fmmt_inside += "'{}'".format(idx)
            break
        idxs_fmmt_inside += "'{}',".format(idx)
    idxs_fmmt = "({})".format(idxs_fmmt_inside)

    return """SELECT
              distinct CAST({} as int) as {}, D_ADDRESS_1
            FROM
              `tc-sc-bi-bigdata-corp-tsod-dev.sandbox_building_type_recognition.oms_raw_data`
            WHERE
              DATE(F_CREACION) BETWEEN "{}"
              AND "{}"
              AND {} IN {};
    """.format(idx_qry, idx_qry, dti, dtf, idx_qry, idxs_fmmt)


def norm_address(address):
    address_norm = "".join([c for c in " ".join(address.lower().split()) if c == " " or c.isalpha() or c.isnumeric()])
    for c, rc in SPANISH_SPECIAL.items():
        address_norm = address_norm.replace(c, rc)
    return address_norm


def group_orders(df_orders=None, idx_col=IDX_COL_IN, cred_json=None, address_col=ADDRESS_COL):
    #TODO: Parse dates
    df_orders_multi_dlv = df_orders.copy()
    bqm = BigQueryManager(cred_json=cred_json, verbose=1)
    dtf = datetime.datetime.fromtimestamp(time.time()).date()
    dti = dtf - datetime.timedelta(days=7)
    idxs = df_orders[idx_col].unique()
    query = get_OMS_query(dti=dti, dtf=dtf, idxs=idxs)
    df_oms = bqm.read_data_gbq(query=query)
    df_oms_multi = df_orders_multi_dlv[[idx_col]].merge(df_oms, left_on=idx_col, right_on=IND_COL_QRY, how="inner")
    df_oms_multi[address_col] = df_oms_multi[address_col].apply(lambda x: norm_address(x))
    df_oms_multi["is_multi"] = 1
    df_oms_multi = df_oms_multi.groupby(by=[IND_COL_QRY, address_col], as_index=False)["is_multi"].sum()
    df_oms_multi["is_multi"] = df_oms_multi["is_multi"].apply(lambda x: int(x > 1))
    df_orders = df_orders.merge(df_oms_multi[[IND_COL_QRY, "is_multi"]], left_on=idx_col, right_on=IND_COL_QRY,
                                how="left")
    df_orders.drop(columns=[IND_COL_QRY], inplace=True)
    df_orders["is_multi"].fillna(inplace=True, value=0)

    return df_orders


def data_frame_to_excel_engine(data_frame):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    data_frame.to_excel(writer, sheet_name='Sheet1')
    writer.save()
    processed_data = output.getvalue()
    return processed_data


def data_frame_to_excel_download_link(data_frame=pd.DataFrame(), download_file_name='results.xlsx',
                                      download_button_message='Download file as xlsx'):
    val = data_frame_to_excel_engine(data_frame)
    b64 = base64.b64encode(val)
    return f'<a href="data:application/octet-stream;base64,{b64.decode()}" download={download_file_name}>{download_button_message}</a>'
