import base64
import datetime
from io import BytesIO
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from tabulate import tabulate
from configs import IDX_COL_IN, IND_COL_QRY, SPANISH_SPECIAL, ADDRESS_COL, DATE_COL, EPOCH, BULDING_KWS


def from_ordinal(ordinal, _epoch=EPOCH):
    """
    Format datetime
    :param ordinal: raw datetime to be formatted
    :type ordinal: float
    :param _epoch: datetime from epoch
    :type _epoch: datetime
    :return: datetime result
    :rtype: datetime
    """
    return _epoch + datetime.timedelta(days=ordinal - 2)


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
    """
    Manager of the interactions with GCP linked account in the project.
    """

    def __init__(self, cred_json=None, verbose=0):
        """
        Constructor of the class. You can initiate using a cred path (using cred_path) or directly with the credentials (cred_json).
        :param cred_json: GCP credential
        :type cred_json: dict
        :param verbose:regularize the number of printed logs
        :type verbose: int
        """
        credentials = service_account.Credentials.from_service_account_info(cred_json)
        self.client = bigquery.Client(location="US", credentials=credentials, project=cred_json["project_id"],
                                      default_query_job_config={})
        self.verbose = verbose

    def load_data_gbq(self, data, table_name, data_set, replace=False, show_table=False):
        """
        Load a pd.DataFrame to Google Big Query (GBQ)
        :param data_set: name of data set
        :type data_set: str
        :param data: data to load
        :type data: pd.DataFrame
        :param table_name: name of the table to load in GBQ
        :type table_name: str
        :param replace: True to replace if the table already exists
        :type replace: bool
        :param show_table: print the log of the execution
        :type show_table: bool
        """
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
        """
        Download a GBQ table to a pd.DataFrame
        :param query: query to get the table
        :type query: str
        :param show_table: print the log of the execution
        :type show_table: bool
        :param as_json: if you want to transform output as dictionary instead of pd.DataFrame
        :type as_json: bool
        :param idx_col: column name of the index (if None generate a default index)
        :type idx_col: str
        :return: respective table
        :rtype: pd.DataFrame
        """
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
    """
    Build parametrize query in order to get important data
    :param dti: start of the range date
    :type dti: str
    :param dtf: end of the range date
    :type dtf: str
    :param idxs: valuos of rows that need to be queried
    :type idxs: list
    :param idx_qry: index column name of the query output
    :type idx_qry: str
    :return:
    :rtype:
    """
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

def contain_num(w):
    for c in w:
        if c.isnumeric:
            return True
    return False

def norm_address(address):
    """
    Change address to a normalized way
    :param address: raw address
    :type address: str
    :return: normalized address
    :rtype: str
    """
    address_norm = "".join([c for c in " ".join(address.lower().split()) if c == " " or c.isalpha() or c.isnumeric()])
    for c, rc in SPANISH_SPECIAL.items():
        address_norm = address_norm.replace(c, rc)

    address_norm_split = address_norm.split()
    res = []
    for i, w in enumerate(address_norm_split):
        if w in BULDING_KWS and len(res) > 1:
            break
        res.append(w)
    return " ".join(res)



def group_orders(df_orders=None, idx_col=IDX_COL_IN, cred_json=None, address_col=ADDRESS_COL, date_col=DATE_COL,
                 min_size=150, batch_th=1, col_multi_name="is_multi"):
    """
    Logic to join multi-orders point candidates.
    :param df_orders: Set of orders to apply the logic
    :type df_orders: pd.DataFrame
    :param idx_col: Column name of table index
    :type idx_col: str
    :param cred_json: GCP credential
    :type cred_json: dict
    :param address_col: Column name of raw address
    :type address_col: str
    :param date_col: Name of column of dates
    :type date_col: str
    :param min_size: minimum size of point by routes
    :type min_size: int
    :param batch_th: maximum number of routes with multi-orders
    :type batch_th: int
    :param col_multi_name: name of label of columns to write number of multi-orders
    :type col_multi_name: str
    :return: Table with the columns to indicate if there are multi-orders
    :rtype: pd.DataFrame
    """
    df_orders_multi_dlv = df_orders.copy()
    bqm = BigQueryManager(cred_json=cred_json, verbose=1)
    df_orders_multi_dlv[date_col] = df_orders_multi_dlv[date_col].apply(lambda x: from_ordinal(x).date())
    dts = df_orders_multi_dlv[date_col].unique()
    dtf = dts.max()
    dti = dts.min()
    idxs = df_orders[idx_col].unique()
    query = get_OMS_query(dti=dti, dtf=dtf, idxs=idxs)
    df_oms = bqm.read_data_gbq(query=query)
    df_oms_multi = df_orders_multi_dlv[[idx_col]].merge(df_oms, left_on=idx_col, right_on=IND_COL_QRY, how="inner")
    df_oms_multi[address_col] = df_oms_multi[address_col].apply(lambda x: norm_address(x))
    df_oms_multi.loc[:, "n_multi"] = 1
    df_oms_multi = df_oms_multi.groupby(by=[IND_COL_QRY, address_col], as_index=False)["n_multi"].sum()
    df_orders = df_orders.merge(df_oms_multi[[IND_COL_QRY, "n_multi"]], left_on=idx_col, right_on=IND_COL_QRY,
                                how="left")
    df_orders.drop(columns=[IND_COL_QRY], inplace=True)
    n_multi = len(df_oms_multi[df_oms_multi["n_multi"] > 1])
    df_orders["n_multi"].fillna(inplace=True, value=0)
    df_orders.sort_values(by="n_multi", ascending=False, inplace=True)
    n_to_select = min(n_multi // min_size, batch_th) * min_size
    if n_to_select == 0:
        n_to_select = n_multi
    df_orders_multi_cand = df_orders.iloc[:n_to_select, :]
    df_orders_no_multi = df_orders.iloc[n_to_select:, :]
    df_orders_multi_cand.loc[:, col_multi_name] = 1
    df_orders_no_multi.loc[:, col_multi_name] = 0
    df_orders_final = df_orders_multi_cand.append(df_orders_no_multi, ignore_index=True)
    return df_orders_final


def data_frame_to_excel_engine(data_frame):
    """
    Transform dataframe to a excel file in order to be exported
    :param data_frame: dataframe to be transformed
    :type data_frame: pd.DataFrame
    :return: excel file
    :rtype: BytesIO
    """
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    data_frame.to_excel(writer, sheet_name='Sheet1')
    writer.save()
    processed_data = output.getvalue()
    return processed_data


def data_frame_to_excel_download_link(data_frame=pd.DataFrame(), download_file_name='results.xlsx',
                                      download_button_message='Download file as xlsx'):
    """
    Transform dataframe to a link to be downloaded
    :param data_frame: dataframe to be transformed
    :type data_frame: pd.DataFrame
    :param download_file_name: name of the file
    :type download_file_name: str
    :param download_button_message: name of the button link
    :type download_button_message: str
    :return: downloadable link
    :rtype: str
    """
    val = data_frame_to_excel_engine(data_frame)
    b64 = base64.b64encode(val)
    return f'<a href="data:application/octet-stream;base64,{b64.decode()}" download={download_file_name}>{download_button_message}</a>'
