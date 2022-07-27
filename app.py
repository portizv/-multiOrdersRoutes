import streamlit as st
import pandas as pd
from utils import (data_frame_to_excel_download_link, group_orders)
from PIL import Image
from configs import PATH_IMAGES
from datetime import datetime
from configs import IDX_COL_IN, DATE_COL

DFLT_SIZE = 125
DFLT_BTCH = 1


# TODO: check RM inputs

def get_response(request, cred, min_size=DFLT_SIZE, batch_th=DFLT_BTCH, idx_col=None, date_col=None):
    try:
        response = group_orders(df_orders=request, cred_json=cred, min_size=min_size, batch_th=batch_th,
                                idx_col=idx_col, date_col=date_col)
    except Exception as e:
        print("Error:", e)
        response = request.copy()
    return response


def callback():
    st.session_state.button_clicked = True

if "button_clicked" not in st.session_state:
    st.session_state.button_clicked = False

st.title("Multi-Orders detector")
st.write("Detecta puntos que tienen multi-ordenes: \n"
         "- Mismo edificio/condominio pero distintos deparatamentos/casas \n"
         "- Mismo punto con distintas subordenes \n")
image = Image.open(PATH_IMAGES / 'multi-box.jpg')
st.image(image)
min_size = st.number_input(label="Cantidad minima de entregas por ruta", min_value=1, value=DFLT_SIZE)
batch_th = st.number_input(label="Cantidad maxima de rutas con multi-entrega", min_value=1, value=DFLT_BTCH)
idx_col_name = st.text_input(label="Nombre columa id subordernes", value=IDX_COL_IN)
dt_col_name = st.text_input(label="Nombre columa fecha", value=DATE_COL)

file = st.file_uploader("Cargar reporte puntos a rutear", type="xlsx")
cred_json = st.secrets["gcp_service_account"]

if st.button("Go", on_click=callback) or st.session_state.button_clicked:
    all_sheet = pd.ExcelFile(file)
    sheets = all_sheet.sheet_names
    sheet_option = st.selectbox('Select sheet', sheets)
    if st.button("Continue", on_click=callback):
        data_frame_request = pd.read_excel(file, sheet_name=sheet_option)
        data_frame_response = get_response(data_frame_request, cred_json, min_size=min_size, batch_th=batch_th,
                                           idx_col=idx_col_name, date_col=dt_col_name)

        tmp_download_link = data_frame_to_excel_download_link(data_frame=data_frame_response,
                                                              download_file_name="results_{}.xlsx".format(
                                                                  datetime.now().strftime("%Y%m%d%H%M%S")))
        st.markdown(tmp_download_link, unsafe_allow_html=True)
        st.balloons()
