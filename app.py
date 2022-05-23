import streamlit as st
import pandas as pd
from utils import (data_frame_to_excel_download_link, group_orders)
from PIL import Image
from configs import PATH_IMAGES
from datetime import datetime


# TODO: deploy
# TODO: check RM inputs

def get_response(request, cred):
    try:
        response = group_orders(df_orders=request, cred_json=cred)
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
file = st.file_uploader("Cargar reporte puntos a rutear", type="xlsx")
cred_json = st.secrets["gcp_service_account"]

if st.button("Go", on_click=callback) or st.session_state.button_clicked:
    all_sheet = pd.ExcelFile(file)
    sheets = all_sheet.sheet_names
    sheet_option = st.selectbox('Select sheet', sheets)
    if st.button("Continue", on_click=callback):
        data_frame_request = pd.read_excel(file, sheet_name=sheet_option)
        data_frame_response = get_response(data_frame_request, cred_json)

        tmp_download_link = data_frame_to_excel_download_link(data_frame=data_frame_response,
                                                              download_file_name="results_{}.xlsx".format(
                                                                  datetime.now().strftime("%Y%m%d%H%M%S")))
        st.markdown(tmp_download_link, unsafe_allow_html=True)
        st.balloons()
