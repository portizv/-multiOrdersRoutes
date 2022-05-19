import streamlit as st
import pandas as pd
from utils import (data_frame_to_excel_download_link, group_orders)
from PIL import Image
from configs import PATH_IMAGES

#TODO: deploy
#TODO: check RM inputs

st.title("Multi-Orders detector")
st.write("Detecta puntos que tienen multi-ordenes: \n"
         "- Mismo edificio/condominio pero distintos deparatamentos/casas \n"
         "- Mismo punto con distintas subordenes \n")
image = Image.open(PATH_IMAGES / 'multi-box.jpg')
st.image(image)
file = st.file_uploader("Cargar reporte puntos a rutear", type="xlsx")
cred_json = st.secrets["gcp_service_account"]

if st.button("Go"):
    data_frame_request = pd.read_excel(file)
    n = len(data_frame_request)
    try:
        data_frame_response = group_orders(df_orders=data_frame_request, cred_json=cred_json)
    except Exception as e:
        print(e)
        data_frame_response = data_frame_request.copy()

    tmp_download_link = data_frame_to_excel_download_link(data_frame=data_frame_response,
                                                          download_file_name="results.xlsx")
    st.markdown(tmp_download_link, unsafe_allow_html=True)
