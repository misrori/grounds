import pandas as pd
import streamlit as st
import os

st.set_page_config(page_title="Hirdetmény összegzés", layout="wide")
st.title("Hirdetmény összegző dashboard")

# data beolvasása it is on processed data folder in batches in pickle format multiple files
# # list the processed data folder and read the pickle than combaine
data_files = os.listdir('processed_data')
data_files = [f for f in data_files if f.endswith('.pickle')]
if not data_files:
    st.error("Nincsenek feldolgozott adatok a 'processed_data' mappában.")
else:
    df_list = []
    for file in data_files:
        file_path = os.path.join('processed_data', file)
        df = pd.read_pickle(file_path)
        df_list.append(df)
    
    # Combine all dataframes into one
    df = pd.concat(df_list, ignore_index=True)




# Szűrési lehetőségek
st.dataframe(df, use_container_width=True)

