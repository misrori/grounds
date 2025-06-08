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
        # add the processed date as a new column the format hirdetmeny_osszegzes_batch_20250606_120949.pickle
        # assuming the file name format is hirdetmeny_osszegzes_batch_YYYYMMDD_HHMMSS.pickle
        # extract the date from the file name and convert it to datetime
        # e.g. hirdetmeny_osszegzes_batch_20250606.pickle
        # extract the date part from the file name
        # and convert it to datetime and timestamp 0250606_120949
        # assuming the file name format is hirdetmeny_osszegzes_batch_YYYYMMDD_HHMMSS.pickle
        # extract the date from the file name
        date_part = file.split('_')[3]
        time_part = file.split('_')[4].replace('.pickle', '')
        df['processed_date'] = pd.to_datetime(date_part + '_' + time_part, format='%Y%m%d_%H%M%S')
        df_list.append(df)

    # Combine all dataframes into one
    df_all = pd.concat(df_list, ignore_index=True)

df = df_all.copy()
# add a dataframe where the processed date is less than 48 hours ago
df['processed_date'] = pd.to_datetime(df['processed_date'])
df = df[df['processed_date'] > pd.Timestamp.now() - pd.Timedelta(hours=48)]

# filter for price above 50m or ingatlanok szama greater than 3
df = df[(df['vételárak összegzése'] > 50_000_000) | (df['helyrajzi számok száma'] > 3)]
# order by vételárak összegzése descending
df = df.sort_values(by='vételárak összegzése', ascending=False).reset_index(drop=True)

# filtered df show
st.subheader("Hirdetmények az utolsó 48 órában (ár > 50M vagy ingatlanok száma > 3)")

st.dataframe(df, use_container_width=True)

# df all order by vételárak összegzése descending
df_all = df_all.sort_values(by='vételárak összegzése', ascending=False).reset_index(drop=True)

st.subheader("Összes hirdetmény összegzés")
st.dataframe(df_all, use_container_width=True)


st.caption(":information_source: Forrás: hirdetmeny_osszegzes.csv")
