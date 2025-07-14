import pandas as pd
import streamlit as st
import os

st.set_page_config(page_title="Hirdetmény összegzés", layout="wide")
st.title("Hirdetmény összegző dashboard")

df_all = pd.read_pickle('all_data.pickle')
df_all['processed_date'] = pd.to_datetime(df_all['processed_date'], format='%Y%m%d_%H%M%S')

df = df_all.copy()

# ad this 3 filter to the user, time, price, and ingatlanok szama
with st.expander("Szűrők", expanded=False):
    time_filter = st.slider(
        "Feldolgozás dátuma",
        min_value=0,
        max_value=5*24,
        value=48,
        step=1
    )
    
    price_filter = st.slider(
        "Vételárak összegzése (millió Ft)",
        min_value=0,
        max_value=100,
        value=50,
        step=1
    )
    ingatlanok_szama_filter = st.slider(
        "Ingatlanok száma",
        min_value=0,
        max_value=10,
        value=3,
        step=1
    )  
# apply the filters
# add a dataframe where the processed date is less than 48 hours ago
df['processed_date'] = pd.to_datetime(df['processed_date'])
df = df[df['processed_date'] > pd.Timestamp.now() - pd.Timedelta(hours=time_filter)]    
# filter for price above the price_filter or ingatlanok szama greater than ingatlanok_szama_filter
df = df[(df['vételárak összegzése'] > price_filter * 1_000_000) | (df['ingatlanok száma'] > ingatlanok_szama_filter)]
# order by vételárak összegzése descending
df = df.sort_values(by='vételárak összegzése', ascending=False).reset_index(drop=True)  
# filtered df show
st.subheader("Szűrt hirdetmények")
#st.dataframe(df, use_container_width=True)
st.dataframe(
    df,
    column_config={
        "Link a részletekhez": st.column_config.LinkColumn()
    }
)



# df all order by vételárak összegzése descending
df_all = df_all.sort_values(by='vételárak összegzése', ascending=False).reset_index(drop=True)

st.subheader("Összes hirdetmény összegzés")
#st.dataframe(df_all, use_container_width=True)

st.dataframe(
    df_all,
    column_config={
        "Link a részletekhez": st.column_config.LinkColumn()
    }
)



st.caption(":information_source: Forrás: hirdetmeny_osszegzes.csv")
