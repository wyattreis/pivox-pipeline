import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")

left_co, cent_co, right_co = st.columns([0.25, 0.5, 0.25])
with cent_co:
    st.title('Freeman Tower Pivox Data Dashboard', anchor=False)