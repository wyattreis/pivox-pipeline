# -------------------------------------------------------------------------------
# Name          Pivox Data Dashboard - Freeman Tower Snow Depth
# Description:  Streamlit dashboard to visualize the pivox snow depth data at
#               the Freeman Tower outside of Boise, Idaho.
# Author:       Wyatt Reis
#               US Army Corps of Engineers
#               Cold Regions Research and Engineering Laboratory (CRREL)
#               Wyatt.K.Reis@usace.army.mil
# Created:      October 2024
# Updated:      October 2024
# -------------------------------------------------------------------------------
import streamlit as st
import pandas as pd
import plotly.express as px

# -------------------------------------------------------------------------------
# Import and plot Pivox data from Github Repo
pivoxData_url = 'https://raw.githubusercontent.com/wyattreis/pivox-pipeline/main/scan_snowdepth_df.csv'
pivoxData = pd.read_csv(pivoxData_url).drop(columns=['SD_mean', 'SD_std'])
pivoxData['datetime'] = pd.to_datetime(pivoxData['datetime'])

# Snow depth timeseries
fig_depth = px.line(pivoxData, x='datetime', y='tif_elev_mean', title='Mean Snow Depth',
              labels={'datetime': '', 'tif_elev_mean': 'Mean Snow Depth (m)'},
              markers=True)

fig_depth.update_layout(
    title_font=dict(size=24, family='Arial'),  # Increase title font size
    xaxis_title_font=dict(size=18),  # Increase x-axis label font size
    yaxis_title_font=dict(size=18)   # Increase y-axis label font size
)

# Snow depth standard deviation timeseries
fig_std = px.line(pivoxData, x='datetime', y='tif_elev_std', title='Snow Depth Standard Deviation',
              labels={'datetime': '', 'tif_elev_std': 'Standard Deviation (m)'},
              markers=True)

fig_std.update_layout(
    title_font=dict(size=24, family='Arial'),  # Increase title font size
    xaxis_title_font=dict(size=18),  # Increase x-axis label font size
    yaxis_title_font=dict(size=18)   # Increase y-axis label font size
)

# -------------------------------------------------------------------------------
st.set_page_config(layout="wide")

left_co, cent_co, right_co = st.columns([0.25, 0.5, 0.25])
with cent_co:
    st.title('Freeman Tower Pivox Data Dashboard', anchor=False)

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(
        "<h2 style='text-align: left; font-family: Arial; font-size: 24px;'>Pivox Data Table</h2>", 
        unsafe_allow_html=True
    )
    st.dataframe(pivoxData)

with col2:
    st.plotly_chart(fig_depth)

with col3:
    st.plotly_chart(fig_std)