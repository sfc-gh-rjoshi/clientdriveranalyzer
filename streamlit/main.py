# Import python packages
import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col,datediff,lit,count,not_
from snowflake.snowpark.types import DateType, StringType
from datetime import date

# Write directly to the app
st.title("Snowflake Client Driver Analyzer")

#st.write("""Project **SnowFilter**.""")

# Get the current credentials
session = get_active_session()
st.session_state.selectedtablename = 'data.vw_sessions'
st.session_state.is_filtered = None
st.session_state.datatable = None
st.session_state.is_deprecated = None
st.session_state.driverlist = None
st.session_state.datatable4chart = None

cv_table = session.table('data.vw_client_version')
apps = cv_table.filter(col('is_supported')).select(col('APP_NAME')).collect()

if(cv_table.filter(datediff("day",col("EOL_DATE"),lit(date.today()))>0).collect() == []):
    st.success('You are using the latest version of this Application!', icon="✅")
else:
    st.warning('You do not have the latest metadata for deprecated drivers.  Kindly refer the documentation to upgrade the application.', icon="⚠️")

config_table = session.table('config.cda_config')
msg = config_table.filter(col('conf_name')=='sessions_last_update').select(col('CONF_VALUE').cast(DateType()).cast(StringType())).limit(1).collect()
st.success('The versions information was last updated on ' + msg[0][0], icon="✅")

col1, col2  = st.columns(2, gap="medium")

with col1:
    st.session_state.is_filtered = st.radio("Do you want to see all records or filter the table by Driver name?",('I would like to see ALL records.', 'I would like to filter the table.'))

df_filtered_table = session.table(st.session_state.selectedtablename)
if st.session_state.is_filtered == "I would like to filter the table.":
    st.session_state.driverlist = st.multiselect('Select the driver(s) to filter on (max 3)', apps, max_selections=3) 
    if not st.session_state.driverlist == [] :
        df_filtered_table = df_filtered_table.filter(col("APP_NAME").isin(st.session_state.driverlist) )
df4chart = df_filtered_table.filter(not_(col("IS_UNKNOWN_DRIVER"))).filter(datediff("day",col("LAST_SEEN"),lit(date.today()))<=7).group_by(["CLIENT_APP_ID","APP_NAME","APP_VERSION","LAST_SEEN","IS_DEPRECATED"]).agg(count("*").alias("DRIVER_COUNT")).select("CLIENT_APP_ID","APP_NAME","APP_VERSION","LAST_SEEN","IS_DEPRECATED","DRIVER_COUNT").limit(10000).to_pandas() 
st.session_state.datatable4chart = df4chart


with col2:
    st.session_state.is_deprecated = st.radio("Do you want to see only the deprecated versions?",('No', 'Yes'))
    if st.session_state.is_deprecated == "Yes":
            df_filtered_table = df_filtered_table.filter(col("IS_DEPRECATED"))

df_filtered_table = df_filtered_table.select("IS_DEPRECATED","APP_NAME","APP_VERSION","LAST_SEEN","USER_NAME","OS","TOOL","IP_ADDRESS").limit(500)
st.session_state.datatable = df_filtered_table

tab1, tab2, tab3 = st.tabs(["Session Driver Table (Last 3 months)", "Weekly deprecation trend (3 months)", "Driver versions seen (last 7 days)"])

with tab1:
    st.dataframe(st.session_state.datatable)

with tab2:
    tdh_table = session.table('data.tbl_deprecation_history')  
    if st.session_state.is_filtered == "I would like to filter the table." and len(st.session_state.driverlist)==1 :
        f_tdh_table = tdh_table.filter(col("CLIENT_NAME").isin(st.session_state.driverlist) ).to_pandas()  
        a = alt.Chart(f_tdh_table).mark_line().encode(
        alt.X('WEEK_OF:T'),
        alt.Y('DEPRECATED_COUNT:Q',title="Deprecated"),
        color=alt.value("#FF0000") 
        )
        b = alt.Chart(f_tdh_table).mark_line().encode(
        alt.X('WEEK_OF:T'),
        alt.Y('SUPPORTED_COUNT:Q',title="Supported"),
        color=alt.value("#00FF00") 
        )
        st.altair_chart(a+b, use_container_width=False)
    elif st.session_state.is_filtered == "I would like to filter the table." and len(st.session_state.driverlist)== 0:
        st.write('Kindly choose a filter to see this chart!')
    else:
        a_tdh_table = tdh_table
        if st.session_state.is_filtered == "I would like to filter the table." and len(st.session_state.driverlist) > 0:
            a_tdh_table = a_tdh_table.filter(col("CLIENT_NAME").isin(st.session_state.driverlist) )        
        a_tdh_table = a_tdh_table.to_pandas()   
        c = alt.Chart(a_tdh_table).mark_line().encode(
        alt.X('WEEK_OF:T'),
        alt.Y('DEPRECATED_COUNT:Q'),
        color='CLIENT_NAME'
        )
        d = alt.Chart(a_tdh_table).mark_line().encode(
        alt.X('WEEK_OF:T'),
        alt.Y('SUPPORTED_COUNT:Q'),
        color='CLIENT_NAME'
        )
        st.altair_chart(c|d, use_container_width=False)
        
    
with tab3:    
    if st.session_state.is_filtered == "I would like to filter the table.":
        c = alt.Chart(st.session_state.datatable4chart).mark_bar().encode(
        alt.X('APP_NAME',title=None),
        alt.Y('sum(DRIVER_COUNT):Q',title="Total Drivers"),#scale={"rangeMax": 10}),
        column='monthdate(LAST_SEEN):T',
        color='CLIENT_APP_ID'
        )
        st.altair_chart(c, use_container_width=False)
    else:
        st.write('Kindly choose a filter to see this chart!')