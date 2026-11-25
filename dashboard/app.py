
from datetime import timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from database import get_engine, run_query
import queries as q


st.set_page_config(
    page_title="Hospital Analytics",
    page_icon="🏥",
    layout="wide"
)


@st.cache_resource
def database_engine():
    return get_engine()


@st.cache_data(ttl=300)
def fetch_data(query, params=None):
    return run_query(
        database_engine(),
        query,
        params
    )


def format_number(value):
    if pd.isna(value):
        return "0"
    return f"{float(value):,.0f}"


st.title("🏥 Hospital Analytics Dashboard")
st.caption(
    "Hospital Data Warehouse | SQL Server | Python"
)


# -------------------------------------------------
# 1. Load filter options
# -------------------------------------------------

try:
    date_bounds = fetch_data(q.DATE_RANGE)
    services_df = fetch_data(q.SERVICES)
    types_df = fetch_data(q.PATIENT_TYPES)
    sources_df = fetch_data(q.SOURCES)

except Exception as error:
    st.error(f"Database connection failed: {error}")
    st.stop()


if (
    date_bounds.empty
    or pd.isna(date_bounds.iloc[0]["MinDate"])
):
    st.warning("No FactService records found.")
    st.stop()


min_date = pd.to_datetime(
    date_bounds.iloc[0]["MinDate"]
).date()

max_date = pd.to_datetime(
    date_bounds.iloc[0]["MaxDate"]
).date()


# -------------------------------------------------
# 2. Sidebar filters
# -------------------------------------------------

st.sidebar.header("Dashboard Filters")

default_start = max(
    min_date,
    max_date - timedelta(days=365)
)

date_selection = st.sidebar.date_input(
    "Date range",
    value=(default_start, max_date),
    min_value=min_date,
    max_value=max_date
)

if len(date_selection) != 2:
    st.info("Please select a start date and end date.")
    st.stop()

start_date, end_date = date_selection

service_options = (
    ["All"]
    + services_df["ServiceName"].dropna().tolist()
)

type_options = (
    ["All"]
    + types_df["PatientType"].dropna().tolist()
)

source_options = (
    ["All"]
    + sources_df["SourceSystem"].dropna().tolist()
)

selected_service = st.sidebar.selectbox(
    "Service",
    service_options
)

selected_patient_type = st.sidebar.selectbox(
    "Patient type",
    type_options
)

selected_source = st.sidebar.selectbox(
    "Source system",
    source_options
)

params = {
    "start_date": start_date,
    "end_date": end_date,
    "service": selected_service,
    "patient_type": selected_patient_type,
    "source": selected_source
}

emergency_params = {
    "start_date": start_date,
    "end_date": end_date,
    "patient_type": selected_patient_type
}


# -------------------------------------------------
# 3. Load reporting data
# -------------------------------------------------

try:
    summary = fetch_data(q.SUMMARY, params)
    daily = fetch_data(q.DAILY, params)
    monthly = fetch_data(q.MONTHLY, params)
    services = fetch_data(q.SERVICE_RANKING, params)
    patient_types = fetch_data(
        q.PATIENT_TYPE_REPORT, params
    )
    monthly_types = fetch_data(
        q.MONTHLY_PATIENT_TYPES, params
    )
    emergency = fetch_data(
        q.EMERGENCY_DAILY,
        emergency_params
    )

except Exception as error:
    st.error(f"Report query failed: {error}")
    st.stop()


if daily.empty:
    st.warning("No records match the selected filters.")
    st.stop()


# -------------------------------------------------
# 4. KPI cards
# -------------------------------------------------

st.subheader("Key Performance Indicators")

kpi = summary.iloc[0]

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Total Visits",
        format_number(kpi["TotalVisits"])
    )

with col2:
    st.metric(
        "Unique Patients",
        format_number(kpi["UniquePatients"])
    )

with col3:
    st.metric(
        "Hospital Cash",
        format_number(kpi["HospitalCash"])
    )

with col4:
    st.metric(
        "Dentistry Tariff Amount",
        format_number(kpi["DentistryTariffAmount"])
    )

st.caption(
    "Amounts are in source-system units. "
    "Dentistry amounts are tariff-based estimates, "
    "not confirmed cash revenue."
)


# -------------------------------------------------
# 5. Daily visits
# -------------------------------------------------

st.subheader("Daily Visits")

daily["VisitDate"] = pd.to_datetime(
    daily["VisitDate"]
)

fig_daily = px.line(
    daily,
    x="VisitDate",
    y="TotalVisits",
    markers=True,
    title="Visits by Day"
)

st.plotly_chart(fig_daily, use_container_width=True)


# -------------------------------------------------
# 6. Monthly trends
# -------------------------------------------------

st.subheader("Monthly Patient & Visit Trends")

monthly["MonthStart"] = pd.to_datetime(
    monthly["MonthStart"]
)

fig_monthly = px.line(
    monthly,
    x="MonthStart",
    y=["TotalVisits", "UniquePatients"],
    markers=True,
    title="Monthly Visits and Unique Patients"
)

st.plotly_chart(
    fig_monthly,
    use_container_width=True
)


# -------------------------------------------------
# 7. Service ranking
# -------------------------------------------------

st.subheader("Service Performance")

left, right = st.columns(2)

with left:
    fig_services = px.bar(
        services,
        x="ServiceName",
        y="TotalVisits",
        title="Total Visits by Service"
    )

    st.plotly_chart(
        fig_services,
        use_container_width=True
    )

with right:
    fig_patients = px.bar(
        services,
        x="ServiceName",
        y="UniquePatients",
        title="Unique Patients by Service"
    )

    st.plotly_chart(
        fig_patients,
        use_container_width=True
    )

st.dataframe(
    services,
    use_container_width=True,
    hide_index=True
)


# -------------------------------------------------
# 8. Patient type analysis
# -------------------------------------------------

st.subheader("Patient Type Analysis")

fig_types = px.bar(
    patient_types,
    x="PatientType",
    y="TotalVisits",
    title="Visits by Patient Type"
)

st.plotly_chart(
    fig_types,
    use_container_width=True
)

if not monthly_types.empty:
    monthly_types["MonthStart"] = pd.to_datetime(
        monthly_types["MonthStart"]
    )

    fig_monthly_types = px.bar(
        monthly_types,
        x="MonthStart",
        y="TotalVisits",
        color="PatientType",
        title="Monthly Visits by Patient Type"
    )

    st.plotly_chart(
        fig_monthly_types,
        use_container_width=True
    )


# -------------------------------------------------
# 9. Daily hospital cash
# -------------------------------------------------

st.subheader("Daily Hospital Cash")

fig_cash = px.line(
    daily,
    x="VisitDate",
    y="HospitalCash",
    markers=True,
    title="Hospital Cash by Day"
)

st.plotly_chart(fig_cash, use_container_width=True)

hospital_days = daily[
    daily["HospitalCash"] != 0
]

if not hospital_days.empty:
    max_hospital_day = hospital_days.loc[
        hospital_days["HospitalCash"].idxmax()
    ]

    st.metric(
        "Highest Hospital Cash Day (Filtered)",
        format_number(max_hospital_day["HospitalCash"])
    )

    st.write(
        "Date:",
        max_hospital_day["VisitDate"].date()
    )


# -------------------------------------------------
# 10. Emergency cash report
# -------------------------------------------------

st.subheader("Emergency Cash Report")

st.caption(
    "This report always shows Hospital Emergency "
    "data. Service and source filters do not "
    "apply; date and patient-type filters do."
)

if not emergency.empty:
    emergency["VisitDate"] = pd.to_datetime(
        emergency["VisitDate"]
    )

    fig_emergency = px.bar(
        emergency,
        x="VisitDate",
        y="EmergencyCash",
        title="Emergency Cash by Day"
    )

    st.plotly_chart(
        fig_emergency,
        use_container_width=True
    )

    max_emergency_day = emergency.loc[
        emergency["EmergencyCash"].idxmax()
    ]

    st.metric(
        "Maximum Daily Emergency Cash",
        format_number(max_emergency_day["EmergencyCash"])
    )

    st.write(
        "Highest cash date:",
        max_emergency_day["VisitDate"].date()
    )

    st.dataframe(
        emergency.sort_values(
            "EmergencyCash",
            ascending=False
        ).head(10),
        hide_index=True,
        use_container_width=True
    )
else:
    st.info("No Emergency records for this date range.")


# -------------------------------------------------
# 11. Export report
# -------------------------------------------------

st.subheader("Download Reports")

st.download_button(
    label="Download Daily Report (CSV)",
    data=daily.to_csv(index=False).encode("utf-8-sig"),
    file_name="hospital_daily_report.csv",
    mime="text/csv"
)

st.caption(
    "Data is retrieved from HospitalDW.dbo.FactService."
)
