import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


st.set_page_config(
    page_title="GêXis Gaúcho",
    page_icon="🍔",
    layout="wide",
)


@st.cache_data
def load_data():
    url_object = URL.create(
        "postgresql+psycopg2",
        username=os.environ["LOGIN"],
        password=os.environ["PASSWORD"],
        host=os.environ["HOST"],
        port=os.environ["PORT"],
        database=os.environ["DATABASE"],
    )
    engine = create_engine(url_object)
    df = pd.read_sql_table(
        "ge_validations_store_normalized", con=engine, schema="great_expectations"
    )
    df["meta.validation_time"] = pd.to_datetime(
        df["meta.validation_time"], format="%Y%m%dT%H%M%S.%fZ"
    )
    df["meta.validation_date"] = df["meta.validation_time"].apply(lambda x: x.date())
    df["meta.validation_date"] = pd.to_datetime(df["meta.validation_date"])
    df["meta.validation_yearmonth"] = df["meta.validation_time"].apply(
        lambda x: pd.Timestamp(f"{x.year}-{x.month}-01")
    )
    df["schema_table_name"] = (
        df["meta.batch_spec.schema_name"] + "." + df["meta.batch_spec.table_name"]
    )

    return df


data = load_data()

"""
# 🍔 GêXis Gaúcho
---
"""

col1, col2 = st.columns([1.5, 2.5])

with col1:
    """
    ## Últimas validation :red[failed]
    """

    last_run = data.groupby(["schema_table_name"])["meta.validation_time"].max()
    last_run = last_run.reset_index()
    last_run_full = last_run.merge(
        data,
        on=[
            "schema_table_name",
            "meta.validation_time",
        ],
        how="inner",
    )

    st.dataframe(
        last_run_full[~last_run_full["success"]].sort_values(
            by="meta.validation_time", ascending=False
        ),
        column_order=[
            "meta.validation_time",
            "schema_table_name",
        ],
        column_config={
            "schema_table_name": st.column_config.TextColumn(
                "schema.table",
            ),
            "meta.validation_time": st.column_config.DatetimeColumn(
                "Validation date",
                format="DD/MM/YYYY",
            ),
        },
        hide_index=True,
    )


with col2:
    """
    ## Percentual de sucesso por dia
    """

    count_success = (
        data.groupby("meta.validation_date")["success"]
        .value_counts()
        .unstack(fill_value=0)
    )
    count_success["success_percentual"] = count_success[True] / (
        count_success[True] + count_success[False]
    )
    count_success = count_success.reset_index()

    fig1 = px.bar(count_success, x="meta.validation_date", y="success_percentual")
    fig1.update_layout(
        width=800,
    )
    fig1.update_traces(marker_color="#F63366", marker_line_width=0)

    st.plotly_chart(fig1, theme="streamlit")

"""
---
"""

col3, col4 = st.columns([1.5, 2.5])

with col3:
    """
    ## Top-10 `schema.table` errors
    """

    top_fail = (
        data[~data["success"]]
        .groupby(["schema_table_name"])["success"]
        .value_counts()
        .unstack(fill_value=0)
        .sort_values(False, ascending=False)
        .head(10)
    )
    top_fail = top_fail.reset_index()

    st.dataframe(
        top_fail,
        column_config={
            "schema_table_name": st.column_config.TextColumn(
                "schema.table",
            ),
            "False": st.column_config.NumberColumn(
                "Qt. Erros",
                format="%d",
            ),
        },
    )

with col4:
    """
    ## Top-10 `schema.table` errors por mês
    """

    top_fail_time = (
        data[~data["success"]]
        .groupby(["schema_table_name", "meta.validation_yearmonth"])["success"]
        .count()
    )
    top_fail_time = top_fail_time.reset_index()

    top_fail_time_pivot = top_fail_time[
        top_fail_time["schema_table_name"].isin(top_fail["schema_table_name"])
    ].pivot_table(
        index="meta.validation_yearmonth",
        columns="schema_table_name",
        values="success",
        fill_value=0,
    )
    top_fail_time_pivot = top_fail_time_pivot.reset_index()

    fig2 = px.line(
        top_fail_time_pivot,
        x="meta.validation_yearmonth",
        y=top_fail_time_pivot.columns,
        #XXX check this
        hover_data={"meta.validation_yearmonth": "|%B %d, %Y"},
    )
    #XXX check this
    fig2.update_xaxes(dtick="M1", tickformat="%b\n%Y")
    fig2.update_layout(
        width=800,
    )

    st.plotly_chart(fig2, theme="streamlit")

"""
---

## Raw data
"""

col5, col6, col7, col8 = st.columns(4)

with col5:
    which_schema = st.selectbox(
        "Schema",
        options=sorted(data["meta.batch_spec.schema_name"].unique()),
        index=None,
    )

with col6:
    which_table = st.selectbox(
        "Table",
        options=(
            sorted(
                data.loc[
                    data["meta.batch_spec.schema_name"] == which_schema,
                    "meta.batch_spec.table_name",
                ].unique()
            )
            if which_schema
            else sorted(data["meta.batch_spec.table_name"].unique())
        ),
        index=None,
    )

with col7:
    which_date_min, which_date_max = st.date_input(
        "Validation date range",
        (data["meta.validation_time"].min(), data["meta.validation_time"].max()),
        format="DD/MM/YYYY",
    )

with col8:
    is_success = st.multiselect("is_success", [True, False], [True, False])

st.markdown(
    f"""
    You selected: `{which_schema}`, `{which_table}`, `{which_date_min}`, `{which_date_max}`, `{is_success}`
"""
)

if which_schema and not which_table:
    df_raw = data[
        (data["meta.batch_spec.schema_name"] == which_schema)
        & (data["meta.validation_time"].dt.date >= which_date_min)
        & (data["meta.validation_time"].dt.date <= which_date_max)
        & (data["success"].isin(is_success))
    ]
elif which_table:
    df_raw = data[
        (data["meta.batch_spec.table_name"] == which_table)
        & (data["meta.validation_time"].dt.date >= which_date_min)
        & (data["meta.validation_time"].dt.date <= which_date_max)
        & (data["success"].isin(is_success))
    ]
else:
    df_raw = data[
        (data["meta.validation_time"].dt.date >= which_date_min)
        & (data["meta.validation_time"].dt.date <= which_date_max)
        & (data["success"].isin(is_success))
    ]

st.dataframe(
    df_raw,
    column_order=[
        "success",
        "meta.validation_time",
        "meta.batch_spec.schema_name",
        "meta.batch_spec.table_name",
        "meta.active_batch_definition.datasource_name",
        "expectation_config.expectation_type",
        "expectation_config.kwargs.max_value",
        "expectation_config.kwargs.min_value",
        "result.observed_value",
    ],
    column_config={
        "success": st.column_config.TextColumn(
            "Rodou?",
        ),
        "meta.validation_time": st.column_config.DatetimeColumn(
            "Validation time",
            format="DD/MM/YYYY",
        ),
        "meta.batch_spec.schema_name": st.column_config.TextColumn(
            "Schema",
        ),
        "meta.batch_spec.table_name": st.column_config.TextColumn(
            "Table",
        ),
        "meta.active_batch_definition.datasource_name": st.column_config.TextColumn(
            "Conn",
        ),
        "expectation_config.expectation_type": st.column_config.TextColumn(
            "Expectation type",
        ),
        "expectation_config.kwargs.max_value": st.column_config.NumberColumn(
            "Max value allowed",
            format="%d",
        ),
        "expectation_config.kwargs.min_value": st.column_config.NumberColumn(
            "Min value allowed",
            format="%d",
        ),
        "result.observed_value": st.column_config.NumberColumn(
            "Value observed",
            format="%d",
        ),
    },
    width=1400,
    hide_index=True,
)
