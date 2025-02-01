import pandas as pd
import polars as pl
import typing as t

from datetime import datetime
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

@model(
    name='gold.calendar',
    kind=dict(
        name=ModelKindName.FULL,
    ),
    columns={
        "_key__date": "binary",
        "date": "date",
        "year": "int",
        "iso_year": "int",
        "quarter": "int",
        "month": "int",
        "month__name": "string",
        "week": "int",
        "weekday": "int",
        "weekday__name": "string",
        "year_month": "string",
        "year_month__name": "string",
        "year_week": "string",
        "iso_week_date": "string",
        "ordinal_date": "string",
        "is_leap_year": "bool",
    }
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> t.Generator[pd.DataFrame, None, None]:
    
    # Load data
    table = context.resolve_table("obsidian_insights.silver.int__uss_bridge")
    source_df = pl.from_pandas(context.fetchdf(f"SELECT _key__date FROM {table} WHERE _key__date IS NOT NULL"))
    
    # Convert binary to text, extract the first 4 characters (year), and cast to int
    year_df = source_df.with_columns(
        pl.col("_key__date").cast(pl.String()).str.slice(0, 4).cast(pl.Int32()).alias("year")
    ).select([
        pl.col("year").min().alias("start_year"),
        pl.col("year").max().alias("end_year")
    ])
    
    # Get min and max year
    start_year = year_df.item(0, "start_year")
    end_year = year_df.item(0, "end_year")
    
    # Generate a full date spine covering all dates in the year range
    date_spine_df = pl.date_range(
        start=pl.lit(f"{start_year}-01-01"),
        end=pl.lit(f"{end_year}-12-31"),
        interval="1d",
        eager=True
    ).to_frame("date")
    
    # Generate the calendar
    calendar_df = date_spine_df.select(
        pl.col("date").cast(pl.String()).cast(pl.Binary()).alias("_key__date"),
        pl.col("date"),
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.iso_year().alias("iso_year"),
        pl.col("date").dt.quarter().alias("quarter"),
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.strftime("%b").alias("month__name"),
        pl.col("date").dt.week().alias("week"),
        pl.col("date").dt.weekday().alias("weekday"),
        pl.col("date").dt.strftime("%a").alias("weekday__name"),
        pl.col("date").dt.strftime("%Y-%m").alias("year_month"),
        pl.col("date").dt.strftime("%Y-%b").alias("year_month__name"),
        pl.col("date").dt.strftime("%G-W%V").alias("year_week"),
        pl.col("date").dt.strftime("%G-W%V-%u").alias("iso_week_date"),
        pl.col("date").dt.strftime("%Y-%j").alias("ordinal_date"),
        pl.col("date").dt.is_leap_year().alias("is_leap_year"),
    )
    
    yield calendar_df.to_pandas()
