import pandas as pd
import click
from mbpy.cli.formatted_click import RichClickGroup, RichClickCommand
from .utils import smtp_shared_options, command_shared_options
from functools import reduce
import numpy as np
import re
import csv

delim = ": "


def quote_specific_columns(x):
    return f'="{x}"'


@click.command("cumulative-combo-attendance", cls=RichClickCommand)
@command_shared_options
@smtp_shared_options
@click.pass_context
def cli(ctx, *args, absent_category_name="Absent", **kwargs):
    """
    Homeroom and class attendance combined
    """
    kwargs["absent_category_name"] = absent_category_name
    from .classes import cli as classes_cli
    from .homerooms import cli as homerooms_cli

    homeroom_df = ctx.invoke(homerooms_cli, reports=False, **kwargs)
    classes_df = ctx.invoke(classes_cli, reports=False, **kwargs)

    # Count of how many classes are not absent per day per studentt
    classes_not_absent_filtered = classes_df.loc[
        classes_df["Status"] != absent_category_name
    ]
    classes_absent_filtered = classes_df.loc[
        classes_df["Status"] == absent_category_name
    ]

    classes_not_absent_grouped = (
        classes_not_absent_filtered.groupby(["Student Id", "Date"])
        .size()
        .reset_index(name="Count of Classes Not Absent")
    )
    classes_absent_grouped = (
        classes_absent_filtered.groupby(["Student Id", "Date"])
        .size()
        .reset_index(name="Count of Classes Absent")
    )

    homeroom_not_absent_filtered = homeroom_df.loc[
        homeroom_df["Status"] != absent_category_name
    ]
    homeroom_absent_filtered = homeroom_df.loc[
        homeroom_df["Status"] == absent_category_name
    ]

    homeroom_not_absent_grouped = (
        homeroom_not_absent_filtered.groupby(["Student Id", "Date"])
        .size()
        .reset_index(name="Count of Homeroom Not Absent")
    )
    homeroom_absent_grouped = (
        homeroom_absent_filtered.groupby(["Student Id", "Date"])
        .size()
        .reset_index(name="Count of Homeroom Absent")
    )

    on_ = ["Student Id", "Date"]
    dfs = [
        classes_not_absent_grouped,
        classes_absent_grouped,
        homeroom_not_absent_grouped,
        homeroom_absent_grouped,
    ]

    ## combined is the backbone table that shows how many absent, not absent in both classes and homeroom
    combined = reduce(
        lambda left, right: pd.merge(left, right, on=on_, how="outer"), dfs
    )
    combined = combined.fillna(0)

    finals = {}
    for name, first_condition_column, second_condition_column, homeroom_filter, classes_filter in [
        (
            "students_marked_absent_in_homeroom_but_not_uniformally_absent_from_classes",
            "Count of Homeroom Absent",
            "Count of Classes Not Absent",
            homeroom_absent_filtered,
            classes_not_absent_filtered
        ),
        (
            "students_marked_not_absent_in_homeroom_but_absent_in_classes",
            "Count of Homeroom Not Absent",
            "Count of Classes Absent",
            homeroom_not_absent_filtered,
            classes_absent_filtered
        ),
    ]:
        ## Only those we are looking for
        first_step = combined.loc[
            (combined[first_condition_column] == 1.0)
            & (combined[second_condition_column] > 0)
        ]

        ## Summary table to get indexes right
        summary = first_step.merge(classes_filter, on=["Student Id", "Date"], how="inner")
        summary[f"Status{delim}Note"] = np.where(
            summary["Note"].str.strip() == "",
            summary["Status"],
            summary["Status"] + delim + '"' + summary["Note"] + '"',
        )
        summary["ClassIndex"] = summary.groupby(["Student Id", "Date"]).cumcount() + 1

        ## Create two pivot tables, one for notes and another for classes based on the summary
        notes_pivot = summary.pivot(
            index=["Student Id", "Date"],
            columns="ClassIndex",
            values=f"Status{delim}Note",
        ).reset_index()
        notes_pivot.columns = [
            f"StatusNote{col}" if isinstance(col, int) else col
            for col in notes_pivot.columns
        ]


        class_pivot = summary.pivot(
            index=["Student Id", "Date"], columns="ClassIndex", values="Class"
        ).reset_index()
        class_pivot.columns = [
            f"Class{col}" if isinstance(col, int) else col for col in class_pivot.columns
        ]


        # Count of how many classes: uses classes_df
        total_class_count = classes_df.groupby(['Student Id', 'Date']).size().reset_index(name='Total Classes')

        ## 
        merged_df = pd.merge(notes_pivot, class_pivot, on=["Student Id", "Date"])
        merged_df = pd.merge(merged_df, total_class_count)
        merged_df = merged_df.fillna("")



        ## Recombine from first step to finalize
        merged_df = merged_df.merge(first_step, on=on_, how="inner")
        max_index = max(
            [
                int(re.search(r"\d+", col).group())
                for col in merged_df.columns
                if "StatusNote" in col
            ]
        )
        extra_columns = [
            'Student Id',
            "Student Name",
            "Year Group",
            "Homeroom Advisor",
            "Grade",
            "Grade #",
            'Status',
            'Note'
        ]

        merged_df = merged_df.merge(
            homeroom_filter[extra_columns],
            on=["Student Id"],
            how='inner'
        )
        cols_order = (
            ["Date", *extra_columns[:-2], 'HR Summary', 'Total Classes']
            + [second_condition_column]
            + [
                item
                for sublist in [
                    (f"Class{i}", f"StatusNote{i}") for i in range(1, max_index + 1)
                ]
                for item in sublist
            ]
        )
        
        merged_df[f"HR Summary"] = np.where(
            merged_df["Note"].str.strip() == "",
            merged_df["Status"],
            merged_df["Status"] + delim + '"' + merged_df["Note"] + '"',
        )
        final = merged_df.drop('Status', axis=1)
        final = final.drop('Note', axis=1)
        final = final[cols_order]
        final['Student Id'] = final['Student Id'].apply(quote_specific_columns)
        final = final.drop_duplicates()

        final.to_csv(f"/tmp/{name}.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
        finals[name] = final
