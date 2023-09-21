from mbpy.cli.formatted_click import click
from mbpy.cli.formatted_click import RichClickGroup, RichClickCommand
from .workweek import WorkWeek
from .attendance_records import get_dates_subquery, get_classes_attendance_records
from mbpy.cli.contexts import ImportContext, pass_settings_context
from mbpy.cli.bulk import bulk_import_all
from mbpy.cli.importers import import_class_attendance_bydates
import pandas as pd
import numpy as np
from .utils import multi_index_readable
from .calculate import build_cumulative_status_is_active
from .utils import smtp_shared_options, command_shared_options
from mbpy.exchanges.smtp import message_exchange, smtp_exchange
import pathlib


@click.command("cumulative-class-attendance", cls=RichClickCommand)
@command_shared_options
@smtp_shared_options
@pass_settings_context
@click.pass_context
def cli(
    ctx,
    settings_obj,
    scope,
    date,
    work_week,
    import_,
    from_,
    to_,
    host,
    port,
    subject,
    body,
    password,
    tls,
    template,
    manual_statuses,
    absent_category_name,
    reports = True
):
    """Output for class attendance"""
    end_date = date
    ww = WorkWeek(work_week)
    if scope in ["weekly", "monthly"]:
        if scope == 'weekly':
            start_date = ww.first_day_of_week(end_date)
    elif scope == "daily":
        start_date = end_date
    else:
        raise NotImplemented

    start_date = ww.first_day_of_week(end_date)


    if import_:
        ctx.obj = ImportContext(incrementally=True, include_archived=True)
        ctx.invoke(bulk_import_all)
        ctx.invoke(
            import_class_attendance_bydates,
            start_date=start_date,
            end_date=end_date,
            weekends=ww.weekends,
        )

    subquery = get_dates_subquery(ww, start_date, end_date)

    with settings_obj.Session() as session:
        raw_df = get_classes_attendance_records(session, start_date, end_date, subquery)

    if reports:
        cumulative = build_cumulative_status_is_active(
            raw_df, absent_column_name=absent_category_name
        )
        for manual_status in manual_statuses:
            if manual_status not in cumulative.columns:
                cumulative[manual_status] = 0

        status_counts = (
            raw_df.rename(columns={"Student Id": "Count"})
            .pivot_table(
                index=["Status"],
                columns=["Grade", "Grade #"],
                values=["Count"],
                aggfunc="count",
                margins=True,
                margins_name="Total",
            )
            .fillna(0)
            .sort_values(by=["Status"], ascending=True)
        )
        status_counts = multi_index_readable(status_counts, sort_by=2)

        status_breakdown = (
            raw_df.rename(columns={"Student Id": "# students"})
            .pivot_table(
                index=["Date", "Status"],
                columns=["Grade", "Grade #"],
                values=["# students"],
                aggfunc="count",
                margins=True,
                margins_name="Total",
            )
            .fillna(0)
        )
        status_breakdown = multi_index_readable(status_breakdown, sort_by=2)

        absent_days = raw_df.loc[raw_df["Status"] == "Absent"].pivot(
            index=[
                "Student Id",
                "Student Name",
                "Class",
                "Grade",
                "Grade #",
                "Day",
                "Period",
            ],
            columns=["Date"],
            values=["Note"],
        )

        absent_days = (
            absent_days.replace({col: np.nan for col in absent_days.columns}, "-")
            .replace({col: "" for col in absent_days.columns}, '"Absent"')
            .sort_values(by=["Grade #", "Student Name"])
        )
        absent_days = multi_index_readable(absent_days, has_margins=False)

        raw_df["Summary"] = raw_df["Status"] + ' "' + raw_df["Note"] + '"'
        student_non_present_summary = raw_df.loc[raw_df["Status"] != "Present"].pivot(
            index=[
                "Student Id",
                "Student Name",
                "Class",
                "Grade",
                "Grade #",
                "Day",
                "Period",
            ],
            columns=["Date"],
            values=["Summary"],
        )
        student_non_present_summary = student_non_present_summary.replace(
            {col: np.nan for col in student_non_present_summary.columns}, "Present"
        ).sort_values(by=["Grade #", "Student Name"])
        student_non_present_summary = multi_index_readable(
            student_non_present_summary, has_margins=False
        )

        message = message_exchange(
            from_,
            to_,
            subject,
            body or "",
            attachments=[
                ("cumulative_absences", "df", cumulative.set_index("Student Id")),
                ("non_presents", None, student_non_present_summary),
                ("status_breakdown", None, status_breakdown),
                ("absent_days", None, absent_days),
                ("raw_data", None, raw_df),
            ],
            template=pathlib.Path(template) if template else None,
            start_date=start_date,
            end_date=end_date,
        )
        smtp_exchange(message, tls, host, port, from_, password)

    return raw_df
