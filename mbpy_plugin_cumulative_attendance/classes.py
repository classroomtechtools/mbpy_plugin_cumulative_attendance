from mbpy.cli.formatted_click import click
from datetime import datetime
from .workweek import WorkWeek
from mbpy.db.schema import YearGroup, Student, Class, ClassAttendanceByDate, Membership
from mbpy.cli.contexts import ImportContext, pass_settings_context
from mbpy.cli.bulk import bulk_import_all
from mbpy.cli.importers import import_class_attendance_bydates
import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy import and_
from .utils import multi_index_readable
from .calculate import build_cumulative_status_is_active
from .utils import shared_options
from .send_email import send_email
from mbpy.exchanges.smtp import message_exchange, smtp_exchange
import pathlib


@click.command("cumulative-class-attendance")
@click.option(
    "--scope", "scope", default="weekly", type=click.Choice(["weekly", "monthly"])
)  # TODO: termly
@click.option(
    "--date",
    "date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=datetime.today(),
)
@click.option(
    "--work-week", type=click.Choice(["mon-fri", "sun-thurs"]), default="mon-fri"
)
@click.option("-i", "--import/--skip-import", "import_", is_flag=True, default=True)
@shared_options
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
    absent_category_name
):
    """Output for class attendance"""
    end_date = date
    ww = WorkWeek(work_week)
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

    date_df = pd.DataFrame(
        pd.date_range(start=start_date, end=end_date), columns=["date"]
    )

    rows = [
        (dte.date(), dte.day_name(), not dte.dayofweek in ww.weekends)
        for dte in date_df["date"]
    ]

    stmts = [
        sa.select(
            *[
                sa.cast(sa.literal(d), sa.String).label("date"),
                sa.cast(sa.literal(y), sa.String).label("day"),
                sa.cast(sa.literal(b), sa.Boolean).label("bool"),
            ]
        )
        if idx == 0
        else sa.select(*[sa.literal(d), sa.literal(y), sa.literal(b)])
        for idx, (d, y, b) in enumerate(rows)
    ]
    subquery_table = sa.union_all(*stmts)

    subquery = subquery_table.cte(name="date_table")
    day_records = []

    with settings_obj.Session() as session:
        attendance_records = (
            session.query(Student, Class, subquery, ClassAttendanceByDate)
            .select_from(Student)
            .join(subquery, subquery.c.bool == True)
            .join(
                ClassAttendanceByDate,
                and_(
                    ClassAttendanceByDate.student_id == Student.id,
                    ClassAttendanceByDate.class_id == Class.id,
                    ClassAttendanceByDate.date == subquery.c.date,
                    ClassAttendanceByDate.status.is_not(None),
                ),
            )
            .join(
                Membership,
                and_(
                    Membership.class_id == ClassAttendanceByDate.class_id,
                    Membership.user_id == ClassAttendanceByDate.student_id,
                ),
            )
            .where(ClassAttendanceByDate.date >= start_date.date())
            .where(ClassAttendanceByDate.date <= end_date.date())
            .where(Membership.deleted_at.is_(None))
            .where(Class.archived == False)
            .where(subquery.c.bool == True)
        )

        for (
            student,
            class_,
            date,
            day,
            _,
            class_attendance,
        ) in attendance_records.all():
            day_record = {
                "Student Id": student.student_id,
                "Student Name": student.display_name,
                "Class": class_.name,
                "Grade": student.class_grade,
                "Grade #": student.class_grade_number - 1,
                "Program": class_.program_code,
                "Date": date,
                "Day": day,
                "Period": class_attendance.period,
                "Status": class_attendance.status,
                "Note": class_attendance.note,
            }
            day_records.append(day_record)

        raw_df = pd.DataFrame.from_records(day_records)

        cumulative = build_cumulative_status_is_active(raw_df)
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
            index=["Student Id", "Student Name", "Class", "Grade", "Grade #"],
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
            index=["Student Id", "Student Name", "Class", "Grade", "Grade #"],
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
                ("raw_data", None, raw_df)
            ],
            template=pathlib.Path(template) if template else None,
            start_date=start_date,
            end_date=end_date,
        )
        smtp_exchange(message, tls, host, port, from_, password)
