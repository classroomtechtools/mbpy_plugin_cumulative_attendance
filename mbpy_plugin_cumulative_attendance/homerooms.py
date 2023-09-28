from mbpy.cli.formatted_click import click
from mbpy.cli.formatted_click import RichClickGroup, RichClickCommand
from .workweek import WorkWeek
from mbpy.db.schema import YearGroup, Student, HRAttendanceByDate, Teacher
from mbpy.cli.contexts import ImportContext, pass_settings_context
from mbpy.cli.bulk import bulk_import_all
from mbpy.cli.importers import import_homeroom_attendance_bydates
import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy import and_
from .utils import multi_index_readable
from .calculate import build_cumulative_status_is_active
from .utils import smtp_shared_options, command_shared_options
from .send_email import send_email
from mbpy.exchanges.smtp_exchange import message_exchange, smtp_exchange
import pathlib
from sqlalchemy.orm import aliased


@click.command("cumulative-hr-attendance", cls=RichClickCommand)
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
    """Output for homeroom attendance"""
    end_date = date
    ww = WorkWeek(work_week)
    if scope in ["weekly", "monthly"]:
        if scope == 'weekly':
            start_date = ww.first_day_of_week(end_date)
    elif scope == "daily":
        start_date = end_date
    else:
        raise NotImplemented

    if import_:
        ctx.obj = ImportContext(incrementally=True, include_archived=True)
        ctx.invoke(bulk_import_all)
        ctx.invoke(
            import_homeroom_attendance_bydates,
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
        ##
        # Get attendance record for every date from within the range found
        TeacherAlias = aliased(Teacher, flat=True)

        attendance_records = (
            session.query(YearGroup, Student, TeacherAlias, subquery, HRAttendanceByDate)
            .join(Student, Student.year_group_id == YearGroup.id)
            .join(TeacherAlias, Student.homeroom_advisor_id == TeacherAlias.id)
            .join(subquery, subquery.c.bool == True)
            .join(
                HRAttendanceByDate,
                and_(
                    HRAttendanceByDate.student_id == Student.id,
                    HRAttendanceByDate.year_group_id == YearGroup.id,
                    HRAttendanceByDate.date == subquery.c.date,
                    HRAttendanceByDate.status.is_not(None),
                ),
            )
        )
        #

        for (
            year_group,
            student,
            homeroom_teacher,
            date,
            day,
            _,
            hr_attendance,
        ) in attendance_records.all():
            day_record = {
                "Student Id": student.student_id,
                "Student Name": student.display_name,
                "Year Group": year_group.name,
                "Homeroom Advisor": homeroom_teacher.full_name,
                "Grade": student.class_grade,
                "Grade #": student.class_grade_number - 1,
                "Program": year_group.program,
                "Date": date,
                "Day": day,
                "Status": hr_attendance.status,
                "Note": hr_attendance.note,
            }
            day_records.append(day_record)

        raw_df = pd.DataFrame.from_records(day_records)
        if raw_df.empty:
            print('no records!')
            return

        if not reports: return raw_df

        cumulative = build_cumulative_status_is_active(raw_df, absent_column_name=absent_category_name)
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
                columns=["Grade", "Grade #", "Homeroom Advisor"],
                values=["# students"],
                aggfunc="count",
                margins=True,
                margins_name="Total",
            )
            .fillna(0)
        )
        status_breakdown = multi_index_readable(status_breakdown, sort_by=2)

        absent_days = raw_df.loc[raw_df["Status"] == "Absent"].pivot(
            index=["Student Id", "Student Name", "Grade", "Grade #", "Homeroom Advisor"],
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
            index=["Student Id", "Student Name", "Grade", "Grade #", "Homeroom Advisor"],
            columns=["Date"],
            values=["Summary"],
        )
        student_non_present_summary = student_non_present_summary.replace(
            {col: np.nan for col in student_non_present_summary.columns}, "Present"
        ).sort_values(by=["Grade #", "Student Name"])
        student_non_present_summary = multi_index_readable(
            student_non_present_summary.reset_index().set_index('Student Id'), has_margins=False
        )

        message = message_exchange(
            from_,
            to_,
            subject,
            body or "",
            attachments=[
                ("cumulative_absences", "df", cumulative.set_index("Student Id")),
                ("not_present", "not_present", student_non_present_summary),
                ("status_breakdown", None, status_breakdown),
                ("absent_days", None, absent_days)
            ],
            template=pathlib.Path(template) if template else None,
            start_date=start_date,
            end_date=end_date,
        )
        smtp_exchange(message, tls, host, port, from_, password)
