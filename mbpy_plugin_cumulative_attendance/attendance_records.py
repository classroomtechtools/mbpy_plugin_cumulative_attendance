import pandas as pd
from mbpy.db.schema import Student, Class, ClassAttendanceByDate, Membership
import sqlalchemy as sa
from sqlalchemy import and_


def get_classes_attendance_records(session, start_date, end_date, dates_subquery):
    day_records = []
    attendance_records = (
        session.query(Student, Class, dates_subquery, ClassAttendanceByDate)
        .select_from(Student)
        .join(dates_subquery, dates_subquery.c.bool == True)
        .join(
            ClassAttendanceByDate,
            and_(
                ClassAttendanceByDate.student_id == Student.id,
                ClassAttendanceByDate.class_id == Class.id,
                ClassAttendanceByDate.date == dates_subquery.c.date,
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
        .where(dates_subquery.c.bool == True)
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

    return pd.DataFrame.from_records(day_records)


def get_dates_subquery(ww, start_date, end_date):
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

    return subquery_table.cte(name="date_table")
