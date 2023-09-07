import pandas as pd


def build_cumulative_status_is_active(
    df: pd.DataFrame, absent_column_name="Absent", unique=None
):
    """ Count how many """
    if df.empty:
        return df

    if unique is None:
        unique = ["Student Id", "Student Name", "Grade", "Grade #"]

    indexes = [df[col] for col in unique]

    status_counts = pd.crosstab(index=indexes, columns=df['Status']).reset_index()
    status_counts = status_counts.sort_values(by=[absent_column_name, "Grade #"], ascending=False)
    return status_counts
