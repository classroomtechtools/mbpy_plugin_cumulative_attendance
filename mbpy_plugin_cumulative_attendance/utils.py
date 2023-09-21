import click
import datetime

def multi_index_readable(df, sort_by=1, show_index=1, has_margins=True):
    """
    Sort multi index dataframe, and collapse to one row
    """
    cols = df.columns
    if has_margins:
        cols, margin = cols[:-1], cols[-1:]
    else:
        cols = [c for c in cols]
    new_cols = [
        "/".join([c[i] for i in show_index])
        if isinstance(show_index, tuple)
        else c[show_index]
        for c in sorted(cols, key=lambda i: i[sort_by])
    ] + (["Total"] if has_margins else [])
    df.columns = new_cols
    return df


def command_shared_options(fn):
    fn = click.option(
        "--scope",
        "scope",
        default="weekly",
        type=click.Choice(["weekly", "monthly", "daily"]),
    )(fn)
    fn = click.option(
        "--date",
        "date",
        type=click.DateTime(formats=["%Y-%m-%d"]),
        default=datetime.datetime.today(),
    )(fn)
    fn = click.option(
        "--work-week", type=click.Choice(["mon-fri", "sun-thurs"]), default="mon-fri"
    )(fn)
    fn = click.option(
        "-i", "--import/--skip-import", "import_", is_flag=True, default=True
    )(fn)
    fn = click.option("--absent-category-name", 'absent_category_name', default="Absent")(fn)
    return fn


def smtp_shared_options(fn):
    fn = click.option("--from", "from_", envvar="MBPY_SMTP_FROM", show_envvar=True)(fn)
    fn = click.option("--to", "to_", multiple=True)(fn)
    fn = click.option("--host", envvar="MBPY_SMTP_HOST", show_envvar=True)(fn)
    fn = click.option("--port", envvar="MBPY_SMTP_PORT", show_envvar=True)(fn)
    fn = click.option("--subject", "subject", envvar="MBPY_SMTP_SUBJECT")(fn)
    fn = click.option("--body", "body")(fn)
    fn = click.option(
        "--password", "password", envvar="MBPY_SMTP_PASSWORD", show_envvar=True
    )(fn)
    fn = click.option(
        "--tls", "tls", envvar="MBPY_SMTP_USE_TLS", is_flag=True, show_envvar=True
    )(fn)
    fn = click.option(
        "--template",
        "template",
        type=click.Path(exists=True),
        help="Path to the template",
    )(fn)
    fn = click.option("--manual-status", "manual_statuses", multiple=True)(fn)
    return fn
