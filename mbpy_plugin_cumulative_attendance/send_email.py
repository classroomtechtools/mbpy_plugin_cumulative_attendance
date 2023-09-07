from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from rich.console import Console
import smtplib
import pandas as pd

import io, ssl


def export_csv(df):
    with io.StringIO() as buffer:
        df.to_csv(buffer, index=False)
        return buffer.getvalue()


def send_email(from_, send_to, subject, body, password, *dataframes):
    if not len(send_to):
        console = Console()
        for name, df in dataframes:
            with pd.option_context('display.max_rows', None):
                console.print(df)
        return

    multipart = MIMEMultipart()

    multipart["From"] = from_
    multipart["To"] = ",".join(send_to)
    multipart["Subject"] = subject

    for filename, df in dataframes:
        attachment = MIMEApplication(export_csv(df), Name=filename)
        attachment["Content-Disposition"] = f'attachment; filename="{filename}"'
        multipart.attach(attachment)

    multipart.add_header("Content-Type", "text/plain")
    multipart.attach(MIMEText(body, "plain"))

    context = ssl.create_default_context()
    data = multipart.as_bytes()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as email:
        email.login(from_, password)
        email.sendmail(from_, send_to, data)
