import os
import smtplib
from email.message import EmailMessage
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def email_workbook( workbook_path: str | Path, subject: str, body: str):

    try:

        workbook_path = Path(workbook_path)

        recipients = [
            email.strip()
            for email in os.environ["EMAIL_TO"].split(",")
            if email.strip()
        ]

        msg = EmailMessage()
        msg["From"] = os.environ["EMAIL_USER"]
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.set_content(body)

        with open(workbook_path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=workbook_path.name,
            )

        with smtplib.SMTP("smtp.mail.me.com", 587) as server:
            server.starttls()
            server.login(
                os.environ["EMAIL_USER"],
                os.environ["EMAIL_PASS"],
            )
            server.send_message(msg)

        return True

    except Exception as e:
        print(f"Failed email with expection - {e}")
        return False
