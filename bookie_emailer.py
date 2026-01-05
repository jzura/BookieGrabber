import os
import smtplib
import logging
from email.message import EmailMessage
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("bookie_grabber.email")


def email_workbook(workbook_path: str | Path, subject: str, body: str) -> bool:
    workbook_path = Path(workbook_path)

    try:
        # -----------------------------
        # Parse recipients
        # -----------------------------
        recipients = [
            email.strip()
            for email in os.environ["EMAIL_TO"].split(",")
            if email.strip()
        ]

        logger.info(f"Preparing email to: {recipients}")
        logger.info(f"Workbook to attach: {workbook_path}")

        if not recipients:
            logger.error("No recipients found in EMAIL_TO env variable.")
            return False

        # -----------------------------
        # Build email
        # -----------------------------
        msg = EmailMessage()
        msg["From"] = os.environ["EMAIL_USER"]
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.set_content(body)

        with open(workbook_path, "rb") as f:
            file_bytes = f.read()
            msg.add_attachment(
                file_bytes,
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=workbook_path.name,
            )

        logger.info(f"Attached file '{workbook_path.name}' ({len(file_bytes)} bytes)")

        # -----------------------------
        # Send email via iCloud SMTP
        # -----------------------------
        logger.info("Connecting to iCloud SMTP server...")
        with smtplib.SMTP("smtp.mail.me.com", 587, timeout=15) as server:
            server.starttls()
            logger.info("Started TLS session")

            server.login(os.environ["EMAIL_USER"], os.environ["EMAIL_PASS"])
            logger.info("Logged into SMTP successfully")

            server.send_message(msg)
            logger.info(f"Email successfully sent to {recipients}")

        return True

    except Exception as e:
        logger.exception(f"Email sending failed: {e}")
        return False
