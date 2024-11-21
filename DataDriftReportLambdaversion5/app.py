import json
import pandas as pd
from evidently.metric_preset import DataDriftPreset
from evidently.report import Report
import boto3
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Initialize AWS clients
s3_client = boto3.client('s3')
ses_client = boto3.client('ses', region_name='us-east-1')  # Replace with your SES region

# Updated S3 bucket configuration
CUR_DATA_BUCKET = "dq6-cur-data-s3"
REF_DATA_BUCKET = "dq6-ref-data-s3"
REPORTS_BUCKET = "dq6-reports-s3"

def load_data_from_s3(bucket: str, key: str):
    """
    Load data from S3 using the provided bucket and key.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = pd.read_csv(response['Body'])
    return data

def generate_drift_report(reference_data: pd.DataFrame, current_data: pd.DataFrame):
    """
    Generate the data drift report using Evidently AI.
    """
    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=reference_data, current_data=current_data)
    return report

def save_report_to_s3(report: Report, bucket: str, key: str):
    """
    Save the generated HTML report to the S3 bucket.
    """
    html_report = report.get_html()
    s3_client.put_object(Bucket=bucket, Key=key, Body=html_report, ContentType='text/html')
    return html_report

def send_email_with_attachment(to_email: str, subject: str, body: str, attachment: bytes, attachment_name: str):
    """
    Send an email with an attachment using SES.
    """
    # Create a MIME message
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = "sagarpatiler@gmail.com"  # Replace with your verified SES email
    msg['To'] = to_email

    # Attach email body
    msg.attach(MIMEText(body, 'plain'))

    # Attach the report
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment)
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename={attachment_name}')
    msg.attach(part)

    # Send the email using SES
    response = ses_client.send_raw_email(
        Source=msg['From'],
        Destinations=[msg['To']],
        RawMessage={'Data': msg.as_string()}
    )
    return response

def lambda_handler(event, context):
    """
    Lambda function handler to detect data drift, save the report to S3, and send the report via SES.
    Triggered when a file is uploaded to the "dq6-cur-data-s3" bucket.
    """
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']

        # Ensure the event is from the correct bucket
        if bucket_name == CUR_DATA_BUCKET:
            print(f"Processing file: {object_key} from bucket: {bucket_name}")
            
            # Define S3 paths
            ref_data_key = 'ref.csv'

            # Load the reference data and the current data (uploaded file)
            reference_data = load_data_from_s3(REF_DATA_BUCKET, ref_data_key)
            current_data = load_data_from_s3(bucket_name, object_key)

            # Generate the data drift report
            report = generate_drift_report(reference_data, current_data)

            # Generate a unique name for the report using UUID
            report_key = f"data_drift_report_{uuid.uuid4().hex}.html"

            # Save the report to S3 and get the HTML content
            html_report = save_report_to_s3(report, REPORTS_BUCKET, report_key)

            # Send the report via email
            email_response = send_email_with_attachment(
                to_email="sagarpatiler@gmail.com",  # Replace with the recipient's email
                subject="Data Quality Report has been generated",
                body="Please find the attached Data Quality Report.",
                attachment=html_report.encode('utf-8'),
                attachment_name="data_quality_report.html"
            )

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Data drift detected, report generated, saved to S3, and emailed successfully',
                    'report_key': report_key,
                    'email_response': email_response
                })
            }
        else:
            print(f"File {object_key} is not in the expected bucket ({CUR_DATA_BUCKET}), skipping.")

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'No files processed from the dq6-cur-data-s3 bucket.'})
    }
