import time
import json
import smtplib
import requests
import configparser
import pandas as pd
from datetime import datetime, date, timedelta
from requests.auth import HTTPBasicAuth
from ldap3 import Server, Connection, SUBTREE, ALL_ATTRIBUTES, MODIFY_REPLACE


class AD:
    def __init__(self):
        self.data = None
        self.employee_data = []
        self.incomplete_data = []
        self.successful_list = []
        self.failed_list = []
        self.completed_list = []
        self.incomplete_list = []
        # config = configparser.ConfigParser()
        # config.read('C:/Users/sourabh.kulkarni/PycharmProjects/prodAD/config.ini')
        # self.entity = config.get('Entity', 'entity_name')
        # self.entity_domain = config.get('Entity', 'entity_domain')
        # self.ldap_host = config.get('LDAP', 'host')
        # self.ldap_port = int(config.get('LDAP', 'port'))
        # self.ldap_username = config.get('LDAP', 'username')
        # self.ldap_password = config.get('LDAP', 'password')
        # self.ldap_search_base = config.get('LDAP', 'search_base')
        self.user_name = "payu_api"
        self.password = "3MzkqYPQJjNvQTX$By8km3Mm@4qdK8IHHYej3&QCav1g@imXh&MqonZSOVAAN9UN"
        self.pending_api_key = "f90935aea88d9d2831cc67ed5874f5d3543a06f62ed0369d8c8b1408515d589459133f3aa0b26b7841f68391003a9de131f7e371bb3eebc09d9e50ec12b04e04"
        self.pending_dataset_key = "376e4798b1ec132a4cd24b2d88b203ce2f9ccba9d085b0f9220421e7e2090d7049edf6819ca4ea6a11e5166c88f273ea9ea89f07737dc4581d59a74c871ed79d"
        self.smtp_server = "smtp.office365.com"
        self.smtp_port = 587
        self.sender = "HRMS.Notification@payu.in"
        self.server_password = "Joz44358"
        self.error_email = "sourabh.kulkarni@payu.in"
        # self.email_list = config.get('Email', 'email_list').split(",")
        # to_address = []
        # for email in self.email_list:
        #     to_address.append("<" + email + ">")
        # self.emails = ", ".join(to_address)

    def message_creation(self, to_address, subject, body):
        message = f"""From: HRMS Notification <HRMS.Notification@payu.in>\nTo:{to_address}
        \nContent-type: text/html\nSubject: {subject}\n\n{body}"""
        return message

    def send_mail(self, message, to_address):
        message = message.encode('utf8')
        try:
            smtpserver = smtplib.SMTP(self.smtp_server, self.smtp_port)
            smtpserver.starttls()
            smtpserver.login(self.sender, self.server_password)
            smtpserver.sendmail(self.sender, to_address, message)
        except smtplib.SMTPException as e:
            print(e)

    def get_darwin_data(self):
        try:
            url = "https://payu.darwinbox.in/masterapi/employee"
            api_key = self.pending_api_key
            datasetKey = self.pending_dataset_key
            date_of_activation = date.today()
            doa = date_of_activation - timedelta(days=1)
            date_of_activation = doa.strftime("%d-%m-%Y 17:00:00")
            # date_of_activation = "10-4-2023"
            print("Getting onboarding employee data from Darwin")
            body = json.dumps({"api_key": api_key, "datasetKey": datasetKey, "last_modified": date_of_activation})
            response = requests.get(url, auth=HTTPBasicAuth(self.user_name, self.password), data=body, verify=False)
            if response.status_code == 200:
                result = response.json()
                if result["status"] == 1:
                    self.data = result["employee_data"]
                else:
                    self.data = {}
            else:
                raise Exception(
                    f"Darwin API has given error while fetching new joining employee data <br>response status code: <b>{response.status_code}</b> <br>error: <b>{response.text}</b>")
        except Exception as e:
            to_address = "<" + self.error_email + ">"
            subject = "Darwin API error while fetching new joinee data"
            body = f"""
            Hi all, <br>
            {str(e)} <br>
            Kindly test the API credentials for more details.
            """
            message = self.message_creation(to_address, subject, body)
            to_address = [self.error_email]
            self.send_mail(message, to_address)

    def send_mail_notification(self):
        try:
            if self.data:
                completed_df = pd.json_normalize(self.data)
                subject = "Email ID created"
                body = f"""
                           Hi all,<br><br>
                           Below mentioned employees email ID have been created today.
                           <br><br>
                           {completed_df.to_html()} <br>
                           <br><br>
                           Thanks and Regards,<br>
                            HRMS Portal"""
                to_address = self.error_email
                # body = body.replace(u"\u2013", "-")
                message = self.message_creation(to_address, subject, body)
                to_address = self.error_email
                self.send_mail(message, to_address)
        except Exception as e:
            to_address = "<" + self.error_email + ">"
            subject = "Darwin API error while sending email to stakeholders"
            body = f"""
            Hi all,<br>

            {str(e)}<br>
            Darwin failed to update the below mentioned employee list:<br>
            str({self.failed_list})<br>

            Kindly look at the API for more details.<br>
                    """
            message = self.message_creation(to_address, subject, body)
            to_address = [self.error_email]
            self.send_mail(message, to_address)


if __name__ == '__main__':
    start = time.time()
    ad = AD()
    # Get details from Darwin API
    ad.get_darwin_data()
    # Validate the data if it is in the right format
    # ad.validate_data()
    # create a sample email id and check if it already exists
    # ad.create_email_ids()
    # Now send the email id back to Darwin
    ad.send_mail_notification()

    print(time.time() - start)
