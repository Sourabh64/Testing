import pandas
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

app_list = ['Apiplayground', 'Application', 'Async', 'B2B', 'Central-ops-app-01', 'Central-ops-app-02', 'Checkoutx', 'Coherence-Backend', 'Compulsive', 'daas-admin-app', 'Dev-Ssot-Airflow-App-01', 'Dev-Ssot-ML-Analytics-App-01', 'Dev-Ssot-ML-Analytics-App-02', 'Dynatrace', 'Eftnet', 'Enach', 'FBRI-SLA-01-azn', 'gitlab', 'GlobalPayments', 'GlobalPaymentsIntegration', 'IndigoOmni', 'inquiry-data', 'InternalInfo', 'Invoicing', 'IrctcIndus', 'Jocata', 'Jumphost', 'marvel-client', 'ML-Analytics-New-01', 'ML-CreditAnalytics-app01', 'Morpheus', 'OmniChannel', 'OneAdmin', 'Payout-Recon', 'payuDB-slave', 'prod-asi-admin--asg', 'prod-asi-app--asg', 'prod-jkcc-app1', 'Prod-obriskscore-app-new01', 'Prod-PgSelector-Logstash', 'Prod-SwitchPay-Logstash', 'ProdResellerAmazonLinuxApp', 'ProdResellerAmazonLinuxBG', 'ProxyService01-azn', 'ProxyService02-azn', 'Recommendationengine', 'redash', 'redash-amz-linux', 'redash-new', 'refund-scala', 'RIntegration', 'RiskMicroservice', 'rto-admin', 'Sarc', 'Securepayments', 'Securepayments-PythonJob', 'SFTP01', 'slave-db-all-application', 'SMB-Analytics', 'Snorkel', 'Snorkel-HDFC', 'SolutionsLogBackup', 'sonarqube', 'Srt-monitor', 'Storecard', 'Trident-Maxwell-payuhash01-amz-2023', 'Unisource', 'Whatsapp', 'Zion']

df = pd.read_excel('Scan_Report_All_Internal_VA_Q4_2024_2025_20250124.xlsx', sheet_name='Vulnerabilities')

df['First Detected'] = pd.to_datetime(df['First Detected'])
df['Aging'] = (datetime.today()-df['First Detected']).dt.days
bins = [0, 30, 60, 90, float('inf')]
labels = ['0-30 days', '31-60 days', '61-90 days', '>90 days']
df['Aging Bucket'] = pd.cut(df['Aging'], bins=bins, labels=labels, right=False)
# filtered_df = df[df['Application name'].isin(app_list)]
grouped = df.groupby(['Application name', 'Severity', 'Aging Bucket']).size().reset_index(name='Count')

pivot_df = grouped.pivot_table(index=['Application name', 'Aging Bucket'], columns='Severity', values='Count', fill_value=0)




# severity_order = [5, 4, 3, 2, 1]
# grouped = grouped[[col for col in severity_order if col in grouped.columns]]
#
# grouped.plot(kind='bar', stacked=True, figsize=(10, 6), colormap='tab10')
# plt.title('Application-wise Severity count')
# plt.xlabel('Application Name')
# plt.ylabel('Count')
# plt.legend(title='Severity')
# plt.tight_layout()
# plt.show()
# app_counts = filtered_df['Application name'].value_counts()
# print(app_counts)
