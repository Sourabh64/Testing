import pyspark

import os
import msal
import io
import requests
import pandas as pd
from zipfile import BadZipFile
import pyarrow
import pandas.errors
from pandas import json_normalize

import sys
import time
# from awsglueml.transforms import EntityDetector
from pyspark.sql.types import ArrayType, StringType, FloatType, DoubleType, StructType, StructField
# from awsglue.dynamicframe import DynamicFrame
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from dateutil.parser import parse
import pytz
import pyspark.sql.functions as sf
from pyspark.sql import DataFrame
from functools import reduce
import os
from datetime import datetime
from urllib.parse import urlparse
import tempfile
import json
import re
import math
import requests
import pandas as pd
from functools import reduce
import base64
from datetime import datetime

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


def get_template(s3, bucket_name, object_name):
    """
    get_template is used to download regex template file from bucket name and
    use json to load the contents.

    Args:
        s3: The boto3 client used to download the template
        bucket_name: The bucket name to download template file from.
        object_name: The path of regex template file in bucket_name.

    Returns:
        the regex templates as a python dict.
    """
    # print("bucket name: ",bucket_name)
    # print("object name: ",object_name)

    with tempfile.TemporaryFile() as data:
        s3.download_fileobj(bucket_name, object_name, data)
        data.seek(0)
        return json.loads(data.read().decode('utf-8'))


class Verhoeff:
    # The multiplication table
    d = [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
         [1, 2, 3, 4, 0, 6, 7, 8, 9, 5],
         [2, 3, 4, 0, 1, 7, 8, 9, 5, 6],
         [3, 4, 0, 1, 2, 8, 9, 5, 6, 7],
         [4, 0, 1, 2, 3, 9, 5, 6, 7, 8],
         [5, 9, 8, 7, 6, 0, 4, 3, 2, 1],
         [6, 5, 9, 8, 7, 1, 0, 4, 3, 2],
         [7, 6, 5, 9, 8, 2, 1, 0, 4, 3],
         [8, 7, 6, 5, 9, 3, 2, 1, 0, 4],
         [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]]

    # The permutation table
    p = [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
         [1, 5, 7, 6, 2, 8, 3, 0, 9, 4],
         [5, 8, 0, 3, 7, 9, 6, 1, 4, 2],
         [8, 9, 1, 6, 0, 4, 3, 5, 2, 7],
         [9, 4, 5, 3, 1, 2, 6, 8, 7, 0],
         [4, 2, 8, 6, 5, 7, 3, 9, 0, 1],
         [2, 7, 9, 3, 8, 0, 6, 4, 1, 5],
         [7, 0, 4, 6, 9, 1, 3, 2, 5, 8]]

    # The inverse table
    inv = [0, 4, 3, 2, 1, 5, 6, 7, 8, 9]

    @staticmethod
    def checksum(number):
        """Calculate the Verhoeff checksum over the provided number. The checksum is returned as an int."""
        c = 0
        num = list(map(int, str(number)))
        for i, n in enumerate(reversed(num)):
            c = Verhoeff.d[c][Verhoeff.p[i % 8][n]]
        return Verhoeff.inv[c]

    @staticmethod
    def validate(number):
        """Validate the number with its checksum digit. Returns True if valid, False otherwise."""
        return Verhoeff.checksum(str(number)) == 0


def luhn_check(card_number):
    # Reverse the card number and convert it to a list of integers
    digits = [int(d) for d in str(card_number)][::-1]

    # Double every second digit from the right (now every odd index because we reversed the list)
    for i in range(1, len(digits), 2):
        doubled_digit = digits[i] * 2

        # If doubling the digit results in a number greater than 9, subtract 9 from it
        if doubled_digit > 9:
            doubled_digit -= 9

        digits[i] = doubled_digit

    # Sum all the digits
    total = sum(digits)

    # If the total modulo 10 is equal to 0, the number is valid according to Luhn's Algorithm
    return total % 10 == 0


def validate_abn(abn):
    # ABN should be a string of 11 digits without any spaces
    if len(abn) != 11 or not abn.isdigit():
        return False

    # Weights used in the checksum calculation
    weights = [10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19]

    # Subtract 1 from the first digit (as per the ABN validation rules)
    adjusted_digits = [int(abn[0]) - 1] + [int(d) for d in abn[1:]]

    # Calculate the weighted sum
    weighted_sum = sum(weight * digit for weight, digit in zip(weights, adjusted_digits))

    # Check if the weighted sum modulo 89 equals 0
    return weighted_sum % 89 == 0


def validate_acn(acn):
    if len(acn) != 9 or not acn.isdigit():
        return False

    weights = [8, 7, 6, 5, 4, 3, 2, 1]
    total = sum(int(num) * weight for num, weight in zip(acn[:-1], weights))
    check_digit = (10 - (total % 10)) % 10

    return check_digit == int(acn[-1])


def check_nhs_number(nhs_number):
    """
    Checks if the given NHS number is valid using the Modulus 11 algorithm.

    :param nhs_number: str, a 10-digit NHS number as a string
    :return: bool, True if the NHS number is valid, False otherwise
    """
    # Ensure the input is a string of 10 digits
    if not (nhs_number.isdigit() and len(nhs_number) == 10):
        return False

    # Weights used for each digit in the calculation (standard for NHS numbers)
    weights = [10, 9, 8, 7, 6, 5, 4, 3, 2]

    # Calculate the sum of weighted digits
    total = sum(int(nhs_number[i]) * weights[i] for i in range(9))

    # Calculate the remainder
    remainder = total % 11

    # Check digit is calculated as 11 minus the remainder
    check_digit = (11 - remainder) % 11

    # If the check digit is 10, it is invalid; it should be 0 instead
    if check_digit == 10:
        return False

    # The last digit of the number should match the check digit
    return int(nhs_number[-1]) == check_digit


def is_alphabets_and_spaces_only(s):
    # Regex pattern to match only letters and spaces
    return bool(re.fullmatch(r'[A-Za-z ]+', s))


class ColumnDetector:
    """
    ColumnDetector is used to detect entities in a column.
    """

    def __init__(self, broadcast_template):
        self.broadcast_template = broadcast_template

    def detect_column(self, col_val, column_name):
        """
        detect_column serves as a function to construct a udf
        to detect entities inside a dataframe.

        sdps_ner predict() sample output:
        sdps_ner.predict('Mike James') = {'Mike James': [{'Identifier': 'ENGLISH-NAME', 'Score': 0.99523854}]}

        Args:
            col_val: The value in a cell to be detected
            column_name: The name of a column

        Returns:
            result = [{'identifier': 'CHINESE-NAME', 'score': 0.5},
                {'identifier': 'ENGLISH-NAME', 'score': 0.4}]
        """
        # default_threshold_score = 0.70
        # threshold_dict = {"PERSON NAME":0.80}
        # print(str(col_val))
        result = []
        if col_val is None or col_val == "":
            return result

        identifiers = self.broadcast_template.value.get('identifiers')

        # iterate through all identifiers(including Regex and ML) to detect entities in col_val
        for identifier in identifiers:
            # Get identifier and skip Glue identifier when identifier type is 2
            identifier_type = identifier.get('type', -1)
            if identifier_type == 2:
                continue

            header_keywords = identifier.get('header_keywords', [])
            # print("header_keywords: ",header_keywords)
            score = 0
            valid_column_header = False

            # column header is valid when no specific column header required for this identifier.
            if not header_keywords or len(header_keywords) == 0:
                valid_column_header = True
            else:
                for keyword in header_keywords:
                    if re.search(keyword, column_name, re.IGNORECASE):
                        valid_column_header = True
                        break
            # Only perform regex matching when this column has valid column header.
            if valid_column_header:
                # Regex matching when identifier classification is 1
                if identifier['classification'] == 1:
                    if identifier['rule']:
                        col_val = str(col_val).strip()
                        if re.search(identifier['rule'], col_val):
                            if identifier['name'] == "AADHAR NUMBER":
                                col_val_temp = str(col_val)
                                col_val_temp = col_val_temp.replace(" ", "").replace("-", "")
                                if Verhoeff.validate(col_val_temp) is False:
                                    score = 0
                                else:
                                    score = 1
                            elif identifier['name'] == "BANK ACCOUNT NUMBER":
                                if luhn_check(str(col_val)) is True:
                                    score = 1
                                else:
                                    score = 0
                            elif identifier['name'] == "CREDIT CARD NUMBER":
                                col_val_temp = str(col_val)
                                col_val_temp = col_val_temp.replace(" ", "").replace("-", "")
                                if luhn_check(col_val_temp) is True:
                                    score = 1
                                else:
                                    score = 0
                            elif identifier['name'] == "UK_NATIONAL_HEALTH_SERVICE_NUMBER":
                                if check_nhs_number(str(col_val)) is True:
                                    score = 1
                                else:
                                    score = 0
                            elif identifier['name'] == "AUSTRALIA_BUSINESS_NUMBER":
                                if validate_abn(str(col_val)) is True:
                                    score = 1
                                else:
                                    score = 0
                            elif identifier['name'] == 'AUSTRALIA_COMPANY_NUMBER':
                                if validate_acn(str(col_val)) is True:
                                    score = 1
                                else:
                                    score = 0
                            else:
                                score = 1
                    else:
                        score = 1


                # Column name matching when classification is 3
                elif identifier['classification'] == 3:
                    # print("Inside classification rule 3")
                    if identifier['name'] == "DATE OF BIRTH":
                        if str(column_name).lower() in header_keywords:
                            score = 1
                        else:
                            score = 0
                    elif identifier['name'] == "USERNAME":
                        # print("USERNAME")
                        if str(column_name).lower() in header_keywords:
                            score = 1
                        else:
                            score = 0
                    elif identifier['name'] == "CVV":
                        # print("CVV")
                        if (str(column_name).lower() in header_keywords) and (str(col_val).strip().isdigit()) and (
                                len(str(col_val).strip()) in [3, 4]):
                            score = 1
                        else:
                            score = 0
                    elif identifier['name'] == "PASSWORD":
                        # print("PASSWORD")
                        if str(column_name).lower() in header_keywords:
                            score = 1
                        else:
                            score = 0
                    elif identifier['name'] == "MERCHANT_ID":
                        if str(column_name).lower() in header_keywords:
                            score = 1
                        else:
                            score = 0
                    elif identifier['name'] == "PERSON_NAME":
                        if is_alphabets_and_spaces_only(str(col_val)):
                            score = 1
                        else:
                            score = 0

                    elif identifier['name'] == "ADDRESS":
                        if str(column_name).lower() in header_keywords:
                            score = 1
                        else:
                            score = 0

                    elif identifier['name'] == "CITY":
                        if str(column_name).lower() in header_keywords:
                            score = 1
                        else:
                            score = 0
                    elif identifier['name'] == "COUNTRY":
                        if str(column_name).lower() in header_keywords:
                            score = 1
                        else:
                            score = 0

                    elif identifier['name'] == "SAD":
                        if str(column_name).lower() in header_keywords:
                            score = 1
                        else:
                            score = 0

            result.append({'identifier': identifier['name'], 'score': float(score)})

        return result

    def create_detect_column_udf(self):
        # Create a udf to detect entities in a column
        detect_column_udf = sf.udf(self.detect_column, ArrayType(
            StructType([StructField('identifier', StringType()), StructField('score', FloatType())])))
        return detect_column_udf


def sample_data(df, column_name, limit):
    """
    sample_data performs sampling on a column and creates a new column in the df with sample data.
    This function is currently not used.

    Args:
        df: the df to provide sample data.
        column_name: the name of original df to collect all the data as a list.
        limit: The number of rows to display as sample data in the df.

    Returns:
        returns the sampled data defined by limit in column_name in df.
    """
    return df.limit(limit).select(sf.collect_list(column_name).alias('sample_data')).collect()[0]['sample_data']


def mask_data(col_val):
    """
    This mask_data is used to created a udf for masking data.
    The input is a list of strings. The column to be detected is a column of lists.
    If a string is longer than 100, we display the first 80 characters,
    and display the following characters using * (at most 20 *)
    """

    def mask_string(s):
        return s
        length = len(s)
        first_80_percent = math.floor(length * 0.8)
        display_length = min(first_80_percent, 80)
        masked_length = min(20, length - display_length)
        return s[:display_length] + '*' * (masked_length)

    return [mask_string(s) for s in col_val]


def create_mask_data_udf():
    mask_data_udf = sf.udf(mask_data, ArrayType(StringType()))
    return mask_data_udf


def sdps_entity_detection(df, threshold, detect_column_udf):
    """
    sdps_entity_detection function aims to perform entity detection in SDPS.

    Args:
        df: the df to be detected.
        threshold: the threshold to filter out the results with score less than thresholdFraction.
        detect_column_udf: the udf to be used for entity detection.

    Returns:
        result_df: the df with column level SDPS detection results."""

    print("Inside sdps_entity_detection")

    rows = df.count()
    # Perform entity detection in SDPS
    identity_columns = {}
    for column in df.columns:
        identity_columns[column] = column + '_identity_types'
        df = df.withColumn(column + '_identity_types', detect_column_udf(f"`{column}`", sf.lit(column)))

    # Summarize the column level detection results
    expr_str = ', '.join([f"'{k}', `{v}`" for k, v in identity_columns.items()])
    expr_str = f"stack({len(identity_columns)}, {expr_str}) as (column_name,identity_types)"

    result_df = df.select(sf.expr(expr_str)) \
        .select('column_name', sf.explode('identity_types')) \
        .select('col.*', '*').groupBy('column_name', 'identifier').agg(sf.sum('score').alias('score')) \
        .withColumn('score', sf.col('score') / rows) \
        .where(f'score > {threshold}') \
        .withColumn('identifier', sf.struct('identifier', 'score')) \
        .groupBy('column_name').agg(sf.collect_list('identifier').alias('identifiers'))

    print("Going outside sdps_entity_detection")

    return result_df


def preprocess_df(df):
    """
    preprocess_df function aims to preprocess the df before performing entity detection.
    Select first 128 characters of each string column.
    """
    df = df.select([sf.col(f"`{c}`").cast("string") for c in df.columns])

    print("First done")
    # Select first 128 characters of each string column
    df = df.select([sf.substring(f"`{c}`", 1, 128).alias(c) if t == "string" else c for c, t in df.dtypes])

    print("Second done")
    return df


def detect_df(df, spark, udf_dict, broadcast_template, table, region, args):
    """
    detect_table is the main function to perform PII detection in a crawler table.

    Args:
        df: the df to be detected.
        spark: the spark session.
        udf_dict: the udf dict containing all udfs to be used for entity detection.
        broadcast_template: the broadcast template to be used for entity detection.
        table: the name of Glue Table.
        region: the region of this SDPS Job.
        args: the args dict containing all the parameters for this SDPS Job.

    Returns:
        result_df: the df with column level detection results.
    """
    print("L1")
    threshold = float(args['DetectionThreshold'])
    detect_column_udf, mask_data_udf = udf_dict['detect_column_udf'], udf_dict['mask_data_udf']
    print("L2")
    table_size = df.count()
    print("Table Size L25: {}".format(table_size))

    if table_size > 0:
        # # Sample the df to size of depth
        # if args['Depth'].isdigit():
        #     depth = int(args['Depth'])
        #     df = df.limit(depth*10)
        #     rows = df.count()
        #     sample_rate = 1.0 if rows <= depth else depth/rows
        sample_rate = 1.0
        df = df.sample(sample_rate)
        df = preprocess_df(df)
        sample_df = df.limit(10)

        print("Entity Level1")
        # Perform entity detection in Glue and SDPS
        print("L3")
        sdps_result_df = sdps_entity_detection(df, threshold, detect_column_udf)
        result_df = sdps_result_df

        # Combine the results with masked sample data
        expr_str = ', '.join([f"'{c}', cast(`{c}` as string)" for c in sample_df.columns])
        expr_str = f"stack({len(sample_df.columns)}, {expr_str}) as (column_name,sample_data)"
        sample_df = sample_df.select(sf.expr(expr_str)).groupBy('column_name').agg(
            sf.collect_list('sample_data').alias('sample_data'))

        data_frame = result_df
        data_frame = data_frame.join(sample_df, data_frame.column_name == sample_df.column_name, 'right') \
            .select(data_frame['identifiers'], sample_df['*'])

        print("data_frame after result_df creation")
        print(data_frame.show(truncate=False))

    # If table size is 0, return an empty df with default schema
    elif table_size == 0:
        empty_df_schema = StructType([
            StructField("identifiers", ArrayType(StructType(
                [StructField("identifier", StringType(), True),
                 StructField("score", DoubleType(), True)]
            ), True), True),
            StructField("column_name", StringType(), True),
            StructField("sample_data", ArrayType(StringType()), True),
        ])

        data_frame = spark.createDataFrame([(None, "", ["Table size is 0"])], empty_df_schema)

    print("Before Metadata")
    # Add metadata columns to the df
    try:
        s3_location, s3_bucket, file_id = table['table_name'], args['DatabaseName'], table.get("file_id", "")
    except Exception as e:
        print(e)
    data_frame = data_frame.withColumn('file_id', sf.lit(file_id))
    data_frame = data_frame.withColumn('account_id', sf.lit(args['AccountId']))
    data_frame = data_frame.withColumn('job_id', sf.lit(args['JobId']))
    data_frame = data_frame.withColumn('run_id', sf.lit(args['RunId']))
    data_frame = data_frame.withColumn('run_database_id', sf.lit(args['RunDatabaseId']))
    data_frame = data_frame.withColumn('database_name', sf.lit(args['DatabaseName']))
    data_frame = data_frame.withColumn('database_type', sf.lit(args['DatabaseType']))
    data_frame = data_frame.withColumn('region', sf.lit(region))
    data_frame = data_frame.withColumn('update_time', sf.from_utc_timestamp(sf.current_timestamp(), 'UTC'))
    data_frame = data_frame.withColumn('s3_location', sf.lit(s3_location))
    data_frame = data_frame.withColumn('s3_bucket', sf.lit(s3_bucket))
    data_frame = data_frame.withColumn('privacy', sf.expr('case when identifiers is null then 0 else 1 end'))
    data_frame = data_frame.withColumn('year', sf.year(sf.col('update_time')))
    data_frame = data_frame.withColumn('month', sf.month(sf.col('update_time')))
    data_frame = data_frame.withColumn('day', sf.dayofmonth(sf.col('update_time')))
    data_frame = data_frame.withColumn('sample_data', mask_data_udf('sample_data'))
    data_frame = data_frame.withColumn('table_size', sf.lit(table_size))

    return data_frame


def save_output(output_path, error_path, spark, incremental_df):
    print("output path: ", output_path)
    before = time.time()
    if incremental_df:
        print("Dataframe Union")
        df = incremental_df
        incremental_df = None
        print("Starting Repartition")

        before = time.time()
        df = df.repartition('year', 'month', 'day', 'run_id')
        # df.show()
        after = time.time()
        diff = after - before
        print("Repartition took {}".format(diff))
        print("Writing Partition By")
        before = time.time()
        df.write.partitionBy('year', 'month', 'day', 'run_id').mode('append').option("compression", "snappy").parquet(
            output_path)

        after = time.time()
        diff = after - before
        print("Time to write partition is {}".format(diff))
    # If error in detect_table, save to error_path
    if error:
        print("Error detecting")
        df = spark.createDataFrame(error)
        df.withColumn('update_time', sf.from_utc_timestamp(sf.current_timestamp(), 'UTC'))
        df = df.repartition(5)
        df.write.mode('append').parquet(error_path)


def table_execution(crawler_tables, start, end, incremental_df, full_database_name, spark, udf_dict, broadcast_template,
                    region, args, output_path, error_path, num_crawler_tables, relationalizeBucket,
                    samplePercentageLimit, rowLimit):
    cnt = 0
    print("Entering Table Execution from: {} to {}".format(start, end))
    for index in range(start, end):
        table = crawler_tables[index]
        print("table: ", table)
        try:
            error = []
            # call detect_table to perform PII detection
            before = time.time()

            raw_df = table['table_data']
            total_rows = raw_df.count()
            print("Total Rows are: {}".format(total_rows))
            currentSamplePercentageLimit = float(samplePercentageLimit) / 100
            print("Row Limit: {} and Sample Percentage Limit: {}".format(rowLimit, currentSamplePercentageLimit))
            sample_size = min(int(rowLimit), int(total_rows * currentSamplePercentageLimit))
            print("Sample Size: {}".format(sample_size))
            sample_fraction = float(sample_size) / total_rows
            # Sampling the DataFrame
            print(sample_fraction)
            raw_df = raw_df.sample(sample_fraction)
            after = time.time()
            diff = after - before
            print("Catalog creation time is is {} for table: {}".format(diff, table['table_name']))
            before = time.time()
            # transformation_ctx = full_database_name + table['Name'] + 'df'
            summarized_result = detect_df(raw_df, spark, udf_dict, broadcast_template, table, region, args)
            after = time.time()
            diff = after - before
            print("Summarisation time is is {} for table: {}".format(diff, table['table_name']))
            before = time.time()
            try:
                if incremental_df is None:
                    incremental_df = summarized_result
                else:
                    incremental_df = incremental_df.unionAll(summarized_result)
            except Exception as ex:
                print("Error : {}".format(ex))

            after = time.time()
            diff = after - before
            cnt += 1
            print("Time for Incremental DF is {} for table: {} with count: {}".format(diff, table['table_name'], cnt))
        except Exception as e:
            # Report error if failed
            s3_location, s3_bucket, file_id = table['table_name'], args['DatabaseName'], table.get('file_id', "")
            data = {
                'account_id': args["AccountId"],
                'region': region,
                'job_id': args['JobId'],
                'run_id': args['RunId'],
                'run_database_id': args['RunDatabaseId'],
                'database_name': args['DatabaseName'],
                'database_type': args['DatabaseType'],
                'table_name': table['Name'],
                's3_location': s3_location,
                's3_bucket': s3_bucket,
                'error_message': str(e),
                'file_id': file_id
            }
            error.append(data)
            # print(f'Error occured detecting table {table}')
            print(e)
    print("Saving output now")
    try:
        save_output(output_path, error_path, spark, incremental_df)
    except Exception as e:
        print("Error while writing output to final bucket:", e)


def flatten_json_auto(data):
    """
    Automatically flattens JSON data, handling both nested dictionaries and lists of lists.
    """
    if isinstance(data, list) and all(isinstance(i, list) for i in data):
        # If the data is a list of lists, convert it directly to a DataFrame
        return pd.DataFrame(data)
    elif isinstance(data, dict):
        data = [data]  # Wrap single dictionary in a list

    # Use json_normalize to flatten the top-level structure for nested dictionaries
    df = json_normalize(data)

    # Continue expanding any nested columns
    while True:
        # Identify columns with nested dictionaries or lists
        nested_columns = [col for col in df.columns if any(isinstance(i, (dict, list)) for i in df[col])]

        if not nested_columns:
            # No more nested columns; exit the loop
            break

        for col in nested_columns:
            # If the column contains lists, explode each list element as a new row
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df = df.explode(col).reset_index(drop=True)

            # For dictionary-like columns, normalize them into separate columns
            elif df[col].apply(lambda x: isinstance(x, dict)).any():
                expanded_cols = pd.json_normalize(df[col].dropna()).add_prefix(f"{col}.")
                df = df.drop(columns=[col]).join(expanded_cols)

    return df


def create_spark_dataframe_from_file(file_content, file_name):
    """
    Create a PySpark DataFrame if the file is an Excel, CSV, Parquet, or JSON file.
    """
    try:
        if file_name.endswith('.xlsx'):
            df_pandas = pd.read_excel(file_content, engine='openpyxl')
            df_spark = spark.createDataFrame(df_pandas)
            print(f"Spark DataFrame created for Excel file: {file_name}")
            return df_spark
        elif file_name.endswith('.xls'):
            df_pandas = pd.read_excel(file_content, engine='xlrd')
            df_spark = spark.createDataFrame(df_pandas)
            print(f"Spark DataFrame created for Excel file: {file_name}")
            return df_spark
        elif file_name.endswith('.csv'):
            file_content.seek(0)
            df_pandas = pd.read_csv(file_content, sep=',', on_bad_lines='skip', engine='python')
            df_spark = spark.createDataFrame(df_pandas)
            print(f"Spark DataFrame created for CSV file: {file_name}")
            return df_spark
        elif file_name.endswith('.parquet'):
            file_content.seek(0)
            df_pandas = pd.read_parquet(file_content)
            df_spark = spark.createDataFrame(df_pandas)
            print(f"Spark DataFrame created for Parquet file: {file_name}")
            return df_spark
        elif file_name.endswith('.json'):
            file_content.seek(0)
            data = json.load(file_content)
            df_pandas = flatten_json_auto(data)  # Flatten JSON before creating a Spark DataFrame
            df_spark = spark.createDataFrame(df_pandas)
            print(f"Spark DataFrame created for JSON file: {file_name}")
            return df_spark
        else:
            print(f"{file_name} is not an Excel, CSV, Parquet, or JSON file.")
            return None
    except (BadZipFile, pd.errors.EmptyDataError, pd.errors.ParserError, json.JSONDecodeError) as e:
        print(f"Error processing {file_name}: {e}")
    except Exception as e:
        print(f"Error processing {file_name}: {e}")
    return None


def fetch_token_for_office_365(tenant_id, client_id, client_secret):
    AUTHORITY = f'https://login.microsoftonline.com/{tenant_id}'
    SCOPE = ['https://graph.microsoft.com/.default']
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=AUTHORITY,
        client_credential=client_secret
    )
    token_response = app.acquire_token_for_client(scopes=SCOPE)
    if 'access_token' in token_response:
        return token_response['access_token']
    print("Error: Unable to fetch access token.")
    return None


def download_and_process_file(row, access_token):
    file_url = row['file_url']
    file_name = row['file_name']
    file_id = row['file_id']

    file_content = download_files(file_url, access_token, file_name)
    if file_content is not None:
        df = create_spark_dataframe_from_file(file_content, file_name)
        if df:
            return {
                "table_name": file_name,
                "table_data": df,
                "file_id": file_id
            }
    return None


def download_files(file_url, access_token, file_name):
    headers = {'Authorization': f'Bearer {access_token}'}
    try:
        response = requests.get(file_url, headers=headers, stream=True)
        response.raise_for_status()
        file_content = io.BytesIO(response.content)
        print(f"Downloaded and stored in memory: {file_name}")
        return file_content
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {file_name}: {e}")
    return None


def process_files_in_parallel(df_file_paths, access_token, max_workers=5):
    """
    Process files in parallel using ThreadPoolExecutor.
    """
    dataframes = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_and_process_file, row, access_token) for _, row in df_file_paths.iterrows()]

        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    print(f"Processed: {result['table_name']}")
                    result["table_data"].show()  # Display the DataFrame content
                    dataframes.append(result)
            except Exception as e:
                print(f"Error in processing file: {e}")

    return dataframes


def get_dataframes_from_s3(spark, s3_bucket, s3_prefix, region):
    """
    Recursively find all CSV, JSON, and Parquet files in an S3 or MinIO bucket and prefix,
    and create a list of PySpark DataFrames from those files.

    Args:
        s3_bucket (str): The name of the S3 or MinIO bucket.
        s3_prefix (str): The prefix (folder path) within the bucket to search.
        region (str): The AWS region where the S3 bucket is located (ignored for MinIO).
        minio (bool): Set to True if accessing MinIO.
        minio_endpoint (str): MinIO endpoint URL.
        minio_access_key (str): MinIO access key.
        minio_secret_key (str): MinIO secret key.

    Returns:
        List[dict]: A list of dictionaries, each containing 'table_name' (S3/MinIO path) and 'table_data' (PySpark DataFrame).
    """

    print("aws s3 client created")
    s3_client = boto3.client('s3', region_name=region)

    dataframes = []

    # List objects in the bucket recursively
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

    print("pages: ", pages)

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv') or key.endswith('.json') or key.endswith('.parquet'):
                # Determine full S3/MinIO path
                full_s3_path = f"s3://{s3_bucket}/{key}"
                s3a_path = f"s3a://{s3_bucket}/{key}"

                try:
                    # Read the file into a DataFrame based on the file extension
                    if key.endswith('.csv'):
                        df = spark.read.csv(s3a_path, header=True, inferSchema=True)
                    elif key.endswith('.json'):
                        df = spark.read.json(s3a_path)
                    elif key.endswith('.parquet'):
                        df = spark.read.parquet(s3a_path)

                    # Append to the list of DataFrames
                    dataframes.append({
                        'table_name': full_s3_path,
                        'table_data': df
                    })
                    print(f"Loaded DataFrame from {full_s3_path}")
                except Exception as e:
                    print(f"Failed to load {full_s3_path}: {e}")

    print("Number of DataFrames loaded: ", len(dataframes))
    # dataframes = dataframes
    # print("size of dataframe: ",dataframes[0]['table_data'].info())
    return dataframes


if __name__ == "__main__":

    start_time_for_job = time.time()

    args = getResolvedOptions(sys.argv, ["AccountId", "JOB_NAME", 'DatabaseName', 'DatabaseType', 'BucketName',
                                         'Depth', 'DetectionThreshold', 'JobId', 'RunId', 'RunDatabaseId', 'TemplateId',
                                         'TemplateSnapshotNo',
                                         'AdminAccountId', 'BaseTime', 'DynamicFrameBucket', 'SamplePercentageLimit',
                                         'RowLimit', "client_id", "client_secret", "tenant_id", "is_o365", "S3PREFIX",
                                         "SaasIntegrationId"])

    region = os.environ['AWS_DEFAULT_REGION']
    result_database = 'sdps_database'
    result_table = 'job_detection_output_table'
    s3_bucket = args['DatabaseName']
    s3_prefix = args['S3PREFIX']

    full_database_name = f"{args['DatabaseType']}-optiq-{args['DatabaseName']}-database"
    output_path = f"s3a://{args['BucketName']}/glue-database/{result_table}/"
    error_path = f"s3a://{args['BucketName']}/glue-database/job_detection_error_table/"
    base_time = parse(args['BaseTime']).replace(tzinfo=pytz.timezone('UTC'))
    relationalizeBucket = args['DynamicFrameBucket']
    samplePercentageLimit = args['SamplePercentageLimit']
    rowLimit = args['RowLimit']
    s3_bucket = args['DatabaseName']
    s3_prefix = args['S3PREFIX']
    job_id = args.get("JobId")
    saas_integration_id = args.get("SaasIntegrationId")
    is_o365 = bool(args.get("is_o365", False))
    run_id = args.get("RunId")

    print("AWS s3")
    s3 = boto3.client('s3', region_name=region)

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # Get the template from s3 and broadcast it to all the executors
    template = get_template(s3, args['BucketName'],
                            f"template/template-{args['TemplateId']}-{args['TemplateSnapshotNo']}.json")
    broadcast_template = sc.broadcast(template)

    before = time.time()
    if is_o365:
        print("Inside o365")
        client_id = args.get("client_id", None)
        client_secret = args.get("client_secret", None)
        tenant_id = args.get("tenant_id", None)

        access_token = fetch_token_for_office_365(tenant_id, client_id, client_secret)
        if not access_token:
            print("Exiting program due to missing access token.")
            exit()

        file_paths = get_dataframes_from_s3(spark, s3_bucket, s3_prefix, region)
        dataframes = [path['table_data'] for path in file_paths]
        combined_df = reduce(lambda df1, df2: df1.union(df2), dataframes)
        combined_df = combined_df.toPandas()

        # print(combined_df.head())

        crawler_tables = process_files_in_parallel(combined_df, access_token, max_workers=5)
        print(len(crawler_tables))

    after = time.time()
    print("Time to Get Crawler Tables: {}".format(after - before))
    # Create UDFs
    column_detector = ColumnDetector(broadcast_template)

    before = time.time()
    detect_column_udf = column_detector.create_detect_column_udf()
    after = time.time()
    diff = after - before
    print("Detect Column UDF Time: {}".format(diff))

    mask_data_udf = create_mask_data_udf()
    udf_dict = dict()
    udf_dict['detect_column_udf'] = detect_column_udf
    udf_dict['mask_data_udf'] = mask_data_udf

    # The detection result is saved every 10 tables
    save_freq = 10
    output = []
    error = []
    cnt = 0
    incremental_df = None
    start_count = 0
    end_count = len(crawler_tables)
    print("Running Job between start count: {} to end count: {}".format(start_count, end_count))
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=10) as executor:
        print("Entering Thread pool")
        futures = []  # To keep track of submitted tasks
        while start_count < end_count:
            end = min(end_count, start_count + save_freq)
            futures.append(
                executor.submit(table_execution, crawler_tables, start_count, end, incremental_df, full_database_name,
                                spark, udf_dict, broadcast_template, region, args, output_path, error_path, end_count,
                                relationalizeBucket, samplePercentageLimit, rowLimit)
            )
            start_count += save_freq

    try:
        url = "https://harmony.optiq.co.in/api/v1/data/saas/scan/result"

        headers = {
            "Content-Type": "application/json"
        }
        current_date = datetime.now()
        year = current_date.year
        month = current_date.month
        day = current_date.day

        data = {
            "saas_integration_id": saas_integration_id,
            "bucket_name": args['BucketName'],
            "key": "glue-database/{}/year={}/month={}/day={}/run_id={}".format(result_table, year, month, day, run_id),
            "job_execution_id": run_id,
            "is_structured": 1
        }

        response = requests.post(url, headers=headers, json=data)

        print(response.status_code)
        print(response.json())
        print("Successfully hit notification api")

    except Exception as e:

        print("Failure in hitting notification api", e)

    end_time_for_job = time.time()
    print("Total Time Taken by Job: ", end_time_for_job - start_time_for_job)
    job.commit()
