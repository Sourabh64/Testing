import json
import time
import pandas as pd
import numpy as np
from datetime import date
from db_testing import DB


start = time.time()
with open('jsons/managed_devices.json', 'r', encoding='utf8') as f:
    data = json.load(f)


# def flatten_data(y):
#     out = {}
#     path_list = []
#
#     def flatten(x, path='root'):
#         if type(x) is dict:
#             for a in x:
#                 flatten(x[a], path + a )
#         elif type(x) is list:
#             i = 0
#             for a in x:
#                 flatten(a, path + '#')
#                 i += 1
#         else:
#             if path[:-1] in out.keys():
#                 out[path[:-1]].append(x)
#             else:
#                 path_list.append(path[:])
#                 out[path[:-1]] = [x]
#
#     flatten(y)
#     return out
col_list = []
df_dict = {}


def normalize_json(data_list):
    df = pd.json_normalize(data_list)
    for col in df.columns:
        if col not in col_list:
            col_list.append(col)
    return df

def get_dtype_conversions(df):
    dtype_conversion_dict = {'int64': 'int8', 'object': 'varchar', 'float64': 'float8', 'datetime64[ns]': 'timestamp',
                             'datetime64': 'timestamp', 'datetime64[ns, tzlocal()]': 'timestamp', 'bool': 'boolean',
                             'datetime64[ns, UTC]': 'timestamp'}
    rs_dtype_list = []
    for i, v in zip(df.dtypes.index, df.dtypes.values):

        rs_dtype = dtype_conversion_dict[str(v)]
        if v == "object":
            try:
                if df[i].str.len().max() < 1:
                    # To avoid varchar(e)
                    rs_dtype += '({})'.format(int(1))
                else:
                    rs_dtype += '({})'.format(int(df[i].str.len().max()))
            except AttributeError:
                print(f"Attribute Error for {i}, {v}")
        rs_dtype_list.append(rs_dtype)
    return rs_dtype_list


def create_query(df, schema, table):
    cols = df.dtypes.index
    rs_dtype_list = get_dtype_conversions(df)
    vars = ', '.join(['{0} {1}'.format(str(x), str(y)) for x, y in zip(cols, rs_dtype_list)])
    query = '''create table IF NOT EXISTS {}.{} ({});'''.format(schema, table, vars)
    return query


def df_col_add(del_row_dict):
    if len(del_row_dict) != 0:
        for path in del_row_dict:
            for col_name in del_row_dict[path]:
                df_dict[f'{path}'] = df_dict[f'{path}'].dropna(subset=col_name)


def data_frame_append(df_data, path, old_path=None, key=None):
    df = df_data
    if path in df_dict:
        df_dict[f'{path}'] = pd.concat([df_dict[f'{path}'], df], axis=0)
        df_dict[f'{path}']['current_key'] = range(1, 1+len(df_dict[f'{path}']))
        if old_path:
            if key in df_dict[f'{old_path}'].columns:
                df_dict[f'{old_path}'].drop(key, axis=1, inplace=True)
        else:
            pass
    else:
        df_dict[f'{path}'] = df_data
        df_dict[f'{path}']['current_key'] = df_dict[f'{path}'].index+1


def process_data(data, path):
    parent_key = 0
    meta_dict = {}
    delete_row_dict = {}
    for each in data:
        parent_key += 1

        def flatten(new_data, path=path, parent_key=None):
            if path not in meta_dict:
                meta_dict[path] = {
                    'current_key': 0,
                    'parent_key': parent_key
                }
            meta_dict[path]['parent_key'] = parent_key
            if isinstance(new_data, dict):
                meta_dict[path]['current_key'] += 1
                for key in new_data:
                    if isinstance(new_data[key], list) and new_data[key]:
                        internal_df = normalize_json(new_data[key])
                        internal_df['parent_key'] = parent_key
                        data_frame_append(internal_df, path + "_" + key, path, key)
                        flatten(new_data[key], path + "_" + key, parent_key)
            elif isinstance(new_data, list) and new_data:
                for key in new_data:
                    if isinstance(key, dict):
                        if meta_dict[path]['current_key'] == 0:
                            parent_key = meta_dict[path]['current_key'] + 1
                            meta_dict[path]['current_key'] += 1
                        else:
                            parent_key = meta_dict[path]['current_key']
                        flatten(key, path, parent_key)
                    elif isinstance(key, str):
                        column_name = path.split("_")[-1]
                        new_df = pd.DataFrame([key], columns=['{}'.format(column_name)])
                        new_df['parent_key'] = parent_key
                        if path in delete_row_dict and column_name not in delete_row_dict[path]:
                            column_list.extend(column_name)
                        else:
                            column_list = [column_name]
                        delete_row_dict[path] = column_list
                        data_frame_append(new_df, path)
                df_col_add(delete_row_dict)

        flatten(each, parent_key=parent_key)


data_frame = normalize_json(data)
data_frame['parent_key'] = range(1, 1+len(data_frame))
data_frame_append(data_frame, 'managed_devices')
process_data(data, 'managed_devices')

count = 0
today =date.today()
for i in df_dict:
    count += 1
    # print(list(df_dict[i].columns.str.lower().values))
    df_dict[i]['date'] = pd.to_datetime(today)
    # if df_dict[i].columns.str.contains('primary').any():
    #     df_dict[i].rename(columns={'primary': 'primary_value'}, inplace=True)
    # for column in df_dict[i]:
    #     # print(df_dict[i]['ci_forward_deployment_enabled'])
    #     if df_dict[i][column].isnull().values.any():
    #         df_dict[i][column] = df_dict[i][column].fillna(np.nan)
    #     else:
    #         df_dict[i][column] = df_dict[i][column].fillna("NA")
    # df_dict[i].columns = df_dict[i].columns.str.replace(".", "_", regex=True)
    # df_dict[i].columns = df_dict[i].columns.str.replace("-", "_", regex=True)
    # query = create_query(df_dict[i], 'AWS', i)
    # print(query)
    df_dict[i].to_csv(f'csvs/{i}.csv', index=False)
db = DB()
# db_response = db.db_process(df_dict)
print(time.time() - start)

