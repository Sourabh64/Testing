import json
import time
import pandas as pd

start = time.time()
with open('nw_report.json', 'r', encoding='utf8') as f:
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


def data_frame_append(df_data, path, old_path=None, key=None, current_key=None):
    df = df_data
    if path in df_dict:
        df_dict[f'{path}'] = pd.concat([df_dict[f'{path}'], df], axis=0)
        # print(len(df_dict[f'{path}']))
        if current_key:
            df_dict[f'{path}']['current_key'] = range(current_key, current_key + len(df_dict[f'{path}']))
        else:
            df_dict[f'{path}']['current_key'] = range(1, 1+len(df_dict[f'{path}']))
        if old_path:
            if key in df_dict[f'{old_path}'].columns:
                df_dict[f'{old_path}'].drop(key, axis=1, inplace=True)
        else:
            pass
    else:
        df_dict[f'{path}'] = df_data
        if current_key:
            df_dict[f'{path}']['current_key'] = range(current_key, current_key + len(df_dict[f'{path}']))
        else:
            df_dict[f'{path}']['current_key'] = df_dict[f'{path}'].index+1


def process_data(data):
    parent_key = 0
    meta_dict = {}
    for each in data:
        parent_key += 1

        def flatten(new_data, path='nw_report', parent_key=None):
            if path not in meta_dict:
                meta_dict[path] = {
                    'current_key': 0,
                    'parent_key': parent_key
                }
            meta_dict[path]['parent_key'] = parent_key  # problem found here while running tags
            if isinstance(new_data, dict):
                meta_dict[path]['current_key'] += 1
                for key in new_data:
                    if isinstance(new_data[key], list) and new_data[key]:
                        internal_df = normalize_json(new_data[key])
                        internal_df['parent_key'] = parent_key
                        current_key = meta_dict[path]['current_key']
                        data_frame_append(internal_df, path+"_"+key, path, key, current_key=current_key)
                        flatten(new_data[key], path+"_"+key, parent_key)
            elif isinstance(new_data, list) and new_data:
                for key in new_data:
                    if isinstance(key, dict):
                        parent_key = meta_dict[path]['current_key']
                        flatten(key, path, parent_key)

        flatten(each, parent_key=parent_key)


data_frame = normalize_json(data)
data_frame['parent_key'] = range(1, 1+len(data_frame))
data_frame_append(data_frame, 'nw_report')
process_data(data)

count = 0
# print(df_dict)
for i in df_dict:
    count += 1
    df_dict[i].to_csv(f'csv/{i}.csv')
print(time.time() - start)

