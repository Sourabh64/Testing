"""
json parser(standard)
added keys for mapping
cleandf: for change in dtype from null(simple) to [],{}(complex)- reset final_df

added custom paths

*** R E M O V E D
    update_parent_key: if no simple element in current table: parent for nxt tbl = parent of current tabl
                       else: parent for nxt tbl = current of current tbl
** N O W
all tables selected will have current_key and parent_key even if they dont have simple cols

dict-new table
[]- brought to parent level, take all combinations as records

maintaining scope during multiple sessions

if col is null initially, then 1. becomes dict: delete col from parent, create new table
                               2. becomes list: traverse: if element is  dict then 1.
                                                          elif list then 2.
                                                          else: str/int, bring to parent level

23/04/21- Added function to generate dest meta tree and custom paths(req by parser) from path list
"""

import itertools
import logging
import pandas as pd


# from commons.utils.utils import Utils


class Json_Parser():
    def __init__(self, custom_paths={}):
        self.log = logging.getLogger()
        self.final_df = {}
        self.meta_dict = {}
        self.temp_list_val = {}
        self.path = []
        self.aller = []
        self.custom_paths = custom_paths

    def generate_dest_meta_info(self, meta_path_list):
        try:
            dest_meta_tree = []
            dest_meta_dict = {}

            if meta_path_list:
                # //////////////////////
                initial_table = ''
                temp_name = meta_path_list[0]
                temp_name = temp_name[1:] if temp_name[0] == '#' else temp_name
                temp_name = temp_name[:-1] if temp_name[-1] == '#' else temp_name
                temp_name = 'root#' + temp_name
                path_list = temp_name.split('#')
                found = False
                for path in path_list[:-1]:
                    initial_table = path if initial_table == '' else initial_table + '#' + path
                    for table_path in meta_path_list:
                        table_path = 'root#' + table_path

                        if initial_table + '#' not in table_path + '#':  # for root#hit & root#hits =>root#hit#/root#hits#
                            initial_table = '#'.join(initial_table.split('#')[:-1])
                            initial_table = 'root' if initial_table == '' else initial_table  # safe side
                            dest_meta_dict[initial_table] = ['current_key', 'parent_key']
                            found = True
                            break

                    if found:
                        break
                if not found:
                    dest_meta_dict[initial_table] = ['current_key', 'parent_key']
                # ///////////////////

                for table_path in meta_path_list:
                    table_path = table_path[1:] if table_path[0] == '#' else table_path
                    table_path = table_path[:-1] if table_path[-1] == '#' else table_path
                    table_path = 'root#' + table_path
                    path_list = table_path.split('#')

                    parent_table_name = '#'.join(path_list[:-1])
                    column_name = path_list[-1]

                    if parent_table_name not in dest_meta_dict:
                        table = ''
                        for path in path_list[:-1]:
                            table = path if table == '' else table + '#' + path
                            if table + '#' not in initial_table + '#':  # safe side
                                if table not in dest_meta_dict:
                                    dest_meta_dict[table] = ['current_key', 'parent_key']

                    dest_meta_dict[parent_table_name].append(column_name)

                for table in dest_meta_dict:
                    formatted_table = table.replace('#', '_')
                    table_block = {"icon": "table", "name": formatted_table, "check": True,
                                   "columns": [], "key": f":{formatted_table}", "type": "TABLE"}
                    for column in dest_meta_dict[table]:
                        column_block = {"icon": "columns", "name": column, "data_type": None,
                                        "check": True, "key": f"{formatted_table}:{column}", "type": "COLUMN"}
                        table_block["columns"].append(column_block)
                    dest_meta_tree.append(table_block)

            return dest_meta_tree, dest_meta_dict
        except Exception as e:
            self.log.error(f"Error while generating meta info: {str(e)}")
            raise

    def clean_df(self, table_path, key):
        if key in self.meta_dict[table_path]['columns']:
            for record in self.final_df[table_path]:
                if key in record:
                    del record[key]
            del self.meta_dict[table_path]['columns'][key]
            self.meta_dict[table_path]['exclude'].append(key)

    def dict_has_simple_element(self, data, table_path):
        has_simple_element = False
        # parent_key = self.meta_dict[table_path]['parent_key']
        for key in data:
            if not (isinstance(data[key], dict) or isinstance(data[key], list)):
                if self.custom_paths == {} or (
                        table_path in self.custom_paths and key in self.custom_paths[table_path]):
                    has_simple_element = True
                    # parent_key = self.meta_dict[table_path]['current_key']
                    break
        return has_simple_element

    def process_data(self, data, table_path='', parent_key=None):
        try:
            temp_dict = {}
            temp_list = []
            is_dict = False
            is_list = False
            if not table_path:
                table_path = 'root'
            if table_path not in self.path:
                self.path.append(table_path)
                self.meta_dict[table_path] = {
                    'current_key': 0,
                    'parent_key': parent_key,
                    'columns': {},
                    'exclude': [],
                }

            self.meta_dict[table_path]['parent_key'] = parent_key

            if isinstance(data, dict):
                self.meta_dict[table_path]['current_key'] += 1
                for key in data:
                    if isinstance(data[key], dict):
                        if data[key] != {}:
                            if key not in self.meta_dict[table_path]['exclude']:
                                self.clean_df(table_path, key)

                            key_for_new_table = table_path + '#' + key
                            # has_simple_element = self.dict_has_simple_element(data, table_path)
                            '''if has_simple_element:
                                parent_key = self.meta_dict[table_path]['current_key']
                            else:
                                parent_key = self.meta_dict[table_path]['parent_key']'''
                            parent_key = self.meta_dict[table_path]['current_key']
                            # if key_for_new_table in self.custom_paths:
                            self.process_data(data[key], table_path=key_for_new_table, parent_key=parent_key)
                    elif isinstance(data[key], list):
                        # self.clean_df(table_path,key)     #**_**_**_**_**_**_ null to nested/str to nested to discuss
                        key_for_new_table = table_path + '#' + key

                        # //////////////////
                        # has_simple_element = self.dict_has_simple_element(data, table_path)
                        '''if has_simple_element:
                            parent_key = self.meta_dict[table_path]['current_key']
                        else:
                            parent_key = self.meta_dict[table_path]['parent_key']'''
                        # //////////////////
                        parent_key = self.meta_dict[table_path]['current_key']
                        self.process_data(data[key], table_path=key_for_new_table, parent_key=parent_key)
                    else:
                        if self.custom_paths == {} or (
                                table_path in self.custom_paths and key in self.custom_paths[table_path]):
                            if table_path not in self.final_df:
                                self.log.info(f"Creating new table {table_path}")
                                self.final_df[table_path] = []
                            if key not in self.meta_dict[table_path]['exclude']:
                                if key not in self.meta_dict[table_path]['columns']:
                                    self.meta_dict[table_path]['columns'][key] = str(type(data[key]))
                                temp_dict[key] = data[key]

                if table_path in self.temp_list_val and self.temp_list_val[table_path]:
                    input_for_permutation = []
                    names = []
                    for list_name in self.temp_list_val[table_path]:
                        if self.custom_paths == {} or (
                                table_path in self.custom_paths and list_name in self.custom_paths[table_path]):
                            if list_name not in self.meta_dict[table_path]['exclude']:
                                if list_name not in self.meta_dict[table_path]['columns']:
                                    self.meta_dict[table_path]['columns'][list_name] = str(type(
                                        self.temp_list_val[table_path][list_name][0]))  # select 1st element for type

                                input_for_permutation.append(self.temp_list_val[table_path][list_name])
                                names.append(list_name)

                    if names:
                        permutations = list(itertools.product(*input_for_permutation))
                        if table_path not in self.final_df:
                            self.log.info(f"Creating new table {table_path}")
                            self.final_df[table_path] = []
                        # self.meta_dict[table_path]['current_key'] -= 1

                        for row in permutations:
                            temp_dict_list = {}
                            if temp_dict:
                                temp_dict_list = temp_dict.copy()  # ambiguous behaviour when temp_dict_list=temp_dict

                            # self.meta_dict[table_path]['current_key'] += 1

                            for col_index in range(len(row)):
                                temp_dict_list[names[col_index]] = row[col_index]

                            # temp_dict_list['current_key'] = len(self.final_df[table_path]) +1         # always correct p_key
                            temp_dict_list['current_key'] = self.meta_dict[table_path]['current_key']  # $%^
                            temp_dict_list['parent_key'] = self.meta_dict[table_path]['parent_key']

                            x = f"{table_path}, {temp_dict_list['current_key']}, {temp_dict_list['parent_key']}"
                            self.aller.append(x)
                            self.final_df[table_path].append(temp_dict_list)

                    self.temp_list_val[table_path].clear()

                elif temp_dict:
                    temp_dict['current_key'] = self.meta_dict[table_path]['current_key']
                    temp_dict['parent_key'] = self.meta_dict[table_path]['parent_key']
                    x = f"{table_path}, {temp_dict['current_key']}, {temp_dict['parent_key']}"
                    self.aller.append(x)
                    self.final_df[table_path].append(temp_dict)

                else:
                    if self.custom_paths == {} or (table_path in self.custom_paths):
                        if table_path not in self.final_df:
                            self.log.info(f"Creating new table {table_path}")
                            self.final_df[table_path] = []
                        temp_dict['current_key'] = self.meta_dict[table_path]['current_key']
                        temp_dict['parent_key'] = self.meta_dict[table_path]['parent_key']
                        x = f"{table_path}, {temp_dict['current_key']}, {temp_dict['parent_key']}"
                        self.aller.append(x)
                        self.final_df[table_path].append(temp_dict)

            elif isinstance(data, list):

                path_list = table_path.split('#')
                parent_table_name = '#'.join(path_list[:-1])
                list_column_name = path_list[-1]

                for i, row in enumerate(data):
                    if isinstance(row, dict):
                        if row != {}:
                            if len(path_list) != 1:
                                if list_column_name not in self.meta_dict[parent_table_name]['exclude']:
                                    self.clean_df(parent_table_name,
                                                  list_column_name)  # **_**_**_ deleting from parent table the simple column
                            key_for_new_table = table_path  # + '#' +'dict'/ +'#'+ str(i+1)
                            if table_path not in self.final_df:
                                self.path.remove(table_path)
                            self.process_data(row, table_path=key_for_new_table, parent_key=parent_key)

                    elif isinstance(row, list):
                        key_for_new_table = table_path  # + '#' +'dict'/ +'#'+ str(i+1)
                        if table_path not in self.final_df:
                            self.path.remove(table_path)
                        self.process_data(row, table_path=key_for_new_table,
                                          parent_key=self.meta_dict[table_path]['parent_key'])

                    else:
                        if self.custom_paths == {} or (
                                parent_table_name in self.custom_paths and list_column_name in self.custom_paths[parent_table_name]):
                            temp_list.append(row)

                if temp_list:
                    if parent_table_name not in self.temp_list_val:
                        self.temp_list_val[parent_table_name] = {}
                    self.temp_list_val[parent_table_name][list_column_name] = temp_list
        except Exception as e:
            self.log.error(f"Error while parsing source data: {str(e)}")
            raise

    def generate_table_df(self):
        try:
            table_df = {}
            for table_name, contents in self.final_df.items():
                table_name = table_name.replace('#', '_')
                self.log.info(f"Creating table {table_name} with {len(contents)} records")
                table_df[table_name] = pd.DataFrame(contents)
                # cols_to_move = ['current_key', 'parent_key']
                # table_df[table_name] = table_df[table_name][ [ col for col in table_df[table_name].columns if col not in cols_to_move ] + cols_to_move ]
            return table_df
        except Exception as e:
            self.log.error(f"Error while generating table dataframes: {str(e)}")
            raise


class Json_Parser_v3():
    def __init__(self, all_paths={}, custom_paths={}, user_preference={'new_paths': 'auto_delete'}):
        self.log = logging.getLogger()
        self.final_df = {}
        self.restream_df = {}
        self.restream_meta_info = {}
        self.meta_dict = {}
        self.temp_list_val = {}
        self.custom_paths = custom_paths
        self.all_paths = all_paths
        self.user_preference = user_preference
        self.source_table_missing_data = {}
        self.etl_name = None

    def generate_dest_meta_info(self, meta_path_list, meta_tree=True):
        try:
            dest_meta_tree = []
            dest_meta_dict = {}
            root_path = 'root'
            if self.etl_name:
                root_path = self.etl_name + "#" + root_path
            if meta_path_list:
                initial_table = ''
                temp_name = meta_path_list[0]
                temp_name = temp_name[1:] if temp_name[0] == '#' else temp_name
                temp_name = temp_name[:-1] if temp_name[-1] == '#' else temp_name
                temp_name = root_path + '#' + temp_name
                path_list = temp_name.split('#')
                found = False
                for path in path_list[:-1]:
                    initial_table = path if initial_table == '' else initial_table + '#' + path
                    for table_path in meta_path_list:
                        table_path = root_path + '#' + table_path

                        if initial_table + '#' not in table_path + '#':  # for root#hit & root#hits =>root#hit#/root#hits#
                            initial_table = '#'.join(initial_table.split('#')[:-1])
                            initial_table = root_path if initial_table == '' else initial_table  # safe side
                            dest_meta_dict[initial_table] = ['current_key', 'parent_key']
                            found = True
                            break

                    if found:
                        break
                if not found:
                    dest_meta_dict[initial_table] = ['current_key', 'parent_key']

                for table_path in meta_path_list:
                    table_path = table_path[1:] if table_path[0] == '#' else table_path
                    table_path = table_path[:-1] if table_path[-1] == '#' else table_path
                    table_path = root_path + '#' + table_path
                    path_list = table_path.split('#')

                    parent_table_name = '#'.join(path_list[:-1])
                    column_name = path_list[-1]

                    if parent_table_name not in dest_meta_dict:
                        table = ''
                        for path in path_list[:-1]:
                            table = path if table == '' else table + '#' + path
                            if table + '#' not in initial_table + '#':  # safe side(for root#hit & root#hits =>root#hit#/root#hits#)
                                if table not in dest_meta_dict:
                                    dest_meta_dict[table] = ['current_key', 'parent_key']

                    dest_meta_dict[parent_table_name].append(column_name)

                if meta_tree:
                    for table in dest_meta_dict:
                        formatted_table = table.replace('#', '_')
                        table_block = {"icon": "table", "name": formatted_table, "check": True,
                                       "columns": [], "key": f":{formatted_table}", "type": "TABLE"}
                        for column in dest_meta_dict[table]:
                            column_block = {"icon": "columns", "name": column, "data_type": None,
                                            "check": True, "key": f"{formatted_table}:{column}", "type": "COLUMN"}
                            table_block["columns"].append(column_block)
                        dest_meta_tree.append(table_block)
                    return dest_meta_tree

            return dest_meta_dict
        except Exception as e:
            self.log.error(f"Error while generating meta info: {str(e)}")
            raise

    def clean_df(self, table_path, key):
        if key in self.meta_dict[table_path]['columns']:
            if table_path in self.final_df:
                for record in self.final_df[table_path]:
                    if key in record:
                        del record[key]
            elif table_path in self.restream_df:
                for record in self.restream_df[table_path]:
                    if key in record:
                        del record[key]
            self.log.info(f"Excluding column {key} from parent table {table_path} as its forming a new table(dict)")
            del self.meta_dict[table_path]['columns'][key]
            if key in self.meta_dict[table_path]['user_conditions']['columns']:
                self.log.info(
                    f"Excluding column {key} from newly found columns of parent table {table_path} as its forming a new table(dict)")
                self.meta_dict[table_path]['user_conditions']['columns'].remove(key)
            self.meta_dict[table_path]['exclude'].append(key)

    def dict_has_simple_element(self, data, table_path):
        has_simple_element = False
        # parent_key = self.meta_dict[table_path]['parent_key']
        for key in data:
            if not (isinstance(data[key], dict) or isinstance(data[key], list)):
                if self.custom_paths == {} or (
                        table_path in self.custom_paths and key in self.custom_paths[table_path]):
                    has_simple_element = True
                    # parent_key = self.meta_dict[table_path]['current_key']
                    break
        return has_simple_element

    def check_if_new_table(self, table_path, temp_dict):
        try:
            if self.meta_dict[table_path]['user_conditions']['columns'] and any(
                    new_key in temp_dict for new_key in self.meta_dict[table_path]['user_conditions']['columns']):
                if self.user_preference['new_paths'] == 'auto_add' and table_path not in self.final_df:
                    # self.log.info(f"Creating table '{table_path}' based on user preference: Auto Add as new columns are found: {self.meta_dict[table_path]['user_conditions']['columns']}")
                    self.final_df[table_path] = []
                elif self.user_preference['new_paths'] == 'restream' and table_path not in self.restream_df:
                    # self.log.info(f"Adding table '{table_path}' to Restream Queue as new columns are found: {self.meta_dict[table_path]['user_conditions']['columns']}")
                    self.restream_df[table_path] = []
                elif self.user_preference['new_paths'] == 'auto_delete' and table_path not in self.final_df:
                    if self.meta_dict[table_path]['user_conditions']['type'] == '':
                        self.final_df[table_path] = []
            else:
                if table_path not in self.final_df:
                    self.log.info(f"Creating table {table_path}")
                    self.final_df[table_path] = []
        except Exception as e:
            self.log.error(f"Error while check_if_new_table: {str(e)}")
            raise

    def check_if_column_needed(self, table_path, key, value):
        try:
            selected_column = False
            if key not in self.meta_dict[table_path]['columns']:
                # if table_path in self.all_paths and key not in self.all_paths[table_path]:
                if self.all_paths != {} and key not in self.all_paths[table_path] and table_path + "#" + key not in self.all_paths:
                    if self.user_preference['new_paths'] != 'auto_delete':
                        if key not in self.meta_dict[table_path]['exclude']:
                            if key not in self.meta_dict[table_path]['user_conditions']['columns']:
                                self.meta_dict[table_path]['user_conditions']['columns'].append(key)
                                self.meta_dict[table_path]['columns'][key] = str(type(value))
                            selected_column = True
                    else:
                        if key not in self.meta_dict[table_path]['user_conditions']['columns']:
                            self.meta_dict[table_path]['user_conditions']['columns'].append(key)
                else:
                    if self.custom_paths == {} or (
                            table_path in self.custom_paths and key in self.custom_paths[table_path]):
                        if key not in self.meta_dict[table_path]['exclude']:
                            self.meta_dict[table_path]['columns'][key] = str(type(value))
                            selected_column = True
            else:
                selected_column = True

            return selected_column
        except Exception as e:
            self.log.error(f"Error while check_if_column_needed: {str(e)}")
            raise

    def push_to_df(self, table_path, temp_dict):
        try:
            # if self.user_preference['new_paths']!='auto_delete':
            if self.meta_dict[table_path]['user_conditions']['columns']:
                restream = False
                if self.user_preference['new_paths'] == 'restream':
                    for col in self.meta_dict[table_path]['user_conditions']['columns']:
                        if col in temp_dict:
                            restream = True  # if row contains any of the newly found columns ->>restream
                            break
                if restream:
                    self.restream_df[table_path].append(temp_dict)
                else:  # can add check for ['new_paths']=='auto_add':
                    self.final_df[table_path].append(temp_dict)
            else:
                self.final_df[table_path].append(temp_dict)
        except Exception as e:
            self.log.error(f"Error while push_to_df: {str(e)}")
            raise

    def process_data(self, data, table_path='', parent_key=None):
        try:
            temp_dict = {}
            temp_list = []
            if not table_path:
                if self.etl_name:
                    table_path = self.etl_name + '#' + 'root'
                else:
                    table_path = 'root'
            if table_path not in self.final_df and table_path not in self.restream_df and table_path not in self.meta_dict:
                self.meta_dict[table_path] = {
                    'current_key': 0,
                    'parent_key': parent_key,
                    'columns': {},
                    'exclude': [],
                    'user_conditions': {
                        'type': '',
                        'columns': []
                    }
                }

            self.meta_dict[table_path]['parent_key'] = parent_key

            if isinstance(data, dict):

                if self.all_paths != {} and table_path not in self.all_paths:
                    if self.meta_dict[table_path]['user_conditions']['type'] == '':
                        self.meta_dict[table_path]['user_conditions']['type'] = self.user_preference['new_paths']
                        self.all_paths[table_path] = []

                self.meta_dict[table_path]['current_key'] += 1
                for key in data:
                    if isinstance(data[key], dict):
                        if data[key] != {}:
                            if key not in self.meta_dict[table_path]['exclude']:
                                self.clean_df(table_path, key)

                            key_for_new_table = table_path + '#' + key
                            parent_key = self.meta_dict[table_path]['current_key']
                            self.process_data(data[key], table_path=key_for_new_table, parent_key=parent_key)
                    elif isinstance(data[key], list):
                        if data[key] != []:
                            key_for_new_table = table_path + '#' + key
                            parent_key = self.meta_dict[table_path]['current_key']
                            self.process_data(data[key], table_path=key_for_new_table, parent_key=parent_key)
                    else:
                        selected_column = self.check_if_column_needed(table_path, key, data[key])

                        if selected_column:
                            temp_dict[key] = data[key]

                if table_path in self.temp_list_val and self.temp_list_val[table_path]:
                    input_for_permutation = []
                    names = []
                    for list_name in self.temp_list_val[table_path]:
                        selected_column = self.check_if_column_needed(table_path, list_name,
                                                                      self.temp_list_val[table_path][list_name])

                        if selected_column:
                            input_for_permutation.append(self.temp_list_val[table_path][list_name])
                            names.append(list_name)

                    if names:
                        permutations = list(itertools.product(*input_for_permutation))

                        for row in permutations:
                            temp_dict_list = {}
                            if temp_dict:
                                temp_dict_list = temp_dict.copy()

                            for col_index in range(len(row)):
                                temp_dict_list[names[col_index]] = row[col_index]

                            temp_dict_list['current_key'] = self.meta_dict[table_path]['current_key']  # $%^
                            temp_dict_list['parent_key'] = self.meta_dict[table_path]['parent_key']

                            self.check_if_new_table(table_path, temp_dict_list)

                            self.push_to_df(table_path, temp_dict_list)

                    self.temp_list_val[table_path].clear()

                elif temp_dict:
                    temp_dict['current_key'] = self.meta_dict[table_path]['current_key']
                    temp_dict['parent_key'] = self.meta_dict[table_path]['parent_key']

                    self.check_if_new_table(table_path, temp_dict)

                    self.push_to_df(table_path, temp_dict)
                else:
                    if self.custom_paths == {} or (table_path in self.custom_paths):
                        temp_dict['current_key'] = self.meta_dict[table_path]['current_key']
                        temp_dict['parent_key'] = self.meta_dict[table_path]['parent_key']

                        self.check_if_new_table(table_path, temp_dict)

                        self.push_to_df(table_path, temp_dict)

            elif isinstance(data, list):

                path_list = table_path.split('#')
                parent_table_name = '#'.join(path_list[:-1])
                list_column_name = path_list[-1]

                for i, row in enumerate(data):
                    if isinstance(row, dict):
                        if row != {}:
                            if len(path_list) != 1:
                                if list_column_name not in self.meta_dict[parent_table_name]['exclude']:
                                    self.clean_df(parent_table_name,
                                                  list_column_name)  # **_**_**_ deleting from parent table the simple column

                            key_for_new_table = table_path
                            # if table_path not in self.final_df :
                            #    self.path.remove(table_path)
                            self.process_data(row, table_path=key_for_new_table, parent_key=parent_key)

                    elif isinstance(row, list):
                        if row != []:
                            key_for_new_table = table_path
                            # if table_path not in self.final_df:
                            #    self.path.remove(table_path)
                            self.process_data(row, table_path=key_for_new_table,
                                              parent_key=self.meta_dict[table_path]['parent_key'])

                    else:
                        if self.custom_paths == {} or (
                                parent_table_name in self.custom_paths and list_column_name in self.custom_paths[parent_table_name]):
                            temp_list.append(row)

                if temp_list:
                    if parent_table_name not in self.temp_list_val:
                        self.temp_list_val[parent_table_name] = {}
                    self.temp_list_val[parent_table_name][list_column_name] = temp_list
        except Exception as e:
            self.log.info(f"Error while parsing source data: {str(e)}")
            raise
        return self.final_df

    # def generate_table_df(self):
    #     try:
    #         final_df = {}
    #         restream_df = {}
    #
    #         ## find columns and tables missing from source
    #         for table_name, columns in self.custom_paths.copy().items():
    #             new_table_name = table_name.replace('#', '_')
    #             if table_name in self.meta_dict:
    #                 for selected_column in columns:
    #                     if selected_column not in ['current_key', 'parent_key']:
    #                         if selected_column not in self.meta_dict[table_name]['columns'].keys():
    #                             if new_table_name not in self.source_table_missing_data:
    #                                 self.source_table_missing_data[new_table_name] = {'table_missing'
