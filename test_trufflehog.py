import pandas as pd

df = pd.read_csv('git_list.csv')
new_dict = df.T.to_dict().values()
another_dict_list = []
proper_dict_list = []
for i in new_dict:
    # print(i)
    mail = i["sourcemetadata_data_git_email"]
    value = mail.split("@")
    if len(value) > 1:
        if value[1] != "payu.in>":
            # print(val[0], val[1])
            new_val = value[1].split(".")
            if len(new_val) > 1 and new_val[1] != "local>":
                # print(mail)
                another_dict_list.append(i)
        else:
            proper_dict_list.append(i)
    else:
        another_dict_list.append(i)
        # print(mail)
        # pass
new_list = df.values.tolist()
new_df = pd.DataFrame(another_dict_list)
another_df = pd.DataFrame(proper_dict_list)
new_df.to_csv('unknown_email_git.csv')
another_df.to_csv('known_email_git.csv')
# print(new_list)
# another_list = []
# for i in new_list:
#     mail = i[3]
#     val = mail.split("@")
#     # print(mail.split("@")[1])
#     if len(val) > 1:
#         if val[1] != "payu.in>":
#             # print(val[0], val[1])
#             new_val = val[1].split(".")
#             if len(new_val) > 1 and new_val[1] != "local>":
#                 print(mail)
#                 another_list.append(i)
#     else:
#         another_list.append(i)
#         print(mail)
#         # pass
