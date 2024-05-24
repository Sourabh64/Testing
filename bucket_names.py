# import json
#
# with open('jsons/s3_bucket_response.json', 'r') as f:
#     data = json.load(f)
# # print(data)
# for each in data:
#     buckets = each['Buckets']
#     # print(buckets)
#     for bucket in buckets:
#         text_file = open('bucket.txt', 'a')
#         text_file.write(bucket['Name']+"\n")
#         text_file.close()
import s3scanner
