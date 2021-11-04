import json

import xmltodict

db_dump_file="/home/bapu/Public/SampleData/gcp_migrate_tbl.xml"

with open(db_dump_file) as xml_file:
    data_dict = xmltodict.parse(xml_file.read())

xml_file.close()

json_data = json.dumps(data_dict)

# Convert string to Python dict
data_dict = json.loads(json_data)
# print(data_dict['database']['table'])

for table_entry in data_dict['database']['table']:
  for x in table_entry['column']:
      if x['@name']=='blob_column':
          note_xml_dict = xmltodict.parse(x['#text'])
          note_json = json.dumps(note_xml_dict)
          print(note_json)
  print("============")

# with open("/home/bapu/Public/SampleData/export.json", "w") as json_file:
#     json_file.write(json_data)
#
# json_file.close()