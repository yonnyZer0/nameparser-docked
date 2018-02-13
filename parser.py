#! /usr/bin/env python

from nameparse import TextProcessingPipeline
from py2_apify import ApifyClient
import json

dataset_name = 'PoC-Robert'


client = ApifyClient({'APIFY_TOKEN': token, 'APIFY_DEFAULT_DATASET_ID': dataset_name})

print(client.getOptions())

print(client.datasets.getOrCreateDataset())

id_ = json.loads(client.datasets.getOrCreateDataset())['data']['id']
print(id_)

client.setOptions({'APIFY_DEFAULT_DATASET_ID': id_})

print(client.datasets.getItems())

parser = TextProcessingPipeline()

#get data from existing dataset
load = json.loads(client.datasets.getItems({'APIFY_DEFAULT_DATASET_ID': id_}))

#delete existing dataset
client.datasets.deleteStore()

# reacreates dataset
id_ = json.loads(client.datasets.getOrCreateDataset( {'APIFY_DEFAULT_DATASET_ID': dataset_name} ))['data']['id']
print(id_)

client.setOptions({'APIFY_DEFAULT_DATASET_ID': id_})

new_load = []

for item in load:#json.loads(client.datasets.getItems({'APIFY_DEFAULT_DATASET_ID': id_})):
    new_load.append( parser.process_item( item ) )

client.pushRecords({'data': new_load})
