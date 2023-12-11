# vendor groups are already stored in mysql
# todo: maybe write a script to import mysql to mongodb

import requests,pymongo,json
from tqdm import tqdm
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process
from datetime import date

dburl = "mongodb://localhost:27017"
myclient = pymongo.MongoClient(dburl)

mydb =  myclient["nimble_live"]
nimble_live_vendors = mydb["vendors"]

mydb1 = myclient["vendortool2"]
validated_vendors = mydb1["vendor"]


mycollection = mydb1["vendor_groups"]

def clean_text(text):
    text = text.lower()
    
    special_characters = "!@#$%^&*()_-+={}[]|\\:;<>,.?/~`"
    
    for char in special_characters:
        text = text.replace(char, '')

    text = ' '.join(text.split())

    return text

vendors = list(validated_vendors.find())
nimble_vendors =list(nimble_live_vendors.find())

vendor_group_ids_ratio =[]
vendor_group_ids_partial_ratio =[] 
vendor_group_ids_token_sort_ratio =[] 
vendor_group_ids_token_set_ratio =[] 

group_id = 1

for vendor in tqdm(vendors):
    # print(vendor["name"])
    vendor_name = clean_text(vendor["name"])
    # print(vendor_name)
    
    for nimble_vendor in tqdm(nimble_vendors):
        # print(nimble_vendor["name"])
        nimble_vendor_name = clean_text(nimble_vendor["name"])
        # print(nimble_vendor_name)
        
        ratio = fuzz.ratio(vendor_name,nimble_vendor_name)
        if ratio >= 80:
            vg = {"uuid":nimble_vendor["id"],"name":vendor["name"],"group_id":group_id,"vendor_name":vendor_name,"nimble_vendor_name":nimble_vendor_name,"ratio":ratio}
            vendor_group_ids_ratio.append(vg)
            
        partial_ratio = fuzz.partial_ratio(vendor_name,nimble_vendor_name)
        if partial_ratio >= 80:
            vg = {"uuid":nimble_vendor["id"],"name":vendor["name"],"group_id":group_id,"vendor_name":vendor_name,"nimble_vendor_name":nimble_vendor_name,"partial_ratio":partial_ratio}
            vendor_group_ids_partial_ratio.append(vg)
            
        token_sort_ratio = fuzz.token_sort_ratio(vendor_name,nimble_vendor_name)
        if token_sort_ratio >= 80:
            vg = {"uuid":nimble_vendor["id"],"name":vendor["name"],"group_id":group_id,"vendor_name":vendor_name,"nimble_vendor_name":nimble_vendor_name,"token_sort_ratio":token_sort_ratio}
            vendor_group_ids_token_sort_ratio.append(vg)
            
        token_set_ratio = fuzz.token_set_ratio(vendor_name,nimble_vendor_name)
        if token_set_ratio >= 80:
            vg = {"uuid":nimble_vendor["id"],"name":vendor["name"],"group_id":group_id,"vendor_name":vendor_name,"nimble_vendor_name":nimble_vendor_name,"token_set_ratio":token_set_ratio}
            vendor_group_ids_token_set_ratio.append(vg)
        
        
            
            
    group_id += 1
    
    if group_id >500:
        break        
            
        
       
    
ratios_dict = {
    'vendor_group_ids_ratio': vendor_group_ids_ratio,
    'vendor_group_ids_partial_ratio': vendor_group_ids_partial_ratio,
    'vendor_group_ids_token_sort_ratio': vendor_group_ids_token_sort_ratio,
    'vendor_group_ids_token_set_ratio': vendor_group_ids_token_set_ratio
}

for ratio_name, ratio_list in ratios_dict.items():
    path = f"data/{ratio_name}_{date.today()}.json"
    print(path)
    with open(path, "w") as json_file:
        json.dump(ratio_list, json_file)




