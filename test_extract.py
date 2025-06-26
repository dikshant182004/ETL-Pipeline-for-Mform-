from etl.scripts.extract import fetch_form
from bson import ObjectId
import json

# Replace with a real ObjectId string from your MongoDB 'forms' collection
sample_form_id = ObjectId("6239564d26321d599edf78da")  

form_json = fetch_form(sample_form_id)

# Preview first 500 characters of extracted JSON
print(form_json)
