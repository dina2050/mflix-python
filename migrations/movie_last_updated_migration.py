from pymongo import MongoClient, UpdateOne
from pymongo.errors import InvalidOperation
from bson import ObjectId
import dateutil.parser as parser

host = "mongodb+srv://m220student:m220password@mflix-s8akn.mongodb.net/test"
mflix = MongoClient(host)["sample_mflix"]

# here we're making sure "lastupdated" exists in the document as a string
predicate = {"lastupdated": {"$exists": True, "$type": "string"}}
# this projection only sends the "lastupdated" and "_id" fields back to the client
projection = {"lastupdated": 1}

cursor = mflix.movies.find(predicate, projection)

updates = []
for doc in cursor:
    doc_id = doc.get('_id')
    lastupdated = doc.get('lastupdated', None)
    updates.append(
        {
            "doc_id": ObjectId(doc_id),
            "lastupdated": parser.parse(lastupdated)
        }
    )

print(f"{len(updates)} documents to update")

try:
    # this will gather UpdateOne operations into a bulk_updates array
    # we target the document with "_id" and then set its "lastupdated" field
    # to the new ISODate type
    bulk_updates = [UpdateOne(
        {"_id": update.get("doc_id")},
        {"$set": {"lastupdated": update.get("lastupdated")}}
    ) for update in updates]

    bulk_results = mflix.movies.bulk_write(bulk_updates)
    print(f"{bulk_results.modified_count} documents updated")

except InvalidOperation:
    print("no updates necessary")
except Exception as e:
    print(str(e))
