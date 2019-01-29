import pymongo
from bson.objectid import ObjectId

# client = pymongo.MongoClient("mongodb://localhost:27017/")
client = pymongo.MongoClient(host='localhost', port=27017)

dbs = client.list_database_names()
if "example" in dbs:
    print("DB already exist!")

# Create DB
db = client["example"]

collections = db.list_collection_names()
if "user" in dbs:
    print("Collection already exist!")

# Create collection
user = db["user"]

# INSERT
record = {"name": "abc", "number": "10000", "url": "https://www.runoob.com"}
x = user.insert_one(record)
print(x.inserted_id)

records = [{
    "name": "def",
    "url": "https://www.runoob.com"
}, {
    "name": "ghi",
    "number": "500"
}, {
    "_id": 5,
    "name": "Zhihu",
    "address": "知乎"
}]
x = user.insert_many(records)
print(x.inserted_ids)

# SELECT
for x in user.find():
    print(x)

for x in user.find().limit(2):
    print(x)

for x in user.find().skip(2).limit(2):
    print(x)

for x in user.find({}, {"_id": 0, "name": 1, "number": 1}):
    print(x)

for x in user.find({"name": "def"}):
    print(x)

for x in user.find({"number": {"$gt": "1000"}}):
    print(x)

for x in user.find({"name": {"$regex": "^d"}}):
    print(x)

# UPDATE
user.update_one({"name": "abc"}, {"$set": {"number": "12345"}})
user.update_many({"name": {"$regex": "^g"}}, {"$set": {"number": "999"}})

# DELETE
user.delete_one({"name": "def"})
user.delete_many({"name": {"$regex": "^g"}})
user.delete_many({})

user.drop()

# SORT
for x in user.find().sort("name"):
    print(x)

for x in user.find().sort("number", -1):
    print(x)
