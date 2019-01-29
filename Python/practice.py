import pymongo
import random
import re
from bson.objectid import ObjectId
from bson.code import Code

# client = pymongo.MongoClient("mongodb://localhost:27017/")
client = pymongo.MongoClient(host='localhost', port=27017)
dbs = client.list_database_names()
db = client["example"]

# create 3 classes for a grade, randomly add 10 students in each class
grade = ["grade_1_1", "grade_1_2", "grade_1_3"]

for g in grade:
    for i in range(1, 11):
        db[g].insert_one({
            "name":
            "student{}".format(i),
            "sex":
            random.randint(1, 2),
            "age":
            random.randint(3, 10),
            "hobby":
            random.sample(
                ["drawing", "dancing", "running", "sleeping", "reading"],
                random.randint(0, 5))
        })

######################################################################
# practice select
######################################################################
# all students in grade_1_2
db["grade_1_2"].find()
# all students in grade_1_2 with age = 4
db["grade_1_2"].find({"age": 4})
# all students in grade_1_2 with age > 4
db["grade_1_2"].find({"age": {"$gt": 4}})
# all students in grade_1_2 with age > 4 and age < 7
db["grade_1_2"].find({"age": {"$gt": 4, "$lt": 7}})
# all students in grade_1_2 with age > 4 and sex = 0
db["grade_1_2"].find({"age": {"$gt": 4}, "sex": 0})
# all students in grade_1_2 with age < 4 or age < 7
db["grade_1_2"].find({"$or": [{"age": {"$lt": 4}}, {"age": {"$gt": 7}}]})
# all students in grade_1_2 with age = 4 or age = 6
db["grade_1_2"].find({"age": {"$in": [4, 6]}})
# all students in grade_1_2 with name containing student1
db["grade_1_2"].find({"name": {"$regex": "student1"}})
# all students in grade_1_2 with name containing student1 or student2
db["grade_1_2"].find({
    "name": {
        "$in": [re.compile("student1"),
                re.compile("student2")]
    }
})
db["grade_1_2"].find({"name": {"$regex": "student[12]"}})
# all students in grade_1_2 with 3 hobbies
db["grade_1_2"].find({"hobby": {"$size": 3}})
# all students in grade_1_2 with drawing hobby
db["grade_1_2"].find({"hobby": "drawing"})
# all students in grade_1_2 with drawing and dancing hobby
db["grade_1_2"].find({"hobby": {"$all": ["drawing", "dancing"]}})
# number of students in grade_1_2 with 3 hobbies
db["grade_1_2"].count_documents({"hobby": {"$size": 3}})
# student number 2 in grade_1_2
db["grade_1_2"].find().skip(1).limit(1)
# students in grade_1_2 ordered
db["grade_1_2"].find().sort("age", 1)
db["grade_1_2"].find().sort("age", -1)
# different ages of students in grade_1_2
db["grade_1_2"].distinct("age")
# different hobbies of students in grade_1_2
db["grade_1_2"].distinct("hobby")

######################################################################
# practice delete
######################################################################
# delete all students in grade_1_2 with age = 4
db["grade_1_2"].delete_many({"age": 4})
# delete one students in grade_1_2 with age = 6
db["grade_1_2"].delete_one({"age": 6})

######################################################################
# practice update
######################################################################
# set student7 in grade_1_2 with age = 8, hobby = dancing + drawing
db["grade_1_2"].update_one({
    "name": "student7"
}, {"$set": {
    "age": 8,
    "hobby": ["dancing", "drawing"]
}})
# set student7 in grade_1_2 add hobby sing
db["grade_1_2"].update_one({"name": "student7"}, {"$push": {"hobby": "sing"}})
# set student7 in grade_1_2 add hobby brag and basketball
db["grade_1_2"].update_one({
    "name": "student7"
}, {"$push": {
    "hobby": {
        "$each": ["brag", "basketball"]
    }
}})
# set student7 in grade_1_2 add hobby sing and brag, but no duplication
db["grade_1_2"].update_one({
    "name": "student7"
}, {"$addToSet": {
    "hobby": {
        "$each": ["brag", "sing"]
    }
}})
# all students in grade_1_2 age plus 1
db["grade_1_2"].update_many({}, {"$inc": {"age": 1}})
# set student7 in grade_1_2 remove sex
db["grade_1_2"].update_one({"name": "student7"}, {"$unset": {"sex": ""}})
# set student7 in grade_1_2 remove first hobby
db["grade_1_2"].update_one({"name": "student7"}, {"$pop": {"hobby": -1}})
# set student7 in grade_1_2 remove last hobby
db["grade_1_2"].update_one({"name": "student7"}, {"$pop": {"hobby": 1}})
# set student7 in grade_1_2 remove sing hobby
db["grade_1_2"].update_one({"name": "student7"}, {"$pull": {"hobby": "sing"}})

for i in range(1, 11):
    db["grade_1_4"].insert_one({
        "name": "student{}".format(i),
        "sex": random.randint(1, 2),
        "age": random.randint(3, 10),
        "score": {
            "chinese": 60 + random.random() * 40,
            "math": 60 + random.random() * 40,
            "english": 60 + random.random() * 40
        }
    })

######################################################################
# practice mapreduce
# reduce function will not be called if there is only 1 element
######################################################################
# calculate sum score for all students in grade_1_4
mapFunc = Code("""
    function() {
        emit(this.name, this.score.chinese + this.score.math + this.score.english);
    }
""")

reduceFunc = Code("""
    function(key, values) {
        return Array.sum(values);
    }
""")
result = db["grade_1_4"].map_reduce(
    mapFunc, reduceFunc, {"inline": 1}, query={})

# calculate sum score for all sex=1 students in grade_1_4
result = db["grade_1_4"].map_reduce(
    mapFunc, reduceFunc, {"inline": 1}, query={"sex": 1})

# calculate sum score and avg score for all sex=1 students in grade_1_4
finalizeFunc = Code("""
    function(key, value) {
        return value / 3;
    }
""")
result = db["grade_1_4"].map_reduce(
    mapFunc, reduceFunc, {"inline": 1}, query={}, finalize=finalizeFunc)
list(result.find())

######################################################################
# practice aggregate
######################################################################
# calculate sum score for all students in grade_1_4
db["grade_1_4"].aggregate([{
    "$group": {
        "_id": "$name",
        "total": {
            "$sum": {
                "$add": ["$score.chinese", "$score.math", "$score.english"]
            }
        }
    }
}])
# calculate sum score for all sex=1 students in grade_1_4
db["grade_1_4"].aggregate(
    [{
        "$match": {
            "sex": 1
        }
    },
     {
         "$group": {
             "_id": "$name",
             "total": {
                 "$sum": {
                     "$add":
                     ["$score.chinese", "$score.math", "$score.english"]
                 }
             }
         }
     }])
# calculate sum score for all sex=1 students in grade_1_4, sort by score desc
db["grade_1_4"].aggregate(
    [{
        "$match": {
            "sex": 1
        }
    },
     {
         "$group": {
             "_id": "$name",
             "total": {
                 "$sum": {
                     "$add":
                     ["$score.chinese", "$score.math", "$score.english"]
                 }
             }
         }
     }, {
         "$sort": {
             "total": -1
         }
     }])
# calculate sum score and avg score for all sex=1 students in grade_1_4
db["grade_1_4"].aggregate(
    [{
        "$match": {
            "sex": 1
        }
    },
     {
         "$group": {
             "_id": "$name",
             "total": {
                 "$sum": {
                     "$add":
                     ["$score.chinese", "$score.math", "$score.english"]
                 }
             }
         }
     }, {
         "$project": {
             "total": 1,
             "avg": {
                 "$divide": ["$total", 3]
             }
         }
     }])
# calculate count of different names for all students in grade_1_4
db["grade_1_4"].aggregate([{"$group": {"_id": "$name", "count": {"$sum": 1}}}])
# calculate count of different names for all students in grade_1_4, only get count > 1
db["grade_1_4"].aggregate([{
    "$group": {
        "_id": "$name",
        "count": {
            "$sum": 1
        }
    }
}, {
    "$match": {
        "count": {
            "$gt": 1
        }
    }
}])

######################################################################
# practice user management
######################################################################

db.command(
    "createUser",
    "class_admin1",
    pwd="123",
    roles=[{
        "role": "readWrite",
        "db": "school"
    }])
db.command("updateUser", "class_admin", pwd="456")
db.authenticate("class_admin", "456")
db.command("usersInfo")
db.command("dropUser", "class_admin1")
db.command("logout")
db.logout()