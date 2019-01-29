let _ = require('lodash');
let MongoClient = require('mongodb').MongoClient;
const uri = 'mongodb://localhost:27017';

(async () => {
  const client = await MongoClient.connect(uri, {
    useNewUrlParser: true
  });
  const db = client.db('example');

  const grade = ['grade_1_1', 'grade_1_2', 'grade_1_3'];
  for (let g of grade) {
    for (let i = 1; i <= 10; i++) {
      await db.collection(g).insertOne({
        name: `student${i}`,
        sex: Math.round(Math.random() * 10) % 2,
        age: Math.round(Math.random() * 6) + 3,
        hobby: _.sampleSize(['drawing', 'dancing', 'running', 'sleeping', 'reading'], _.random(0, 4))
      })
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////
  // practice select
  ///////////////////////////////////////////////////////////////////////////////////
  // all students in grade_1_2
  console.log(await db.collection('grade_1_2').find().toArray());
  // all students in grade_1_2 with age = 4
  console.log(await db.collection('grade_1_2').find({
    age: 4
  }).toArray());
  // all students in grade_1_2 with age > 4
  console.log(await db.collection('grade_1_2').find({
    age: {
      $gt: 4
    }
  }).toArray());
  // all students in grade_1_2 with age > 4 and age < 7
  console.log(await db.collection('grade_1_2').find({
    age: {
      $gt: 4,
      $lt: 7
    }
  }).toArray());
  // all students in grade_1_2 with age > 4 and sex = 0
  console.log(await db.collection('grade_1_2').find({
    age: {
      $gt: 4
    },
    sex: 0
  }).toArray());
  // all students in grade_1_2 with age < 4 or age < 7
  db["grade_1_2"].find({
    "$or": [{
      "age": {
        "$lt": 4
      }
    }, {
      "age": {
        "$gt": 7
      }
    }]
  })
  console.log(await db.collection('grade_1_2').find({
    $or: [{
      age: {
        $lt: 4
      }
    }, {
      age: {
        $gt: 7
      }
    }]
  }).toArray());
  // all students in grade_1_2 with age = 4 or age = 6
  db["grade_1_2"].find({
    "age": {
      "$in": [4, 6]
    }
  })
  console.log(await db.collection('grade_1_2').find({
    age: {
      $in: [4, 6]
    }
  }).toArray());
  // all students in grade_1_2 with name containing student1
  console.log(await db.collection('grade_1_2').find({
    name: {
      $regex: 'student1'
    }
  }).toArray());
  // all students in grade_1_2 with name containing student1 or student2
  console.log(await db.collection('grade_1_2').find({
    name: {
      $regex: 'student[12]'
    }
  }).toArray());
  console.log(await db.collection('grade_1_2').find({
    name: {
      $in: [new RegExp('student1'), new RegExp('student2')]
    }
  }).toArray());
  // all students in grade_1_2 with 3 hobbies
  console.log(await db.collection('grade_1_2').find({
    hobby: {
      $size: 3
    }
  }).toArray());
  // all students in grade_1_2 with drawing hobby
  console.log(await db.collection('grade_1_2').find({
    hobby: 'drawing'
  }).toArray());
  // all students in grade_1_2 with drawing and dancing hobby
  db["grade_1_2"].find({
    "hobby": {
      "$all": ["drawing", "dancing"]
    }
  })
  console.log(await db.collection('grade_1_2').find({
    hobby: {
      $all: ['drawing', 'dancing']
    }
  }).toArray());
  // number of students in grade_1_2 with 3 hobbies
  console.log(await db.collection('grade_1_2').countDocuments({
    hobby: {
      $size: 3
    }
  }));
  // student number 2 in grade_1_2
  console.log(await db.collection('grade_1_2').find().skip(1).limit(1).toArray());
  // students in grade_1_2 ordered
  console.log(await db.collection('grade_1_2').find().sort({
    age: 1
  }).toArray());
  console.log(await db.collection('grade_1_2').find().sort({
    age: -1
  }).toArray());
  // different ages of students in grade_1_2
  console.log(await db.collection('grade_1_2').distinct("age"));
  // different hobbies of students in grade_1_2
  console.log(await db.collection('grade_1_2').distinct("hobby"));



  ///////////////////////////////////////////////////////////////////////////////////
  // practice delete
  ///////////////////////////////////////////////////////////////////////////////////
  // delete all students in grade_1_2 with age = 4
  await db.collection('grade_1_2').deleteMany({
    age: 4
  })
  // delete one students in grade_1_2 with age = 6
  await db.collection('grade_1_2').deleteOne({
    age: 6
  })



  ///////////////////////////////////////////////////////////////////////////////////
  // practice update
  ///////////////////////////////////////////////////////////////////////////////////
  // set student7 in grade_1_2 with age = 8, hobby = dancing + drawing
  await db.collection('grade_1_2').updateOne({
    name: "student7"
  }, {
    $set: {
      age: 8,
      hobby: ["dancing", "drawing"]
    }
  });
  // set student7 in grade_1_2 add hobby sing
  await db.collection('grade_1_2').updateOne({
    name: "student7"
  }, {
    $push: {
      hobby: "sing"
    }
  });
  // set student7 in grade_1_2 add hobby brag and basketball
  await db.collection('grade_1_2').updateOne({
    name: "student7"
  }, {
    $push: {
      hobby: {
        $each: ["brag", "basketball"]
      }
    }
  });
  // set student7 in grade_1_2 add hobby sing and brag, but no duplication
  await db.collection('grade_1_2').updateOne({
    name: "student7"
  }, {
    $addToSet: {
      hobby: {
        $each: ["brag", "sing"]
      }
    }
  });
  // all students in grade_1_2 age plus 1
  await db.collection('grade_1_2').updateMany({}, {
    $inc: {
      "age": 1
    }
  });
  // set student7 in grade_1_2 remove sex
  await db.collection('grade_1_2').updateOne({
    name: "student7"
  }, {
    $unset: {
      sex: ""
    }
  });
  // set student7 in grade_1_2 remove first hobby
  await db.collection('grade_1_2').updateOne({
    name: "student7"
  }, {
    $pop: {
      "hobby": -1
    }
  });
  // set student7 in grade_1_2 remove last hobby
  await db.collection('grade_1_2').updateOne({
    name: "student7"
  }, {
    $pop: {
      "hobby": 1
    }
  });
  // set student7 in grade_1_2 remove sing hobby
  await db.collection('grade_1_2').updateOne({
    name: "student7"
  }, {
    $pull: {
      "hobby": "sing"
    }
  });



  for (let i of _.range(1, 11)) {
    db.collection('grade_1_4').insertOne({
      name: `student${i}`,
      sex: _.random(1, 2),
      age: _.random(3, 10),
      score: {
        "chinese": _.random(60, 100, true),
        "math": _.random(60, 100, true),
        "english": _.random(60, 100, true)
      }
    })
  }

  ///////////////////////////////////////////////////////////////////////////////////
  // practice mapreduce
  // reduce function will not be called if there is only 1 element
  ///////////////////////////////////////////////////////////////////////////////////
  // calculate sum score for all students in grade_1_4
  const mapFunc = function () {
    emit(this.name, this.score.chinese + this.score.math + this.score.english);
  }
  const reduceFunc = function (key, values) {
    return Array.sum(values);
  }
  console.log(await db.collection("grade_1_4").mapReduce(mapFunc, reduceFunc, {
    out: {
      inline: 1
    }
  }));
  // calculate sum score for all sex=1 students in grade_1_4
  console.log(await db.collection("grade_1_4").mapReduce(mapFunc, reduceFunc, {
    out: {
      inline: 1
    },
    query: {
      sex: 1
    }
  }));
  // calculate sum score and avg score for all sex=1 students in grade_1_4
  finalizeFunc = function (key, value) {
    return {
      total: value,
      avg: value / 3
    };
  }
  console.log(await db.collection("grade_1_4").mapReduce(mapFunc, reduceFunc, {
    out: {
      inline: 1
    },
    query: {
      sex: 1
    },
    finalize: finalizeFunc
  }));



  ///////////////////////////////////////////////////////////////////////////////////
  // practice aggregate
  ///////////////////////////////////////////////////////////////////////////////////
  // calculate sum score for all students in grade_1_4
  console.log(await db.collection("grade_1_4").aggregate([{
    $group: {
      _id: "$name",
      total: {
        $sum: {
          $add: ["$score.chinese", "$score.math", "$score.english"]
        }
      }
    }
  }]).toArray());
  // calculate sum score for all sex=1 students in grade_1_4
  console.log(await db.collection("grade_1_4").aggregate(
    [{
        $match: {
          sex: 1
        }
      },
      {
        $group: {
          _id: "$name",
          total: {
            $sum: {
              $add: ["$score.chinese", "$score.math", "$score.english"]
            }
          }
        }
      }
    ]).toArray());
  // calculate sum score for all sex=1 students in grade_1_4, sort by score desc
  console.log(await db.collection("grade_1_4").aggregate(
    [{
        $match: {
          sex: 1
        }
      },
      {
        $group: {
          _id: "$name",
          total: {
            $sum: {
              $add: ["$score.chinese", "$score.math", "$score.english"]
            }
          }
        }
      },
      {
        $sort: {
          total: -1
        }
      }
    ]).toArray());
  // calculate sum score and avg score for all sex=1 students in grade_1_4
  console.log(await db.collection("grade_1_4").aggregate(
    [{
        $match: {
          sex: 1
        }
      },
      {
        $group: {
          _id: "$name",
          total: {
            $sum: {
              $add: ["$score.chinese", "$score.math", "$score.english"]
            }
          }
        }
      },
      {
        $project: {
          total: 1,
          avg: {
            $divide: ['$total', 3]
          }
        }
      }
    ]).toArray());
  // calculate count of different names for all students in grade_1_4
  console.log(await db.collection("grade_1_4").aggregate([{
    $group: {
      _id: "$name",
      count: {
        $sum: 1
      }
    }
  }]).toArray());
  // calculate count of different names for all students in grade_1_4, only get count > 1
  console.log(await db.collection("grade_1_4").aggregate([{
      $group: {
        _id: "$name",
        count: {
          $sum: 1
        }
      }
    },
    {
      $match: {
        count: {
          $gt: 1
        }
      }
    }
  ]).toArray());


  ///////////////////////////////////////////////////////////////////////////////////
  // practice user management
  ///////////////////////////////////////////////////////////////////////////////////

  db.command({
    createUser: "class_admin1",
    pwd: "123",
    roles: [{
      "role": "readWrite",
      "db": "school"
    }]
  });
  console.log(await db.command({
    usersInfo: 1
  }));
  await db.removeUser('class_admin1');

  client.close();
})();