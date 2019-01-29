let MongoClient = require('mongodb').MongoClient;
const uri = 'mongodb://localhost:27017';

(async () => {
  const client = await MongoClient.connect(uri, {
    useNewUrlParser: true
  });
  const db = client.db('example');
  const dbs = await db.admin().listDatabases();
  console.log(dbs.databases);

  const user = db.collection('user');

  //INSERT
  const record = {
    name: "abc",
    number: 10000,
    url: "https://www.runoob.com"
  };
  const x = await user.insertOne(record);
  console.log(x.insertedId)

  const records = [{
    name: "def",
    url: "https://www.runoob.com"
  }, {
    name: "ghi",
    number: 500
  }, {
    _id: 5,
    name: "Zhihu",
    address: "知乎"
  }];

  const y = await user.insertMany(records);
  console.log(y.insertedIds)

  //SELECT
  console.log(await user.find({}).toArray());
  console.log(await user.find({}).limit(2).toArray());
  console.log(await user.find({}).skip(2).limit(2).toArray());
  console.log(await user.find({}, {
    projection: {
      _id: 0,
      name: 1,
      number: 1
    }
  }).toArray());
  console.log(await user.find({
    name: 'def'
  }).toArray());
  console.log(await user.find({
    number: {
      '$gt': 1000
    }
  }).toArray());
  console.log(await user.find({
    name: {
      '$regex': '^d'
    }
  }).toArray());

  //UPDATE
  await user.updateOne({
    name: "abc"
  }, {
    '$set': {
      number: 12345
    }
  })
  await user.updateMany({
    name: {
      '$regex': '^g'
    }
  }, {
    '$set': {
      number: 999
    }
  })

  //SORT
  console.log(await user.find({}).sort('name', 1).toArray());
  console.log(await user.find({}).sort('number', -1).toArray());

  //DELETE
  await user.deleteOne({
    name: "def"
  });
  await user.deleteMany({
    name: {
      '$regex': '^g'
    }
  })
  await user.deleteMany({})

  await user.drop()

  client.close();
})();