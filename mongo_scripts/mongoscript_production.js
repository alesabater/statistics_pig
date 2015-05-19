db = connect("prod-mongodb-01-a.awsservers.mobiquitynetworks.com:27017/backend");

db.statistics.drop();

db.createCollection("statistics");

db.statistics.ensureIndex({
    "ts.b" : -1,
    "ts.e" : -1,
    "ts.u" : 1,
    "nd" : 1,
    "kd.k" : 1
})
