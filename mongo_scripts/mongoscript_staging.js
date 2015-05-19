db = connect("staging-mongodb-01.awsservers.mobiquitynetworks.com:27017/backend");

db.statistics.drop();

db.createCollection("statistics");

db.statistics.ensureIndex({
    "ts.b" : -1,
    "ts.e" : -1,
    "ts.u" : 1,
    "nd" : 1,
    "kd.k" : 1
})

