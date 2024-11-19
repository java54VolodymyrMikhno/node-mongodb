import MongoConnection from "./mongo/MongoConnection.mjs";
const DB_NAME = "sample_mflix";
const COLLECTION_MOVIES_NAME = "movies";
const mongoConnection = new MongoConnection(process.env.MONGO_URI, DB_NAME);
const collectionMovies = mongoConnection.getCollection(COLLECTION_MOVIES_NAME);
collectionMovies.find({}).limit(1).project({ 'title': 1, '_id': 0 }).toArray()
    .then(data => console.log('second', data))