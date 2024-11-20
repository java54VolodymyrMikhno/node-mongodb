import MongoConnection from "./mongo/MongoConnection.mjs";
const DB_NAME = "sample_mflix";
const COLLECTION_MOVIES_NAME = "movies";
const COLLECTION_COMMENTS_NAME = "comments";
const mongoConnection = new MongoConnection(process.env.MONGO_URI, DB_NAME);
const collectionMovies = mongoConnection.getCollection(COLLECTION_MOVIES_NAME);
const collectionComments = mongoConnection.getCollection(COLLECTION_COMMENTS_NAME);

collectionComments.aggregate([
    {
        '$lookup': {
            'from': 'movies',
            'localField': 'movie_id',
            'foreignField': '_id',
            'as': 'movieDetalies'
        }
    }, {
        '$match': {
            'movieDetalies': {
                '$not': {
                    '$size': 0
                }
            }
        }
    }, {
        '$limit': 5
    }, {
        '$replaceRoot': {
            'newRoot': {
                '$mergeObjects': [
                    {
                        'title': {
                            '$arrayElemAt': [
                                '$movieDetalies.title', 0
                            ]
                        }
                    }, '$$ROOT'
                ]
            }
        }
    }, {
        '$project': {
            'movieDetalies': 0,
            'movie_id': 0
        }
    }
]).toArray()
    .then(data => console.log('third', data));