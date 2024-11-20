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
    .then(data => console.log('first', data));

    collectionMovies.aggregate(
        [
            {
              '$facet': {
                'allMoviesAvgRatting': [
                  {
                    '$group': {
                      '_id': null, 
                      'averageRating': {
                        '$avg': '$imdb.rating'
                      }
                    }
                  }
                ], 
                'filteredMovies': [
                  {
                    '$match': {
                      'year': 2010, 
                      'genres': 'Comedy'
                    }
                  }, {
                    '$project': {
                      'title': 1, 
                      'imdb.rating': 1, 
                      'year': 1, 
                      'genres': 1
                    }
                  }
                ]
              }
            }, {
              '$project': {
                'averageRating': {
                  '$arrayElemAt': [
                    '$allMoviesAvgRatting.averageRating', 0
                  ]
                }, 
                'filteredMovies': 1
              }
            }, {
              '$unwind': {
                'path': '$filteredMovies'
              }
            }, {
              '$addFields': {
                'filteredMovies.avgRating': '$averageRating'
              }
            }, {
              '$match': {
                '$expr': {
                  '$gt': [
                    '$filteredMovies.imdb.rating', '$filteredMovies.avgRating'
                  ]
                }
              }
            }, {
              '$project': {
                'title': '$filteredMovies.title'
              }
            }
          ] 
    ).toArray()
    .then(data => console.log('second', data));