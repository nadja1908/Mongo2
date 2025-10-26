#!/bin/bash

if ! mongo mongo_database --eval "db.stats()" &> /dev/null; then
    # Create the database
    mongo mongo_database --eval "db.getSiblingDB('mongo_database').createCollection('dummy')"
fi

# Check if MongoDB database exists
if ! mongo mongo_database --eval "db.stats()" &> /dev/null; then
    # Import movie data if dump folder is present
    if [ -d "/dump" ]; then
        mongorestore --db mongo_database --collection movie_collection ./dump/movies.bson
        mongorestore --db mongo_database --collection movie_collection ./dump/movieDetails.bson
        mongorestore --db mongo_database --collection movie_collection ./dump/moviesScratch.bson
        mongorestore --db mongo_database --collection movie_collection ./dump/reviews.bson
        mongorestore --db mongo_database --collection movie_collection ./dump/system.indexes.bson
    fi

    # Import county population data if zips.json is present
    if [ -f "/zips.json" ]; then
        mongoimport --db mongo_database --collection population_collection /zips.json
    fi
fi
