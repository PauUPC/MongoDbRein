from pymongo import MongoClient
from bson.code import Code
from tkinter import Tk
from tkinter.filedialog import askopenfilename
import csv

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'test'
MONGO_USER = 'user'
MONGO_PASS = 'toor'
MONGO_COLLECTION = 'collection'
MONGO_COLLECTION_RESULT = 'mapreduce'


def get_mongo_db():
    # get database and auth
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    try:
        db.authenticate(MONGO_USER, MONGO_PASS)
    except:
        return None
    return db


def create_collection(collection):
    # clean collection befor inserts
    collection.remove({})

    # open file dialog
    # Tk().withdraw()
    # filename = askopenfilename()

    # static file
    filename = '../doc/groceries.csv'

    # csv to doc and insert
    with open(filename, 'r') as inputCsv:
        reader = csv.reader(inputCsv)
        for line in list(reader):
            document = {
                "list": line
            }
            collection.insert(document)


def map_reduce(collection):
    mapper = Code("""
    function() {
        this.list.forEach(function(content){
            emit(content,1);
            });
        };
    """)

    reducer = Code("""
    function(key,values) {
        var total = 0;
        for (var i = 0; i < values.length; i++) {
            total += values[i];
            }
            return total;
        };
    """)

    result = collection.map_reduce(mapper, reducer, MONGO_COLLECTION_RESULT, full_response=True)
    print(result)


def core():
    db = get_mongo_db()
    collection = db[MONGO_COLLECTION]
    # create_collection(collection)
    result = db[MONGO_COLLECTION_RESULT]
    result.remove({})
    map_reduce(collection)
    cursor = result.find({})
    for doc in cursor:
        print(doc)


if __name__ == '__main__':
    core()
