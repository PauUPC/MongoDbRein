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


def get_mongo_db():
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    try:
        db.authenticate(MONGO_USER, MONGO_PASS)
    except:
        return None
    return db


def create_collection(collection):
    collection.remove({})
    Tk().withdraw()
    filename = askopenfilename()
    with open(filename, 'r') as inputCsv:
        parsed = csv.DictReader(inputCsv, delimiter=',', quotechar='"')
        for document in parsed:
            collection.insert(document)


def core():
    db = get_mongo_db()
    collection = db[MONGO_COLLECTION]
    create_collection(collection)

    # print the full collection
    # cursor = collection.find({})
    # for document in cursor:
    #     print(document)


if __name__ == '__main__':
    core()
