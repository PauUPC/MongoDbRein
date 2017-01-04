from pymongo import MongoClient
from bson.code import Code
from tkinter import Tk
from tkinter import Button, Label, ttk
from tkinter.filedialog import askopenfilename
from tkinter.simpledialog import askstring
from tkinter.messagebox import showinfo
import csv

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'test'
MONGO_USER = 'user'
MONGO_PASS = 'toor'
MONGO_COLLECTION = 'collection'
MONGO_COLLECTION_RESULT_SINGLE_COUNT = 'mapreduce'
MONGO_COLLECTION_RESULT_PAIRS = 'mapreducepairs'


# connectem a la base de dades i auntentiquem, retorna la base de dades
def get_mongo_db():
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    try:
        db.authenticate(MONGO_USER, MONGO_PASS)
    except:
        return None
    return db


# crea una col·lecció nova donat un fitxer csv, si la col·lecció ja existeix la sobreescriu.
def create_collection(collection):
    # netejem la col·lecció
    collection.remove({})

    # demanem el arxiu csv
    filename = askopenfilename()
    if not filename:
        filename = '../doc/groceries.csv'

    # csv to doc i inserim
    with open(filename, 'r') as inputCsv:
        reader = csv.reader(inputCsv)
        for line in list(reader):
            document = {
                "list": line
            }
            collection.insert(document)


# funcio que calcula i guarda en dues col·leccións noves el recompte d'items i de parelles d'items
def map_reduce(collection):
    mapper = Code("""
    function() {
        this.list.forEach(function(key){
            emit(key,{count: 1});
            });
        };
    """)

    reducer = Code("""
    function(key,values) {
        var sum = 0;
        values.forEach(function(values) {
            sum += values['count'];
        });
        return {count: sum};
    };
    """)

    result = collection.map_reduce(mapper, reducer, MONGO_COLLECTION_RESULT_SINGLE_COUNT, full_response=True)
    print("single", result)

    mapper = Code("""
    function() {
        for (var i = 0; i < this.list.length - 1; i++) {
            for (var j = i; j < this.list.length - 1; j++) {
                emit({A: this.list[i], B: this.list[j+1]}, {count: 1});
            }
        }
    };
    """)

    reducer = Code("""
    function(key,values) {
        var sum = 0;
        values.forEach(function(values) {
            sum += values['count'];
        });
        return {count: sum};
    };
    """)

    result = collection.map_reduce(mapper, reducer, MONGO_COLLECTION_RESULT_PAIRS, full_response=True)
    print("pairs", result)


# funció que a partir dels recompte d'items i el recompte de parelles d'items, donats uns llindars de
# suport i confiança en calcula el nº de regles d'associació.
def recount(db):
    suport_p = askstring("Llindars", "suport en %")
    confiança_p = askstring("Llindars", "confiança en %")
    if not suport_p:
        suport_p = '1'
    if not confiança_p:
        confiança_p = '1'

    result_single_count = db[MONGO_COLLECTION_RESULT_SINGLE_COUNT]
    result_pairs = db[MONGO_COLLECTION_RESULT_PAIRS]

    count = result_pairs.count()
    regles = 0

    for doc in result_pairs.find():
        pairCount = doc['value']['count']
        pairA = doc['_id']['A']
        pairB = doc['_id']['B']
        countA = result_single_count.find_one({"_id": pairA})['value']['count']
        print("A: " + pairA + " B: " + pairB + " CountAB: " + str(pairCount) +
              " CountA: " + str(countA))
        suport = pairCount / count
        confiança = pairCount / countA

        if(suport > (float(suport_p)) * 0.01) and (confiança > (float(confiança_p) * 0.01)):
            regles += 1
            # print("res: " + str(suport) + " " + str(confiança))

    showinfo("resultat per suport: " + suport_p + "% confiança: " + confiança_p + "%",
             "nº de regles d'associació trobades: " + str(regles))


# core de la app
def core(new):
    db = get_mongo_db()
    collection = db[MONGO_COLLECTION]

    if new:
        create_collection(collection)
        result_single_count = db[MONGO_COLLECTION_RESULT_SINGLE_COUNT]
        result_single_count.remove({})
        result_pairs = db[MONGO_COLLECTION_RESULT_PAIRS]
        result_pairs.remove({})
        map_reduce(collection)

    recount(db)


if __name__ == '__main__':
    root = Tk()
    root.geometry("250x200")
    root.wm_title("REIN")

    w = Label(root, text="Programa per calcular les regles \n d'associació d'un corpus")
    w.pack()

    B1 = Button(root, text="Calcula amb una nova col·lecció", command=lambda: core(1))
    B1.pack()

    B2 = Button(root, text="Calcula amb la col·lecció existent", command=lambda: core(0))
    B2.pack()

    root.mainloop()

