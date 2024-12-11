"""
Connexion `a MySQL : R´ecup´erez les donn´ees depuis la table texts cr´e´ee `a
l’´etape pr´ec´edente.
• Tokenisation des textes : Utilisez le tokenizer de votre choix pour tokenizer les
donn´ees. Si vous avez du mal `a choisir, gpt2 ou distilbert-base-uncased peuvent
ˆetre de bonnes options.
• Connexion `a MongoDB : Connectez-vous `a la base MongoDB locale.
• Insertion dans la collection MongoDB : Cr´eez une collection appel´ee wikitext
dans la base curated et ins´erez les donn´ees tokenis´ees tout en conservant les
m´etadonn´ees.
Voici un exemple de structure de document JSON ins´er´e dans MongoDB :
{
"id": "1",
"text": "This is a sample sentence.",
"tokens": [2023, 2003, 1037, 7099, 6251, 1012],
"metadata": {
"source": "mysql",
"processed_at": "2024-01-01T10:00:00Z"
}
}
"""
import pandas as pd
import boto3
import sqlite3
from transformers import DistilBertTokenizer
from tqdm import tqdm


def connect_to_sqlite():
    """
    Connects to a SQLite database.

    Returns:
    conn: Connection object to the SQLite database.
    cursor: Cursor object for executing SQL queries.
    """
    conn = sqlite3.connect('../sqlite_DB.db')
    cursor = conn.cursor()
    print("Connected to SQLite database.")
    return conn, cursor

def text_tokenization_distilbert_base_uncased(text, tokenizer):
    """
    Tokenizes the input text using the DistilBERT tokenizer.

    Parameters:
    text (str): Text to tokenize.

    Returns:
    list: List of token IDs.
    """
    # Tokenize the text
    if text is None:
        text = ""
    tokens = tokenizer.encode(text, add_special_tokens=True)
    return tokens

def connect_to_mongo():
    """
    Connects to a MongoDB database.

    Returns:
    pymongo.MongoClient: Connection object to the MongoDB database.
    """
    from pymongo import MongoClient
    print("Connecting to MongoDB...")
    # Connect to the MongoDB server
    client = MongoClient('mongodb://localhost:27017/')
    print("Connected to MongoDB.")
    return client

def insert_into_mongo(client, db_name, collection_name, data_list):
    """
    Inserts data into a MongoDB collection.

    Parameters:
    client (pymongo.MongoClient): Connection object to the MongoDB database.
    db_name (str): Name of the MongoDB database.
    collection_name (str): Name of the MongoDB collection.
    data_list (list): List of dictionaries to insert into the collection.
    """
    db = client[db_name]
    collection = db[collection_name]
    for data in tqdm(data_list, desc="Inserting data into MongoDB"):
        collection.insert_one(data)
    print("Data inserted into MongoDB collection.")

if __name__ == "__main__":
    # Connect to SQLite database
    conn, cursor = connect_to_sqlite()

    # Retrieve data from the 'texts' table
    cursor.execute('SELECT * FROM texts')
    data = cursor.fetchall()
    print("Retrieved data from SQLite database.")
    print(cursor.execute('SELECT COUNT(*) FROM texts').fetchone()[0], " rows in from SQlite table.")

    # Connect to MongoDB
    client = connect_to_mongo()

    # Tokenize the text data using DistilBERT
    tokenized_data = []
    tokenizer = DistilBertTokenizer.from_pretrained('distilbert-base-uncased')

    for row in tqdm(data, desc="Tokenizing data"):
        text_id, text = row
        tokens = text_tokenization_distilbert_base_uncased(text, tokenizer)
        tokenized_data.append({
            'id': text_id,
            'text': text,
            'tokens': tokens,
            'metadata': {
                'source': 'sqlite',
                'processed_at': '2024-01-01T10:00:00Z'
            }
        })

    print("Text data tokenized using DistilBERT.")

    # Insert tokenized data into the 'wikitext' collection
    insert_into_mongo(client, 'curated', 'wikitext', tokenized_data)
    # Close the SQLite connection
    conn.close()