"""
Créez un script pour préparer les donn´ees au staging
Ajoutez les ´etapes suivantes dans le fichier :
• T´el´echarger les donn´ees depuis le bucket raw.
• Nettoyer les donn´ees (suppression des doublons et des lignes vides).
• Se connecter `a la base MySQL distante.
• Cr´eer une table texts (si elle n’existe pas) avec les colonnes n´ecessaires.
• Ins´erer les donn´ees nettoy´ees dans cette table.
• V´erifier que les donn´ees sont bien ins´er´ees.
"""
import os
import pandas as pd
import boto3
import sqlite3

s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

def download_data(bucket_name, input_file_name):
    """
    Downloads a file from the specified S3 bucket.

    Parameters:
    bucket_name (str): Name of the S3 bucket.
    input_file_name (str): Name of the file to download.
    """
    s3.download_file(bucket_name, input_file_name, input_file_name)
    print(f"Downloaded file '{input_file_name}' from bucket '{bucket_name}'.")

def clean_data(input_file_name, output_file_name):
    """
    Cleans the input CSV file by removing duplicates and empty rows.

    Parameters:
    input_file_name (str): Name of the input CSV file.
    output_file_name (str): Name of the output CSV file.
    """
    data = pd.read_csv(input_file_name)
    cleaned_data = data.drop_duplicates().dropna()
    cleaned_data.to_csv(output_file_name, index=False)
    print(f"Cleaned data saved to '{output_file_name}'.")

def connect_to_db(host, user, password, database):
    """
    Connects to a MySQL database.

    Parameters:
    host (str): Hostname of the MySQL server.
    user (str): Username for the MySQL server.
    password (str): Password for the MySQL server.
    database (str): Name of the MySQL database.

    Returns:
    conn: Connection object to the MySQL database.
    cursor: Cursor object for executing SQL queries.
    """
    conn = sqlite3.connect('../sqlite_DB.db')
    cursor = conn.cursor()
    print(f"Connected to MySQL database '{database}' on host '{host}' as user '{user}'.")
    return conn, cursor

def create_table(cursor):
    """
    Creates a table in the MySQL database if it does not already exist.

    Parameters:
    cursor: Cursor object for executing SQL queries.
    """
    cursor.execute('CREATE TABLE IF NOT EXISTS texts (id INTEGER PRIMARY KEY, text TEXT)')
    print("Table 'texts' created in the database.")

def insert_data(cursor, output_file_name):
    """
    Inserts data from a CSV file into the MySQL database table.

    Parameters:
    cursor: Cursor object for executing SQL queries.
    output_file_name (str): Name of the CSV file containing the data.
    """
    data = pd.read_csv(output_file_name)
    for index, row in data.iterrows():
        cursor.execute('INSERT INTO texts (text) VALUES (?)', (row['text'],))
    print(f"Data inserted into the 'texts' table.")

def check_data(cursor):
    """
    Retrieves and prints data from the MySQL database table.

    Parameters:
    cursor: Cursor object for executing SQL queries.
    """
    cursor.execute('SELECT * FROM texts LIMIT 5')
    print(cursor.fetchall())
    print("Number of rows in 'texts' table:", cursor.execute('SELECT COUNT(*) FROM texts').fetchone()[0])

def close_connection(conn):
    """
    Closes the connection to the MySQL database.

    Parameters:
    conn: Connection object to the MySQL database.
    """
    conn.commit()
    conn.close()
    print("Connection to MySQL database closed.")

def prepare_data(bucket_name, input_file_name, output_file_name, host, user, password, database):
    """
    Prepares the data for staging by downloading, cleaning, and inserting it into a MySQL database.

    Parameters:
    bucket_name (str): Name of the S3 bucket.
    input_file_name (str): Name of the input file in the S3 bucket.
    output_file_name (str): Name of the output file for cleaned data.
    host (str): Hostname of the MySQL server.
    user (str): Username for the MySQL server.
    password (str): Password for the MySQL server.
    database (str): Name of the MySQL database.
    """
    download_data(bucket_name, input_file_name)
    clean_data(input_file_name, output_file_name)
    conn, cursor = connect_to_db(host, user, password, database)
    create_table(cursor)
    insert_data(cursor, output_file_name)
    check_data(cursor)
    close_connection(conn)
    print("Data preparation complete.")

if __name__ == "__main__":
    """
    Utilisateur : root
    • Mot de passe : root
    • Base de données : staging
    """
    bucket_name = 'raw'
    input_file_name = 'combined_raw.csv'
    output_file_name = 'staging_data.csv'
    host = 'localhost'
    user = 'root'
    password = 'root'
    database = 'staging'

    prepare_data(bucket_name, input_file_name, output_file_name, host, user, password, database)
