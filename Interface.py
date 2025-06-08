#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import psycopg2.extensions
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

# Get database configuration from environment variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '1234')

DATABASE_NAME = DB_NAME

# Constants for partition table prefixes
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
MAX_RATING = 5.0

def getopenconnection(user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME):
    connection = psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='" + DB_HOST + "' password='" + password + "'")
    return connection

def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    
    # Create table with all columns initially
    cur.execute(f"""
        CREATE TABLE {ratingstablename} (
            userid INTEGER,
            extra1 CHAR,
            movieid INTEGER, 
            extra2 CHAR,
            rating FLOAT,
            extra3 CHAR,
            timestamp BIGINT
        )
    """)
    
    # Use COPY command for fast data loading
    with open(ratingsfilepath, 'r') as f:
        cur.copy_from(f, ratingstablename, sep=':')
    
    # Drop columns in a single command
    cur.execute(f"ALTER TABLE {ratingstablename} DROP COLUMN extra1, DROP COLUMN extra2, DROP COLUMN extra3, DROP COLUMN timestamp")
    
    con.commit()
    cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    con = openconnection
    cur = con.cursor()
    delta = 5.0 / numberofpartitions
    
    # Create all tables first for better transaction handling
    for i in range(numberofpartitions):
        table_name = f"{RANGE_TABLE_PREFIX}{i}"
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT)")
    
    # Insert data using direct approach
    for i in range(numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = f"{RANGE_TABLE_PREFIX}{i}"
        
        if i == 0:
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE rating >= {minRange} AND rating <= {maxRange}
            """)
        else:
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE rating > {minRange} AND rating <= {maxRange}
            """)
    
    con.commit()
    cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    con = openconnection
    cur = con.cursor()
    
    # Create all tables first
    for i in range(numberofpartitions):
        table_name = f"{RROBIN_TABLE_PREFIX}{i}"
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT)")
    
    # Add row numbers to the source table for efficient partitioning
    cur.execute(f"""
        CREATE TEMPORARY TABLE temp_table AS
        SELECT userid, movieid, rating, 
               ROW_NUMBER() OVER() AS row_id
        FROM {ratingstablename}
    """)
    
    # Insert into partitions based on mod calculation
    for i in range(numberofpartitions):
        table_name = f"{RROBIN_TABLE_PREFIX}{i}"
        cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            SELECT userid, movieid, rating
            FROM temp_table
            WHERE MOD((row_id - 1), {numberofpartitions}) = {i}
        """)
    
    # Drop temporary table
    cur.execute("DROP TABLE temp_table")
    
    con.commit()
    cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cur = con.cursor()
    
    # Insert into main table
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)", 
                (userid, itemid, rating))
    
    # Get row count and calculate partition
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = cur.fetchone()[0]
    
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, con)
    index = (total_rows - 1) % numberofpartitions
    
    # Insert into partition
    table_name = f"{RROBIN_TABLE_PREFIX}{index}"
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)",
                (userid, itemid, rating))
    
    con.commit()
    cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cur = con.cursor()
    
    # # Insert into main table
    # cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)",
    #             (userid, itemid, rating))
    
    # Calculate appropriate range partition
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, con)
    
    delta = 5.0 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index -= 1
    
    # Insert into partition
    table_name = f"{RANGE_TABLE_PREFIX}{index}"
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)",
                (userid, itemid, rating))
    
    con.commit()
    cur.close()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to my database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=%s', (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute(f'CREATE DATABASE {dbname}')  # Create the database
    else:
        print(f'A database named {dbname} already exists')

    # Clean up
    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    cur = openconnection.cursor()
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE %s", (prefix + '%',))
    count = cur.fetchone()[0]
    cur.close()
    return count