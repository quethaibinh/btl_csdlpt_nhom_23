#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get database configuration from environment variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'movielens')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '1234')

DATABASE_NAME = DB_NAME

# Constants for partition table prefixes
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
MAX_RATING = 5.0

def getopenconnection(user=DB_USER, password=DB_PASSWORD, dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='" + DB_HOST + "' port='" + DB_PORT + "' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    create_db(DATABASE_NAME)
    cur = openconnection.cursor()
    try:
        # Create table and load data in one transaction
        cur.execute("CREATE TABLE IF NOT EXISTS %s (userid INTEGER, extra1 CHAR, movieid INTEGER, extra2 CHAR, rating FLOAT, extra3 CHAR, timestamp BIGINT)" % ratingstablename)
        cur.copy_from(open(ratingsfilepath), ratingstablename, sep=':')
        cur.execute("ALTER TABLE %s DROP COLUMN extra1, DROP COLUMN extra2, DROP COLUMN extra3, DROP COLUMN timestamp" % ratingstablename)
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise e
    finally:
        cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    cur = openconnection.cursor()
    try:
        delta = MAX_RATING / numberofpartitions
        
        # Create tables and insert data in a batch
        for i in range(0, numberofpartitions):
            min_range = i * delta
            max_range = min_range + delta
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
            
            # Create partition table
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT)")
            
            # Insert data with appropriate range condition
            if i == 0:
                cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) SELECT userid, movieid, rating FROM {ratingstablename} WHERE rating >= {min_range} AND rating <= {max_range}")
            else:
                cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) SELECT userid, movieid, rating FROM {ratingstablename} WHERE rating > {min_range} AND rating <= {max_range}")
        
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise e
    finally:
        cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    cur = openconnection.cursor()
    try:
        # Create all tables first
        for i in range(numberofpartitions):
            table_name = f"{RROBIN_TABLE_PREFIX}{i}"
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT)")
        
        # Fetch all data at once and distribute using modulo
        cur.execute(f"SELECT userid, movieid, rating, (ROW_NUMBER() OVER())-1 as rnum FROM {ratingstablename}")
        rows = cur.fetchall()
        
        # Batch inserts for each partition
        for i in range(numberofpartitions):
            table_name = f"{RROBIN_TABLE_PREFIX}{i}"
            partition_data = [(row[0], row[1], row[2]) for row in rows if row[3] % numberofpartitions == i]
            if partition_data:
                args_str = ','.join(cur.mogrify("(%s,%s,%s)", x).decode('utf-8') for x in partition_data)
                cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES {args_str}")
        
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise e
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.execute("select count(*) from " + ratingstablename + ";");
    total_rows = (cur.fetchall())[0][0]
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows-1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()
    
# def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
#     """
#     Function to insert a new row into the main table and specific partition based on round robin
#     approach.
#     """
#     cur = openconnection.cursor()
#     try:
#         # Insert into main table
#         cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)", 
#                    (userid, itemid, rating))
        
#         # Get total count of rows to determine partition
#         cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
#         total_rows = cur.fetchone()[0]
        
#         # Calculate partition index
#         numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
#         index = (total_rows - 1) % numberofpartitions
#         table_name = f"{RROBIN_TABLE_PREFIX}{index}"
        
#         # Insert into partition
#         cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)", 
#                    (userid, itemid, rating))
        
#         openconnection.commit()
#     except Exception as e:
#         openconnection.rollback()
#         raise e
#     finally:
#         cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    cur = openconnection.cursor()
    try:
        # Insert into main table
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)",
                   (userid, itemid, rating))
        
        # Calculate partition index
        numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
        delta = MAX_RATING / numberofpartitions
        index = int(rating / delta)
        
        # Handle edge case
        if rating % delta == 0 and index != 0:
            index = index - 1
            
        table_name = f"{RANGE_TABLE_PREFIX}{index}"
        
        # Insert into partition
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)",
                   (userid, itemid, rating))
        
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise e
    finally:
        cur.close()


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    try:
        # Check if an existing database with the same name exists
        cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=%s', (dbname,))
        count = cur.fetchone()[0]
        if count == 0:
            cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
        else:
            print('A database named {0} already exists'.format(dbname))
    finally:
        # Clean up
        cur.close()
        con.close()


def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    cur = openconnection.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE %s", (prefix + '%',))
        count = cur.fetchone()[0]
        return count
    finally:
        cur.close()
