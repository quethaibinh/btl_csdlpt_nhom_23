# Constants for partition table prefixes
RANGE_TABLE_PREFIX = 'range_part'
MAX_RATING = 5.0


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