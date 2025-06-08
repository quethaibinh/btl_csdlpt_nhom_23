# Constants for partition table prefixes
RROBIN_TABLE_PREFIX = 'rrobin_part'
MAX_RATING = 5.0



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


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    cur = openconnection.cursor()
    try:
        # Insert into main table
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)", 
                   (userid, itemid, rating))
        
        # Get total count of rows to determine partition
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
        total_rows = cur.fetchone()[0]
        
        # Calculate partition index
        numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
        index = (total_rows - 1) % numberofpartitions
        table_name = f"{RROBIN_TABLE_PREFIX}{index}"
        
        # Insert into partition
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)", 
                   (userid, itemid, rating))
        
        openconnection.commit()
    except Exception as e:
        openconnection.rollback()
        raise e
    finally:
        cur.close()