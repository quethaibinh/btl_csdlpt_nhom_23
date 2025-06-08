from db_connect import create_db, getopenconnection

DATABASE_NAME = 'dds_assgn1'


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