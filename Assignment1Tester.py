#
# Tester for the assignement1
#
import Interface as MyAssignment
import time

DATABASE_NAME = MyAssignment.DATABASE_NAME

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'ratings.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 20  # Number of lines in the input file

with open(INPUT_FILE_PATH, 'r') as f:
    lines = f.readlines()
    ACTUAL_ROWS_IN_INPUT_FILE = len(lines)
    print(f"Number of lines in the input file: {ACTUAL_ROWS_IN_INPUT_FILE}")
    

import psycopg2
import traceback
import testHelper

def time_function(func_name, func, *args, **kwargs):
    """Measure and print execution time of a function"""
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"{func_name} execution time: {execution_time:.6f} seconds")
    return result

if __name__ == '__main__':
    try:
        testHelper.createdb(DATABASE_NAME)

        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            testHelper.deleteAllPublicTables(conn)

            # Test loadratings with timing
            print("\n--- Testing loadratings ---")
            start_time = time.time()
            [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            execution_time = time.time() - start_time
            print(f"loadratings execution time: {execution_time:.6f} seconds")
            if result:
                print("loadratings function pass!")
            else:
                print("loadratings function fail!")

            # Test rangepartition with timing
            print("\n--- Testing rangepartition ---")
            start_time = time.time()
            [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            execution_time = time.time() - start_time
            print(f"rangepartition execution time: {execution_time:.6f} seconds")
            if result:
                print("rangepartition function pass!")
            else:
                print("rangepartition function fail!")

            # Test rangeinsert with timing
            print("\n--- Testing rangeinsert ---")
            start_time = time.time()
            # ALERT:: Use only one at a time i.e. uncomment only one line at a time and run the script
            [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
            # [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 0, conn, '0')
            execution_time = time.time() - start_time
            print(f"rangeinsert execution time: {execution_time:.6f} seconds")
            if result:
                print("rangeinsert function pass!")
            else:
                print("rangeinsert function fail!")

            testHelper.deleteAllPublicTables(conn)
            
            # Load ratings again for roundrobin tests
            print("\n--- Loading ratings for roundrobin tests ---")
            start_time = time.time()
            MyAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)
            execution_time = time.time() - start_time
            print(f"loadratings execution time: {execution_time:.6f} seconds")

            # Test roundrobinpartition with timing
            print("\n--- Testing roundrobinpartition ---")
            start_time = time.time()
            [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            execution_time = time.time() - start_time
            print(f"roundrobinpartition execution time: {execution_time:.6f} seconds")
            if result:
                print("roundrobinpartition function pass!")
            else:
                print("roundrobinpartition function fail")

            # Test roundrobininsert with timing
            print("\n--- Testing roundrobininsert ---")
            start_time = time.time()
            # ALERT:: Change the partition index according to your testing sequence.
            # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '0')
            # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '1')
            # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '2')
            # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '3')
            [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '4')
            execution_time = time.time() - start_time
            print(f"roundrobininsert execution time: {execution_time:.6f} seconds")
            if result:
                print("roundrobininsert function pass!")
            else:
                print("roundrobininsert function fail!")

            choice = input('Press enter to Delete all tables? ')
            if choice == '':
                testHelper.deleteAllPublicTables(conn)
            if not conn.close:
                conn.close()

    except Exception as detail:
        traceback.print_exc()