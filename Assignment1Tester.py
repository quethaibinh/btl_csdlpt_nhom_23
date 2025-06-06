#
# Tester for the assignement1
#
DATABASE_NAME = 'dds_assgn1'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'ratings.dat'

import psycopg2
import traceback
import testHelper
import range
import roundRobin
import LoadRating
import db_connect

with open(INPUT_FILE_PATH, 'r', encoding='utf-8') as f:
    line_count = sum(1 for _ in f)
ACTUAL_ROWS_IN_INPUT_FILE = line_count  # Number of lines in the input file
print(f"Number of rows in input file: {ACTUAL_ROWS_IN_INPUT_FILE}")


if __name__ == '__main__':
    try:
        db_connect.create_db(DATABASE_NAME)

        with db_connect.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            testHelper.deleteAllPublicTables(conn)

            [result, e] = testHelper.testloadratings(LoadRating, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print("loadratings function pass!")
            else:
                print("loadratings function fail!")

            # [result, e] = testHelper.testrangepartition(range, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            # if result :
            #     print("rangepartition function pass!")
            # else:
            #     print("rangepartition function fail!")

            # # ALERT:: Use only one at a time i.e. uncomment only one line at a time and run the script
            # # [result, e] = testHelper.testrangeinsert(range, RATINGS_TABLE, 100, 2, 3, conn, '2')
            # [result, e] = testHelper.testrangeinsert(range, RATINGS_TABLE, 100, 2, 0, conn, '0')
            # if result:
            #     print("rangeinsert function pass!")
            # else:
            #     print("rangeinsert function fail!")

            # testHelper.deleteAllPublicTables(conn)
            # LoadRating.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)

            [result, e] = testHelper.testroundrobinpartition(roundRobin, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print("roundrobinpartition function pass!")
            else:
                print("roundrobinpartition function fail")

            # ALERT:: Change the partition index according to your testing sequence.
            # [result, e] = testHelper.testroundrobininsert(roundRobin, RATINGS_TABLE, 100, 1, 3, conn, '0')
            [result, e] = testHelper.testroundrobininsert(roundRobin, RATINGS_TABLE, 100, 1, 3, conn, '1')
            # [result, e] = testHelper.testroundrobininsert(roundRobin, RATINGS_TABLE, 100, 1, 3, conn, '2')
            if result :
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