#!/usr/bin/python
#
# Test script to verify the configuration
#

from Interface import getopenconnection, DATABASE_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD

def test_connection():
    """
    Function to test the connection to the database.
    """
    print("Testing connection with the following parameters:")
    print(f"DB_HOST: {DB_HOST}")
    print(f"DB_PORT: {DB_PORT}")
    print(f"DATABASE_NAME: {DATABASE_NAME}")
    print(f"DB_USER: {DB_USER}")
    print(f"DB_PASSWORD: {DB_PASSWORD}")
    
    try:
        # Try to connect to the database
        conn = getopenconnection()
        print("Connection successful!")
        
        # Check if the database exists
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DATABASE_NAME,))
        exists = cur.fetchone()
        
        if exists:
            print(f"Database '{DATABASE_NAME}' exists.")
        else:
            print(f"Database '{DATABASE_NAME}' does not exist.")
        
        # Close the connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_connection() 