import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get database connection parameters from environment variables
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

def get_connection():
    """
    Create and return a connection to the PostgreSQL database
    """
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        print(f"Successfully connected to PostgreSQL database: {db_name}")  
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        raise

def LoadRatings(filepath):
    """
    Load ratings data from the specified file into the Ratings table in PostgreSQL.
    
    Args:
        filepath (str): Absolute path to the ratings.dat file
    
    Returns:
        None
    """
    # Get database connection
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Create Ratings table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Ratings (
                UserID INTEGER,
                MovieID INTEGER,
                Rating FLOAT
            )
        """)
        
        # Clear existing data from the Ratings table
        cursor.execute("DELETE FROM Ratings")
        
        # Read and insert data from the ratings.dat file
        with open(filepath, 'r') as file:
            for line in file:
                # Parse line (format: UserID::MovieID::Rating::Timestamp)
                parts = line.strip().split('::')
                if len(parts) >= 3:
                    user_id = int(parts[0])
                    movie_id = int(parts[1])
                    rating = float(parts[2])
                    
                    # Insert data into the Ratings table
                    cursor.execute(
                        "INSERT INTO Ratings (UserID, MovieID, Rating) VALUES (%s, %s, %s)",
                        (user_id, movie_id, rating)
                    )
        
        # Commit the transaction
        conn.commit()
        print(f"Successfully loaded ratings data from {filepath}")
        
    except Exception as e:
        # Roll back the transaction in case of error
        conn.rollback()
        print(f"Error loading ratings data: {e}")
        raise
    finally:
        # Close cursor but don't close connection as per requirements
        cursor.close()

def Range_Partition(ratings_table, n):
    """
    Partition the ratings table into n partitions based on uniform ranges of the Rating attribute.
    
    Args:
        ratings_table (str): Name of the table containing ratings data
        n (int): Number of partitions to create
    
    Returns:
        None
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Drop existing range partitions if they exist
        for i in range(n):
            table_name = f"range_part{i}"
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create n partitions with uniform ranges
        min_rating = 0
        max_rating = 5
        range_size = (max_rating - min_rating) / n
        
        # Create partition tables
        for i in range(n):
            table_name = f"range_part{i}"
            
            # Calculate range boundaries for this partition
            lower_bound = min_rating + i * range_size
            upper_bound = min_rating + (i + 1) * range_size
            
            # Create the partition table
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    UserID INTEGER,
                    MovieID INTEGER,
                    Rating FLOAT
                )
            """)
            
            # For the first partition
            if i == 0:
                cursor.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM {ratings_table}
                    WHERE Rating >= {lower_bound} AND Rating <= {upper_bound}
                """)
            # For other partitions
            else:
                cursor.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM {ratings_table}
                    WHERE Rating > {lower_bound} AND Rating <= {upper_bound}
                """)
        
        conn.commit()
        print(f"Successfully created {n} range partitions for {ratings_table}")
        
    except Exception as e:
        conn.rollback()
        print(f"Error creating range partitions: {e}")
        raise
    finally:
        cursor.close()

def RoundRobin_Partition(ratings_table, n):
    """
    Partition the ratings table into n partitions using the round robin method.
    
    Args:
        ratings_table (str): Name of the table containing ratings data
        n (int): Number of partitions to create
    
    Returns:
        None
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Drop existing round robin partitions if they exist
        for i in range(n):
            table_name = f"rrobin_part{i}"
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create n round robin partition tables
        for i in range(n):
            table_name = f"rrobin_part{i}"
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    UserID INTEGER,
                    MovieID INTEGER,
                    Rating FLOAT
                )
            """)
        
        # Get all rows from the ratings table
        cursor.execute(f"SELECT UserID, MovieID, Rating FROM {ratings_table}")
        rows = cursor.fetchall()
        
        # Insert rows into round robin partitions
        for idx, row in enumerate(rows):
            partition_idx = idx % n
            table_name = f"rrobin_part{partition_idx}"
            
            cursor.execute(f"""
                INSERT INTO {table_name} (UserID, MovieID, Rating)
                VALUES (%s, %s, %s)
            """, row)
        
        conn.commit()
        print(f"Successfully created {n} round robin partitions for {ratings_table}")
        
    except Exception as e:
        conn.rollback()
        print(f"Error creating round robin partitions: {e}")
        raise
    finally:
        cursor.close()

def Range_Insert(ratings_table, user_id, movie_id, rating):
    """
    Insert a new rating into the ratings table and the appropriate range partition.
    
    Args:
        ratings_table (str): Name of the table containing ratings data
        user_id (int): User ID for the new rating
        movie_id (int): Movie ID for the new rating
        rating (float): Rating value
    
    Returns:
        None
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Insert into the main ratings table first
        cursor.execute(f"""
            INSERT INTO {ratings_table} (UserID, MovieID, Rating)
            VALUES (%s, %s, %s)
        """, (user_id, movie_id, rating))
        
        # Get the number of range partitions by counting tables with range_part prefix
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name LIKE 'range_part%'
        """)
        n_partitions = cursor.fetchone()[0]
        
        if n_partitions > 0:
            # Calculate partition ranges
            min_rating = 0
            max_rating = 5
            range_size = (max_rating - min_rating) / n_partitions
            
            # Determine the appropriate partition
            for i in range(n_partitions):
                lower_bound = min_rating + i * range_size
                upper_bound = min_rating + (i + 1) * range_size
                
                # For the first partition
                if i == 0:
                    if lower_bound <= rating <= upper_bound:
                        table_name = f"range_part{i}"
                        break
                # For other partitions
                else:
                    if lower_bound < rating <= upper_bound:
                        table_name = f"range_part{i}"
                        break
            
            # Insert into the appropriate partition
            cursor.execute(f"""
                INSERT INTO {table_name} (UserID, MovieID, Rating)
                VALUES (%s, %s, %s)
            """, (user_id, movie_id, rating))
        
        conn.commit()
        print(f"Successfully inserted rating into {ratings_table} and range partition")
        
    except Exception as e:
        conn.rollback()
        print(f"Error inserting into range partition: {e}")
        raise
    finally:
        cursor.close()

def RoundRobin_Insert(ratings_table, user_id, movie_id, rating):
    """
    Insert a new rating into the ratings table and the appropriate round robin partition.
    
    Args:
        ratings_table (str): Name of the table containing ratings data
        user_id (int): User ID for the new rating
        movie_id (int): Movie ID for the new rating
        rating (float): Rating value
    
    Returns:
        None
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Insert into the main ratings table first
        cursor.execute(f"""
            INSERT INTO {ratings_table} (UserID, MovieID, Rating)
            VALUES (%s, %s, %s)
        """, (user_id, movie_id, rating))
        
        # Get the number of round robin partitions by counting tables with rrobin_part prefix
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name LIKE 'rrobin_part%'
        """)
        n_partitions = cursor.fetchone()[0]
        
        if n_partitions > 0:
            # Get the count of rows in the main table to determine the next partition
            cursor.execute(f"SELECT COUNT(*) FROM {ratings_table}")
            row_count = cursor.fetchone()[0]
            
            # The new row is the row_count'th row (1-indexed), so the partition is (row_count-1) % n_partitions
            partition_idx = (row_count - 1) % n_partitions
            table_name = f"rrobin_part{partition_idx}"
            
            # Insert into the appropriate partition
            cursor.execute(f"""
                INSERT INTO {table_name} (UserID, MovieID, Rating)
                VALUES (%s, %s, %s)
            """, (user_id, movie_id, rating))
        
        conn.commit()
        print(f"Successfully inserted rating into {ratings_table} and round robin partition")
        
    except Exception as e:
        conn.rollback()
        print(f"Error inserting into round robin partition: {e}")
        raise
    finally:
        cursor.close()

if __name__ == "__main__":
    conn = get_connection()
