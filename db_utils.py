import sqlite3
import cx_Oracle
import os

def connect_local_db(db_name='record.db'):
    """Establishes a connection to a local SQLite database."""
    try:
        # Ensure the directory for the database exists if db_name includes a path
        db_path = os.path.abspath(db_name)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print(f"Successfully connected to local SQLite DB: {db_path}")
        return conn, cursor
    except sqlite3.Error as e:
        print(f"Error connecting to local SQLite DB {db_name}: {e}")
        return None, None
    except Exception as e:
        print(f"An unexpected error occurred with SQLite connection: {e}")
        return None, None

def connect_remote_db():
    """Establishes a connection to a remote Oracle database."""
    # DSN format: "hostname:port/service_name"
    # For Oracle, often the service name is used, or SID.
    # The provided "Data Source=10.142.9.101:1521/ORCL" looks like a TNS entry or easy connect string.
    # cx_Oracle.makedsn(host, port, service_name) can be used, or the full string directly.

    db_host = "10.142.9.101"
    db_port = 1521
    db_service_name = "ORCL" # From "ORCL" in the data source string
    db_user = "C##HDLTZ"
    db_password = "HDLTZ"

    try:
        # Attempt 1: Using makedsn - preferred for clarity if service_name is correct
        dsn = cx_Oracle.makedsn(db_host, db_port, service_name=db_service_name)
        conn = cx_Oracle.connect(user=db_user, password=db_password, dsn=dsn)
        cursor = conn.cursor()
        print(f"Successfully connected to remote Oracle DB: {db_host}/{db_service_name}")
        return conn, cursor
    except cx_Oracle.DatabaseError as e:
        print(f"Oracle DatabaseError (attempt 1 with service_name='{db_service_name}'): {e}")
        # Attempt 2: Using SID if service_name failed (common confusion)
        # The string "ORCL" could be a SID.
        try:
            print(f"Retrying Oracle connection with SID='{db_service_name}'...")
            dsn_sid = cx_Oracle.makedsn(db_host, db_port, sid=db_service_name)
            conn_sid = cx_Oracle.connect(user=db_user, password=db_password, dsn=dsn_sid)
            cursor_sid = conn_sid.cursor()
            print(f"Successfully connected to remote Oracle DB (using SID): {db_host}/{db_service_name}")
            return conn_sid, cursor_sid
        except cx_Oracle.DatabaseError as e_sid:
            print(f"Oracle DatabaseError (attempt 2 with SID='{db_service_name}'): {e_sid}")
            # Attempt 3: Directly using the provided Data Source string
            # This is less common for cx_Oracle.connect's dsn parameter, which usually expects a TNS string or Easy Connect string
            # The dsn parameter of cx_Oracle.connect() expects a net service name or an Easy Connect string.
            # The provided string "10.142.9.101:1521/ORCL" is an Easy Connect string.
            direct_dsn_string = f"{db_host}:{db_port}/{db_service_name}"
            try:
                print(f"Retrying Oracle connection with Easy Connect string: '{direct_dsn_string}'...")
                conn_direct = cx_Oracle.connect(user=db_user, password=db_password, dsn=direct_dsn_string)
                cursor_direct = conn_direct.cursor()
                print(f"Successfully connected to remote Oracle DB (using Easy Connect string): {direct_dsn_string}")
                return conn_direct, cursor_direct
            except cx_Oracle.DatabaseError as e_direct:
                print(f"Oracle DatabaseError (attempt 3 with Easy Connect string='{direct_dsn_string}'): {e_direct}")
                print("All Oracle connection attempts failed.")
                return None, None
    except Exception as e:
        print(f"An unexpected error occurred with Oracle connection: {e}")
        return None, None

if __name__ == '__main__':
    # Test local SQLite connection
    print("Testing Local SQLite Connection...")
    local_conn, local_cursor = connect_local_db("test_records.db")
    if local_conn and local_cursor:
        print("Local SQLite connection successful.")
        # Example: Create a table and insert a row
        try:
            local_cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)")
            local_cursor.execute("INSERT INTO test_table (name) VALUES (?)", ("Test Name",))
            local_conn.commit()
            print("SQLite test table created and data inserted.")
            local_cursor.execute("SELECT * FROM test_table")
            print("Data from SQLite test_table:", local_cursor.fetchall())
        except sqlite3.Error as e:
            print(f"SQLite test error: {e}")
        finally:
            local_conn.close()
            print("Local SQLite connection closed.")
    else:
        print("Local SQLite connection failed.")

    # Test remote Oracle connection
    # Note: This will attempt to connect to the actual Oracle DB.
    # Ensure network access and correct credentials if uncommenting.
    print("\nTesting Remote Oracle Connection...")
    print("NOTE: This requires network access to the Oracle DB and correct credentials.")
    print("If this hangs or fails, check network/firewall and credentials from TASK.md.")

    # Check if Oracle client is initialized (helpful for diagnosing some cx_Oracle issues)
    try:
        cx_Oracle.init_oracle_client(lib_dir=os.environ.get("ORACLE_HOME"))
        print("Oracle client initialized (or already initialized).")
    except Exception as e_init:
        print(f"Oracle client initialization issue (might be okay if client is already in PATH/LD_LIBRARY_PATH): {e_init}")
        print("Proceeding with connection attempt anyway...")

    remote_conn, remote_cursor = connect_remote_db()
    if remote_conn and remote_cursor:
        print("Remote Oracle connection successful.")
        # Example: Query current date from Oracle
        try:
            remote_cursor.execute("SELECT SYSDATE FROM DUAL")
            result = remote_cursor.fetchone()
            print(f"Oracle DUAL SYSDATE: {result}")
        except cx_Oracle.DatabaseError as e:
            print(f"Oracle test query error: {e}")
        finally:
            if remote_conn: # Ensure connection exists before closing
                remote_conn.close()
                print("Remote Oracle connection closed.")
    else:
        print("Remote Oracle connection failed.")

def query_local_data(conn, search_date=None, search_id=None):
    """Queries data from the local SQLite 'records' table."""
    if not conn:
        print("No local SQLite connection provided.")
        return None

    cursor = None
    try:
        cursor = conn.cursor()

        # Base query
        query = "SELECT id, event_date, data FROM records"
        conditions = []
        params = []

        if search_date:
            # Assuming search_date is a string in 'YYYY-MM-DD' format
            conditions.append("event_date = ?")
            params.append(search_date)

        if search_id:
            conditions.append("id LIKE ?") # Use LIKE for partial matches if desired
            params.append(f"%{search_id}%") # Example: partial match

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        print(f"Executing local query: {query} with params: {params}")
        cursor.execute(query, params)
        results = cursor.fetchall()
        return results
    except sqlite3.Error as e:
        print(f"Error querying local SQLite DB: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during local query: {e}")
        return None
    # cursor is implicitly closed when it goes out of scope if not part of connection management

def query_remote_data(conn, search_date=None, search_id=None):
    """
    Queries data from the remote Oracle 'LDCLJ_TIAOMA' table.
    search_date should be a string 'YYYY-MM-DD'.
    search_id will be matched against MRLCODE.
    """
    if not conn:
        print("No remote Oracle connection provided.")
        return None

    cursor = None
    try:
        cursor = conn.cursor()

        # For Oracle, it's good practice to list columns to avoid issues with table changes
        # and to control order. Let's assume some columns for now.
        # Adjust these to actual columns in LDCLJ_TIAOMA if different.
        query = "SELECT GID, MRLCODE, BARCODE, PRODUCT_NAME, TO_CHAR(CREATE_DATE, 'YYYY-MM-DD HH24:MI:SS') AS CREATE_DATE_STR, REMARK FROM LDCLJ_TIAOMA"
        conditions = []
        params = {} # Use named parameters for Oracle

        if search_date:
            # search_date is expected as 'YYYY-MM-DD' string
            conditions.append("TRUNC(CREATE_DATE) = TO_DATE(:search_date_param, 'YYYY-MM-DD')")
            params['search_date_param'] = search_date

        if search_id:
            conditions.append("MRLCODE LIKE :search_id_param")
            params['search_id_param'] = f"%{search_id}%" # Example: partial match

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Add a row limit to avoid fetching too much data accidentally in a test environment
        # This is Oracle specific syntax for row limiting
        if conditions:
            query += " AND ROWNUM <= 100"
        else:
            query += " WHERE ROWNUM <= 100"

        print(f"Executing remote query: {query} with params: {params}")
        cursor.execute(query, params)
        results = cursor.fetchall()

        # If results are fetched, get column names for the QTableWidget headers
        if results:
            col_names = [desc[0] for desc in cursor.description]
            return col_names, results
        else:
            # Return default column names even if no results
            # These should match the SELECT statement
            default_cols = ["GID", "MRLCODE", "BARCODE", "PRODUCT_NAME", "CREATE_DATE_STR", "REMARK"]
            return default_cols, []

    except cx_Oracle.DatabaseError as e:
        print(f"Error querying remote Oracle DB: {e}")
        return None, None # Return two Nones as we expect (col_names, results)
    except Exception as e:
        print(f"An unexpected error occurred during remote query: {e}")
        return None, None
    # cursor is implicitly closed when it goes out of scope

def upload_data_to_remote(remote_conn, data_to_upload):
    """
    Uploads data to the remote Oracle 'LDCLJ_TIAOMA' table.
    data_to_upload is a list of dictionaries, each with keys 'id', 'event_date', 'data'.
    """
    if not remote_conn:
        print("No remote Oracle connection provided for upload.")
        return False, 0

    if not data_to_upload:
        print("No data provided for upload.")
        return True, 0 # No data is not an error, 0 rows uploaded

    cursor = None
    rows_uploaded = 0
    try:
        cursor = remote_conn.cursor()

        # GID should be unique, typically from a sequence or sys_guid()
        # For simplicity, let's use sys_guid() if GID is not part of data_to_upload
        # However, LDCLJ_TIAOMA might have triggers for GID.
        # Let's assume GID needs to be generated if not provided.
        # The task description implies mapping local columns to MRLCODE, CREATE_DATE, UDA1.
        # Other fields like GID might have defaults or need to be handled.
        # For now, assuming GID, BARCODE, PRODUCT_NAME etc. are nullable or have defaults.
        # We will only insert MRLCODE, CREATE_DATE, UDA1.
        # A more robust solution would fetch LDCLJ_TIAOMA's schema or have it predefined.

        # Field mapping:
        # Local 'id' (TEXT) -> Remote MRLCODE (VARCHAR2)
        # Local 'event_date' (TEXT, 'YYYY-MM-DD') -> Remote CREATE_DATE (DATE)
        # Local 'data' (TEXT) -> Remote UDA1 (VARCHAR2)

        # Prepare data for executemany
        # executemany expects a list of tuples/dictionaries
        prepared_data = []
        for row in data_to_upload:
            # Assuming row is a dict with 'id', 'event_date', 'data'
            # Or a tuple (id, event_date, data)
            if isinstance(row, dict):
                prepared_data.append({
                    'mrlcode': row['id'],
                    'create_date_val': row['event_date'], # Will use TO_DATE in SQL
                    'uda1': row['data']
                })
            elif isinstance(row, (list, tuple)) and len(row) == 3:
                 prepared_data.append({
                    'mrlcode': row[0],
                    'create_date_val': row[1],
                    'uda1': row[2]
                })
            else:
                print(f"Skipping invalid row format: {row}")
                continue

        if not prepared_data:
            print("No valid data rows to upload after formatting.")
            return True, 0

        # SQL statement for insertion
        # Using named parameters for clarity
        # GID is often a primary key, ensure it's handled (e.g. SYS_GUID() or sequence)
        # For this example, we'll try inserting only specified fields and let others be default/null.
        # If GID is mandatory and has no default/trigger, this will fail.
        # A common pattern for GID: INSERT INTO LDCLJ_TIAOMA (GID, MRLCODE, CREATE_DATE, UDA1) VALUES (SYS_GUID(), :mrlcode, TO_DATE(:create_date_val, 'YYYY-MM-DD'), :uda1)
        # For now, let's assume GID is not strictly required from client or handled by trigger.
        sql = """
            INSERT INTO LDCLJ_TIAOMA (MRLCODE, CREATE_DATE, UDA1)
            VALUES (:mrlcode, TO_DATE(:create_date_val, 'YYYY-MM-DD'), :uda1)
        """

        # Using executemany for efficiency
        # Set batcherrors=True to continue processing even if some rows fail
        cursor.executemany(sql, prepared_data, batcherrors=True)

        errors = cursor.getbatcherrors()
        failed_rows = len(errors)
        rows_uploaded = len(prepared_data) - failed_rows

        if failed_rows > 0:
            print(f"Uploaded {rows_uploaded} rows successfully.")
            print(f"Failed to upload {failed_rows} rows due to errors:")
            for i, error in enumerate(errors):
                print(f"  Error for row {error.offset} (0-indexed in batch): {error.message}")
            remote_conn.rollback() # Rollback if any errors occurred in the batch
            print("Upload transaction rolled back due to errors.")
            return False, rows_uploaded # Indicate partial success or failure
        else:
            remote_conn.commit()
            print(f"Successfully uploaded {rows_uploaded} rows.")
            return True, rows_uploaded

    except cx_Oracle.DatabaseError as e:
        print(f"Oracle DatabaseError during upload: {e}")
        if remote_conn:
            remote_conn.rollback()
        return False, 0
    except Exception as e:
        print(f"An unexpected error occurred during upload: {e}")
        if remote_conn:
            remote_conn.rollback()
        return False, 0
    finally:
        if cursor:
            cursor.close()
