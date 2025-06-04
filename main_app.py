import sys
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QRadioButton, QLineEdit, QDateEdit, QTableWidget, QPushButton,
    QLabel, QGroupBox
)
from PySide6.QtCore import QDate

# Import database utility functions
from db_utils import connect_local_db, connect_remote_db, query_local_data, query_remote_data, upload_data_to_remote
from PySide6.QtWidgets import QMessageBox, QTableWidgetItem


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        # Database connection placeholders
        self.current_conn = None
        self.current_cursor = None

        self.setWindowTitle("Data Application")
        self.setGeometry(100, 100, 800, 600)

        # Main widget and layout
        main_widget = QWidget(self)
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)

        # --- Data Source Selection ---
        source_groupbox = QGroupBox("Data Source")
        source_layout = QHBoxLayout()
        self.radio_local = QRadioButton("Local")
        self.radio_remote = QRadioButton("Remote")
        self.radio_remote.setChecked(True) # Default back to Remote
        source_layout.addWidget(self.radio_local)
        source_layout.addWidget(self.radio_remote)
        source_groupbox.setLayout(source_layout)
        main_layout.addWidget(source_groupbox)

        # --- Search Criteria ---
        search_groupbox = QGroupBox("Search Criteria")
        search_layout = QHBoxLayout()

        # ID Search
        id_label = QLabel("ID:")
        self.id_input = QLineEdit()
        self.id_input.setPlaceholderText("Enter ID to search")
        search_layout.addWidget(id_label)
        search_layout.addWidget(self.id_input)

        # Date Search
        date_label = QLabel("Date:")
        self.date_input = QDateEdit()
        self.date_input.setDate(QDate.currentDate())
        self.date_input.setCalendarPopup(True)
        search_layout.addWidget(date_label)
        search_layout.addWidget(self.date_input)

        search_groupbox.setLayout(search_layout)
        main_layout.addWidget(search_groupbox)

        # --- Data Display Table ---
        self.table_widget = QTableWidget()
        self.table_widget.setColumnCount(5) # Placeholder columns
        self.table_widget.setHorizontalHeaderLabels(["Col1", "Col2", "Col3", "Col4", "Col5"])
        main_layout.addWidget(self.table_widget)

        # --- Action Buttons ---
        action_layout = QHBoxLayout()

        # Search Button (Placeholder, functionality to be added)
        self.search_button = QPushButton("Search Data")
        action_layout.addWidget(self.search_button)

        # Upload Button
        self.upload_button = QPushButton("Upload Data")
        self.upload_button.setEnabled(False) # Disabled by default
        action_layout.addWidget(self.upload_button)

        main_layout.addLayout(action_layout)

        # --- Connect signals ---
        self.radio_local.toggled.connect(self.on_source_changed)
        self.radio_remote.toggled.connect(self.on_source_changed)
        self.search_button.clicked.connect(self.handle_search)
        self.upload_button.clicked.connect(self.handle_upload) # Connect upload button


    def on_source_changed(self):
        # Clear previous search results and fields
        self.table_widget.setRowCount(0)
        self.table_widget.setColumnCount(0) # Clear headers too
        self.id_input.clear()
        self.date_input.setDate(QDate.currentDate()) # Reset date

        if self.radio_local.isChecked():
            self.upload_button.setEnabled(True)
            print("Local source selected. Upload button enabled.")
            self.connect_db(local_source=True)
        elif self.radio_remote.isChecked():
            self.upload_button.setEnabled(False)
            print("Remote source selected. Upload button disabled.")
            self.connect_db(local_source=False)

        if not self.radio_local.isChecked() and not self.radio_remote.isChecked():
            self.upload_button.setEnabled(False)
            # Important: ensure any existing connection is closed if no source is selected
            if self.current_conn and not self.radio_local.isChecked() and not self.radio_remote.isChecked() :
                 self.disconnect_db()


    def disconnect_db(self):
        if self.current_conn:
            try:
                self.current_conn.close()
                print("Closed existing DB connection.")
            except Exception as e:
                print(f"Error closing existing DB connection: {e}")
            finally:
                self.current_conn = None
                self.current_cursor = None

    def connect_db(self, local_source=True):
        self.disconnect_db() # Close any existing connection first

        if local_source:
            print("Attempting to connect to Local DB (record.db)...")
            # For local, we might want to ensure the DB and table exist
            # The connect_local_db already creates the directory for the db file.
            # We can add a check here to create the 'records' table if it doesn't exist.
            # connect_local_db now returns conn, cursor.
            # We need to handle the cursor carefully.
            raw_conn, initial_cursor = connect_local_db('record.db')
            if raw_conn:
                self.current_conn = raw_conn # Store the connection object
                temp_cursor = None # Explicitly manage cursor for setup
                try:
                    temp_cursor = self.current_conn.cursor()

                    temp_cursor.execute("DROP TABLE IF EXISTS records")
                    self.current_conn.commit() # Commit DROP TABLE
                    print("Dropped existing 'records' table (if any) for clean setup.")

                    temp_cursor.execute("""
                        CREATE TABLE records (
                            id TEXT PRIMARY KEY,
                            event_date TEXT,
                            data TEXT
                        )
                    """)
                    self.current_conn.commit() # Commit CREATE TABLE
                    print("Created new 'records' table.")

                    sample_data = [
                        ('ID001', '2024-01-15', 'Sample data A for local DB'),
                        ('ID002', '2024-01-16', 'Sample data B for local DB'),
                        ('ID003', '2024-01-15', 'Sample data C, also 2024-01-15'),
                        ('ID004', '2023-12-20', 'Older data for testing date filter'),
                    ]
                    temp_cursor.executemany("INSERT INTO records (id, event_date, data) VALUES (?,?,?)", sample_data)
                    self.current_conn.commit() # Commit INSERT
                    print("Added sample data to local 'records' table.")
                except sqlite3.Error as e_sqlite:
                    print(f"SQLite error during local DB setup: {e_sqlite}")
                    self.disconnect_db()
                except Exception as e:
                    print(f"Error setting up local table or inserting sample data: {e}")
                    self.disconnect_db()
                finally:
                    if temp_cursor:
                        temp_cursor.close()
                if initial_cursor and not initial_cursor.is_closed if hasattr(initial_cursor, 'is_closed') else initial_cursor is not None: # Check if cursor exists and is not already closed
                    initial_cursor.close()
            else:
                self.disconnect_db()
        else: # Remote
            print("Attempting to connect to Remote Oracle DB...")
            self.current_conn, remote_cursor_temp = connect_remote_db()
            if remote_cursor_temp: # Close cursor if one was returned by connect_remote_db
                remote_cursor_temp.close()

        if self.current_conn:
            print("DB connection successful.")
            QMessageBox.information(self, "Database Connection", "Successfully connected to the database.")
        else:
            print("DB connection failed.")
            # Ensure self.current_conn is None if connection failed
            self.current_conn = None
            self.current_cursor = None # Should also be None
            QMessageBox.warning(self, "Database Connection", "Failed to connect to the database. Please check console for errors.\nEnsure prerequisites are met (e.g., Oracle client, network access).")
            # Update button states if connection failed
            if self.radio_local.isChecked(): # If it was an attempt to connect to local
                 self.upload_button.setEnabled(False) # Disable upload if local connection failed


    def handle_search(self):
        if not self.current_conn:
            QMessageBox.warning(self, "Search Error", "No database connection. Please select a data source and ensure connection was successful.")
            # print("Search Error: No database connection.") # Keep for debugging if needed
            return

        search_id_text = self.id_input.text().strip()
        search_date_qdate = self.date_input.date()
        # Convert QDate to 'YYYY-MM-DD' string for DB query
        search_date_str = search_date_qdate.toString("yyyy-MM-dd")

        # Use None if fields are empty, assuming query functions handle None
        search_id = search_id_text if search_id_text else None
        # For date, decide if an empty/default date means "no filter" or "filter by today"
        # For now, let's assume we always pass the date from QDateEdit

        results = []
        col_headers = []

        try:
            if self.radio_local.isChecked():
                print(f"Querying local data with date: {search_date_str}, id: {search_id}")
                query_results = query_local_data(self.current_conn, search_date_str, search_id)
                if query_results is None: # Indicates an error in query_local_data
                    QMessageBox.warning(self, "Search Error", "Error querying local database. Check console for details.")
                    results = [] # Ensure results is empty list on error
                    col_headers = ["ID", "Event Date", "Data"] # Still set headers for consistency
                elif not query_results: # Empty list, no data found
                    QMessageBox.information(self, "Search Info", "No data found in local database for the criteria.")
                    results = []
                    col_headers = ["ID", "Event Date", "Data"]
                else: # Data found
                    results = query_results
                    col_headers = ["ID", "Event Date", "Data"]

            elif self.radio_remote.isChecked():
                print(f"Querying remote data with date: {search_date_str}, id: {search_id}")
                # query_remote_data returns (col_names, results_data)
                col_names_remote, query_results_remote = query_remote_data(self.current_conn, search_date_str, search_id)
                if col_names_remote and query_results_remote is not None: # Check both as query_remote_data can return (default_cols, [])
                    results = query_results_remote
                    col_headers = col_names_remote
                elif col_names_remote: # No results but got headers
                     results = []
                     col_headers = col_names_remote
                     QMessageBox.information(self, "Search", "No remote data found for the criteria.")
                else: # Error in query_remote_data
                    QMessageBox.warning(self, "Search Error", "Error querying remote database. Check console.")
                    # print("Search Error: Error querying remote database.") # Keep for debugging if needed
                    return


            # Populate QTableWidget
            self.table_widget.clearContents()
            if not results and not col_headers:
                 self.table_widget.setRowCount(0)
                 self.table_widget.setColumnCount(0)
                 if not (self.radio_remote.isChecked() and col_names_remote is None):
                    QMessageBox.information(self, "Search Info", "No data found for the specified criteria. (Query failed or remote error with no headers)")
                 return

            if not results and col_headers: # No data, but headers are available
                self.table_widget.setColumnCount(len(col_headers))
                self.table_widget.setHorizontalHeaderLabels(col_headers)
                self.table_widget.setRowCount(0)
                # Message for "no data found" is handled above for local, and by query_remote_data for remote if it returns (default_cols, [])
                # So, this specific block might only need to ensure table is empty.
                # However, if local query returned [] and message was shown, we don't need another one.
                # The specific "No data found" messages are now more contextual.
                return


            self.table_widget.setColumnCount(len(col_headers))
            self.table_widget.setHorizontalHeaderLabels(col_headers)
            self.table_widget.setRowCount(len(results))

            for row_idx, row_data in enumerate(results):
                for col_idx, cell_data in enumerate(row_data):
                    item = QTableWidgetItem(str(cell_data) if cell_data is not None else "")
                    self.table_widget.setItem(row_idx, col_idx, item)

            # This specific case for "no results but headers exist" is now better handled by messages within specific query paths.
            # if not results and col_headers:
            # (This check is mostly redundant due to prior specific messages)

        except Exception as e:
            print(f"Error during search or populating table: {e}")
            QMessageBox.critical(self, "Search Error", f"An unexpected error occurred during search: {e}")
            self.table_widget.setRowCount(0)
            self.table_widget.setColumnCount(0)

    def handle_upload(self):
        if not self.radio_local.isChecked():
            QMessageBox.information(self, "Upload Info", "Data upload is only available from the 'Local' data source.")
            return

        if not self.current_conn: # This check is against the currently active connection for the local source
            QMessageBox.warning(self, "Upload Error", "Local database is not connected. Cannot retrieve data to upload.")
            return

        # Get data from QTableWidget
        rows_to_upload_raw = []
        selected_items = self.table_widget.selectedItems()

        if selected_items:
            # Get selected rows if any items are selected
            # A bit complex as selectedItems() gives individual cells. We need unique rows.
            selected_rows_indices = sorted(list(set(item.row() for item in selected_items)))
            for row_idx in selected_rows_indices:
                row_data = []
                for col_idx in range(self.table_widget.columnCount()): # Assuming 3 columns for local: id, event_date, data
                    item = self.table_widget.item(row_idx, col_idx)
                    row_data.append(item.text() if item else "")
                if len(row_data) == 3: # Ensure it's a full row
                    rows_to_upload_raw.append(tuple(row_data))
        else:
            # Get all rows from the table if no specific rows are selected
            for row_idx in range(self.table_widget.rowCount()):
                row_data = []
                for col_idx in range(self.table_widget.columnCount()): # Assuming 3 columns
                    item = self.table_widget.item(row_idx, col_idx)
                    row_data.append(item.text() if item else "")
                if len(row_data) == 3:
                     rows_to_upload_raw.append(tuple(row_data))

        if not rows_to_upload_raw:
            QMessageBox.information(self, "Upload Info", "No data selected or available in the table to upload.")
            return

        # Format data for upload_data_to_remote (list of dicts)
        data_for_upload = []
        for row_tuple in rows_to_upload_raw:
            data_for_upload.append({
                'id': row_tuple[0],         # MRLCODE
                'event_date': row_tuple[1], # CREATE_DATE (needs TO_DATE in SQL)
                'data': row_tuple[2]        # UDA1
            })

        print(f"Attempting to upload {len(data_for_upload)} rows to remote Oracle DB.")

        # Connect to remote Oracle DB for upload
        remote_upload_conn, temp_cursor = connect_remote_db()
        if temp_cursor: temp_cursor.close() # Not used here

        if not remote_upload_conn:
            QMessageBox.critical(self, "Upload Error", "Failed to connect to Remote Oracle DB for upload. Check console.")
            return

        success, num_uploaded = upload_data_to_remote(remote_upload_conn, data_for_upload)

        try: # Close connection cleanly
            remote_upload_conn.close()
        except Exception as e:
            print(f"Error closing remote upload connection: {e}")

        if success:
            QMessageBox.information(self, "Upload Success", f"Successfully uploaded {num_uploaded} rows to the remote database.")
        else:
            QMessageBox.warning(self, "Upload Failed", f"Failed to upload all data. {num_uploaded} rows might have been uploaded before an error. Check console for details.")


if __name__ == '__main__':
    # Required for Xvfb if not using a real display
    import os
    if os.environ.get("QT_QPA_PLATFORM") is None and os.environ.get("DISPLAY") is None:
        os.environ["QT_QPA_PLATFORM"] = "xcb" # or "wayland" or other, depending on system

    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
