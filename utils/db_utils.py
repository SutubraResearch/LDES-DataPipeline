import sqlite3
import shutil
import os
from sqlite3 import Error


class SQLiteDBManager:
    def __init__(self):
        self.conn = None

    def create_connection(self, db_file):
        """Create a database connection to the SQLite database specified by db_file
        :param db_file: database file
        """
        try:
            self.conn = sqlite3.connect(db_file)
        except Error as e:
            print(e)

    def execute_query(self, query, data=None, many=False):
        """Execute a single or multiple queries using the database connection
        :param query: a SQL query
        :param data: data to pass into the query, if applicable
        :param many: if True, execute the query for each item in data
        """
        try:
            cur = self.conn.cursor()
            if data:
                if many:
                    cur.executemany(query, data)
                else:
                    cur.execute(query, data)
            else:
                cur.execute(query)
            self.conn.commit()
        except Error as e:
            print(e)

    def close_connection(self):
        """ Close a database connection """
        try:
            self.conn.close()
        except Error as e:
            print(e)

    def populate_table_with_query_bulk(self, query, data):
        with self.conn:
            self.conn.executemany(query, data)

def copy_and_rename_sqlite_template(template_path: str, dest_dir: str, file_name: str) -> str:
    """Copy the SQLite template file and rename it according to the configuration"""
    # Ensure destination directory exists, create if not.
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    dest_path = os.path.join(dest_dir, file_name)

    # If the destination DB exists, delete it first.
    if os.path.exists(dest_path):
        os.remove(dest_path)

    shutil.copy(template_path, dest_path)

    return dest_path
