# MIT License
#
# Copyright (c) 2024 Dezen | freedom block by block
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import re
import os
import uuid
import time
import tempfile
import sqlite3
import zipfile
from typing import List, Optional
from datetime import datetime, timedelta

from smartdrive.validator.models.models import MinerWithChunk, SubChunk, File, Chunk, Block

from communex.types import Ss58Address


class Database:
    _database_file_path = None
    _database_export_file_path = None

    def __init__(self, database_file_path: str, database_export_file_path: str):
        """
        Initialize the DatabaseManager and create the database schema if it does not exist.

        This constructor initializes the database manager with the specified database file paths
        and creates the necessary tables in the database if they do not already exist. It also sets
        the SQLite auto vacuum mode to FULL.

        Params:
            database_file_path (str): The file path to the main database file.
            database_export_file_path (str): The file path to the database export file.

        Raises:
            sqlite3.Error: If there is an error creating the database tables.
        """
        self._database_file_path = database_file_path
        self._database_export_file_path = database_export_file_path

        if not self._database_exists():
            connection = sqlite3.connect(self._database_file_path)
            with connection:
                cursor = connection.cursor()
                cursor.execute("PRAGMA auto_vacuum=FULL;")

                create_file_table = '''
                    CREATE TABLE file (
                        uuid TEXT PRIMARY KEY,
                        user_ss58_address INTEGER
                    )
                '''

                create_file_expiration_table = '''
                    CREATE TABLE file_expiration (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        file_uuid TEXT,
                        expiration_ms INTEGER,
                        created_at INTEGER,
                        FOREIGN KEY (file_uuid) REFERENCES file(uuid) ON DELETE CASCADE
                    )
                '''

                create_chunk_table = '''
                    CREATE TABLE chunk (
                        uuid TEXT PRIMARY KEY,
                        file_uuid TEXT,
                        FOREIGN KEY (file_uuid) REFERENCES file(uuid) ON DELETE CASCADE
                    )
                '''

                create_sub_chunk_table = '''
                    CREATE TABLE sub_chunk (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        chunk_uuid TEXT,
                        start INTEGER,
                        end INTEGER,
                        data TEXT,
                        FOREIGN KEY (chunk_uuid) REFERENCES chunk(uuid) ON DELETE CASCADE
                    )
                '''

                create_miner_chunk_table = '''
                    CREATE TABLE miner_chunk (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        miner_ss58_address TEXT,
                        chunk_uuid TEXT,
                        FOREIGN KEY (chunk_uuid) REFERENCES chunk(uuid) ON DELETE CASCADE
                    )
                '''

                create_block_table = '''
                    CREATE TABLE block (
                        id BIGINT PRIMARY KEY
                    )
                '''

                create_miner_response = '''
                    CREATE TABLE miner_response (
                        miner_ss58_address TEXT,
                        action VARCHAR,
                        succeed INTEGER,
                        time REAL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                '''

                try:
                    _create_table_if_not_exists(cursor, 'file', create_file_table)
                    _create_table_if_not_exists(cursor, 'file_expiration', create_file_expiration_table)
                    _create_table_if_not_exists(cursor, 'chunk', create_chunk_table)
                    _create_table_if_not_exists(cursor, 'sub_chunk', create_sub_chunk_table)
                    _create_table_if_not_exists(cursor, 'miner_chunk', create_miner_chunk_table)
                    _create_table_if_not_exists(cursor, 'block', create_block_table)
                    _create_table_if_not_exists(cursor, 'miner_response', create_miner_response)
                    connection.commit()

                except sqlite3.Error as e:
                    print(f"Database error: {e}")
                    self._delete_database()
                    raise

    def _database_exists(self) -> bool:
        """
        Checks if the database file exists.

        This method checks if the database file specified in the instance exists
        in the file system.

        Returns:
            bool: True if the database file exists, False otherwise.
        """
        return os.path.isfile(self._database_file_path)

    def _delete_database(self) -> None:
        """
        Deletes the database file if it exists.

        This method deletes the database file specified in the instance if it exists
        in the file system.
        """
        if self._database_exists():
            os.remove(self._database_file_path)

    def get_database_block(self) -> Optional[int]:
        """
        Retrieves the latest database block, if exists.

        Returns:
            Optional[int]: The latest database block as an integer if successful, or None if
            an error occurs or if the 'block' table is empty.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()
            cursor.execute("SELECT id FROM block ORDER BY id DESC LIMIT 1")
            result = cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return None
        finally:
            if connection:
                connection.close()

    def export_database(self) -> Optional[str]:
        """
        Exports the structure and data of the SQLite database to a temporary ZIP file containing a SQL file.

        Returns:
            Optional[str]: The path to the ZIP file, or None if an error occurs.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.sql') as temp_sql_file:
                temp_sql_file_path = temp_sql_file.name

                # Regular expression to find the table name in CREATE TABLE statements
                create_table_regex = re.compile(r"CREATE TABLE\s+`?(\w+)`?\s*\(")

                # Write the structure and data to the temporary SQL file
                with connection:
                    sql_dump = ""
                    for line in connection.iterdump():
                        if line.startswith("CREATE TABLE"):
                            match = create_table_regex.match(line)
                            if match:
                                table_name = match.group(1)
                                drop_table_stmt = f"DROP TABLE IF EXISTS `{table_name}`;\n"
                                sql_dump += drop_table_stmt
                        sql_dump += f'{line}\n'

                    temp_sql_file.write(sql_dump.encode('utf-8'))

            # Create the ZIP file in the specified directory
            with zipfile.ZipFile(self._database_export_file_path, 'w') as zipf:
                zipf.write(temp_sql_file_path, os.path.basename(temp_sql_file_path))

            # Clean up the temporary SQL file
            os.remove(temp_sql_file_path)

            print(f"Database exported successfully to ZIP file: {self._database_export_file_path}")
            return self._database_export_file_path

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return None

        finally:
            if connection:
                connection.close()

    def import_database(self, sql_file_path: str) -> None:
        """
        Imports the content of the specified SQL file into the SQLite database.

        Params:
            sql_file_path (str): The path to the SQL file containing the database schema and data.

        Raises:
            Exception: If an error occurs while reading the SQL file or executing the script.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            with open(sql_file_path, 'r') as sql_file:
                sql_script = sql_file.read()
            connection.executescript(sql_script)
            print("Database imported successfully.")

        except Exception as e:
            print(f"Error importing database - {e}")

        finally:
            if connection:
                connection.close()

    def insert_file(self, file: File) -> Optional[str]:
        """
        Inserts a file and its associated chunks into the database.

        This method inserts a new file record and its associated chunks into the database.
        It ensures that all operations are performed within a single transaction to maintain
        data integrity.

        Params:
            file (File): File to be inserted

        Returns:
            str: The uuid of the inserted file record, or None if the operation was not successful.

        Raises:
            sqlite3.Error: If an error occurs during the database transaction.
        """
        try:
            connection = sqlite3.connect(self._database_file_path)
            with connection:
                cursor = connection.cursor()
                connection.execute('BEGIN TRANSACTION')

                file_uuid = f"{int(time.time())}_{str(uuid.uuid4())}"

                cursor.execute('''
                    INSERT INTO file (uuid, user_ss58_address)
                    VALUES (?, ?)
                ''', (file_uuid, file.user_owner_ss58address,))

                if file.has_expiration():
                    cursor.execute('''
                        INSERT INTO file_expiration (file_uuid, expiration_ms, created_at)
                        VALUES (?, ?, ?)
                    ''', (file_uuid, file.expiration_ms, file.created_at,))

                for chunk in file.chunks:
                    cursor.execute('''
                        INSERT INTO chunk (uuid, file_uuid)
                        VALUES (?, ?)
                    ''', (chunk.chunk_uuid, file_uuid))

                    cursor.execute('''
                        INSERT INTO miner_chunk (miner_ss58_address, chunk_uuid)
                        VALUES (?, ?)
                    ''', (chunk.miner_owner_ss58address, chunk.chunk_uuid))

                    if chunk.sub_chunk:
                        cursor.execute('''
                            INSERT INTO sub_chunk (chunk_uuid, start, end, data)
                            VALUES (?, ?, ?, ?)
                        ''', (chunk.chunk_uuid, chunk.sub_chunk.start, chunk.sub_chunk.end, chunk.sub_chunk.data))

                connection.commit()

            return file_uuid

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return None


    def check_if_file_exists(self, user_ss58_address: str, file_uuid: str) -> bool:
        """
        Check if a file exists in the database.

        This function checks if a file with a given UUID exists in the database
        for a specific user identified by their SS58 address.

        Params:
            user_ss58_address: The SS58 address of the user who owns the file.
            file_uuid: The UUID of the file to be checked.

        Returns:
            bool: True if the file exists, False otherwise.

        Raises:
            None explicitly, but logs an error message if an SQLite error occurs.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()
            cursor.execute(
                "SELECT 1 FROM file WHERE file.uuid = ? AND file.user_ss58_address = ? ",
                (f'{file_uuid}', f'{user_ss58_address}')
            )
            result = cursor.fetchone()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            result = False
        finally:
            if connection:
                connection.close()
        return result is not None

    def get_miner_chunks(self, file_uuid: str) -> List[MinerWithChunk]:
        """
        Retrieves the miners' addresses and chunk hashes associated with a given file name.

        Params:
            file_uuid (str): The name of the file to search for.

        Returns:
            List[MinerWithChunk]: A list of MinerChunk objects containing the miners' addresses and chunk hashes.
        """
        query = """
            SELECT mc.miner_ss58_address, c.uuid
            FROM miner_chunk mc
            INNER JOIN chunk c ON mc.chunk_uuid = c.uuid
            INNER JOIN file f ON c.file_uuid = f.uuid
            WHERE f.uuid = ?;
        """
        connection = None
        result: List[MinerWithChunk] = []
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()
            cursor.execute(query, (file_uuid,))
            rows = cursor.fetchall()
            result = [MinerWithChunk(ss58_address=row[0], chunk_uuid=row[1]) for row in rows]
        except sqlite3.Error as e:
            print(f"Database error: {e}")
        finally:
            if connection:
                connection.close()

        return result

    def get_files_with_expiration(self, user_ss58_address: Ss58Address) -> List[File]:
        """
        Retrieve a list of files with expiration information for a specific user.

        This function queries the database to find files associated with a given user's SS58 address
        that have expiration information. It constructs a list of `File` objects, each containing
        associated chunks and sub-chunks.

        Params:
            user_ss58_address (Ss58Address): The SS58 address of the user whose files are to be retrieved.

        Returns:
            List[File]: A list of `File` objects containing expiration information, chunks, and sub-chunks.

        Raises:
            sqlite3.Error: If there is an error accessing the database.
        """
        files = []
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()

            query = '''
                SELECT 
                    f.uuid AS file_uuid, 
                    f.user_ss58_address,
                    fe.created_at, 
                    fe.expiration_ms
                FROM 
                    file f
                JOIN 
                    file_expiration fe ON f.uuid = fe.file_uuid
                WHERE 
                    f.user_ss58_address = ?
            '''

            cursor.execute(query, (user_ss58_address,))
            rows = cursor.fetchall()

            for row in rows:
                file_uuid, user_owner_uuid, created_at, expiration_ms = row

                # A file with expiration only can contain one chunk
                miner_chunk_query = '''
                    SELECT 
                        mc.miner_ss58_address, 
                        mc.chunk_uuid 
                    FROM 
                        miner_chunk mc
                    JOIN 
                        chunk c ON mc.chunk_uuid = c.uuid
                    WHERE 
                        c.file_uuid = ?
                    LIMIT 1
                '''
                cursor.execute(miner_chunk_query, (file_uuid,))
                miner_chunk = cursor.fetchone()

                if miner_chunk:
                    miner_ss58_address, chunk_uuid = miner_chunk

                    sub_chunk_query = '''
                        SELECT 
                            id, 
                            start, 
                            end,
                            data
                        FROM 
                            sub_chunk 
                        WHERE 
                            chunk_uuid = ?
                        LIMIT 1
                    '''
                    cursor.execute(sub_chunk_query, (chunk_uuid,))
                    sub_chunk_row = cursor.fetchone()

                    sub_chunk = None
                    if sub_chunk_row:
                        sub_chunk_id, start, end, data = sub_chunk_row
                        sub_chunk = SubChunk(sub_chunk_id, start, end, chunk_uuid, data)

                    chunk = Chunk(miner_ss58_address, chunk_uuid, file_uuid, sub_chunk)
                    file = File(user_owner_uuid, file_uuid, [chunk], created_at, expiration_ms)
                    files.append(file)

        except sqlite3.Error as e:
            print(f"Database error: {e}")

        finally:
            if connection:
                connection.close()

        return files

    def remove_file(self, file_uuid: str) -> bool:
        """
        Remove a file from the database.

        This function removes a file from the database identified by its UUID.
        It ensures that foreign key constraints are enabled to handle cascading deletions properly.

        Params:
            file_uuid: The UUID of the file to be removed.

        Returns:
            bool: True if the file was successfully removed, False otherwise.

        Raises:
            None explicitly, but logs an error message if an SQLite error occurs.
        """
        try:
            connection = sqlite3.connect(self._database_file_path)
            with connection:
                cursor = connection.cursor()
                cursor.execute("PRAGMA foreign_keys = ON;")
                cursor.execute("DELETE FROM file WHERE uuid = ?", (file_uuid,))
                connection.commit()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return False

        return True

    def insert_miner_response(self, miner_ss58_address: Ss58Address, action: str, succeed: bool, time: float):
        """
        Insert a record into the miner_response table.

        Params:
            miner_ss58_address (Ss58Address): The Ss58address of the miner.
            action (str): The action performed by the miner.
            succeed (bool): Boolean indicating if the response was succeeded or not.
            time (float): The time taken by the miner to respond in seconds.

        Raises:
            sqlite3.Error: If there is an error inserting the record into the database.
        """
        try:
            connection = sqlite3.connect(self._database_file_path)
            with connection:
                cursor = connection.cursor()

                cursor.execute('''
                    INSERT INTO miner_response (miner_ss58_address, action, succeed, time)
                    VALUES (?, ?, ?, ?)
                ''', (miner_ss58_address, action, succeed, time))

                connection.commit()
        except sqlite3.Error as e:
            print(f"Database error: {e}")

    def get_successful_responses_and_total(self, miner_ss58_address: Ss58Address, action: str = None,
                                           days_interval: int = 7) -> tuple:
        """
        Get the number of successful 'store' responses and the total number of successful responses for a given miner.

        Params:
            miner_ss58_address (int): The Ss58address of the miner.
            action (str, optional): The action to filter responses ('store' or None). Defaults to None.

        Returns:
            tuple: A tuple containing the number of successful 'store' responses and the total number of responses.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()

            if action is None:
                first_condition = "COUNT(CASE WHEN succeed = 1 THEN 1 END)"
                second_condition = "COUNT(*)"
                params = (miner_ss58_address,)
            elif action == "store":
                time_limit = datetime.now() - timedelta(days=days_interval)
                first_condition = "COUNT(CASE WHEN action = 'store' AND succeed = 1 AND timestamp >= ? THEN 1 END)"
                second_condition = "COUNT(CASE WHEN action = 'store' AND timestamp >= ? THEN 1 END)"
                params = (time_limit, time_limit, miner_ss58_address,)
            cursor.execute(f'''
                SELECT 
                    {first_condition} AS successful_store_responses,
                    {second_condition} AS total_store_responses
                FROM miner_response
                WHERE miner_ss58_address = ?
            ''', params)

            successful_responses, total_responses = cursor.fetchone()

            return successful_responses, total_responses

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return 0, 0
        finally:
            if connection:
                connection.close()

    def get_avg_miner_response_time(self, miner_ss58_address: Ss58Address) -> float:
        """
        Get the average response time for a given miner.

        Params:
            miner_ss58_address (str): The Ss58address of the miner.

        Returns:
            float: The average response time for the miner, or 0 if there is an error or no data is available.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()

            cursor.execute('''
                SELECT AVG(time)
                FROM miner_response
                WHERE miner_ss58_address = ?
            ''', (miner_ss58_address,))

            result = cursor.fetchone()
            return result[0] if result else None

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return 0
        finally:
            if connection:
                connection.close()

    def create_block(self, block_number: int) -> int | None:
        try:
            connection = sqlite3.connect(self._database_file_path)
            with connection:
                cursor = connection.cursor()

                cursor.execute(f'''
                    INSERT INTO block (id) VALUES ({block_number})
                ''')

                connection.commit()

            return cursor.lastrowid
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return None


def _create_table_if_not_exists(cursor: sqlite3.Cursor, table_name: str, create_statement: str) -> None:
    """
    Checks if a table exists in the database, and creates it if it does not.

    This method checks if a table with the given name exists in the database.
    If the table does not exist, it executes the provided SQL create statement
    to create the table.

    Params:
        cursor (sqlite3.Cursor): The cursor object used to execute SQL commands.
        table_name (str): The name of the table to check.
        create_statement (str): The SQL statement to create the table if it does not exist.
    """
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    result = cursor.fetchone()

    if result is None:
        cursor.execute(create_statement)
