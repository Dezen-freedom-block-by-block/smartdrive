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
import tempfile
import sqlite3
import zipfile
from typing import List, Optional, Union
from datetime import datetime, timedelta

from substrateinterface import Keypair

from smartdrive.models.event import MinerProcess, StoreEvent, Event, Action, StoreParams, StoreInputParams, RemoveEvent, \
    RemoveParams, RemoveInputParams, RetrieveEvent, EventParams, RetrieveInputParams, ValidateEvent, UserEvent
from smartdrive.models.block import Block
from smartdrive.validator.api.middleware.sign import verify_data_signature
from smartdrive.validator.config import config_manager
from smartdrive.validator.models.models import MinerWithChunk, SubChunk, File, Chunk

from communex.types import Ss58Address


class Database:
    _database_file_path = None
    _database_export_file_path = None

    def __init__(self):
        """
        Initialize the DatabaseManager and create the database schema if it does not exist.

        This constructor initializes the database manager with the specified database file paths
        and creates the necessary tables in the database if they do not already exist. It also sets
        the SQLite auto vacuum mode to FULL.

        Raises:
            sqlite3.Error: If there is an error creating the database tables.
        """
        self._database_file_path = config_manager.config.database_file
        self._database_export_file_path = config_manager.config.database_export_file

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
                        expiration_ms INTEGER NOT NULL,
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
                        id BIGINT PRIMARY KEY,
                        proposer_ss58_address TEXT,
                        proposer_signature TEXT
                    )
                '''

                # TODO: Don't store sub_chunk info in params must be stored in MineProcess once the split system it's completed
                create_event_table = '''
                    CREATE TABLE events (
                        uuid TEXT PRIMARY KEY,
                        validator_ss58_address TEXT NOT NULL,
                        event_type INTEGER NOT NULL,
                        file_uuid TEXT NOT NULL,
                        sub_chunk_start INTEGER,
                        sub_chunk_end INTEGER,
                        sub_chunk_encoded TEXT,
                        event_signed_params TEXT NOT NULL,
                        user_ss58_address TEXT,
                        file TEXT,
                        input_signed_params TEXT,
                        block_id INTEGER NOT NULL,
                        FOREIGN KEY (block_id) REFERENCES block(id) ON DELETE CASCADE
                    )
                '''

                create_miner_process = '''
                    CREATE TABLE miner_processes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        chunk_uuid TEXT,
                        miner_ss58_address TEXT NOT NULL,
                        succeed BOOLEAN,
                        processing_time REAL,
                        event_uuid TEXT NOT NULL,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (event_uuid) REFERENCES events(uuid) ON DELETE CASCADE
                    )
                '''

                try:
                    _create_table_if_not_exists(cursor, 'file', create_file_table)
                    _create_table_if_not_exists(cursor, 'file_expiration', create_file_expiration_table)
                    _create_table_if_not_exists(cursor, 'chunk', create_chunk_table)
                    _create_table_if_not_exists(cursor, 'sub_chunk', create_sub_chunk_table)
                    _create_table_if_not_exists(cursor, 'miner_chunk', create_miner_chunk_table)
                    _create_table_if_not_exists(cursor, 'block', create_block_table)
                    _create_table_if_not_exists(cursor, 'event', create_event_table)
                    _create_table_if_not_exists(cursor, 'miner_process', create_miner_process)
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

    def clear_database(self):
        """
        Clear all records from the database and reset autoincrement values.
        """
        connection = sqlite3.connect(self._database_file_path)
        with connection:
            cursor = connection.cursor()
            try:
                # Get the list of all table names
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()

                for table in tables:
                    table_name = table[0]
                    if table_name != "sqlite_sequence":
                        cursor.execute(f"DELETE FROM {table_name};")
                        cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table_name}';")

                connection.commit()
            except sqlite3.Error as e:
                print(f"Error clearing database: {e}")
                raise

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

    def insert_file(self, file: File) -> bool:
        """
        Inserts a file and its associated chunks into the database.

        This method inserts a new file record and its associated chunks into the database.
        It ensures that all operations are performed within a single transaction to maintain
        data integrity.

        Params:
            file (File): File to be inserted

        Returns:
            bool: True if the operation was successful, or False if was not successful.

        Raises:
            sqlite3.Error: If an error occurs during the database transaction.
        """
        try:
            connection = sqlite3.connect(self._database_file_path)
            with connection:
                cursor = connection.cursor()
                connection.execute('BEGIN TRANSACTION')

                cursor.execute('''
                    INSERT INTO file (uuid, user_ss58_address)
                    VALUES (?, ?)
                ''', (file.file_uuid, file.user_owner_ss58address,))

                if file.has_expiration():
                    cursor.execute('''
                        INSERT INTO file_expiration (file_uuid, expiration_ms, created_at)
                        VALUES (?, ?, ?)
                    ''', (file.file_uuid, file.expiration_ms, file.created_at,))

                for chunk in file.chunks:
                    cursor.execute('''
                        INSERT INTO chunk (uuid, file_uuid)
                        VALUES (?, ?)
                    ''', (chunk.chunk_uuid, file.file_uuid))

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

            return True

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return False

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

    def get_files_with_expiration(self) -> List[File]:
        """
        Retrieve a list of files with expiration information.

        This function queries the database to find files that have expiration information. It constructs a list of `File`
         objects, each containing associated chunks and sub-chunks.

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
            '''

            cursor.execute(query)
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

    def get_miner_processes(self, miner_ss58_address: str, days_interval=None) -> tuple:
        """
        Retrieve the total number of calls and the number of failed calls in a given time period of a miner.

        Params:
            miner_ss58_address (str): Ss58address of the miner.
            days_interval (int, optional): The number of days to look back from the current time.
                                           If None, retrieves all records.

        Returns:
            tuple: A tuple with failed calls, total calls and average time response.
        """
        # Calculate the time range based on the days_interval
        end_time = datetime.now().date()
        start_time = None

        if days_interval is not None:
            start_time = (datetime.now() - timedelta(days=days_interval)).date()

        # Build the query
        query = """
        SELECT COUNT(*) as total_calls, 
               SUM(CASE WHEN succeed = 0 THEN 1 ELSE 0 END) as failed_calls
        FROM miner_processes
        WHERE miner_ss58_address = ?
        """
        params = [miner_ss58_address]

        if start_time is not None:
            query += " AND DATE(timestamp) BETWEEN ? AND ?"
            params.extend([start_time, end_time])

        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()

            cursor.execute(query, params)
            result = cursor.fetchone()

            total_calls = result[0] if result[0] is not None else 0
            failed_calls = result[1] if result[1] is not None else 0

            return total_calls, failed_calls
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return 0, 0
        finally:
            if connection:
                connection.close()

    def get_last_block(self) -> Optional[int]:
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

    def create_block(self, block: Block) -> bool:
        """
        Creates a block in the database with its associated events and miner processes.

        Parameters:
            block (Block): The block to be created in the database.

        Returns:
            bool: True if the block and its events are successfully created, False otherwise.
        """
        print(f"CREATING BLOCK - {block.block_number}")
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            connection.row_factory = sqlite3.Row
            with connection:
                cursor = connection.cursor()
                connection.execute('BEGIN TRANSACTION')

                # Insert block
                cursor.execute('INSERT INTO block (id, proposer_ss58_address, proposer_signature) VALUES (?, ?, ?)', (block.block_number, block.proposer_ss58_address, block.proposer_signature))

                # Insert events and associated miner processes
                for event in block.events:
                    self._insert_event(cursor, event, block.block_number)

                connection.commit()
            return True
        except sqlite3.Error as e:
            if connection:
                connection.rollback()
            print(f"Database error: {e}")
            return False
        finally:
            if connection:
                connection.close()

    def _insert_event(self, cursor, event: Union[StoreEvent, RemoveEvent, RetrieveEvent, ValidateEvent], block_id: int):
        """
        Inserts an event into the database with its associated miner processes.

        Parameters:
            cursor (sqlite3.Cursor): The database cursor.
            event Union[StoreEvent, RemoveEvent, RetrieveEvent, ValidateEvent]: The specific Event object (StoreEvent, RemoveEvent, RetrieveEvent, ValidateEvent).
            block_id (int): The ID of the block to which the event belongs.
        """
        event_type = event.get_event_action().value
        validator_ss58_address = event.validator_ss58_address
        event_signed_params = event.event_signed_params

        # Initialize common fields
        file_uuid = event.event_params.file_uuid
        sub_chunk_start = None
        sub_chunk_end = None
        sub_chunk_encoded = None
        user_ss58_address = None
        input_signed_params = None
        file = None

        if isinstance(event, UserEvent):
            user_ss58_address = event.user_ss58_address
            input_signed_params = event.input_signed_params

        # Populate specific fields based on event type
        if isinstance(event, StoreEvent):
            sub_chunk_start = event.event_params.sub_chunk_start
            sub_chunk_end = event.event_params.sub_chunk_end
            sub_chunk_encoded = event.event_params.sub_chunk_encoded
            file = event.input_params.file

        cursor.execute('''
            INSERT INTO events (uuid, validator_ss58_address, event_type, file_uuid, sub_chunk_start, sub_chunk_end, sub_chunk_encoded, event_signed_params, user_ss58_address, file, input_signed_params, block_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
        event.uuid, validator_ss58_address, event_type, file_uuid, sub_chunk_start, sub_chunk_end, sub_chunk_encoded,
        event_signed_params, user_ss58_address, file, input_signed_params, block_id))

        # Insert miner processes if applicable
        for miner_process in event.event_params.miners_processes:
            self._insert_miner_process(cursor, miner_process, event.uuid)

    def _insert_miner_process(self, cursor, miner_process: MinerProcess, event_uuid: str) -> bool:
        """
        Inserts a miner process into the database.

        Parameters:
            cursor (sqlite3.Cursor): The database cursor.
            miner_process (MinerProcess): The miner process to be inserted.
            event_uuid (str): The UUID of the event to which the miner process belongs.

        Returns:
            bool: True if the miner process is successfully inserted, False otherwise.
        """
        try:
            cursor.execute('''
                INSERT INTO miner_processes (chunk_uuid, miner_ss58_address, succeed, processing_time, event_uuid)
                VALUES (?, ?, ?, ?, ?)
            ''', (miner_process.chunk_uuid, miner_process.miner_ss58_address, miner_process.succeed, miner_process.processing_time, event_uuid))
            return True
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return False

    def get_blocks(self, start: int, end: int) -> Optional[List[Block]]:
        """
        Retrieve blocks and their associated events and miner processes from the database.

        Params:
            start (int): The starting block ID.
            end (int): The ending block ID.

        Returns:
            Optional[List[Block]]: A list of Block objects, or None if there is a database error.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()

            query = '''
                SELECT 
                    b.id AS block_id, b.proposer_signature, b.proposer_ss58_address,
                    e.uuid AS event_uuid, e.validator_ss58_address, e.event_type, e.file_uuid, e.sub_chunk_start, e.sub_chunk_end, e.sub_chunk_encoded, e.event_signed_params, e.user_ss58_address, e.file, e.input_signed_params,
                    m.chunk_uuid, m.miner_ss58_address, m.succeed, m.processing_time
                FROM block b
                LEFT JOIN events e ON b.id = e.block_id
                LEFT JOIN miner_processes m ON e.uuid = m.event_uuid
                WHERE b.id BETWEEN ? AND ?
            '''

            cursor.execute(query, (start, end))
            rows = cursor.fetchall()

            blocks = {}
            events = {}
            for row in rows:
                block_id = row['block_id']
                if block_id not in blocks:
                    blocks[block_id] = Block(
                        block_number=block_id,
                        events=[],
                        proposer_signature=row["proposer_signature"],
                        proposer_ss58_address=Ss58Address(row["proposer_ss58_address"]),
                    )

                event_uuid = row['event_uuid']
                if event_uuid:
                    if event_uuid not in events:
                        events[event_uuid] = self._build_event_from_row(row)
                        blocks[block_id].events.append(events[event_uuid])

                    miner_process = self._build_miner_process_from_row(row)
                    if miner_process and miner_process not in events[event_uuid].event_params.miners_processes:
                        events[event_uuid].event_params.miners_processes.append(miner_process)

            return list(blocks.values())

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return None
        finally:
            if connection:
                connection.close()

    def _build_event_from_row(self, row) -> Union[StoreEvent, RemoveEvent, RetrieveEvent, ValidateEvent]:
        """
        Build an Event object from a database row.

        Params:
            row (dict): A dictionary containing the row data from the database.

        Returns:
            Union[StoreEvent, RemoveEvent, RetrieveEvent, ValidateEvent]: An instance of an Event subclass (StoreEvent, RemoveEvent, RetrieveEvent, or ValidateEvent).

        Raises:
            ValueError: If the event type is unknown.
        """
        event_type = row['event_type']
        event_params = {
            "file_uuid": row['file_uuid'],
            "miners_processes": []
        }

        if event_type == Action.STORE.value:
            event_params.update({
                "sub_chunk_start": row['sub_chunk_start'],
                "sub_chunk_end": row['sub_chunk_end'],
                "sub_chunk_encoded": row['sub_chunk_encoded']
            })
            event = StoreEvent(
                uuid=row['event_uuid'],
                validator_ss58_address=row['validator_ss58_address'],
                event_params=StoreParams(**event_params),
                event_signed_params=row['event_signed_params'],
                user_ss58_address=row['user_ss58_address'],
                input_params=StoreInputParams(file=row['file']),
                input_signed_params=row['input_signed_params']
            )
        elif event_type == Action.REMOVE.value:
            event = RemoveEvent(
                uuid=row['event_uuid'],
                validator_ss58_address=row['validator_ss58_address'],
                event_params=RemoveParams(**event_params),
                event_signed_params=row['event_signed_params'],
                user_ss58_address=row['user_ss58_address'],
                input_params=RemoveInputParams(file_uuid=row['file_uuid']),
                input_signed_params=row['input_signed_params']
            )
        elif event_type == Action.RETRIEVE.value:
            event = RetrieveEvent(
                uuid=row['event_uuid'],
                validator_ss58_address=row['validator_ss58_address'],
                event_params=EventParams(**event_params),
                event_signed_params=row['event_signed_params'],
                user_ss58_address=row['user_ss58_address'],
                input_params=RetrieveInputParams(file_uuid=row['file_uuid']),
                input_signed_params=row['input_signed_params']
            )
        elif event_type == Action.VALIDATION.value:
            event = ValidateEvent(
                uuid=row['event_uuid'],
                validator_ss58_address=row['validator_ss58_address'],
                event_params=EventParams(**event_params),
                event_signed_params=row['event_signed_params']
            )
        else:
            raise ValueError(f"Unknown event type: {event_type}")

        return event

    def _build_miner_process_from_row(self, row) -> Optional[MinerProcess]:
        """
        Build a MinerProcess object from a database row.

        Params:
            row (dict): A dictionary containing the row data from the database.

        Returns:
            MinerProcess: An instance of MinerProcess, or None if chunk_uuid is None.
        """
        if row['event_uuid'] is None:
            return None
        return MinerProcess(
            chunk_uuid=row['chunk_uuid'],
            miner_ss58_address=row['miner_ss58_address'],
            succeed=row['succeed'],
            processing_time=row['processing_time']
        )


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
