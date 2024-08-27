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

from smartdrive.commune.models import ModuleInfo
from smartdrive.models.event import StoreEvent, Action, StoreParams, StoreInputParams, RemoveEvent, RemoveInputParams, \
    EventParams, UserEvent, ChunkParams, ValidationEvent
from smartdrive.models.block import Block
from smartdrive.validator.config import config_manager
from smartdrive.validator.models.models import MinerWithChunk, File, Chunk

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
                        user_ss58_address INTEGER,
                        total_chunks INTEGER,
                        removed INTEGER DEFAULT 0
                    )
                '''

                create_chunk_table = '''
                    CREATE TABLE chunk (
                        uuid TEXT PRIMARY KEY,
                        file_uuid TEXT,
                        event_uuid TEXT,
                        miner_ss58_address TEXT,
                        chunk_index INTEGER,
                        FOREIGN KEY (file_uuid) REFERENCES file(uuid) ON DELETE CASCADE,
                        FOREIGN KEY (event_uuid) REFERENCES events(uuid) ON DELETE CASCADE
                    )
                '''

                create_block_table = '''
                    CREATE TABLE block (
                        id BIGINT PRIMARY KEY,
                        proposer_ss58_address TEXT,
                        proposer_signature TEXT
                    )
                '''

                create_event_table = '''
                    CREATE TABLE events (
                        uuid TEXT PRIMARY KEY,
                        validator_ss58_address TEXT NOT NULL,
                        event_type INTEGER NOT NULL,
                        file_uuid TEXT NOT NULL,
                        file_created_at INTEGER,
                        file_expiration_ms INTEGER,
                        event_signed_params TEXT NOT NULL,
                        user_ss58_address TEXT,
                        file TEXT,
                        input_signed_params TEXT,
                        block_id INTEGER NOT NULL,
                        FOREIGN KEY (block_id) REFERENCES block(id) ON DELETE CASCADE
                    )
                '''

                create_validation_table = '''
                    CREATE TABLE validation (
                        chunk_uuid TEXT PRIMARY KEY,
                        file_uuid TEXT NOT NULL,
                        miner_ss58_address TEXT NOT NULL,
                        user_ss58_address INTEGER NOT NULL,
                        sub_chunk_start INTEGER NOT NULL,
                        sub_chunk_end INTEGER NOT NULL,
                        sub_chunk_encoded TEXT NOT NULL,
                        expiration_ms INTEGER,
                        created_at INTEGER
                    )
                '''

                try:
                    _create_table_if_not_exists(cursor, 'file', create_file_table)
                    _create_table_if_not_exists(cursor, 'chunk', create_chunk_table)
                    _create_table_if_not_exists(cursor, 'block', create_block_table)
                    _create_table_if_not_exists(cursor, 'event', create_event_table)
                    _create_table_if_not_exists(cursor, 'validation', create_validation_table)
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

    def insert_file(self, file: File, event_uuid: str) -> bool:
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
                    INSERT INTO file (uuid, user_ss58_address, total_chunks)
                    VALUES (?, ?, ?)
                ''', (file.file_uuid, file.user_owner_ss58address, file.total_chunks))

                for chunk in file.chunks:
                    cursor.execute('''
                        INSERT INTO chunk (uuid, file_uuid, event_uuid, miner_ss58_address, chunk_index)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (chunk.chunk_uuid, file.file_uuid, event_uuid, chunk.miner_ss58_address, chunk.chunk_index))

                connection.commit()

            return True

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return False

    def get_file(self, user_ss58_address: str, file_uuid: str) -> File | None:
        """
        Get a file from the database.

        This function gets a file with a given UUID in the database
        for a specific user identified by their SS58 address.

        Params:
            user_ss58_address: The SS58 address of the user who owns the file.
            file_uuid: The UUID of the file to be checked.

        Returns:
            file: file if the file exists, None otherwise.

        Raises:
            None explicitly, but logs an error message if an SQLite error occurs.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()
            cursor.execute(
                "SELECT uuid, user_ss58_address, total_chunks FROM file WHERE file.uuid = ? AND file.user_ss58_address = ? AND removed = 0 ",
                (f'{file_uuid}', f'{user_ss58_address}')
            )
            row = cursor.fetchone()
            if row is not None:
                result = File(user_owner_ss58address=row[1], total_chunks=row[2], file_uuid=row[0], chunks=[], created_at=None, expiration_ms=None)
            else:
                result = None
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            result = None
        finally:
            if connection:
                connection.close()
        return result

    def get_chunks(self, file_uuid: str) -> List[MinerWithChunk]:
        """
        Retrieves the miners' addresses and chunk hashes associated with a given file name.

        Params:
            file_uuid (str): The name of the file to search for.

        Returns:
            List[MinerWithChunk]: A list of MinerChunk objects containing the miners' addresses and chunk hashes.
        """
        query = """
            SELECT c.miner_ss58_address, c.uuid, c.chunk_index
            FROM chunk c
            INNER JOIN file f ON c.file_uuid = f.uuid
            WHERE f.uuid = ? AND f.removed = 0;
        """
        connection = None
        result: List[MinerWithChunk] = []
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()
            cursor.execute(query, (file_uuid,))
            rows = cursor.fetchall()
            result = [MinerWithChunk(ss58_address=row[0], chunk_uuid=row[1], chunk_index=row[2]) for row in rows]
        except sqlite3.Error as e:
            print(f"Database error: {e}")
        finally:
            if connection:
                connection.close()

        return result

    def get_validation_events_with_expiration(self) -> List[ValidationEvent]:
        """
        Retrieve a list of validation events with expiration information.

        This function queries the database to find files that have expiration information. It constructs a list of `ValidationEvent`
         objects.

        Returns:
            List[ValidationEvent]: A list of `ValidationEvent` objects.

        Raises:
            sqlite3.Error: If there is an error accessing the database.
        """
        validation_events: list[ValidationEvent] = []
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()

            query = '''
                SELECT chunk_uuid, miner_ss58_address, sub_chunk_start, sub_chunk_end, sub_chunk_encoded,
                           expiration_ms, created_at, file_uuid, user_ss58_address
                FROM validation
                WHERE expiration_ms IS NOT NULL AND created_at IS NOT NULL
            '''

            cursor.execute(query)
            rows = cursor.fetchall()

            for row in rows:
                validation_event = ValidationEvent(
                    uuid=row["chunk_uuid"],
                    miner_ss58_address=row["miner_ss58_address"],
                    sub_chunk_start=row["sub_chunk_start"],
                    sub_chunk_end=row["sub_chunk_end"],
                    sub_chunk_encoded=row["sub_chunk_encoded"],
                    expiration_ms=row["expiration_ms"],
                    created_at=row["created_at"],
                    file_uuid=row["file_uuid"],
                    user_owner_ss58_address=row["user_ss58_address"]
                )
                validation_events.append(validation_event)

        except sqlite3.Error as e:
            print(f"Database error: {e}")

        finally:
            if connection:
                connection.close()

        return validation_events

    def remove_file(self, file_uuid: str) -> bool:
        """
        Mark a file as removed in the database.

        This function mark a file as removed from the database identified by its UUID. It also removes the entries in the
        validation table since it will not be validated in the future.

        Params:
            file_uuid: The UUID of the file to mark as removed.

        Returns:
            bool: True if the file was successfully marked as removed, False otherwise.

        Raises:
            None explicitly, but logs an error message if an SQLite error occurs.
        """
        try:
            connection = sqlite3.connect(self._database_file_path)
            with connection:
                cursor = connection.cursor()
                cursor.execute("PRAGMA foreign_keys = ON;")
                cursor.execute("UPDATE file SET removed = 1 WHERE uuid = ?", (file_uuid,))
                cursor.execute("DELETE FROM validation WHERE file_uuid = ?", (file_uuid,))
                connection.commit()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return False

        return True

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
        Creates a block in the database with its associated events.

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

                # Insert events
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

    def _insert_event(self, cursor, event: Union[StoreEvent, RemoveEvent], block_id: int):
        """
        Inserts an event into the database.

        Parameters:
            cursor (sqlite3.Cursor): The database cursor.
            event Union[StoreEvent, RemoveEvent]: The specific Event object (StoreEvent, RemoveEvent).
            block_id (int): The ID of the block to which the event belongs.
        """
        event_type = event.get_event_action().value
        validator_ss58_address = event.validator_ss58_address
        event_signed_params = event.event_signed_params

        # Initialize common fields
        file_uuid = event.event_params.file_uuid
        file_created_at = None
        file_expiration_ms = None
        user_ss58_address = None
        input_signed_params = None
        file = None

        if isinstance(event, UserEvent):
            user_ss58_address = event.user_ss58_address
            input_signed_params = event.input_signed_params

        # Populate specific fields based on event type
        if isinstance(event, StoreEvent):
            file_created_at = event.event_params.created_at
            file_expiration_ms = event.event_params.expiration_ms
            file = event.input_params.file

        cursor.execute('''
            INSERT INTO events (uuid, validator_ss58_address, event_type, file_uuid, file_created_at, file_expiration_ms, event_signed_params, user_ss58_address, file, input_signed_params, block_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
        event.uuid, validator_ss58_address, event_type, file_uuid, file_created_at, file_expiration_ms,
        event_signed_params, user_ss58_address, file, input_signed_params, block_id))

    def get_blocks(self, start: int, end: int) -> Optional[List[Block]]:
        """
        Retrieve blocks and their associated events from the database.

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
                    e.uuid AS event_uuid, e.validator_ss58_address, e.event_type, e.file_uuid, e.file_created_at, e.file_expiration_ms, e.event_signed_params, e.user_ss58_address, e.file, e.input_signed_params,
                    c.uuid AS chunk_uuid, c.miner_ss58_address, c.chunk_index
                FROM block b
                LEFT JOIN events e ON b.id = e.block_id
                LEFT JOIN chunk c ON c.event_uuid = e.uuid
                WHERE b.id BETWEEN ? AND ?
                ORDER BY b.id, e.uuid, c.uuid
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

                    if isinstance(events[event_uuid].event_params, StoreParams):
                        chunk = ChunkParams(
                            uuid=row['chunk_uuid'],
                            chunk_index=row['chunk_index'],
                            miner_ss58_address=row['miner_ss58_address']
                        )
                        events[event_uuid].event_params.chunks_params.append(chunk)

            return list(blocks.values())

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return None
        finally:
            if connection:
                connection.close()

    def insert_validation_events(self, validation_events: list[ValidationEvent]) -> bool:
        """
        Insert a list of validation events into the validation table.

        This function takes a list of ValidationEvent objects and inserts each event's details into the validation table
        within a single transaction. If any error occurs during the insertion, the transaction is rolled back.

        Params:
            validation_events (list[ValidationEvent]): A list of ValidationEvent objects to be inserted into the validation table.

        Returns:
            bool: True if the insertion is successful, False otherwise.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            connection.row_factory = sqlite3.Row
            with connection:
                cursor = connection.cursor()
                connection.execute('BEGIN TRANSACTION')

                for validation_event in validation_events:
                    cursor.execute('''
                        INSERT INTO validation (chunk_uuid, miner_ss58_address, sub_chunk_start, sub_chunk_end, sub_chunk_encoded, file_uuid, expiration_ms, created_at, user_ss58_address)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (validation_event.uuid, validation_event.miner_ss58_address, validation_event.sub_chunk_start, validation_event.sub_chunk_end, validation_event.sub_chunk_encoded, validation_event.file_uuid, validation_event.expiration_ms, validation_event.created_at, validation_event.user_owner_ss58_address))

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

    def get_validation_events_without_expiration(self, registered_miners: list[ModuleInfo]) -> list[ValidationEvent] | None:
        """
        Retrieves validation events records for the given registered miners from the database.

        Params:
            registered_miners (list[ModuleInfo]): A list of registered miners.

        Returns:
            list[ChunkParams] : A list of ChunkEvent from the validation table for the specified miners or None.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()

            miner_addresses = [miner.ss58_address for miner in registered_miners]
            validation_events = []

            for miner_address in miner_addresses:
                query = '''
                    SELECT chunk_uuid, miner_ss58_address, sub_chunk_start, sub_chunk_end, sub_chunk_encoded,
                           expiration_ms, created_at, file_uuid, user_ss58_address
                    FROM validation
                    WHERE miner_ss58_address = ? AND expiration_ms IS NULL AND created_at IS NULL
                    ORDER BY RANDOM()
                    LIMIT 1
                '''
                cursor.execute(query, (miner_address,))
                row = cursor.fetchone()
                if row:
                    validation_events.append(
                        ValidationEvent(
                            uuid=row["chunk_uuid"],
                            miner_ss58_address=row["miner_ss58_address"],
                            sub_chunk_start=row["sub_chunk_start"],
                            sub_chunk_end=row["sub_chunk_end"],
                            sub_chunk_encoded=row["sub_chunk_encoded"],
                            expiration_ms=row["expiration_ms"],
                            created_at=row["created_at"],
                            file_uuid=row["file_uuid"],
                            user_owner_ss58_address=row["user_ss58_address"]
                        )
                    )

            return validation_events

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return None
        finally:
            if connection:
                connection.close()

    def _build_event_from_row(self, row) -> Union[StoreEvent, RemoveEvent]:
        """
        Build an Event object from a database row.

        Params:
            row (dict): A dictionary containing the row data from the database.

        Returns:
            Union[StoreEvent, RemoveEvent]: An instance of an Event subclass (StoreEvent, RemoveEvent).

        Raises:
            ValueError: If the event type is unknown.
        """
        event_type = row['event_type']
        event_params = {
            "file_uuid": row['file_uuid']
        }

        if event_type == Action.STORE.value:
            event_params.update({
                "created_at": row['file_created_at'],
                "expiration_ms": row['file_expiration_ms'],
                "chunks_params": []
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
                event_params=EventParams(**event_params),
                event_signed_params=row['event_signed_params'],
                user_ss58_address=row['user_ss58_address'],
                input_params=RemoveInputParams(file_uuid=row['file_uuid']),
                input_signed_params=row['input_signed_params']
            )
        else:
            raise ValueError(f"Unknown event type: {event_type}")

        return event


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
