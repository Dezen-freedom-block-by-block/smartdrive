#  MIT License
#
#  Copyright (c) 2024 Dezen | freedom block by block
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

import os
import sqlite3
import time
from sqlite3 import Cursor
from typing import List, Optional, Union

from smartdrive.logging_config import logger
from smartdrive.commune.models import ModuleInfo
from smartdrive.models.event import StoreEvent, Action, StoreParams, StoreInputParams, RemoveEvent, RemoveInputParams, \
    EventParams, UserEvent, ChunkParams, ValidationEvent, StoreRequestEvent
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
                        file_size_bytes BIGINT,
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
                        signed_block TEXT
                    )
                '''

                create_event_table = '''
                    CREATE TABLE events (
                        uuid TEXT PRIMARY KEY,
                        validator_ss58_address TEXT NOT NULL,
                        event_type INTEGER NOT NULL,
                        file_uuid TEXT NOT NULL,
                        event_signed_params TEXT NOT NULL,
                        user_ss58_address TEXT,
                        file_hash TEXT,
                        file_size_bytes BIGINT,
                        expiration_at INTEGER,
                        approved BOOLEAN,
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

                except sqlite3.Error:
                    logger.error("Database error", exc_info=True)
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

    def insert_file(self, cursor: Cursor, file: File, event_uuid: str):
        """
        Inserts a file and its associated chunks into the database.

        This method inserts a new file record and its associated chunks into the database.

        Params:
            cursor (Cursor): The database cursor
            file (File): File to be inserted
            event_uuid (str): The event UUID where the File was created

        Raises:
            sqlite3.Error: If an error occurs during the database transaction.
        """
        cursor.execute('''
            INSERT INTO file (uuid, user_ss58_address, total_chunks, file_size_bytes)
            VALUES (?, ?, ?, ?)
        ''', (file.file_uuid, file.user_owner_ss58address, file.total_chunks, file.file_size_bytes))

        for chunk in file.chunks:
            cursor.execute('''
                INSERT INTO chunk (uuid, file_uuid, event_uuid, miner_ss58_address, chunk_index)
                VALUES (?, ?, ?, ?, ?)
            ''', (chunk.chunk_uuid, file.file_uuid, event_uuid, chunk.miner_ss58_address, chunk.chunk_index))

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
                result = File(user_owner_ss58address=row[1], total_chunks=row[2], file_uuid=row[0], chunks=[])
            else:
                result = None
        except sqlite3.Error:
            logger.error("Database error", exc_info=True)
            result = None
        finally:
            if connection:
                connection.close()
        return result

    def get_files_by_user(self, user_ss58_address: str) -> list[File]:
        """
        Get all files and their associated chunks from the database for a specific user
        identified by their SS58 address.

        This function retrieves all files that belong to a specific user (identified by their SS58 address)
        from the database, excluding those that have been marked as removed, along with their associated chunks.

        Params:
            user_ss58_address: The SS58 address of the user who owns the files.

        Returns:
            A list of File objects with associated chunks if any files exist, an empty list otherwise.

        Raises:
            None explicitly, but logs an error message if an SQLite error occurs.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()

            # Obtener los archivos del usuario que no han sido eliminados
            cursor.execute(
                "SELECT uuid, user_ss58_address, total_chunks, file_size_bytes FROM file WHERE file.user_ss58_address = ? AND removed = 0",
                (f'{user_ss58_address}',)
            )
            file_rows = cursor.fetchall()

            files = []
            for file_row in file_rows:
                file_uuid = file_row[0]

                # Obtener los chunks asociados al archivo actual
                cursor.execute(
                    "SELECT uuid, chunk_index, miner_ss58_address FROM chunk WHERE file_uuid = ?",
                    (file_uuid,)
                )
                chunk_rows = cursor.fetchall()

                # Crear una lista de chunks asociados a este archivo
                chunks = [
                    Chunk(chunk_uuid=row[0], chunk_index=row[1], miner_ss58_address=row[2], file_uuid=file_uuid)
                    for row in chunk_rows
                ]

                # Crear el objeto File con los chunks asociados
                file = File(
                    user_owner_ss58address=file_row[1],
                    total_chunks=file_row[2],
                    file_uuid=file_row[0],
                    file_size_bytes=file_row[3],
                    chunks=chunks
                )

                files.append(file)

            result = files

        except sqlite3.Error:
            logger.error("Database error", exc_info=True)
            result = []
        finally:
            if connection:
                connection.close()

        return result

    def get_unique_user_ss58_addresses(self) -> list[str]:
        """
        Get all unique user_ss58_address from the database.

        This function retrieves all distinct user SS58 addresses from the 'file' table
        in the database where the 'removed' flag is 0.

        Returns:
            List of unique user_ss58_address if successful, empty list otherwise.

        Raises:
            None explicitly, but logs an error message if an SQLite error occurs.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()
            cursor.execute(
                "SELECT DISTINCT user_ss58_address FROM file WHERE removed = 0"
            )
            rows = cursor.fetchall()
            result = [row[0] for row in rows]
        except sqlite3.Error:
            logger.error("Database error", exc_info=True)
            result = []
        finally:
            if connection:
                connection.close()
        return result

    def get_total_file_size_by_user(self, user_ss58_address: str, only_files: bool = False) -> int:
        """
        Get the total size of all files owned by a user.

        This function calculates the total file size in bytes for a user
        identified by their SS58 address.

        Params:
            user_ss58_address: The SS58 address of the user.

        Returns:
            total_size: The total file size in bytes for the given user.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()
            if only_files:
                cursor.execute(
                    """
                    SELECT SUM(file_size_bytes)
                    FROM file
                    WHERE user_ss58_address = ?
                    AND removed = 0
                    """,
                    (user_ss58_address,)
                )
            else:
                cursor.execute(
                    """
                        WITH active_events AS (
                        SELECT
                            COALESCE(SUM(e.file_size_bytes), 0) AS active_event_size
                        FROM
                            events e
                        LEFT JOIN
                            file f
                        ON
                            e.file_uuid = f.uuid
                        WHERE
                            e.user_ss58_address = ?
                            AND e.event_type = ?
                            AND e.expiration_at > ?
                            AND f.uuid IS NULL
                    ),
                    final_files AS (
                        SELECT
                            COALESCE(SUM(file_size_bytes), 0) AS file_size
                        FROM
                            file
                        WHERE
                            user_ss58_address = ?
                            AND removed = 0
                    )
                    SELECT
                        (SELECT file_size FROM final_files) + (SELECT active_event_size FROM active_events) AS total_storage_used;
                    """,
                    (user_ss58_address, Action.STORE_REQUEST.value, int(time.time()), user_ss58_address,)
                )
            result = cursor.fetchone()
            if result and result[0] is not None:
                total_size = result[0]
            else:
                total_size = 0
        except sqlite3.Error:
            logger.error("Database error", exc_info=True)
            total_size = 0
        finally:
            if connection:
                connection.close()
        return total_size

    def get_chunks(self, file_uuid: str) -> List[MinerWithChunk]:
        """
        Retrieves the miners' addresses and chunk hashes associated with a given file UUID.

        Params:
            file_uuid (str): The UUID of the file to search for.

        Returns:
            List[MinerWithChunk]: A list of MinerWithChunk objects.
        """
        query = """
            SELECT c.miner_ss58_address, c.uuid, c.chunk_index
            FROM chunk c
            INNER JOIN file f ON c.file_uuid = f.uuid
            WHERE f.uuid = ? AND f.removed = 0;
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()
            cursor.execute(query, (file_uuid,))
            rows = cursor.fetchall()
            return [
                MinerWithChunk(
                    ss58_address=row[0],
                    chunk_uuid=row[1],
                    chunk_index=row[2]
                )
                for row in rows
            ]
        except sqlite3.Error:
            logger.error("Database error", exc_info=True)
            return []
        finally:
            if connection:
                connection.close()

    def get_validation_events_by_file_uuid(self, file_uuid: str) -> List[ValidationEvent]:
        """
        Retrieve a list of validation events with expiration information.

        This function queries the database to find validations by its file UUID. It constructs a list of `ValidationEvent`
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
            cursor = connection.cursor()

            query = """
                SELECT chunk_uuid, miner_ss58_address, sub_chunk_start, sub_chunk_end, sub_chunk_encoded,
                    expiration_ms, created_at, file_uuid, user_ss58_address
                FROM validation
                WHERE validation.file_uuid = ?
            """

            cursor.execute(query, (file_uuid,))
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

        except sqlite3.Error:
            logger.error("Database error", exc_info=True)
            return []
        finally:
            if connection:
                connection.close()

        return validation_events

    def get_store_request_event_approvement(self, store_request_event_uuid: str) -> Union[bool, None]:
        """
        Retrieve a StoreRequestEvent from the database based on the given UUID.

        This function attempts to retrieve a storage request event from a database using the provided event UUID.
        If the event is found in the database, it is returned as a `StoreRequestEvent` object. If the event is not found
        or a database error occurs, the function returns `None`.

        Args:
            store_request_event_uuid (str): The UUID of the storage request event to be retrieved.

        Returns:
            Union[StoreRequestEvent, None]:
                - An instance of `StoreRequestEvent` containing the event details if found.
                - `None` if the event is not found or if a database error occurs.

        Raises:
            sqlite3.Error: If a database error occurs during the execution of the query.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()

            query = """
                    SELECT approved
                    FROM events
                    WHERE uuid = ? AND event_type = ? AND expiration_at > ?
                """

            cursor.execute(query, (store_request_event_uuid, Action.STORE_REQUEST.value, int(time.time()),))
            row = cursor.fetchone()
            if row:
                return row[0] == 1
            else:
                return None

        except sqlite3.Error:
            logger.error("Database error", exc_info=True)
            return None
        finally:
            if connection:
                connection.close()

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

        except sqlite3.Error:
            logger.error("Database error", exc_info=True)

        finally:
            if connection:
                connection.close()

        return validation_events

    def remove_file(self, file_uuid: str, cursor: Optional[Cursor] = None) -> bool:
        """
        Mark a file as removed in the database.

        This function mark a file as removed from the database identified by its UUID. It also removes the entries in the
        validation table since it will not be validated in the future.

        Params:
            cursor (sqlite3.Cursor): The database cursor.
            file_uuid: The UUID of the file to mark as removed.

        Raises:
            sqlite3.Error: If an error occurs during the database transaction.
        """
        if cursor is None:
            try:
                with sqlite3.connect(self._database_file_path) as connection:
                    cursor = connection.cursor()
                    cursor.execute("UPDATE file SET removed = 1 WHERE uuid = ?", (file_uuid,))
                    cursor.execute("DELETE FROM validation WHERE file_uuid = ?", (file_uuid,))
                    connection.commit()
                return True
            except sqlite3.Error as e:
                logger.error(f"Error removing file {file_uuid}: {str(e)}", exc_info=True)
                return False

        else:
            cursor.execute("UPDATE file SET removed = 1 WHERE uuid = ?", (file_uuid,))
            cursor.execute("DELETE FROM validation WHERE file_uuid = ?", (file_uuid,))
            return True

    def get_last_block_number(self) -> Optional[int]:
        """
        Retrieves the latest database block number, if exists.

        Returns:
            Optional[int]: The latest database block number as an integer if successful, or None if
            an error occurs or if the 'block' table is empty.
        """
        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            cursor = connection.cursor()
            cursor.execute("SELECT id FROM block ORDER BY id DESC LIMIT 1")
            result = cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error:
            logger.error("Database error", exc_info=True)
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
        logger.info(f"Creating block - {block.block_number}")

        connection = None
        try:
            connection = sqlite3.connect(self._database_file_path)
            connection.row_factory = sqlite3.Row
            with connection:
                cursor = connection.cursor()
                connection.execute('BEGIN TRANSACTION')

                cursor.execute(
                    'INSERT INTO block (id, proposer_ss58_address, signed_block) VALUES (?, ?, ?)',
                    (block.block_number, block.proposer_ss58_address, block.signed_block)
                )

                for event in block.events:
                    self._process_event(cursor, event, block.block_number)

                connection.commit()
            return True
        except sqlite3.Error:
            if connection:
                connection.rollback()
            logger.error("Database error", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Failed to create block: {str(e)}", exc_info=True)
            return False
        finally:
            if connection:
                connection.close()

    def _process_event(self, cursor, event: Union[StoreEvent, RemoveEvent], block_id: int):
        """
        Processes an event and executes the necessary operations in the database.

        Parameters:
            cursor (sqlite3.Cursor): The database cursor.
            event Union[StoreEvent, RemoveEvent]: The specific Event object (StoreEvent, RemoveEvent).
            block_id (int): The ID of the block to which the event belongs.

        Raises:
            sqlite3.Error: If an error occurs during the database transaction.
        """
        event_type = event.get_event_action().value
        validator_ss58_address = event.validator_ss58_address
        event_signed_params = event.event_signed_params

        file_uuid = event.event_params.file_uuid
        user_ss58_address = None
        input_signed_params = None
        file_hash = None
        file_size_bytes = None
        expiration_at = None
        approved = None

        if isinstance(event, UserEvent):
            user_ss58_address = event.user_ss58_address
            input_signed_params = event.input_signed_params

        elif isinstance(event, StoreEvent):
            total_chunks_index = set()
            chunks = []
            for chunk in event.event_params.chunks_params:
                total_chunks_index.add(chunk.chunk_index)
                chunks.append(
                    Chunk(
                        miner_ss58_address=Ss58Address(chunk.miner_ss58_address),
                        chunk_uuid=chunk.uuid,
                        file_uuid=event.event_params.file_uuid,
                        chunk_index=chunk.chunk_index
                    )
                )
            file = File(
                user_owner_ss58address=event.user_ss58_address,
                total_chunks=len(total_chunks_index),
                file_uuid=event.event_params.file_uuid,
                chunks=chunks,
                file_size_bytes=event.input_params.file_size_bytes
            )
            self.insert_file(cursor=cursor, file=file, event_uuid=event.uuid)

            file_hash = event.input_params.file_hash

        elif isinstance(event, StoreRequestEvent):
            file_hash = event.input_params.file_hash
            file_size_bytes = event.input_params.file_size_bytes
            expiration_at = event.event_params.expiration_at
            approved = event.event_params.approved

        elif isinstance(event, RemoveEvent):
            self.remove_file(cursor=cursor, file_uuid=event.event_params.file_uuid)

        cursor.execute(
            '''
            INSERT INTO events (uuid, validator_ss58_address, event_type, file_uuid, event_signed_params, user_ss58_address, file_hash, input_signed_params, block_id, file_size_bytes, expiration_at, approved)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (event.uuid, validator_ss58_address, event_type, file_uuid, event_signed_params, user_ss58_address, file_hash, input_signed_params, block_id, file_size_bytes, expiration_at, approved)
        )

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
                    b.id AS block_id, b.signed_block, b.proposer_ss58_address,
                    e.uuid AS event_uuid, e.validator_ss58_address, e.event_type, e.file_uuid, e.event_signed_params, e.user_ss58_address, e.file_hash, e.input_signed_params,
                    c.uuid AS chunk_uuid, c.miner_ss58_address, c.chunk_index,
                    f.file_size_bytes
                FROM block b
                LEFT JOIN events e ON b.id = e.block_id
                LEFT JOIN chunk c ON c.event_uuid = e.uuid
                LEFT JOIN file f ON f.uuid = c.file_uuid
                WHERE b.id BETWEEN ? AND ?
                ORDER BY b.id, e.uuid, c.uuid, f.uuid
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
                        signed_block=row["signed_block"],
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

        except sqlite3.Error:
            logger.error("Database error", exc_info=True)
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
        except sqlite3.Error:
            if connection:
                connection.rollback()
            logger.error("Database error", exc_info=True)
            return False
        finally:
            if connection:
                connection.close()

    def get_random_validation_events_without_expiration_per_miners(self, miners: list[ModuleInfo]) -> list[ValidationEvent] | None:
        """
        Retrieves only one random validation event per miner for the given registered miners from the database.

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

            miner_addresses = [miner.ss58_address for miner in miners]
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

        except sqlite3.Error:
            logger.error("Database error", exc_info=True)
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
            "file_uuid": row['file_uuid'],
            "chunks_params": [],
            "created_at": None,
            "expiration_ms": None
        }

        if event_type == Action.STORE.value:
            event = StoreEvent(
                uuid=row['event_uuid'],
                validator_ss58_address=row['validator_ss58_address'],
                event_params=StoreParams(**event_params),
                event_signed_params=row['event_signed_params'],
                user_ss58_address=row['user_ss58_address'],
                input_params=StoreInputParams(file_hash=row['file_hash'], file_size_bytes=row['file_size_bytes']),
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
