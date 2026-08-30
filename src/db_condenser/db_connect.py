import getpass
import sys
import time
from datetime import datetime

import mysql.connector
import psycopg

from db_condenser.config_reader import DbConnectInfo, DbType


class DbConnection:
    def __init__(self, connection):
        self.connection = connection

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()


class LoggingCursor:
    def __init__(self, cursor, verbose=False):
        self.inner_cursor = cursor
        self._verbose = verbose

    def execute(self, query, params=None):
        start_time = time.time()
        if self._verbose:
            print("Beginning query @ {}:\n\t{}".format(str(datetime.now()), query))
            sys.stdout.flush()
        retval = self.inner_cursor.execute(query, params)
        if self._verbose:
            print("\tQuery completed in {}s".format(time.time() - start_time))
            sys.stdout.flush()
        return retval

    def __getattr__(self, name):
        return self.inner_cursor.__getattribute__(name)

    def __exit__(self, a, b, c):
        return self.inner_cursor.__exit__(a, b, c)

    def __enter__(self):
        return LoggingCursor(self.inner_cursor.__enter__(), self._verbose)


# small wrapper to the connection class that gives us a common interface to the cursor()
# method across MySQL and Postgres. This one is for Postgres
class PsqlConnection(DbConnection):
    def __init__(self, connect, read_repeatable, verbose=False):
        connection_args = dict(
            dbname=connect.db_name,
            user=connect.user,
            password=connect.password,
            host=connect.host,
            port=connect.port,
        )

        if connect.ssl_mode:
            connection_args["sslmode"] = connect.ssl_mode

        DbConnection.__init__(self, psycopg.connect(**connection_args))
        self._verbose = verbose
        if read_repeatable:
            self.connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ

    def cursor(self, name=None, withhold=False):
        return LoggingCursor(
            self.connection.cursor(name=name, withhold=withhold), self._verbose
        )


# small wrapper to the connection class that gives us a common interface to the cursor()
# method across MySQL and Postgres. This one is for MySQL
class MySqlConnection(DbConnection):
    def __init__(
        self, connect, read_repeatable, verbose=False, connect_to_database=True
    ):
        connection_args = dict(
            host=connect.host,
            port=connect.port,
            user=connect.user,
            password=connect.password,
        )
        if connect_to_database:
            connection_args["database"] = connect.db_name
        DbConnection.__init__(self, mysql.connector.connect(**connection_args))

        self.db_name = connect.db_name
        self._verbose = verbose

        if read_repeatable:
            self.connection.start_transaction(isolation_level="REPEATABLE READ")

    def cursor(self, name=None, withhold=False):
        return LoggingCursor(self.connection.cursor(), self._verbose)


class DbConnect:
    def __init__(self, db_type: DbType, connection_info: DbConnectInfo, verbose=False):
        if connection_info.password is None:
            connection_info.password = getpass.getpass(
                "Enter password for {0} on host {1}: ".format(
                    connection_info.user_name, connection_info.host
                )
            )

        self.user = connection_info.user_name
        self.password = connection_info.password
        self.host = connection_info.host
        self.port = connection_info.port
        self.db_name = connection_info.db_name
        self.ssl_mode = connection_info.ssl_mode
        self.__db_type = db_type
        self._verbose = verbose

    def get_db_connection(
        self, read_repeatable=False
    ) -> PsqlConnection | MySqlConnection:
        if self.__db_type == DbType.POSTGRES:
            return PsqlConnection(self, read_repeatable, self._verbose)
        elif self.__db_type == DbType.MYSQL:
            return MySqlConnection(self, read_repeatable, self._verbose)
        else:
            raise ValueError("unknown db_type " + self.__db_type)

    def get_server_connection(self) -> MySqlConnection:
        if self.__db_type != DbType.MYSQL:
            raise ValueError("server-level connections are only supported for MySQL")
        return MySqlConnection(self, False, self._verbose, connect_to_database=False)
