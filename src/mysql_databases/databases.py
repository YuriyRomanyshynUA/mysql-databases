import logging
import contextlib
from mysql.connector.connection import MySQLConnection
from mysql.connector import Error as MySqlError


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


__all__ = ["DatabasesError", "Databases", "MySqlDB"]


class DatabasesError(Exception):
    pass


class Databases:

    _instances_ = {}

    @staticmethod
    def init_database(settings=None, name="default"):
        if Databases._instances_.get(name) is None:
            try:
                db = MySqlDB(settings)
                db.init_connection()
                Databases._instances_[name] = {"db": db, "settings": settings}
                return Databases._instances_[name]["db"]
            except MySqlError as error:
                logger.exception(
                    f"[Database: {name}] Failed to connect to database. Errorcode - {error.errno}"
                )
                raise error from None
        else:
            db_dict = Databases._instances_.get(name)
            if db_dict["db"].is_connected():
                raise DatabasesError(f"Database {name} is already init!")
            try:
                db_dict["settings"] = settings
                db_dict["db"] = MySqlDB(settings)
                return db_dict["db"]
            except MySqlError as error:
                logger.exception(
                    f"[Database: {name}] Failed to connect to database. Errorcode - {error.errno}"
                )
                raise error from None

    @staticmethod
    def get_database(name="default"):
        if Databases._instances_.get(name) is None:
            raise DatabasesError("Database connection does not exist")
        else:
            database = Databases._instances_[name]["db"]
            settings = Databases._instances_[name]["settings"]
            if not database.is_connected():
                try:
                    database.reconnect(
                        attempts = settings.get("DB_RECONNECT_ATTEMPTS", 3),
                        delay = settings.get("DB_RECONNECT_DELAY", 10),
                    )
                except MySqlError as error:
                    logger.warning(
                        f"[Database: {name}] Failed to reconnect to database. Errorcode - {error.errno}. "
                    )
                    database = MySqlDB(settings)
                    Databases._instances_[name]["db"] = database
            return database

    @staticmethod
    def close_database(name="default"):
        if Databases._instances_.get(name) is not None:
            logger.info(f"Closing {name} db connection")
            Databases._instances_[name]["db"].close()

    @staticmethod
    def close_all_databases():
        for name, db_dict in Databases._instances_.items():
            logger.info(f"Closing {name} db connection")
            db_dict["db"].close()

    @staticmethod
    def cursor(name="default", **kwargs):
        return Databases.get_database(name).cursor(**kwargs)

    @staticmethod
    def fetchone(query, params=None, name="default", **kwargs):
        try:
            return Databases.get_database(name).fetchone(query, params, **kwargs)
        except MySqlError as error:
            logger.exception(
                f"[Database: {name}] Failed to execute query. Errorcode - {error.errno}. "
                f"Query:\n{query}\n"
                f"Params:\n{str(params)}"
            )
            raise error from None

    @staticmethod
    def fetchall(query, params=None, name="default", **kwargs):
        try:
            return Databases.get_database(name).fetchall(query, params, **kwargs)
        except MySqlError as error:
            logger.exception(
                f"[Database: {name}] Failed to execute query. Errorcode - {error.errno}. "
                f"Query:\n{query}\n"
                f"Params:\n{str(params)}"
            )
            raise error from None

    @staticmethod
    def rollback(name="default"):
        try:
            return Databases.get_database(name).rollback()
        except MySqlError as error:
            logger.exception(
                f"[Database: {name}] Failed to commit changes. Errorcode - {error.errno}."
            )
            raise error from None

    @staticmethod
    def commit(name="default"):
        try:
            return Databases.get_database(name).commit()
        except MySqlError as error:
            logger.exception(
                f"[Database: {name}] Failed to commit changes. Errorcode - {error.errno}."
            )
            raise error from None


class MySqlDB:

    def __init__(self, settings):
        self.settings = dict(**settings)
        self.connection = None

    def init_connection(self):
        if self.connection is None:
            self.connection = MySQLConnection(
                host = self.settings.get('DB_HOST', '127.0.0.1'),
                port = self.settings.get('DB_PORT', 3306),
                user = self.settings.get('DB_USER', 'root'),
                password = self.settings.get('DB_PASSWORD', ''),
                database = self.settings.get('DB_NAME', 'mysql'),
            )

    def is_connected(self):
        if self.connection is None:
            self.init_connection()
        return self.connection.is_connected()

    def reconnect(self, attempts, delay):
        if self.connection is None:
            self.init_connection()
        return self.connection.reconnect()

    def rollback(self):
        if self.connection is None:
            self.init_connection()
        self.connection.rollback()
        
    def commit(self):
        if self.connection is None:
            self.init_connection()
        self.connection.commit()

    @contextlib.contextmanager
    def cursor(self, buffered=True, rollback_on_error=True, **kwargs):
        if self.connection is None:
            self.init_connection()
        cursor = None
        try:
            kwargs = kwargs or {}
            kwargs.setdefault("dictionary", True)
            kwargs.setdefault("buffered", buffered)
            cursor = self.connection.cursor(**kwargs)
            yield cursor
        except MySqlError as error:
            if rollback_on_error:
                self.connection.rollback()
            raise error from None
        finally:
            if cursor:
                cursor.close()

    def fetchone(self, query, params=None, **kwargs):
        if self.connection is None:
            self.init_connection()
        cursor = None
        try:
            kwargs = kwargs or {"dictionary": True}
            kwargs.setdefault("buffered", True)
            cursor = self.connection.cursor(**kwargs)
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result
        finally:
            if cursor:
                cursor.close()

    def fetchall(self, query, params=None, **kwargs):
        if self.connection is None:
            self.init_connection()
        cursor = None
        try:
            kwargs = kwargs or {"dictionary": True}
            cursor = self.connection.cursor(**kwargs)
            cursor.execute(query, params)
            result = cursor.fetchall()
            return result
        finally:
            if cursor:
                cursor.close()

    def close(self):
        if self.connection is not None:
            self.connection.close()
