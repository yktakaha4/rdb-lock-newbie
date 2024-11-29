from os import environ
from unittest import TestCase

import psycopg
from mysql import connector


class MySqlBaseTest(TestCase):
    maxDiff = None

    def create_connection(self, root=False):
        conn = connector.connect(
            host=environ["MYSQL_HOST"],
            port=environ["MYSQL_PORT"],
            user=environ["MYSQL_USER"] if not root else "root",
            password=(
                environ["MYSQL_PASSWORD"]
                if not root
                else environ["MYSQL_ROOT_PASSWORD"]
            ),
            database=environ["MYSQL_DATABASE"],
        )
        conn.autocommit = False
        self._connections.append(conn)
        return conn

    def setup_tables(self, query):
        conn = self.create_connection(root=True)
        with conn.cursor() as cur:
            for q in query.split(";"):
                cleaned = q.strip()
                if not cleaned:
                    continue
                cur.execute(cleaned)
        conn.commit()

    def setUp(self):
        self._connections = []

    def tearDown(self):
        for conn in self._connections:
            if conn.is_connected():
                conn.rollback()
                conn.close()


class PostgresqlBaseTest(TestCase):
    maxDiff = None

    def create_connection(self):
        params = {
            "user": environ["POSTGRES_USER"],
            "password": environ["POSTGRES_PASSWORD"],
            "host": environ["POSTGRES_HOST"],
            "port": environ["POSTGRES_PORT"],
            "dbname": environ["POSTGRES_DB"],
        }
        conn = psycopg.connect(
            conninfo=" ".join([f"{k}={v}" for k, v in params.items()]),
            autocommit=False,
        )
        self._connections.append(conn)
        return conn

    def setup_tables(self, query):
        conn = self.create_connection()
        with conn.cursor() as cur:
            for q in query.split(";"):
                cleaned = q.strip()
                if not cleaned:
                    continue
                cur.execute(cleaned)
        conn.commit()

    def setUp(self):
        self._connections = []

    def tearDown(self):
        for conn in self._connections:
            conn.rollback()
            conn.close()
