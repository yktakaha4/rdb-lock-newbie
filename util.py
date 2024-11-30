from os import environ
from unittest import IsolatedAsyncioTestCase, TestCase

import psycopg
from mysql import connector
from mysql.connector import aio as aio_connector
from tabulate import tabulate


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
        conn.cmd_query("SET max_execution_time = 5000")
        conn.cmd_query("SET innodb_lock_wait_timeout = 5")
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
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def assertTableEqual(self, expected, actual):
        self.assertEqual(type(expected), str)
        self.assertEqual(type(actual), list)

        expected_table = expected.strip()
        actual_table = tabulate(
            actual, headers="keys", tablefmt="psql", stralign="left"
        )
        if expected_table != actual_table:
            self.fail(f"Expected:\n{expected_table}\n\nActual:\n{actual_table}")


class MySqlAsyncBaseTest(IsolatedAsyncioTestCase):
    maxDiff = None

    async def create_connection(self, root=False):
        conn = await aio_connector.connect(
            host=environ["MYSQL_HOST"],
            port=int(environ["MYSQL_PORT"]),
            user=environ["MYSQL_USER"] if not root else "root",
            password=(
                environ["MYSQL_PASSWORD"]
                if not root
                else environ["MYSQL_ROOT_PASSWORD"]
            ),
            database=environ["MYSQL_DATABASE"],
        )
        await conn.set_autocommit(False)
        self._connections.append(conn)
        return conn

    async def setup_tables(self, query):
        conn = await self.create_connection(root=True)
        cur = await conn.cursor()
        for q in query.split(";"):
            cleaned = q.strip()
            if not cleaned:
                continue
            await cur.execute(cleaned)

        await conn.commit()

    async def asyncSetUp(self):
        self._connections = []

    async def asyncTearDown(self):
        for conn in self._connections:
            try:
                await conn.close()
            except Exception:
                pass

    def assertTableEqual(self, expected, actual):
        self.assertEqual(type(expected), str)
        self.assertEqual(type(actual), list)

        expected_table = expected.strip()
        actual_table = tabulate(
            actual, headers="keys", tablefmt="psql", stralign="left"
        )
        if expected_table != actual_table:
            self.fail(f"Expected:\n{expected_table}\n\nActual:\n{actual_table}")


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
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def assertTableEqual(self, expected, actual):
        self.assertEqual(type(expected), str)
        self.assertEqual(type(actual), list)

        expected_table = expected.strip()
        actual_table = tabulate(
            actual, headers="keys", tablefmt="psql", stralign="left"
        )
        if expected_table != actual_table:
            self.fail(f"Expected:\n{expected_table}\n\nActual:\n{actual_table}")
