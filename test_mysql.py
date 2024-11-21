from os import environ
from unittest import TestCase

from mysql import connector


class MySQLTest(TestCase):
    def create_connection(self):
        conn = connector.connect(
            host=environ["MYSQL_HOST"],
            port=environ["MYSQL_PORT"],
            user=environ["MYSQL_USER"],
            password=environ["MYSQL_PASSWORD"],
            database=environ["MYSQL_DATABASE"],
        )
        self._connections.append(conn)
        return conn

    def setUp(self):
        self._connections = []

        initial_query = """
        DROP TABLE IF EXISTS `lock_sample`;
        CREATE TABLE `lock_sample` (
            `id` bigint(20) NOT NULL,
            `val1` int(11) NOT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        INSERT INTO `lock_sample`
            (`id`, `val1`)
        VALUES
            (1, 1),
            (2, 2),
            (3, 10),
            (4, 10),
            (5, 4),
            (6, 10);

        DROP TABLE IF EXISTS `another_table`;
        CREATE TABLE `another_table` (
            `id` bigint(20) NOT NULL,
            `val1` int(11) NOT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        INSERT INTO `another_table`
            (`id`, `val1`)
        VALUES
            (1, 1),
            (2, 2),
            (3, 3);
        """

        conn = self.create_connection()
        with conn.cursor() as cur:
            for query in [q.strip() for q in initial_query.split(";") if q.strip()]:
                cur.execute(query)
        conn.commit()

    def tearDown(self):
        for conn in self._connections:
            conn.close()

    def test_connection(self):
        conn = self.create_connection()

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM lock_sample")
            actual = cur.fetchone()[0]

        self.assertEqual(actual, 6)
