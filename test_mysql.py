from os import environ
from unittest import TestCase

from mysql import connector


class MySQLTest(TestCase):
    def setUp(self):
        self.connection = create_connection()

    def tearDown(self):
        self.connection.close()

    def test_connection(self):
        self.assertIsNotNone(self.connection)


def create_connection():
    return connector.connect(
        host=environ["MYSQL_HOST"],
        port=environ["MYSQL_PORT"],
        user=environ["MYSQL_USER"],
        password=environ["MYSQL_PASSWORD"],
        database=environ["MYSQL_DATABASE"],
    )
