from os import environ
from unittest import TestCase

import psycopg
from psycopg.rows import dict_row


class PostgresqlLockTest(TestCase):
    """
    https://qiita.com/behiron/items/571562ea33b8212a4c32
    """

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

    def test_lock(self):
        self.setup_tables(
            """
        drop table if exists users;
        create table users
        (
            id  integer constraint users_pkey primary key,
            user_type   integer
        );
        INSERT INTO users (id, user_type) VALUES (1,1);
        INSERT INTO users (id, user_type) VALUES (2,1);
        INSERT INTO users (id, user_type) VALUES (3,1);
        """
        )

        check_lock_query = """
        select
         l.pid,
         l.locktype,
         c.relname as table_name,
         l.page,
         l.tuple,
         l.transactionid,
         l.mode,
         l.granted, -- trueの場合lock取得済み
         s.state
        from
         pg_locks l
         left join pg_class c  on (l.relation=c.oid)
         left join pg_stat_activity s on (l.pid=s.pid)
        where
         l.mode <> 'AccessShareLock' -- レコードが多くなり見辛くなるので対象外
         and
         l.pid <> pg_backend_pid()  -- このクエリ実行自体は対象外
        order by
         l.pid;
        """

        t_a_conn = self.create_connection()
        t_a_cur = t_a_conn.cursor()
        t_a_cur.execute("SELECT pg_backend_pid()")
        t_a_pid = t_a_cur.fetchone()[0]

        t_b_conn = self.create_connection()
        t_b_cur = t_b_conn.cursor()
        t_b_cur.execute("SELECT pg_backend_pid()")
        t_b_pid = t_b_cur.fetchone()[0]

        t_c_conn = self.create_connection()
        t_c_cur = t_c_conn.cursor()
        t_c_cur.execute("SELECT pg_backend_pid()")
        t_c_pid = t_c_cur.fetchone()[0]

        t_check_conn = self.create_connection()
        t_check_cur = t_check_conn.cursor(row_factory=dict_row)

        # それぞれBEGINだけした様子
        t_a_cur.execute("BEGIN")
        t_b_cur.execute("BEGIN")
        t_c_cur.execute("BEGIN")
        t_check_cur.execute(check_lock_query)
        actual = t_check_cur.fetchall()

        self.assertEqual(len(actual), 3)
        self.assertEqual(actual[0]["pid"], t_a_pid)
        self.assertEqual(actual[1]["pid"], t_b_pid)
        self.assertEqual(actual[2]["pid"], t_c_pid)
        for row in actual:
            self.assertEqual(row["mode"], "ExclusiveLock")
            self.assertEqual(row["state"], "idle in transaction")
