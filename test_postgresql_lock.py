import threading
from os import environ
from unittest import TestCase

import psycopg
from psycopg.rows import dict_row
from tabulate import tabulate


class PostgresqlLockTest(TestCase):
    """
    https://qiita.com/behiron/items/571562ea33b8212a4c32
    """

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

    def test_lock_basic_example(self):
        """
        基本的な例

        id=1を3つのセッションがselect for updateすることを想定し、pidが順番にselect for updateしていく。
        """

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
         l.pid,
         l.locktype,
         c.relname,
         l.page,
         l.tuple,
         l.mode,
         l.granted,
         s.state;
        """

        t_a_conn = self.create_connection()
        t_a_cur = t_a_conn.cursor()
        t_a_cur.execute("SELECT pg_backend_pid()")
        p_a = str(t_a_cur.fetchone()[0]).rjust(5)

        t_b_conn = self.create_connection()
        t_b_cur = t_b_conn.cursor()
        t_b_cur.execute("SELECT pg_backend_pid()")
        p_b = str(t_b_cur.fetchone()[0]).rjust(5)

        t_c_conn = self.create_connection()
        t_c_cur = t_c_conn.cursor()
        t_c_cur.execute("SELECT pg_backend_pid()")
        p_c = str(t_c_cur.fetchone()[0]).rjust(5)

        t_check_conn = self.create_connection()
        t_check_cur = t_check_conn.cursor(row_factory=dict_row)

        # それぞれBEGINだけした様子
        t_a_cur.execute("BEGIN")
        t_b_cur.execute("BEGIN")
        t_c_cur.execute("BEGIN")

        t_check_cur.execute(check_lock_query)
        actual = t_check_cur.fetchall()
        actual_table = tabulate(
            actual, headers="keys", tablefmt="psql", stralign="left"
        )

        self.assertEqual(
            actual_table,
            f"""
+-------+------------+--------------+--------+---------+-----------------+---------------+-----------+---------------------+
|   pid | locktype   | table_name   | page   | tuple   | transactionid   | mode          | granted   | state               |
|-------+------------+--------------+--------+---------+-----------------+---------------+-----------+---------------------|
| {p_a} | virtualxid |              |        |         |                 | ExclusiveLock | True      | idle in transaction |
| {p_b} | virtualxid |              |        |         |                 | ExclusiveLock | True      | idle in transaction |
| {p_c} | virtualxid |              |        |         |                 | ExclusiveLock | True      | idle in transaction |
+-------+------------+--------------+--------+---------+-----------------+---------------+-----------+---------------------+
""".strip(),
        )

        # Aがselect for update
        t_a_cur.execute("SELECT * FROM users WHERE id=1 FOR UPDATE")
        t_a_cur.execute("SELECT txid_current()")
        t_a = str(t_a_cur.fetchone()[0]).rjust(5)

        t_check_cur.execute(check_lock_query)
        actual = t_check_cur.fetchall()
        actual_table = tabulate(
            actual, headers="keys", tablefmt="psql", stralign="right"
        )

        self.assertEqual(
            actual_table,
            f"""
+-------+---------------+--------------+--------+---------+-----------------+---------------+-----------+---------------------+
|   pid |      locktype |   table_name |   page |   tuple |   transactionid |          mode |   granted |               state |
|-------+---------------+--------------+--------+---------+-----------------+---------------+-----------+---------------------|
| {p_a} |      relation |        users |        |         |                 |  RowShareLock |      True | idle in transaction |
| {p_a} |      relation |   users_pkey |        |         |                 |  RowShareLock |      True | idle in transaction |
| {p_a} | transactionid |              |        |         |           {t_a} | ExclusiveLock |      True | idle in transaction |
| {p_a} |    virtualxid |              |        |         |                 | ExclusiveLock |      True | idle in transaction |
| {p_b} |    virtualxid |              |        |         |                 | ExclusiveLock |      True | idle in transaction |
| {p_c} |    virtualxid |              |        |         |                 | ExclusiveLock |      True | idle in transaction |
+-------+---------------+--------------+--------+---------+-----------------+---------------+-----------+---------------------+
""".strip(),
        )

        # Bがselect for update
        thread_b = threading.Thread(
            target=t_b_cur.execute, args=("SELECT * FROM users WHERE id=1 FOR UPDATE",)
        )
        thread_b.start()
        thread_b.join(timeout=0.1)

        t_check_cur.execute(check_lock_query)
        actual = t_check_cur.fetchall()
        actual_table = tabulate(
            actual, headers="keys", tablefmt="psql", stralign="right"
        )

        self.assertEqual(
            actual_table,
            f"""
+-------+---------------+--------------+--------+---------+-----------------+---------------------+-----------+---------------------+
|   pid |      locktype |   table_name |   page |   tuple |   transactionid |                mode |   granted |               state |
|-------+---------------+--------------+--------+---------+-----------------+---------------------+-----------+---------------------|
| {p_a} |      relation |        users |        |         |                 |        RowShareLock |      True | idle in transaction |
| {p_a} |      relation |   users_pkey |        |         |                 |        RowShareLock |      True | idle in transaction |
| {p_a} | transactionid |              |        |         |           {t_a} |       ExclusiveLock |      True | idle in transaction |
| {p_a} |    virtualxid |              |        |         |                 |       ExclusiveLock |      True | idle in transaction |
| {p_b} |      relation |        users |        |         |                 |        RowShareLock |      True | idle in transaction |
| {p_b} |      relation |   users_pkey |        |         |                 |        RowShareLock |      True | idle in transaction |
| {p_b} | transactionid |              |        |         |           {t_a} |           ShareLock |     False | idle in transaction |
| {p_b} |         tuple |        users |      0 |       1 |                 | AccessExclusiveLock |      True | idle in transaction |
| {p_b} |    virtualxid |              |        |         |                 |       ExclusiveLock |      True | idle in transaction |
| {p_c} |    virtualxid |              |        |         |                 |       ExclusiveLock |      True | idle in transaction |
+-------+---------------+--------------+--------+---------+-----------------+---------------------+-----------+---------------------+
""".strip(),
        )

        # Cがselect for update
        thread_c = threading.Thread(
            target=t_c_cur.execute, args=("SELECT * FROM users WHERE id=1 FOR UPDATE",)
        )
        thread_c.start()
        thread_c.join(timeout=0.1)

        t_check_cur.execute(check_lock_query)
        actual = t_check_cur.fetchall()
        actual_table = tabulate(
            actual, headers="keys", tablefmt="psql", stralign="right"
        )

        self.assertEqual(
            actual_table,
            f"""
+-------+---------------+--------------+--------+---------+-----------------+---------------------+-----------+---------------------+
|   pid |      locktype |   table_name |   page |   tuple |   transactionid |                mode |   granted |               state |
|-------+---------------+--------------+--------+---------+-----------------+---------------------+-----------+---------------------|
| {p_a} |      relation |        users |        |         |                 |        RowShareLock |      True | idle in transaction |
| {p_a} |      relation |   users_pkey |        |         |                 |        RowShareLock |      True | idle in transaction |
| {p_a} | transactionid |              |        |         |           {t_a} |       ExclusiveLock |      True | idle in transaction |
| {p_a} |    virtualxid |              |        |         |                 |       ExclusiveLock |      True | idle in transaction |
| {p_b} |      relation |        users |        |         |                 |        RowShareLock |      True | idle in transaction |
| {p_b} |      relation |   users_pkey |        |         |                 |        RowShareLock |      True | idle in transaction |
| {p_b} | transactionid |              |        |         |           {t_a} |           ShareLock |     False | idle in transaction |
| {p_b} |         tuple |        users |      0 |       1 |                 | AccessExclusiveLock |      True | idle in transaction |
| {p_b} |    virtualxid |              |        |         |                 |       ExclusiveLock |      True | idle in transaction |
| {p_c} |      relation |        users |        |         |                 |        RowShareLock |      True | idle in transaction |
| {p_c} |      relation |   users_pkey |        |         |                 |        RowShareLock |      True | idle in transaction |
| {p_c} |         tuple |        users |      0 |       1 |                 | AccessExclusiveLock |     False | idle in transaction |
| {p_c} |    virtualxid |              |        |         |                 |       ExclusiveLock |      True | idle in transaction |
+-------+---------------+--------------+--------+---------+-----------------+---------------------+-----------+---------------------+
""".strip(),
        )
