import re
import threading

from psycopg.rows import dict_row

from util import PostgresqlBaseTest


class PostgresqlInsightTest(PostgresqlBaseTest):
    """
    https://www.insight-ltd.co.jp/tech_blog/postgresql/pg-knowledge/750/
    """

    def test_log_lock_waits(self):
        """
        ロックしているプロセスのpidを特定する方法
        """
        self.setup_tables(
            """
        drop table if exists test_table_a;
        create table test_table_a
        (
            i  integer primary key,
            val varchar(255)
        );
        insert into test_table_a values (1, 'a');
        """
        )

        t_a_conn = self.create_connection()
        t_a_cur = t_a_conn.cursor()
        t_a_cur.execute("SELECT pg_backend_pid()")
        p_a = str(t_a_cur.fetchone()[0]).rjust(5)

        t_b_conn = self.create_connection()
        t_b_cur = t_b_conn.cursor()
        t_b_cur.execute("SELECT pg_backend_pid()")
        p_b = str(t_b_cur.fetchone()[0]).rjust(5)

        t_check_conn = self.create_connection()
        t_check_cur = t_check_conn.cursor(row_factory=dict_row)

        t_a_cur.execute("BEGIN")
        t_b_cur.execute("BEGIN")

        t_a_cur.execute("SELECT 'lock from a' FROM test_table_a WHERE i = 1 FOR UPDATE")

        thread_b = threading.Thread(
            target=lambda: t_b_cur.execute(
                "SELECT 'lock from b' FROM test_table_a WHERE i = 1 FOR UPDATE"
            )
        )
        thread_b.start()
        thread_b.join(timeout=0.5)

        """
        ロックの競合元となっているプロセスの情報の確認
        """
        check_lock_query = """
        select
            waiting.pid as w_pid,
            waiting.query as w_query,
            blocking.pid as b_pid,
            blocking.query as b_query
        from (
            select
                pid,
                unnest(pg_blocking_pids(pid)) as blocking_pid
            from
                pg_stat_activity) as locks
            join
                pg_stat_activity as waiting
            on
                locks.pid = waiting.pid
            join
                pg_stat_activity as blocking
            on
                locks.blocking_pid = blocking.pid
            order by
                waiting.pid,
                blocking.pid;
        """

        t_check_cur.execute(check_lock_query)
        actual = t_check_cur.fetchall()

        self.assertTableEqual(
            f"""
+---------+---------------------------------------------------------------+---------+---------------------------------------------------------------+
|   w_pid | w_query                                                       |   b_pid | b_query                                                       |
|---------+---------------------------------------------------------------+---------+---------------------------------------------------------------|
|   {p_b} | SELECT 'lock from b' FROM test_table_a WHERE i = 1 FOR UPDATE |   {p_a} | SELECT 'lock from a' FROM test_table_a WHERE i = 1 FOR UPDATE |
+---------+---------------------------------------------------------------+---------+---------------------------------------------------------------+
        """,
            actual,
        )

        t_a_cur.execute("ROLLBACK")
        thread_b.join()

        """
        ロック競合の確認方法
        """
        # log_lock_waits を on にする
        t_check_cur.execute("show log_lock_waits")
        actual = t_check_cur.fetchall()
        self.assertEqual(actual[0]["log_lock_waits"], "on")

        with open("/var/log/postgresql/postgresql.log") as f:
            lines = [l.rstrip() for l in f if f"[{p_b.strip()}]" in l]
        self.assertEqual(len(lines), 2)
        self.assertIn("there is already a transaction in progress", lines[0])
        self.assertIn(
            "SELECT 'lock from b' FROM test_table_a WHERE i = 1 FOR UPDATE", lines[1]
        )
