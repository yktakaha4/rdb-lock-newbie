from os import environ
from unittest import TestCase

from mysql import connector


class MySqlLockTest(TestCase):
    """
    https://blog.tiqwab.com/2018/06/10/innodb-locking.html
    """

    def create_connection(self):
        conn = connector.connect(
            host=environ["MYSQL_HOST"],
            port=environ["MYSQL_PORT"],
            user=environ["MYSQL_USER"],
            password=environ["MYSQL_PASSWORD"],
            database=environ["MYSQL_DATABASE"],
        )
        conn.autocommit = False
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
            conn.close()

    def test_consistent_read_and_locking_read(self):
        """
        consistent read と locking read

        トランザクション中の読み取り操作は大別すると 2 種類あります。
        - https://dev.mysql.com/doc/refman/5.7/en/innodb-consistent-read.html
        - https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html

        consistent read
        - トランザクション内での (locking read ではない) 読み取りは、そのトランザクション中はじめに read した時点のスナップショットを参照するような挙動になる
        - スナップショットは全テーブル対象 (クエリしたテーブルのみではない)
        - スナップショットの仕組みは undo ログに基づくので lock は取得しない

        locking read
        - shared lock をとるクエリと exclusive lock をとるクエリがある
        - locking read したレコードに関してはスナップショットではなくコミット済みの最新の値が取得される
        """

        self.setup_tables(
            """
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

        DROP TABLE IF EXISTS `another_sample`;
        CREATE TABLE `another_sample` (
            `id` bigint(20) NOT NULL,
            `val1` int(11) NOT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        INSERT INTO `another_sample`
            (`id`, `val1`)
        VALUES
            (1, 1),
            (2, 2),
            (3, 3);
        """
        )

        t_a_conn = self.create_connection()
        t_a_cur = t_a_conn.cursor()

        t_b_conn = self.create_connection()
        t_b_cur = t_b_conn.cursor()

        t_a_cur.execute("BEGIN")

        # TB がレコード更新
        t_b_cur.execute("BEGIN")
        t_b_cur.execute("UPDATE lock_sample SET val1 = 3 WHERE id = 2")
        t_b_cur.execute("COMMIT")

        # TA がトランザクション初 read。
        # スナップショットはトランザクション開始時ではなくこのタイミングに基づく。
        # なので TB で commit したものが読める。
        t_a_cur.execute("SELECT * FROM lock_sample")
        actual = t_a_cur.fetchall()
        self.assertListEqual(
            actual,
            [
                (1, 1),
                (2, 3),
                (3, 10),
                (4, 10),
                (5, 4),
                (6, 10),
            ],
        )

        # 次の TB による変更は TA からは読まれない。
        t_b_cur.execute("UPDATE lock_sample SET val1 = 6 WHERE id = 2")
        t_b_cur.execute("UPDATE another_sample SET val1 = 10 WHERE id = 1")
        t_b_cur.execute("COMMIT")

        t_a_cur.execute("SELECT * FROM lock_sample")
        actual = t_a_cur.fetchall()
        self.assertListEqual(
            actual,
            [
                (1, 1),
                (2, 3),
                (3, 10),
                (4, 10),
                (5, 4),
                (6, 10),
            ],
        )

        t_a_cur.execute("SELECT * FROM another_sample")
        actual = t_a_cur.fetchall()
        self.assertListEqual(
            actual,
            [
                (1, 1),
                (2, 2),
                (3, 3),
            ],
        )

        # ただし locking read するとその値が読まれる
        t_a_cur.execute("SELECT * FROM lock_sample WHERE id = 2 FOR UPDATE")
        actual = t_a_cur.fetchall()
        self.assertListEqual(
            actual,
            [
                (2, 6),
            ],
        )
