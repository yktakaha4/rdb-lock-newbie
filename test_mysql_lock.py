import re
from os import environ
from unittest import TestCase

from mysql import connector


class MySqlLockTest(TestCase):
    """
    https://blog.tiqwab.com/2018/06/10/innodb-locking.html
    """

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

    def test_innodb_locking(self):
        """
        InnoDB Locking

        - ロックには粒度が色々ある (e.g. レコード、テーブル)
        - ざっくりとはここで挙げる種類と上の shared or exlusive の組み合わせで普段扱うロックを捉えられるはず
            - 有り得ない組み合わせとかフラグみたいな概念もありそうなのであくまでざっくりと
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
            (3, 3),
            (4, 3),
            (5, 4),
            (8, 8);
        """
        )

        """
        InnoDB標準モニターおよびロックモニターが有効化されていること
        """
        with self.create_connection() as conn:
            cur = conn.cursor()

            cur.execute("show global variables like 'innodb_status_output'")
            self.assertEqual(cur.fetchone()[1], "ON")

            cur.execute("show global variables like 'innodb_status_output_locks'")
            self.assertEqual(cur.fetchone()[1], "ON")

        """
        record lock

        - index record へのロック
        - record をロックするという場合、実際はインデックス上のレコードをロックしている
            - インデックスが定義されていないテーブルでも内部で作成したインデックスを使用する
        """
        with self.create_connection(root=True) as conn:
            cur = conn.cursor(buffered=True)

            # RECORD LOCKS や locks rec but not gap が書かれている行から、ここでは lock_sample テーブルの PRIMARY インデックス 上には record exclusive lock を取得していることがわかります。
            cur.execute("SELECT * FROM lock_sample WHERE id = 2 FOR UPDATE")
            cur.execute("SHOW ENGINE INNODB STATUS")
            _, _, status = cur.fetchall()[0]
            self.assertRegex(
                status,
                re.compile(
                    r"^RECORD LOCKS space id \d+ page no 4 n bits 80 index PRIMARY of table `mysql`\.`lock_sample` trx id \d+ lock_mode X locks rec but not gap$",
                    re.MULTILINE,
                ),
            )

        """
        gap lock

        - index records 間のスペースに対するロック
        - ファントムリードの防止
            - なので (MySQL の) REPEATABLE READ では必要だが READ COMMITED では発生しない
        """
        with self.create_connection(root=True) as conn:
            cur = conn.cursor(buffered=True)

            cur.execute("SELECT * FROM lock_sample")
            actual = cur.fetchall()
            # id = 6 は存在しない
            self.assertListEqual(
                actual,
                [
                    (1, 1),
                    (2, 2),
                    (3, 3),
                    (4, 3),
                    (5, 4),
                    (8, 8),
                ],
            )

            # SHOW ENGINE INNODB STATUS では RECORD LOCKS ... locks gap before rec と表示されます。
            cur.execute("SELECT * FROM lock_sample WHERE id = 6 FOR UPDATE")
            cur.execute("SHOW ENGINE INNODB STATUS")
            _, _, status = cur.fetchall()[0]
            self.assertRegex(
                status,
                re.compile(
                    r"^RECORD LOCKS space id \d+ page no 4 n bits 80 index PRIMARY of table `mysql`\.`lock_sample` trx id \d+ lock_mode X locks gap before rec$",
                    re.MULTILINE,
                ),
            )

        """
        next-key lock

        - record lock と gap lock の組み合わせ
        """
        with self.create_connection(root=True) as conn:
            cur = conn.cursor(buffered=True)

            cur.execute("SELECT * FROM lock_sample")
            actual = cur.fetchall()
            # id = 6 は存在しない
            self.assertListEqual(
                actual,
                [
                    (1, 1),
                    (2, 2),
                    (3, 3),
                    (4, 3),
                    (5, 4),
                    (8, 8),
                ],
            )

            # RECORD LOCKS の行で lock_mode X で終わっているのが record lock, gap lock との違いになります。
            # FIXME: 例で説明されていた WHERE id BETWEEN 6 AND 7 ではギャップロックになった
            cur.execute("SELECT * FROM lock_sample WHERE id >= 6 FOR UPDATE")
            cur.execute("SHOW ENGINE INNODB STATUS")
            _, _, status = cur.fetchall()[0]
            self.assertRegex(
                status,
                re.compile(
                    r"^RECORD LOCKS space id \d+ page no 4 n bits 80 index PRIMARY of table `mysql`\.`lock_sample` trx id \d+ lock_mode X$",
                    re.MULTILINE,
                ),
            )

    def test_dml_exclusive_lock(self):
        """
        DML 文は暗黙的に exclusive lock を取る
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
            (3, 3),
            (4, 3),
            (5, 4),
            (8, 8);
        """
        )

        with self.create_connection(root=True) as conn:
            cur = conn.cursor(buffered=True)

            cur.execute("UPDATE lock_sample SET val1 = 10 WHERE id = 2")
            cur.execute("SHOW ENGINE INNODB STATUS")
            _, _, status = cur.fetchall()[0]
            self.assertRegex(
                status,
                re.compile(
                    r"^RECORD LOCKS space id \d+ page no 4 n bits 80 index PRIMARY of table `mysql`\.`lock_sample` trx id \d+ lock_mode X locks rec but not gap$",
                    re.MULTILINE,
                ),
            )

    def test_lock_using_select(self):
        """
        検索に使用された行がロックの対象になる

        例えば以下のようにインデックスを設定していない列を条件に指定すると、検索はテーブル全体を対象にしないといけないために各レコードと supremum に next-key lock を取得するようです。
        ここで出てくる supremum とは MySQL が内部的に持つ上限値を表すレコードです。このため他トランザクションからはこのテーブルに update, insert が一切行えない状況になります。
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
            (3, 3),
            (4, 3),
            (5, 4),
            (8, 8);
        """
        )

        with self.create_connection(root=True) as conn:
            cur = conn.cursor(buffered=True)

            cur.execute("SELECT * FROM lock_sample WHERE val1 = 2 FOR UPDATE")
            cur.execute("SHOW ENGINE INNODB STATUS")
            _, _, status = cur.fetchall()[0]
            self.assertRegex(
                status,
                re.compile(
                    r"^RECORD LOCKS space id \d+ page no 4 n bits 80 index PRIMARY of table `mysql`\.`lock_sample` trx id \d+ lock_mode X$",
                    re.MULTILINE,
                ),
            )
