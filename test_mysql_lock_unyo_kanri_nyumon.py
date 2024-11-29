import threading

from tabulate import tabulate

from util import MySqlBaseTest


class MySqlLockUnyoKanriNyumonTest(MySqlBaseTest):
    """
    https://gihyo.jp/book/2024/978-4-297-14184-4
    """

    def test_innodb_layer_lock(self):
        """
        InnoDB によるロック
        """

        self.setup_tables(
            """
        DROP TABLE IF EXISTS `t1`;
        CREATE TABLE `t1` (
          `num` int NOT NULL,
          `val` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
          `val_length` int unsigned NOT NULL,
          PRIMARY KEY (`num`),
          KEY `idx_vallength` (`val_length`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        INSERT INTO `t1`
            (`num`, `val`, `val_length`)
        VALUES
            (1, 'one', 3),
            (2, 'two', 3),
            (3, 'three', 5),
            (5, 'five', 4);
        """
        )

        conn1 = self.create_connection()
        conn2 = self.create_connection()
        conn3 = self.create_connection()
        conn_chk = self.create_connection(root=True)

        cur1 = conn1.cursor(dictionary=True)
        cur2 = conn2.cursor(dictionary=True)
        cur3 = conn3.cursor(dictionary=True)
        cur_chk = conn_chk.cursor(dictionary=True)

        cur1.execute("BEGIN")
        cur2.execute("BEGIN")
        cur3.execute("BEGIN")

        """
        デフォルトのトランザクションレベルは REPEATABLE-READ
        """
        cur_chk.execute("SHOW VARIABLES LIKE '%isolation%'")
        self.assertEqual(cur_chk.fetchone()["Value"], "REPEATABLE-READ")

        """
        セカンダリインデックスを使った場合のロックは、セカンダリインデックスに対するロックと行そのものであるクラスタインデックスに対するロックを両方保持します。
        """
        cur1.execute("SELECT * FROM t1 WHERE val_length = 3 FOR UPDATE")

        check_lock_query = """
         select
            OBJECT_NAME,
            INDEX_NAME,
            LOCK_TYPE,
            LOCK_MODE,
            LOCK_STATUS,
            LOCK_DATA
        from
            performance_schema.data_locks
        order by 1, 2, 3, 4, 5, 6;
        """

        cur_chk.execute(check_lock_query)
        actual = cur_chk.fetchall()

        """
        idx_vallengthのval_length=3のレコードと、それに対応するnum = 1、num = 2のクラスタインデックスがロックの対象
        """
        self.assertTableEqual(
            """
+---------------+---------------+-------------+---------------+---------------+-------------+
| OBJECT_NAME   | INDEX_NAME    | LOCK_TYPE   | LOCK_MODE     | LOCK_STATUS   | LOCK_DATA   |
|---------------+---------------+-------------+---------------+---------------+-------------|
| t1            |               | TABLE       | IX            | GRANTED       |             |
| t1            | idx_vallength | RECORD      | X             | GRANTED       | 3, 1        |
| t1            | idx_vallength | RECORD      | X             | GRANTED       | 3, 2        |
| t1            | idx_vallength | RECORD      | X,GAP         | GRANTED       | 4, 5        |
| t1            | PRIMARY       | RECORD      | X,REC_NOT_GAP | GRANTED       | 1           |
| t1            | PRIMARY       | RECORD      | X,REC_NOT_GAP | GRANTED       | 2           |
+---------------+---------------+-------------+---------------+---------------+-------------+
""",
            actual,
        )

        """
        val_lengthのinfimum（無限小）と3の間のギャップがロックされているため、
        このロックが解放されるまでの間はval_lengthが0、1、2になるような（データ型がint unsignedなので負の値はありませんが、signedならば負の値も含まれます）INSERT、UPDATEはブロックされます
        """
        thread2 = threading.Thread(
            target=cur2.execute,
            args=("INSERT INTO t1 (num, val, val_length) values (10, 'ju', 2)",),
        )
        thread2.start()

        check_lock_waits_query = """
        select
            locked_table_name,
            locked_index,
            locked_type,
            waiting_query,
            waiting_lock_mode,
            blocking_query,
            blocking_lock_mode
        from
            sys.innodb_lock_waits
        order by 1, 2, 3, 4, 5, 6, 7;
        """

        cur_chk.execute(check_lock_waits_query)
        actual = cur_chk.fetchall()

        self.assertTableEqual(
            """
+---------------------+----------------+---------------+------------------------------------------------------------+------------------------+------------------+----------------------+
| locked_table_name   | locked_index   | locked_type   | waiting_query                                              | waiting_lock_mode      | blocking_query   | blocking_lock_mode   |
|---------------------+----------------+---------------+------------------------------------------------------------+------------------------+------------------+----------------------|
| t1                  | idx_vallength  | RECORD        | INSERT INTO t1 (num, val, val_length) values (10, 'ju', 2) | X,GAP,INSERT_INTENTION |                  | X                    |
+---------------------+----------------+---------------+------------------------------------------------------------+------------------------+------------------+----------------------+
""",
            actual,
        )

        """
        REPEATABLE-READのロック指定なしSELECTはロックを必要としません。
        そのため、ロック範囲に含まれるWHERE val_length = 3やWHERE val_length = 1（行は存在しないが）およびWHERE num = 1もロック待ちになることはありません。
        """
        cur3.execute("SELECT * FROM t1 WHERE val_length = 3")
        self.assertEqual(len(cur3.fetchall()), 2)

        cur3.execute("SELECT * FROM t1 WHERE val_length = 1")
        self.assertEqual(len(cur3.fetchall()), 0)

        cur3.execute("SELECT * FROM t1 WHERE num = 1")
        self.assertEqual(len(cur3.fetchall()), 1)
