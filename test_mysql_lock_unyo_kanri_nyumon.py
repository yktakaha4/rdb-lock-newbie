import asyncio
import threading

from util import MySqlAsyncBaseTest, MySqlBaseTest


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

        conn_chk = self.create_connection(root=True)
        cur_chk = conn_chk.cursor(dictionary=True)

        """
        デフォルトのトランザクションレベルは REPEATABLE-READ
        """
        cur_chk.execute("SHOW VARIABLES LIKE '%isolation%'")
        self.assertEqual(cur_chk.fetchone()["Value"], "REPEATABLE-READ")

        """
        セカンダリインデックスを使った場合のロックは、セカンダリインデックスに対するロックと行そのものであるクラスタインデックスに対するロックを両方保持します。
        """
        conn1 = self.create_connection()
        cur1 = conn1.cursor()
        cur1.execute("BEGIN")
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
        conn2 = self.create_connection()
        cur2 = conn2.cursor()
        cur2.execute("BEGIN")

        def operation2():
            cur2.execute("INSERT INTO t1 (num, val, val_length) values (10, 'ju', 2)")

        thread2 = threading.Thread(target=operation2)
        thread2.start()
        thread2.join(timeout=0.1)

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
        conn3 = self.create_connection()
        cur3 = conn3.cursor()

        cur3.execute("BEGIN")

        cur3.execute("SELECT * FROM t1 WHERE val_length = 3")
        self.assertEqual(len(cur3.fetchall()), 2)

        cur3.execute("SELECT * FROM t1 WHERE val_length = 1")
        self.assertEqual(len(cur3.fetchall()), 0)

        cur3.execute("SELECT * FROM t1 WHERE num = 1")
        self.assertEqual(len(cur3.fetchall()), 1)

        """
        FOR SHAREやFOR UPDATE付きの場合、分離レベルがSERIALIZABLEのときはDELETEの排他ロックと競合するため待たされることになります。
        """
        conn1.rollback()
        cur1.execute("SELECT * FROM t1 WHERE val_length = 3 FOR UPDATE")

        conn4 = self.create_connection()
        cur4 = conn4.cursor()
        cur4.execute("BEGIN")

        def operation4():
            cur4.execute("SELECT * FROM t1 WHERE val_length = 3 FOR SHARE")

        thread4 = threading.Thread(target=operation4)
        thread4.start()
        thread4.join(timeout=0.1)

        cur_chk.execute(check_lock_waits_query)
        actual = cur_chk.fetchall()

        self.assertTableEqual(
            """
+---------------------+----------------+---------------+-------------------------------------------------+---------------------+------------------+----------------------+
| locked_table_name   | locked_index   | locked_type   | waiting_query                                   | waiting_lock_mode   | blocking_query   | blocking_lock_mode   |
|---------------------+----------------+---------------+-------------------------------------------------+---------------------+------------------+----------------------|
| t1                  | idx_vallength  | RECORD        | SELECT * FROM t1 WHERE val_length = 3 FOR SHARE | S                   |                  | X                    |
+---------------------+----------------+---------------+-------------------------------------------------+---------------------+------------------+----------------------+
""",
            actual,
        )

    def test_observe_lock(self):
        """
        ロックの観測
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

        """
        sys.innodb_lock_waitsビューがあらかじめ用意されているので、多くの場合はこのビューをクエリすることで有用な情報が得られます。
        """
        conn1 = self.create_connection()
        cur1 = conn1.cursor()
        cur1.execute("BEGIN")
        cur1.execute("SELECT * FROM t1 WHERE val_length = 3 FOR UPDATE")

        conn2 = self.create_connection()
        cur2 = conn2.cursor()
        cur2.execute("BEGIN")

        def operation2():
            cur2.execute("SELECT 'operation2 history'")
            cur2.reset()
            cur2.execute("INSERT INTO t1 (num, val, val_length) values (10, 'ju', 2)")

        thread2 = threading.Thread(target=operation2)
        thread2.start()
        thread2.join(timeout=0.1)

        conn_chk = self.create_connection(root=True)
        cur_chk = conn_chk.cursor(dictionary=True)
        check_lock_waits_query = """
        select
            locked_table_schema,
            locked_table_name,
            locked_index,
            waiting_query,
            blocking_query
        from
            sys.innodb_lock_waits
        order by 1, 2, 3, 4, 5;
        """
        cur_chk.execute(check_lock_waits_query)

        actual = cur_chk.fetchall()
        self.assertTableEqual(
            """
+-----------------------+---------------------+----------------+------------------------------------------------------------+------------------+
| locked_table_schema   | locked_table_name   | locked_index   | waiting_query                                              | blocking_query   |
|-----------------------+---------------------+----------------+------------------------------------------------------------+------------------|
| mysql                 | t1                  | idx_vallength  | INSERT INTO t1 (num, val, val_length) values (10, 'ju', 2) |                  |
+-----------------------+---------------------+----------------+------------------------------------------------------------+------------------+
            """,
            actual,
        )

        """
        実際にどのクエリが該当のInnoDBロックを取得したかを特定する方法はありません
        必要に応じてperformance_schema.events_statements_historyテーブルをたどって過去のクエリを探索する必要があります
        """
        check_lock_query = """
        select
            b_t.THREAD_ID as blocking_thread_id,
            w_t.THREAD_ID as waiting_thread_id
        from
            sys.innodb_lock_waits w
        left join
            information_schema.INNODB_TRX as b_trx
        on
            w.blocking_trx_id = b_trx.trx_id
        left join
            performance_schema.threads as b_t
        on
            b_trx.trx_mysql_thread_id = b_t.PROCESSLIST_ID
        left join
            information_schema.INNODB_TRX as w_trx
        on
            w.waiting_trx_id = w_trx.trx_id
        left join
            performance_schema.threads as w_t
        on
            w_trx.trx_mysql_thread_id = w_t.PROCESSLIST_ID
        """

        cur_chk.execute(check_lock_query)
        actual = cur_chk.fetchall()
        blocking_thread_id = actual[0]["blocking_thread_id"]
        waiting_thread_id = actual[0]["waiting_thread_id"]
        self.assertIsNotNone(blocking_thread_id)
        self.assertIsNotNone(waiting_thread_id)

        check_history_query = """
        select
            sql_text
        from
            performance_schema.events_statements_history
        where
            thread_id = %s
        order by event_id desc
        limit 1
        """

        cur_chk.execute(check_history_query, (blocking_thread_id,))
        actual = cur_chk.fetchall()
        self.assertTableEqual(
            """
+--------------------------------------------------+
| sql_text                                         |
|--------------------------------------------------|
| SELECT * FROM t1 WHERE val_length = 3 FOR UPDATE |
+--------------------------------------------------+
        """,
            actual,
        )

        cur_chk.execute(check_history_query, (waiting_thread_id,))
        actual = cur_chk.fetchall()
        self.assertTableEqual(
            """
+-----------------------------+
| sql_text                    |
|-----------------------------|
| SELECT 'operation2 history' |
+-----------------------------+
        """,
            actual,
        )


class MySqlLockUnyoKanriNyumonAsyncTest(MySqlAsyncBaseTest):
    """
    https://gihyo.jp/book/2024/978-4-297-14184-4
    """

    async def test_innodb_layer_lock(self):
        """
        InnoDB によるロック
        """

        await self.setup_tables(
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

        conn_chk = await self.create_connection(root=True)
        cur_chk = await conn_chk.cursor(dictionary=True)

        """
        デフォルトのトランザクションレベルは REPEATABLE-READ
        """
        await cur_chk.execute("SHOW VARIABLES LIKE '%isolation%'")
        actual = await cur_chk.fetchall()
        self.assertEqual(actual[0]["Value"], "REPEATABLE-READ")

        """
        セカンダリインデックスを使った場合のロックは、セカンダリインデックスに対するロックと行そのものであるクラスタインデックスに対するロックを両方保持します。
        """
        conn1 = await self.create_connection()
        cur1 = await conn1.cursor()
        await cur1.execute("BEGIN")
        await cur1.execute("SELECT * FROM t1 WHERE val_length = 3 FOR UPDATE")

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

        await cur_chk.execute(check_lock_query)
        actual = await cur_chk.fetchall()

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
        conn2 = await self.create_connection()
        cur2 = await conn2.cursor()
        await cur2.execute("BEGIN")
        executed = asyncio.create_task(
            cur2.execute("INSERT INTO t1 (num, val, val_length) values (10, 'ju', 2)")
        )
        try:
            await asyncio.wait_for(executed, timeout=0.1)
        except asyncio.TimeoutError:
            pass

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

        await cur_chk.execute(check_lock_waits_query)
        actual = await cur_chk.fetchall()
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
