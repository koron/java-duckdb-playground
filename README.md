## DuckDB's pit fall (0.2.2)

*   毎回 `conn.setAutoCommit(false)` しないといけない
    `commit` or `rollback` するとauto commitフラグが落ちてるらしい
*   PreparedStatemnt のバッチモードには未対応
    通常のStatementは不明
