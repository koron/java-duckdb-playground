package net.kaoriya.playground.duckdb;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.TimeUnit;

public class DuckDB_06_Update {

    @State(Scope.Benchmark)
    public static class DuckDB extends Database.DuckDB {
        @Setup
        public void init() throws Exception {
            stmt.execute("CREATE TABLE integers AS SELECT i % 5 AS i, i % 100 AS j FROM range(0, 10000000) tbl(i);");
            conn.commit();
        }
    }

    @State(Scope.Benchmark)
    public static class SQLite3 extends Database.SQLite3 {
        @Setup
        public void init() throws Exception {
            stmt.execute("CREATE TABLE integers (i int, j int)");
            try (var p = conn.prepareStatement("INSERT INTO integers VALUES(?, ?)")) {
                for (var i = 0; i < 10000000; i++) {
                    p.setInt(1, i % 5);
                    p.setInt(2, i % 100);
                    p.execute();
                }
            }
            conn.commit();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1)
    public void BulkByWhere_01_DuckDB(DuckDB db) throws Exception {
        db.conn.setAutoCommit(false);
        db.stmt.executeUpdate("UPDATE integers SET j = j + 1 WHERE i = 0");
        db.stmt.executeUpdate("UPDATE integers SET j = j + 2 WHERE i = 1");
        db.stmt.executeUpdate("UPDATE integers SET j = j + 3 WHERE i = 2");
        db.stmt.executeUpdate("UPDATE integers SET j = j + 4 WHERE i = 3");
        db.stmt.executeUpdate("UPDATE integers SET j = j + 5 WHERE i = 4");
        db.conn.commit();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1)
    public void BulkByWhere_01_SQLite3(SQLite3 db) throws Exception {
        db.conn.setAutoCommit(false);
        db.stmt.executeUpdate("UPDATE integers SET j = j + 1 WHERE i = 0");
        db.stmt.executeUpdate("UPDATE integers SET j = j + 2 WHERE i = 1");
        db.stmt.executeUpdate("UPDATE integers SET j = j + 3 WHERE i = 2");
        db.stmt.executeUpdate("UPDATE integers SET j = j + 4 WHERE i = 3");
        db.stmt.executeUpdate("UPDATE integers SET j = j + 5 WHERE i = 4");
        db.conn.commit();
    }

}
