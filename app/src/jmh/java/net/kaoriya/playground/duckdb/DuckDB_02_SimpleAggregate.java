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

public class DuckDB_02_SimpleAggregate {

    @State(Scope.Benchmark)
    public static class DuckDB extends Database.DuckDB {
        @Setup
        public void init() throws Exception {
            stmt.execute("CREATE TABLE integers AS SELECT i % 5 AS i FROM range(0, 10000000) tbl(i);");
            conn.commit();
        }
    }

    @State(Scope.Benchmark)
    public static class SQLite3 extends Database.SQLite3 {
        @Setup
        public void init() throws Exception {
            stmt.execute("CREATE TABLE integers (i int)");
            try (var p = conn.prepareStatement("INSERT INTO integers VALUES(?)")) {
                for (var i = 0; i < 10000000; i++) {
                    p.setInt(1, i % 5);
                    p.execute();
                }
            }
            conn.commit();
        }
    }

    private static void runBenchmark(Database db) throws Exception {
        try (var rs = db.stmt.executeQuery("SELECT SUM(i) FROM integers")) {
            while (rs.next()) {
                if (rs.getInt(1) != 20000000) {
                    throw new RuntimeException("unexpected result");
                }
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 10)
    public void DuckDB(DuckDB db) throws Exception {
        runBenchmark(db);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 5)
    public void SQLite3(SQLite3 db) throws Exception {
        runBenchmark(db);
    }
}
