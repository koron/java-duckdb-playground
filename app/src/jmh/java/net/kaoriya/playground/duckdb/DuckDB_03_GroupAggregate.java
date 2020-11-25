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

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class DuckDB_03_GroupAggregate {

    Connection conn;
    Statement stmt;

    @Setup
    public void openDB() throws Exception {
        conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
        stmt = conn.createStatement();
        stmt.execute("CREATE TABLE integers AS SELECT i % 5 AS i, i % 100 AS j FROM range(0, 10000000) tbl(i);");
    }

    @TearDown
    public void closeDB() throws Exception {
        stmt.close();
        stmt = null;
        conn.close();
        conn = null;
    }

    final static int[] expecteds = new int[]{95000000, 97000000, 99000000, 101000000, 103000000};

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 10)
    public void aggregation() throws Exception {
        try (var rs = stmt.executeQuery("SELECT i, SUM(j) FROM integers GROUP BY i ORDER BY i")) {
            while (rs.next()) {
                if (expecteds[rs.getInt(1)] != rs.getInt(2)) {
                    throw new RuntimeException("unexpected result");
                }
            }
        }
    }
}
