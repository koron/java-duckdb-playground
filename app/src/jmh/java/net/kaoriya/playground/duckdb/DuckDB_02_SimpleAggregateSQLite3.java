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
public class DuckDB_02_SimpleAggregateSQLite3 {

    Connection conn;
    Statement stmt;

    @Setup
    public void openDB() throws Exception {
        conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:");
        conn.setAutoCommit(false);
        stmt = conn.createStatement();
        stmt.execute("CREATE TABLE integers (i int)");
        try (var p = conn.prepareStatement("INSERT INTO integers VALUES(?)")) {
            for (var i = 0; i < 10000000; i++) {
                p.setInt(1, i % 5);
                p.execute();
            }
        }
        conn.commit();
    }

    @TearDown
    public void closeDB() throws Exception {
        stmt.close();
        stmt = null;
        conn.close();
        conn = null;
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 10)
    public void aggregation() throws Exception {
        try (var rs = stmt.executeQuery("SELECT SUM(i) FROM integers")) {
            while (rs.next()) {
                var sum = rs.getInt(1);
                if (sum != 20000000) {
                    throw new RuntimeException(String.format("unexpected result: actual=%d", sum));
                }
            }
        }
    }
}
