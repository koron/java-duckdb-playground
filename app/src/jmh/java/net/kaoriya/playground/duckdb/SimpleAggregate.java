package net.kaoriya.playground.duckdb;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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
public class SimpleAggregate {

    Connection conn;
    Statement stmt;

    @Setup
    public void openDB() throws Exception {
        conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
        stmt = conn.createStatement();
        stmt.execute("CREATE TABLE integers AS SELECT i % 5 AS i FROM range(0, 10000000) tbl(i);");
    }

    @TearDown
    public void closeDB() throws Exception {
        stmt.close();
        stmt = null;
        conn.close();
        conn = null;
    }

    @Benchmark
    public void aggregation() throws Exception {
        try (var rs = stmt.executeQuery("SELECT SUM(i) FROM integers")) {
            while (rs.next()) {
                if (rs.getInt(1) != 20000000) {
                    throw new RuntimeException("unexpected result");
                }
            }
        }
    }
}
