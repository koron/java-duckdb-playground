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

import java.net.URL;
import java.util.concurrent.TimeUnit;

public class DuckDB_04_Import {

    @State(Scope.Benchmark)
    public static class DuckDB extends Database.DuckDB {}

    static String datadir() {
        return System.getProperty("project.dir")+ "/tmp/";
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1)
    public void CSV_02_SimpleAggregate(DuckDB db) throws Exception {
        try (var rs = db.stmt.executeQuery("SELECT SUM(i) FROM read_csv_auto('" + datadir() + "simple.csv')")) {
            while (rs.next()) {
                if (rs.getInt(1) != 20000000) {
                    throw new RuntimeException("unexpected result");
                }
            }
        }
    }

    final static int[] expecteds = new int[]{95000000, 97000000, 99000000, 101000000, 103000000};

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1)
    public void CSV_03_GroupAggregate(DuckDB db) throws Exception {
        try (var rs = db.stmt.executeQuery("SELECT i, SUM(j) FROM read_csv_auto('" + datadir() + "group.csv') GROUP BY i ORDER BY i")) {
            while (rs.next()) {
                if (expecteds[rs.getInt(1)] != rs.getInt(2)) {
                    throw new RuntimeException("unexpected result");
                }
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1)
    public void Parquet_02_SimpleAggregate(DuckDB db) throws Exception {
        try (var rs = db.stmt.executeQuery("SELECT SUM(i) FROM parquet_scan('" + datadir() + "simple.parquet')")) {
            while (rs.next()) {
                if (rs.getInt(1) != 20000000) {
                    throw new RuntimeException("unexpected result");
                }
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1)
    public void Parquet_03_GroupAggregate(DuckDB db) throws Exception {
        try (var rs = db.stmt.executeQuery("SELECT i, SUM(j) FROM parquet_scan('" + datadir() + "group.parquet') GROUP BY i ORDER BY i")) {
            while (rs.next()) {
                if (expecteds[rs.getInt(1)] != rs.getInt(2)) {
                    throw new RuntimeException("unexpected result");
                }
            }
        }
    }

}
