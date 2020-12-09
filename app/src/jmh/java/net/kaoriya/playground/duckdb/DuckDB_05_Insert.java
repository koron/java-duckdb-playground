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

public class DuckDB_05_Insert {

    @State(Scope.Benchmark)
    public static class DuckDB extends Database.DuckDB {
        java.sql.PreparedStatement insert;
        int count;
        @Setup
        public void init() throws Exception {
            stmt.execute("CREATE TABLE integers (i int, j int)");
            conn.commit();
            insert = conn.prepareStatement("INSERT INTO integers VALUES(?, ?)");
            count = 0;
        }
        @TearDown
        public void close1() throws Exception {
            insert.close();
            insert = null;
        }
    }

    @State(Scope.Benchmark)
    public static class SQLite3 extends Database.SQLite3 {
        java.sql.PreparedStatement insert;
        int count;
        @Setup
        public void init() throws Exception {
            stmt.execute("CREATE TABLE integers (i int, j int)");
            conn.commit();
            insert = conn.prepareStatement("INSERT INTO integers VALUES(?, ?)");
            count = 0;
        }
        @TearDown
        public void close1() throws Exception {
            insert.close();
            insert = null;
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
    public void Separately_DuckDB(DuckDB db) throws Exception {
        db.conn.setAutoCommit(false);
        db.insert.setInt(1, db.count % 5);
        db.insert.setInt(2, db.count % 100);
        db.count++;
        db.insert.execute();
        db.conn.commit();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
    public void Separately_SQLite3(SQLite3 db) throws Exception {
        db.conn.setAutoCommit(false);
        db.insert.setInt(1, db.count % 5);
        db.insert.setInt(2, db.count % 100);
        db.count++;
        db.insert.execute();
        db.conn.commit();
    }

    /*
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1)
    public void Batch_DuckDB(DuckDB db) throws Exception {
        db.conn.setAutoCommit(false);
        try (var p = db.conn.prepareStatement("INSERT INTO integers VALUES(?, ?)")) {
            for (var i = 0; i < 1000000; i++) {
                p.setInt(1, i % 5);
                p.setInt(2, i % 100);
                p.addBatch();
            }
            p.executeBatch();
        }
        db.conn.commit();
    }
    */

    /*
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1)
    public void Batch_SQLite3(SQLite3 db) throws Exception {
        db.conn.setAutoCommit(false);
        try (var p = db.conn.prepareStatement("INSERT INTO integers VALUES(?, ?)")) {
            for (var i = 0; i < 1000000; i++) {
                p.setInt(1, i % 5);
                p.setInt(2, i % 100);
                p.addBatch();
            }
            p.executeBatch();
        }
        db.conn.commit();
    }
    */

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1)
    public void SingleCommit_100K_DuckDB(DuckDB db) throws Exception {
        db.conn.setAutoCommit(false);
        try (var p = db.conn.prepareStatement("INSERT INTO integers VALUES(?, ?)")) {
            for (var i = 0; i < 100000; i++) {
                p.setInt(1, i % 5);
                p.setInt(2, i % 100);
                p.execute();
            }
        }
        db.conn.commit();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 10)
    public void SingleCommit_100K_SQLite3(SQLite3 db) throws Exception {
        db.conn.setAutoCommit(false);
        try (var p = db.conn.prepareStatement("INSERT INTO integers VALUES(?, ?)")) {
            for (var i = 0; i < 100000; i++) {
                p.setInt(1, i % 5);
                p.setInt(2, i % 100);
                p.execute();
            }
        }
        db.conn.commit();
    }

}
