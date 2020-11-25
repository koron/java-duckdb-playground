package net.kaoriya.playground.duckdb;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

public class DuckDB_01_OpenDatabase {

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
    public void DuckDB() throws Exception {
        try(var conn = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {}
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
    public void SQLite3() throws Exception {
        try(var conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")) {}
    }
}
