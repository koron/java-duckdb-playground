package net.kaoriya.playground.duckdb;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

public class OpenBenchmark {
    @Benchmark
    public void openDuckDB() throws Exception {
        try(var conn = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {}
    }

    @Benchmark
    public void openSQLite3() throws Exception {
        try(var conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")) {}
    }
}
