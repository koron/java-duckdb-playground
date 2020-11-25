package net.kaoriya.playground.duckdb;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.sql.Connection;
import java.sql.Statement;

@State(Scope.Benchmark)
public class Database {
    public final String url;
    public Connection conn;
    public Statement stmt;

    protected Database(String url) {
        this.url = url;
    }

    @Setup
    public void open() throws Exception{
        conn = java.sql.DriverManager.getConnection(url);
        conn.setAutoCommit(false);
        stmt = conn.createStatement();
    }

    @TearDown
    public void close() throws Exception {
        stmt.close();
        stmt = null;
        conn.close();
        conn = null;
    }

    public static class DuckDB extends Database {
        public DuckDB() {
            super("jdbc:duckdb:");
        }
    }

    public static class SQLite3 extends Database {
        public SQLite3() {
            super("jdbc:sqlite::memory:");
        }
    }
}
