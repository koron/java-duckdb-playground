## How to reproduce

1.  Generate test data

    ```console
    $ mkdir tmp

    $ go run ../utils/gen_simple_csv.go > tmp/simple.csv
    $ go run ../utils/gen_group_csv.go > tmp/group.csv

    $ go run ../utils/gen_simple_pq.go
    $ go run ../utils/gen_group_pq.go
    $ mv *.parquet tmp/
    ```

2.  Modify path to test data

    Change `DuckDB_04_Import.DIR` to fit your env.

3.  Run benchmark

    ```console
    $ ./gradlew jmh
    ```

4.  Check resultS

    Check `build/reports/jmh/results.txt`
