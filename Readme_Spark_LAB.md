# Spark / Iceberg Lab: My Answers

## Environment

**What happens to a running Spark job if the service is restarted mid-lab?**
The job stops. In `local[*]` mode the driver and the executors share one JVM on the pod, so a restart kills everything and I have to recreate the session. The STS credentials are also temporary: if the lab runs too long they expire and S3 returns a 403 error, even without a restart.

**Why does `SimpleAWSCredentialsProvider` fail here?**
It only uses the access key and the secret key. STS credentials also need the session token, otherwise S3 rejects the request. `TemporaryAWSCredentialsProvider` sends all three.

**Problem I ran into: version mismatch.**
`CREATE NAMESPACE` first failed with `IncompatibleClassChangeError`. My environment ran Spark 4.3.0, but the Iceberg jar I picked (`iceberg-spark-runtime-4.1_2.13`) is built for Spark 4.1. To fix it, I created a venv with `pyspark==4.1.*` (Spark 4.1.4) in `/tmp/spark41`. That pyspark does not ship `hadoop-aws`, so I added `hadoop-aws:3.4.2` (same version as the bundled `hadoop-client`) and the AWS SDK bundle through `spark.jars.packages`. I also ran `unset SPARK_HOME PYTHONPATH` so it would not pick up the Spark 4.3 in `/opt/spark`. After that, everything worked.

## Bronze

The schema was inferred correctly (`birthdate` is a `date`). The address has a `\n` between the street and the city line, which is why I split it into two lines.

## Silver

**Explain the output of the state query.**
It counts users per state and how many have no city. For `AE` (3 users) and `AP` (2 users), `missing_city` equals the number of users: these are military addresses (APO/DPO) with no city. `FM` and `VI` are territories, not states.

**DataFrame vs SQL: which one for what?**
Both give the same Catalyst plan and the same result: my `assert` checks on counts and columns passed. For a quick one-off exploration I would use SQL, because it is shorter to write. For a model reused by several colleagues I would use the DataFrame API inside functions, because it is easier to test and reuse. If the team only knows SQL, then SQL is the better choice.

**Where else could `regexp_extract` silently return `""`?**
On the `state` and `zip_code` columns. If an address does not match the format, they become `""` instead of NULL. In `groupBy("state")` all those rows would be grouped as one fake state, whereas NULLs would be treated as missing values. I only fixed `city`, so I should handle these two columns the same way.

## Writing Parquet

**How many part-files are in `silver/spark/users.parquet/` and why?**
One `part-00000-...snappy.parquet` per table, plus a `_SUCCESS` file. The data is small, so there are few partitions, and Spark writes one file per output partition.

**What do you lose with `coalesce(1)`?**
Parallelism: a single task writes all the data. On a large dataset this is slow, can overload one executor's memory, and does not scale.

## Iceberg

**Which files hold the data and which describe the table?**
In `warehouse/silver/orders/`, the `data/` folder holds the Parquet file with the actual rows. The `metadata/` folder describes the table: `v1.metadata.json` (schema and snapshots), the manifest `...-m0.avro`, the `snap-....avro` snapshot file, and `version-hint.text` which tells a reader which version is current. In `silver/spark/orders.parquet/` there are only data files and `_SUCCESS`, nothing that describes the table.

**What does a reader see if another job overwrites the Parquet directory during a read?**
It can see files disappearing, a mix of old and new files, or get an error. With Iceberg, the reader stays on its snapshot until it finishes and the new commit is atomic, so it never sees a half-written table.

**What can go wrong if two jobs commit at the same time, and what does a catalog add?**
With this catalog the pointer to the current version is just a file in the bucket, and S3 has no atomic compare-and-swap here. Two jobs could both create the same new version and one would overwrite the other, so a commit is lost. A catalog service (REST, JDBC, Glue) does an atomic swap, handles concurrency, and makes the losing job retry or fail instead of silently overwriting.

## Comparison with the dbt lab

*(Note: I need to run the DuckDB queries to confirm the exact differences below.)*

**What differs between the two schemas?**
Spark has no UUID type, so I cast `uuid` to `string` for `order_id` and `user_id`. Consumers of the silver layer now get strings instead of UUIDs: they lose format validation and may have to cast them back to join with UUID columns from the dbt output. I also need to check `ordered_at` with `DESCRIBE` (timestamp precision or timezone may differ).

**Which layout is easier to read, which one scales?**
A single object per model is easier for a consumer to read. A directory of part-files is a bit less convenient (you need a `*.parquet` glob or a Spark-style reader), but it scales better because many tasks can write and read in parallel.

**What did this lab use instead of dbt tests, and what would be needed for the same guarantees?**
I only used `assert` statements on row counts. To get the same guarantees as `unique`, `not_null`, `accepted_values` and `relationships`, I would have to write the checks myself (for example duplicate counts, null counts, a check on allowed values for `sex`, and an anti-join between orders and users), or use a library like Great Expectations, Soda or Deequ.

**What plays the role of `ref()` and `source()`?**
In the notebook, only the order of the cells. Nothing builds a dependency graph automatically. An orchestrator such as Airflow or Dagster, or a framework like dbt, would be needed for that.

**Why are the build times not directly comparable?**
Spark pays for JVM startup and Ivy package resolution, while DuckDB runs in-process with almost no startup. `dbt build` also includes running tests, and the notebook does not. On top of that, the datasets are tiny, so the numbers mostly measure overhead rather than real processing speed.

**Which tool for each case?**
- Daily batch of 10 GB: dbt with DuckDB. It fits on one machine and is simpler.
- Daily batch of 10 TB: Spark. It needs distributed processing.
- Team of analysts who only know SQL: dbt with DuckDB, since it is SQL-first and has built-in tests and documentation.
