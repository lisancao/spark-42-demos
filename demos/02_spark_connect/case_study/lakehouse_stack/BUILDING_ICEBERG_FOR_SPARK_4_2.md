# Building an Apache Iceberg Runtime for Spark 4.2

> **Status: unsupported.** Apache Iceberg does not publish a Spark 4.2 build. Maven Central's newest
> Spark runtime is `iceberg-spark-runtime-4.1_2.13`, and the Iceberg source tree has integration
> modules for Spark 3.5, 4.0 and 4.1 only. The procedure below produces a local build. It is not an
> Iceberg release, carries no support from either project, and should not be used in production.
>
> Nothing else in this Spark Connect material depends on it. Connect is catalog-agnostic: Demo A
> and the blog post use no catalog at all, and the migration in Demo B works the same way
> against any catalog Spark supports.

## Why a Rebuild Is Required

An Iceberg runtime jar is a Data Source V2 connector. Spark ships the DSv2 *interfaces*
(`TableCatalog`, `Table`, `ScanBuilder`, `RelationCatalog`); Iceberg ships an *implementation* of
them, compiled against a specific Spark version. When Spark changes those interfaces, a jar built
for the previous version no longer satisfies them.

Spark 4.2 changes two things that matter here.

**1. `View` became a class.** In 4.1, `org.apache.spark.sql.connector.catalog.View` was an
interface. In 4.2 it is a class implementing `Relation`, constructed through a builder. A connector
can no longer implement it. Loading the 4.1 jar on 4.2 fails at class-load time:

```
java.lang.IncompatibleClassChangeError: class org.apache.iceberg.spark.source.SparkView
can not implement org.apache.spark.sql.connector.catalog.View, because it is not an interface
```

Because `SparkCatalog` references that class, the failure disables the whole catalog rather than
just views: tables, reads and writes all stop working.

**2. Combined table and view catalogs must implement `RelationCatalog`.** Spark 4.2 rejects a
catalog that implements `TableCatalog` and `ViewCatalog` separately:

```
Catalog 'ice' implements both TableCatalog and ViewCatalog directly. Catalogs that expose both
tables and views must implement RelationCatalog instead, which centralizes the cross-cutting rules
(shared identifier namespace, cross-type collision rejection, single-RPC perf entry points).
```

`RelationCatalog` extends both and adds one abstract method, `loadRelation(Identifier)`, with
defaults for `loadTable`, `loadView`, `tableExists` and `viewExists`.

## Prerequisites

| | |
|---|---|
| Iceberg source | a clone of `apache/iceberg` |
| JDK | 17 or 21 |
| Spark | 4.2.0, for the resulting jar |
| Disk | roughly 2 GB for the Gradle build |

## Procedure

### 1. Create the Module

Spark integrations live in per-version directories. Copy the 4.1 module:

```bash
cd <iceberg-source>
cp -r spark/v4.1 spark/v4.2
```

### 2. Point the Module at Spark 4.2

In `spark/v4.2/build.gradle`:

```groovy
String sparkMajorVersion = '4.2'     // was '4.1'
```

and replace every `libs.versions.spark41` reference with `libs.versions.spark42`.

Add the version to `gradle/libs.versions.toml`:

```toml
spark42 = "4.2.0"
```

### 3. Register the Module

Add `4.2` to the known versions in `gradle.properties`:

```properties
systemProp.knownSparkVersions=3.5,4.0,4.1,4.2
```

Add a block to `settings.gradle`, mirroring the existing 4.1 block:

```groovy
if (sparkVersions.contains("4.2")) {
    include ":iceberg-spark:spark-4.2_2.13"
    include ":iceberg-spark:spark-extensions-4.2_2.13"
    include ":iceberg-spark:spark-runtime-4.2_2.13"
    project(":iceberg-spark:spark-4.2_2.13").projectDir = file('spark/v4.2/spark')
    project(":iceberg-spark:spark-4.2_2.13").name = "iceberg-spark-4.2_2.13"
    project(":iceberg-spark:spark-extensions-4.2_2.13").projectDir = file('spark/v4.2/spark-extensions')
    project(":iceberg-spark:spark-extensions-4.2_2.13").name = "iceberg-spark-extensions-4.2_2.13"
    project(":iceberg-spark:spark-runtime-4.2_2.13").projectDir = file('spark/v4.2/spark-runtime')
    project(":iceberg-spark:spark-runtime-4.2_2.13").name = "iceberg-spark-runtime-4.2_2.13"
}
```

Then add a branch to the dispatcher in `spark/build.gradle`:

```groovy
if (sparkVersions.contains("4.2")) {
    apply from: file("$projectDir/v4.2/build.gradle")
}
```

This step is easy to overlook. Without it the projects are registered but their build file is never
evaluated, and the `shadowJar` task does not exist:

```
Cannot locate tasks that match ':iceberg-spark:iceberg-spark-runtime-4.2_2.13:shadowJar'
```

### 4. Adapt to the 4.2 API

Four source changes in `spark/v4.2/spark/src/main/java/org/apache/iceberg/spark/`.

**a. Remove the geospatial and nanosecond-timestamp accessors.** Spark 4.2 has no geospatial
support: `GeographyVal`, `GeometryVal` and `TimestampNanosVal` do not exist in the distribution.
Delete the corresponding `@Override` methods and imports from `source/StructInternalRow.java` and
`data/SparkParquetReaders.java`.

**b. Add `getBinaryView(int)`** to those same two classes. Spark 4.2's `SpecializedGetters` declares
it as abstract:

```java
@Override
public BinaryView getBinaryView(int ordinal) {
  return isNullAt(ordinal) ? null : BinaryView.fromBytes(getBinaryInternal(ordinal));
}
```

**c. Replace `SparkView` with a converter.** Since `View` can no longer be implemented, build one
instead. Delete `source/SparkView.java` and add a class that constructs a Spark `View` from an
Iceberg view:

```java
return new org.apache.spark.sql.connector.catalog.View.Builder()
    .withSchema(SparkSchemaUtil.convert(icebergView.schema()))
    .withProperties(properties)
    .withQueryText(icebergView.sqlFor("spark").sql())
    .withCurrentCatalog(currentCatalog)
    .withCurrentNamespace(currentNamespace)
    .withQueryColumnNames(queryColumnNames)
    .build();
```

**d. Implement `RelationCatalog`.** In `BaseCatalog.java`, replace `TableCatalog` + `ViewCatalog`
with `RelationCatalog`, and add `loadRelation` to `SparkCatalog` and `SparkSessionCatalog`:

```java
@Override
public Relation loadRelation(Identifier ident) throws NoSuchTableException {
  try {
    return loadTable(ident);
  } catch (NoSuchTableException tableMissing) {
    try {
      return loadView(ident);
    } catch (NoSuchViewException viewMissing) {
      throw tableMissing;
    }
  }
}
```

Update the `ViewCatalog` methods to the 4.2 signatures: `createView(Identifier, View)` and
`replaceView(Identifier, View)`. Remove `alterView`: Spark 4.2 has no `ViewChange` type.

### 5. Build

```bash
./gradlew --no-build-cache \
  -DsparkVersions=4.2 -DscalaVersion=2.13 \
  -DhiveVersions= -DflinkVersions= -DkafkaVersions= \
  :iceberg-spark:iceberg-spark-runtime-4.2_2.13:shadowJar
```

The jar lands in `spark/v4.2/spark-runtime/build/libs/` and is roughly 47 MB.

Use `--no-build-cache` when you want to confirm the build reproduces. Without it Gradle restores the
jar from cache and reports success without compiling anything.

## Using the Runtime JAR

```bash
./sbin/start-connect-server.sh --wait \
  --jars /path/to/iceberg-spark-runtime-4.2_2.13.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.ice.type=jdbc \
  --conf spark.sql.catalog.ice.uri=jdbc:postgresql://host:5432/catalog \
  --conf spark.sql.catalog.ice.jdbc.schema-version=V1 \
  --conf spark.sql.catalog.ice.warehouse=/warehouse
```

Two notes on that command.

**`jdbc.schema-version=V1` is required for views** on a JDBC catalog. Without it, view operations
fail with `JDBC catalog is initialized without view support`.

**Not every catalog type supports views.** `HadoopCatalog` is a table-only catalog in every Spark
version. Views require `JdbcCatalog`, `HiveCatalog`, `NessieCatalog`, `InMemoryCatalog` or
`RESTCatalog`.

You will also see this at server startup, at WARN level:

```
Cannot use org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
to configure session extensions.
java.lang.ClassNotFoundException: ...IcebergSparkSessionExtensions
```

It is expected and harmless. It applies to the Connect server's bootstrap session, which is built
before `--jars` reach the classpath; the per-client sessions created afterwards load the extensions
normally. Confirm extensions are working by using them rather than by reading the log: run a
`MERGE INTO` or a `CALL <catalog>.system.<procedure>`.

## Verifying the Build

`verify_iceberg_runtime.py` in this directory exercises the capabilities a pipeline depends on:

```bash
python verify_iceberg_runtime.py sc://localhost:15002 --catalog ice
```

Against a JDBC catalog with `schema-version=V1`, all of the following pass on Spark 4.2.0. Against a
`HadoopCatalog` the view checks are reported SKIP rather than FAIL, because that catalog type is
table-only regardless of the runtime:

| Capability | |
|---|---|
| `CREATE NAMESPACE` | pass |
| create table, read back | pass |
| `MERGE INTO` | pass |
| `CALL <catalog>.system.expire_snapshots` | pass |
| metadata tables (`.snapshots`) | pass |
| `ALTER TABLE ... ADD COLUMN` | pass |
| time travel (`VERSION AS OF`) | pass |
| row-level `DELETE FROM` | pass |
| `CREATE VIEW` / `SELECT` / `SHOW VIEWS` / `DROP VIEW` | pass |

## Contributing Upstream

These changes are what Iceberg needs for Spark 4.2 support, and the `RelationCatalog` migration in
particular is not specific to any one deployment. If you carry this patch, consider opening an
Iceberg issue rather than maintaining a private fork: a supported release is better for everyone
than a local build.

Note that Spark 5.0 changes this area again: the connector `View` type is replaced by `ViewInfo`,
and the combined interface is named `TableViewCatalog`. Neither exists in 4.2, so a 4.2 port is not
forward-compatible with 5.0 as written.
