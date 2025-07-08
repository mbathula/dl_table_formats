from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os, time, json
from datetime import datetime, timedelta
import pandas as pd

# Configuration
SPARK_VERSION = "3.5"
ICEBERG_VERSION = "1.9.1"
DELTA_VERSION = "3.3.0"

# Paths
ICEBERG_WAREHOUSE_PATH = "/tmp/iceberg/iceberg_warehouse"
DELTA_TABLE_PATH = "/tmp/delta/default/"
ICEBERG_TABLE_PATH = "/tmp/iceberg/iceberg_warehouse/default/"

# Test configurations
TEST_CONFIGS = {
    "small": {"total_rows": 10_000_000, "batch_size": 1_000_000, "number_of_days":30},
    "medium": {"total_rows": 50_000_000, "batch_size": 5_000_000, "number_of_days":60},
    "large": {"total_rows": 100_000_000, "batch_size": 10_000_000, "number_of_days":90}
}

def cleanup_previous_data():
    """Clean up previous test data"""
    paths = [DELTA_TABLE_PATH, ICEBERG_TABLE_PATH, ICEBERG_WAREHOUSE_PATH]
    for path in paths:
        if os.path.exists(path):
            os.system(f"rm -rf {path}/*")
    print("Cleanup complete.")

class PerformanceTracker:
    def __init__(self):
        self.results = []
    
    def measure_time(self, func, operation_name, *args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        
        self.results.append({
            "operation": operation_name,
            "duration": duration,
            "timestamp": datetime.now().isoformat()
        })
        
        print(f"{operation_name}: {duration:.2f} seconds")
        return result, duration
    
    def get_results_df(self):
        return pd.DataFrame(self.results)

def create_spark_session():
    """Create Spark session with configurations"""
    return (
        SparkSession.builder
        .appName("OptimizedIcebergDeltaComparison")
        .master("local[*]")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}_2.12:{ICEBERG_VERSION},io.delta:delta-spark_2.12:{DELTA_VERSION}")
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
        .config("spark.sql.catalog.iceberg_catalog.warehouse", ICEBERG_WAREHOUSE_PATH)
        # Optimized configurations
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.columnarReaderBatchSize", "4096")
        # Delta specific optimizations
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
        .getOrCreate()
    )


def generate_test_data(spark, total_rows, batch_size, number_of_days=30):
    """Generate test data with better distribution"""
    print(f"Generating {total_rows:,} rows of test data...")
    
    def generate_batch(start_index, batch_size):
        return spark.range(start_index, start_index + batch_size) \
            .withColumn("customer_id", concat_ws("_", lit("cust"), col("id") % 100_000).cast("string")) \
            .withColumn("product_id", concat_ws("_", lit("prod"), col("id") % 10_000).cast("string")) \
            .withColumn("category", concat_ws("_", lit("cat"), col("id") % 50).cast("string")) \
            .withColumn("transaction_time", date_sub(current_timestamp(), (rand() * number_of_days).cast("int"))) \
            .withColumn("location", concat_ws("_", lit("loc"), col("id") % 1000).cast("string")) \
            .withColumn("price", round((rand() * 1000 + 1), 2).cast("double")) \
            .withColumn("quantity", (rand() * 10 + 1).cast("int")) \
            .withColumn("discount", round((rand() * 0.3), 3).cast("double")) \
            .withColumn("revenue", round(col("price") * col("quantity") * (1 - col("discount")), 2))
    
    start_time = time.time()
    dataframes = [generate_batch(i, batch_size) for i in range(0, total_rows, batch_size)]
    
    retail_df = dataframes[0]
    for df in dataframes[1:]:
        retail_df = retail_df.union(df)
    
    # Add transaction_date column
    retail_df = retail_df.withColumn("transaction_date", to_date(col("transaction_time"))).drop("transaction_time")
    
    end_time = time.time()
    print(f"Data generation completed in {end_time - start_time:.2f} seconds")
    return retail_df

def test_delta_performance(spark, df, tracker):
    """Test Delta Lake performance with liquid clustering"""
    print("\n=== DELTA LAKE PERFORMANCE TESTS ===")
    
    # Test 1: Standard Delta table (no clustering)
    delta_table_standard = "batch_test_delta_standard"
    spark.sql(f"DROP TABLE IF EXISTS default.{delta_table_standard}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS default.{delta_table_standard} (
            id LONG,
            customer_id STRING,
            product_id STRING,
            category STRING,
            location STRING,
            price DOUBLE,
            quantity INTEGER,
            discount DOUBLE,
            revenue DOUBLE,
            transaction_date DATE
        ) 
        USING DELTA
        LOCATION '{DELTA_TABLE_PATH}{delta_table_standard}'
    """)
    
    # Write standard Delta table
    _, delta_std_write_time = tracker.measure_time(
        lambda: df.write.format("delta").mode("overwrite").save(f"{DELTA_TABLE_PATH}{delta_table_standard}"),
        "Delta Standard Write"
    )
    
    # Test 2: Delta table with liquid clustering
    delta_table_clustered = "batch_test_delta_clustered"
    spark.sql(f"DROP TABLE IF EXISTS default.{delta_table_clustered}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS default.{delta_table_clustered} (
            id LONG,
            customer_id STRING,
            product_id STRING,
            category STRING,
            location STRING,
            price DOUBLE,
            quantity INTEGER,
            discount DOUBLE,
            revenue DOUBLE,
            transaction_date DATE
        ) 
        USING DELTA
        CLUSTER BY (transaction_date, category)
        LOCATION '{DELTA_TABLE_PATH}{delta_table_clustered}'
    """)
    
    # Write clustered Delta table
    _, delta_clustered_write_time = tracker.measure_time(
        lambda: df.write.format("delta").mode("overwrite").save(f"{DELTA_TABLE_PATH}{delta_table_clustered}"),
        "Delta Clustered Write"
    )
    
    # Optimize both tables
    tracker.measure_time(
        lambda: spark.sql(f"OPTIMIZE default.{delta_table_standard}"),
        "Delta Standard Optimize"
    )
    
    tracker.measure_time(
        lambda: spark.sql(f"OPTIMIZE default.{delta_table_clustered}"),
        "Delta Clustered Optimize"
    )
    
    # Read performance tests
    _, delta_std_read_time = tracker.measure_time(
        lambda: spark.read.format("delta").load(f"{DELTA_TABLE_PATH}{delta_table_standard}").count(),
        "Delta Standard Read (Full Scan)"
    )
    
    _, delta_clustered_read_time = tracker.measure_time(
        lambda: spark.read.format("delta").load(f"{DELTA_TABLE_PATH}{delta_table_clustered}").count(),
        "Delta Clustered Read (Full Scan)"
    )
    
    # Filtered read tests (leveraging clustering)
    _, delta_std_filtered_time = tracker.measure_time(
        lambda: spark.sql(f"""
            SELECT category, COUNT(*), AVG(revenue) 
            FROM default.{delta_table_standard} 
            WHERE transaction_date >= '2024-12-01' 
            GROUP BY category
        """).collect(),
        "Delta Standard Filtered Read"
    )
    
    _, delta_clustered_filtered_time = tracker.measure_time(
        lambda: spark.sql(f"""
            SELECT category, COUNT(*), AVG(revenue) 
            FROM default.{delta_table_clustered} 
            WHERE transaction_date >= '2024-12-01' 
            GROUP BY category
        """).collect(),
        "Delta Clustered Filtered Read"
    )
    
    return {
        "standard_table": delta_table_standard,
        "clustered_table": delta_table_clustered,
        "write_times": {
            "standard": delta_std_write_time,
            "clustered": delta_clustered_write_time
        },
        "read_times": {
            "standard_full": delta_std_read_time,
            "clustered_full": delta_clustered_read_time,
            "standard_filtered": delta_std_filtered_time,
            "clustered_filtered": delta_clustered_filtered_time
        }
    }

def test_iceberg_performance(spark, df, tracker):
    """Test Iceberg performance with hidden partitioning and sorting"""
    print("\n=== ICEBERG PERFORMANCE TESTS ===")
    
    # Test 1: Standard Iceberg table (no partitioning)
    iceberg_table_standard = "iceberg_catalog.default.batch_test_iceberg_standard"
    spark.sql(f"DROP TABLE IF EXISTS {iceberg_table_standard}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {iceberg_table_standard} (
            id LONG,
            customer_id STRING,
            product_id STRING,
            category STRING,
            location STRING,
            price DOUBLE,
            quantity INTEGER,
            discount DOUBLE,
            revenue DOUBLE,
            transaction_date DATE
        ) USING iceberg
    """)
    
    # Write standard Iceberg table
    _, iceberg_std_write_time = tracker.measure_time(
        lambda: df.writeTo(iceberg_table_standard).using("iceberg").createOrReplace(),
        "Iceberg Standard Write"
    )
    
    # Test 2: Iceberg table with hidden partitioning (bucketing by date)
    iceberg_table_bucketed = "iceberg_catalog.default.batch_test_iceberg_bucketed"
    spark.sql(f"DROP TABLE IF EXISTS {iceberg_table_bucketed}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {iceberg_table_bucketed} (
            id LONG,
            customer_id STRING,
            product_id STRING,
            category STRING,
            location STRING,
            price DOUBLE,
            quantity INTEGER,
            discount DOUBLE,
            revenue DOUBLE,
            transaction_date DATE
        ) USING iceberg
        PARTITIONED BY (bucket(10, transaction_date))
    """)
    
    # Write bucketed Iceberg table
    _, iceberg_bucketed_write_time = tracker.measure_time(
        lambda: df.writeTo(iceberg_table_bucketed).using("iceberg").createOrReplace(),
        "Iceberg Bucketed Write"
    )
    
    # Test 3: Iceberg table with transform partitioning (monthly partitioning)
    iceberg_table_transform = "iceberg_catalog.default.batch_test_iceberg_transform"
    spark.sql(f"DROP TABLE IF EXISTS {iceberg_table_transform}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {iceberg_table_transform} (
            id LONG,
            customer_id STRING,
            product_id STRING,
            category STRING,
            location STRING,
            price DOUBLE,
            quantity INTEGER,
            discount DOUBLE,
            revenue DOUBLE,
            transaction_date DATE
        ) USING iceberg
        PARTITIONED BY (days(transaction_date))
    """)
    
    # Write transform partitioned Iceberg table
    _, iceberg_transform_write_time = tracker.measure_time(
        lambda: df.writeTo(iceberg_table_transform).using("iceberg").createOrReplace(),
        "Iceberg Transform Write"
    )
    
    # Optimize Iceberg tables
    for table_name in [iceberg_table_standard, iceberg_table_bucketed, iceberg_table_transform]:
        tracker.measure_time(
            lambda t=table_name: spark.sql(f"""
                CALL iceberg_catalog.system.rewrite_data_files(
                    table => '{t}',
                    options => map('min-input-files', '5', 'target-file-size-bytes', '134217728')
                )
            """),
            f"Iceberg Optimize {table_name.split('.')[-1]}"
        )
    
    # Read performance tests
    _, iceberg_std_read_time = tracker.measure_time(
        lambda: spark.table(iceberg_table_standard).count(),
        "Iceberg Standard Read (Full Scan)"
    )
    
    _, iceberg_bucketed_read_time = tracker.measure_time(
        lambda: spark.table(iceberg_table_bucketed).count(),
        "Iceberg Bucketed Read (Full Scan)"
    )
    
    _, iceberg_transform_read_time = tracker.measure_time(
        lambda: spark.table(iceberg_table_transform).count(),
        "Iceberg Transform Read (Full Scan)"
    )
    
    # Filtered read tests (leveraging partitioning)
    _, iceberg_std_filtered_time = tracker.measure_time(
        lambda: spark.sql(f"""
            SELECT category, COUNT(*), AVG(revenue) 
            FROM {iceberg_table_standard} 
            WHERE transaction_date >= '2024-12-01' 
            GROUP BY category
        """).collect(),
        "Iceberg Standard Filtered Read"
    )
    
    _, iceberg_bucketed_filtered_time = tracker.measure_time(
        lambda: spark.sql(f"""
            SELECT category, COUNT(*), AVG(revenue) 
            FROM {iceberg_table_bucketed} 
            WHERE transaction_date >= '2024-12-01' 
            GROUP BY category
        """).collect(),
        "Iceberg Bucketed Filtered Read"
    )
    
    _, iceberg_transform_filtered_time = tracker.measure_time(
        lambda: spark.sql(f"""
            SELECT category, COUNT(*), AVG(revenue) 
            FROM {iceberg_table_transform} 
            WHERE transaction_date >= '2024-12-01' 
            GROUP BY category
        """).collect(),
        "Iceberg Transform Filtered Read"
    )
    
    return {
        "standard_table": iceberg_table_standard,
        "bucketed_table": iceberg_table_bucketed,
        "transform_table": iceberg_table_transform,
        "write_times": {
            "standard": iceberg_std_write_time,
            "bucketed": iceberg_bucketed_write_time,
            "transform": iceberg_transform_write_time
        },
        "read_times": {
            "standard_full": iceberg_std_read_time,
            "bucketed_full": iceberg_bucketed_read_time,
            "transform_full": iceberg_transform_read_time,
            "standard_filtered": iceberg_std_filtered_time,
            "bucketed_filtered": iceberg_bucketed_filtered_time,
            "transform_filtered": iceberg_transform_filtered_time
        }
    }

def test_concurrent_operations(spark, df, delta_results, iceberg_results, tracker):
    """Test concurrent read/write operations"""
    print("\n=== CONCURRENT OPERATIONS TESTS ===")
    
    # Test concurrent reads on Delta
    delta_table = delta_results["clustered_table"]
    iceberg_table = iceberg_results["transform_table"]
    
    # Simulate concurrent reads by running multiple queries
    import threading
    
    def run_concurrent_queries(table_name, query_type, num_queries=3):
        def run_query(query_id):
            if query_type == "delta":
                return spark.sql(f"""
                    SELECT location, COUNT(*), SUM(revenue) 
                    FROM {table_name} 
                    WHERE transaction_date >= date_sub(current_date(), 15)
                    GROUP BY location
                    ORDER BY SUM(revenue) DESC
                    LIMIT 10
                """).collect()
            else:  # iceberg
                return spark.sql(f"""
                    SELECT product_id, COUNT(*), AVG(price) 
                    FROM {table_name} 
                    WHERE price > 100 
                    GROUP BY product_id
                    ORDER BY AVG(price) DESC
                    LIMIT 10
                """).collect()
        
        threads = []
        results = []
        start_time = time.time()
        
        for i in range(num_queries):
            thread = threading.Thread(target=lambda i=i: results.append(run_query(i)))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        end_time = time.time()
        return end_time - start_time
    
    # Test concurrent Delta reads
    delta_concurrent_time = run_concurrent_queries(delta_table, "delta")
    tracker.results.append({
        "operation": "Delta Concurrent Reads",
        "duration": delta_concurrent_time,
        "timestamp": datetime.now().isoformat()
    })
    
    # Test concurrent Iceberg reads
    iceberg_concurrent_time = run_concurrent_queries(iceberg_table, "iceberg")
    tracker.results.append({
        "operation": "Iceberg Concurrent Reads",
        "duration": iceberg_concurrent_time,
        "timestamp": datetime.now().isoformat()
    })
    
    print(f"Delta Concurrent Reads: {delta_concurrent_time:.2f} seconds")
    print(f"Iceberg Concurrent Reads: {iceberg_concurrent_time:.2f} seconds")

def test_update_operations(spark, delta_results, iceberg_results, tracker):
    """Test update and merge operations"""
    print("\n=== UPDATE/MERGE OPERATIONS TESTS ===")
    
    delta_table = delta_results["clustered_table"]
    iceberg_table = iceberg_results["transform_table"]
    try:
        # Test Delta update
        _, delta_update_time = tracker.measure_time(
            lambda: spark.sql(f"""
                UPDATE {delta_table} 
                SET discount = discount * 1.1 
                WHERE transaction_date >= date_sub(current_date(), 7)
            """),
            "Delta Update Operation"
        )
    except Exception(e):
         print(f"UPDATE {delta_table} failed")
    
    try:
        # Test Iceberg update
        _, iceberg_update_time = tracker.measure_time(
            lambda: spark.sql(f"""
                UPDATE {iceberg_table} 
                SET discount = discount * 1.1 
                WHERE transaction_date >= date_sub(current_date(), 7)
            """),
            "Iceberg Update Operation"
        )
    except Exception(e):
        print(f"UPDATE {iceberg_table}  failed")
    
    return {
        "delta_update_time": delta_update_time,
        "iceberg_update_time": iceberg_update_time
    }

def test_time_travel_queries(spark, delta_results, iceberg_results, tracker):
    """Test time travel capabilities"""
    print("\n=== TIME TRAVEL TESTS ===")
    
    delta_table = delta_results["clustered_table"]
    iceberg_table = iceberg_results["transform_table"]
    
    # Get current timestamp for Delta
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Test Delta time travel
    version = spark.sql(f"SELECT * FROM {iceberg_table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    
    _, delta_time_travel_time = tracker.measure_time(
        lambda: spark.sql(f"""
            SELECT COUNT(*) 
            FROM {delta_table} 
            VERSION AS OF '0'
        """).collect(),
        "Delta Time Travel Query"
    )
    
    # Get Iceberg snapshot info
    snapshots = spark.sql(f"SELECT * FROM {iceberg_table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    if snapshots:
        snapshot_id = snapshots[0]["snapshot_id"]
        
        # Test Iceberg time travel
        _, iceberg_time_travel_time = tracker.measure_time(
            lambda: spark.sql(f"""
                SELECT COUNT(*) 
                FROM {iceberg_table} 
                VERSION AS OF {snapshot_id}
            """).collect(),
            "Iceberg Time Travel Query"
        )
    
    return {
        "delta_time_travel_time": delta_time_travel_time,
        "iceberg_time_travel_time": iceberg_time_travel_time if snapshots else None
    }

def generate_performance_report(tracker, test_size):
    """Generate comprehensive performance report"""
    results_df = tracker.get_results_df()
    
    print(f"\n{'='*60}")
    print(f"PERFORMANCE COMPARISON REPORT - {test_size.upper()} DATASET")
    print(f"{'='*60}")
    
    # Group results by operation type
    write_ops = results_df[results_df['operation'].str.contains('Write')]
    read_ops = results_df[results_df['operation'].str.contains('Read')]
    optimize_ops = results_df[results_df['operation'].str.contains('Optimize')]
    update_ops = results_df[results_df['operation'].str.contains('Update')]
    
    print("\n📝 WRITE OPERATIONS:")
    for _, row in write_ops.iterrows():
        print(f"  {row['operation']}: {row['duration']:.2f}s")
    
    print("\n📖 READ OPERATIONS:")
    for _, row in read_ops.iterrows():
        print(f"  {row['operation']}: {row['duration']:.2f}s")
    
    print("\n⚡ OPTIMIZATION OPERATIONS:")
    for _, row in optimize_ops.iterrows():
        print(f"  {row['operation']}: {row['duration']:.2f}s")
    
    print("\n🔄 UPDATE OPERATIONS:")
    for _, row in update_ops.iterrows():
        print(f"  {row['operation']}: {row['duration']:.2f}s")
    
    # Calculate averages
    delta_write_avg = write_ops[write_ops['operation'].str.contains('Delta')]['duration'].mean()
    iceberg_write_avg = write_ops[write_ops['operation'].str.contains('Iceberg')]['duration'].mean()
    
    delta_read_avg = read_ops[read_ops['operation'].str.contains('Delta')]['duration'].mean()
    iceberg_read_avg = read_ops[read_ops['operation'].str.contains('Iceberg')]['duration'].mean()
    
    print(f"\n📊 SUMMARY:")
    print(f"  Delta Average Write Time: {delta_write_avg:.2f}s")
    print(f"  Iceberg Average Write Time: {iceberg_write_avg:.2f}s")
    print(f"  Delta Average Read Time: {delta_read_avg:.2f}s")
    print(f"  Iceberg Average Read Time: {iceberg_read_avg:.2f}s")
    
    # Performance insights
    print(f"\n🎯 INSIGHTS:")
    if delta_write_avg < iceberg_write_avg:
        print(f"  ✓ Delta writes are {((iceberg_write_avg/delta_write_avg - 1) * 100):.1f}% faster")
    else:
        print(f"  ✓ Iceberg writes are {((delta_write_avg/iceberg_write_avg - 1) * 100):.1f}% faster")
    
    if delta_read_avg < iceberg_read_avg:
        print(f"  ✓ Delta reads are {((iceberg_read_avg/delta_read_avg - 1) * 100):.1f}% faster")
    else:
        print(f"  ✓ Iceberg reads are {((delta_read_avg/iceberg_read_avg - 1) * 100):.1f}% faster")
    
    return results_df

def main():
    """Main execution function"""
    print("🚀 Starting Optimized Delta vs Iceberg Performance Comparison")
    print("=" * 70)
    
    # Initialize components
    cleanup_previous_data()
    spark = create_spark_session()
    tracker = PerformanceTracker()
    
    # Choose test size
    test_size = "large"  # Change to "small" / "medium" / "large"as needed
    config = TEST_CONFIGS[test_size]
    
    try:
        # Generate test data
        df = generate_test_data(spark, config["total_rows"], config["batch_size"], config["number_of_days"])
        
        # Run performance tests
        delta_results = test_delta_performance(spark, df, tracker)
        iceberg_results = test_iceberg_performance(spark, df, tracker)
        
        # Advanced tests
        test_concurrent_operations(spark, df, delta_results, iceberg_results, tracker)
        #test_update_operations(spark, delta_results, iceberg_results, tracker)
        test_time_travel_queries(spark, delta_results, iceberg_results, tracker)
        
        # Generate report
        results_df = generate_performance_report(tracker, test_size)
        
        # Save results to CSV
        results_df.to_csv(f"/tmp/performance_results_{test_size}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", index=False)
        
    finally:
        spark.stop()
        print("\n✅ Test completed successfully!")

if __name__ == "__main__":
    main()