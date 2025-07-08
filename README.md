## Small 10M
============================================================
PERFORMANCE COMPARISON REPORT - SMALL DATASET
============================================================

📝 WRITE OPERATIONS:
  Delta Standard Write: 15.44s
  Delta Clustered Write: 10.33s
  Iceberg Standard Write: 3.44s
  Iceberg Bucketed Write: 2.01s
  Iceberg Transform Write: 1.90s

📖 READ OPERATIONS:
  Delta Standard Read (Full Scan): 0.12s
  Delta Clustered Read (Full Scan): 0.08s
  Delta Standard Filtered Read: 0.79s
  Delta Clustered Filtered Read: 0.45s
  Iceberg Standard Read (Full Scan): 0.05s
  Iceberg Bucketed Read (Full Scan): 0.03s
  Iceberg Transform Read (Full Scan): 0.02s
  Iceberg Standard Filtered Read: 0.76s
  Iceberg Bucketed Filtered Read: 0.61s
  Iceberg Transform Filtered Read: 0.59s
  Delta Concurrent Reads: 0.67s
  Iceberg Concurrent Reads: 0.95s

⚡ OPTIMIZATION OPERATIONS:
  Delta Standard Optimize: 0.12s
  Delta Clustered Optimize: 0.08s
  Iceberg Optimize batch_test_iceberg_standard: 8.11s
  Iceberg Optimize batch_test_iceberg_bucketed: 7.66s
  Iceberg Optimize batch_test_iceberg_transform: 7.50s

🔄 UPDATE OPERATIONS:
  Delta Update Operation: 16.03s
  Iceberg Update Operation: 12.29s

📊 SUMMARY:
  Delta Average Write Time: 12.89s
  Iceberg Average Write Time: 2.45s
  Delta Average Read Time: 0.42s
  Iceberg Average Read Time: 0.43s

🎯 INSIGHTS:
  ✓ Iceberg writes are 425.8% faster
  ✓ Delta reads are 2.0% faster

## Medium 50M

============================================================
PERFORMANCE COMPARISON REPORT - MEDIUM DATASET
============================================================

📝 WRITE OPERATIONS:
  Delta Standard Write: 20.54s
  Delta Clustered Write: 15.76s
  Iceberg Standard Write: 9.40s
  Iceberg Bucketed Write: 7.36s
  Iceberg Transform Write: 7.12s

📖 READ OPERATIONS:
  Delta Standard Read (Full Scan): 0.15s
  Delta Clustered Read (Full Scan): 0.09s
  Delta Standard Filtered Read: 1.05s
  Delta Clustered Filtered Read: 0.69s
  Iceberg Standard Read (Full Scan): 0.05s
  Iceberg Bucketed Read (Full Scan): 0.02s
  Iceberg Transform Read (Full Scan): 0.02s
  Iceberg Standard Filtered Read: 0.96s
  Iceberg Bucketed Filtered Read: 0.97s
  Iceberg Transform Filtered Read: 0.79s
  Delta Concurrent Reads: 2.12s
  Iceberg Concurrent Reads: 2.56s

⚡ OPTIMIZATION OPERATIONS:
  Delta Standard Optimize: 32.66s
  Delta Clustered Optimize: 32.33s
  Iceberg Optimize batch_test_iceberg_standard: 10.63s
  Iceberg Optimize batch_test_iceberg_bucketed: 10.02s
  Iceberg Optimize batch_test_iceberg_transform: 10.19s

🔄 UPDATE OPERATIONS:

📊 SUMMARY:
  Delta Average Write Time: 18.15s
  Iceberg Average Write Time: 7.96s
  Delta Average Read Time: 0.82s
  Iceberg Average Read Time: 0.77s

🎯 INSIGHTS:
  ✓ Iceberg writes are 128.0% faster
  ✓ Iceberg reads are 6.5% faster
## Large 100M

