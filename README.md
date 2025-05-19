# BergLoom  

BergLoom is a high-performance Iceberg table compaction runtime built on Rust, optimized specifically for large-scale data lakes. The project name, which means "iceberg weaver," symbolizes its core function of weaving scattered Iceberg data files into an efficient and orderly data structure.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## 🌟 Core Highlights

- **Rust-Native Performance**: Low-latency, high-throughput compaction with memory safety guarantees
- **DataFusion Engine**: Leverages Apache DataFusion for query planning and vectorized execution
- **Iceberg Native Support**: Full compliance with Iceberg table formats via iceberg-rs
- **Multi-Cloud Ready**: Currently supports AWS S3, with plans for Azure Blob Storage and GCP Cloud Storage

## 🛠️ Basic Functionality

- **Full Compaction**: Merges all data files in an Iceberg table and removes old files
- **Deletion Support**:
  - Positional deletions (POS_DELETE)
  - Equality deletions (EQ_DELETE)

## 📝 Code Example

```rust
// 1. Build iceberg catalog with iceberg rust https://github.com/apache/iceberg-rust
  let catalog = Catalog::default();
// 2. Build compaction with config
  let compaction_config = Arc::new(CompactionConfig {
      batch_parallelism: Some(parallelism),
      target_partitions: Some(parallelism),
      data_file_prefix: None,
  });
  let compaction = Compaction::new(compaction_config, self.catalog);
//3. run compact
  compaction
    .compact(CompactionType::Full(self.table_ident))
    .await
    .unwrap();
```

## 🗺️ Roadmap

### Runtime Enhancements
- [ ] Incremental compaction support
- [ ] Merge-on-read performance optimization
- [ ] Standalone scheduler component

### Iceberg Features
- [ ] Partition evolution support
- [ ] Schema evolution support

### Cloud Support
- [ ] Azure Blob Storage integration
- [ ] GCP Cloud Storage integration