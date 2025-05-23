/*
 * Copyright 2025 BergLoom
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use bergloom_codegen::compactor::RewriteFilesStat;
use iceberg::spec::DataFile;
use iceberg::{Catalog, TableIdent};

use crate::executor::{
    ExecutorType, InputFileScanTasks, RewriteFilesRequest, RewriteFilesResponse,
    create_compaction_executor,
};
use crate::{CompactionConfig, CompactionExecutor};
use crate::{CompactionError, Result};
use futures_async_stream::for_await;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration; // Keep for CommitManagerConfig if needed elsewhere, or for direct use in backon

use backon::ExponentialBuilder;
use backon::Retryable;

use crate::executor::DataFusionExecutor;

pub enum CompactionType {
    Full(TableIdent),
}
pub struct Compaction {
    pub config: Arc<CompactionConfig>,
    pub executor: Box<dyn CompactionExecutor>,
    pub catalog: Arc<dyn Catalog>,
}

impl Compaction {
    pub fn new(
        config: Arc<CompactionConfig>,
        catalog: Arc<dyn Catalog>,
        executor_type: ExecutorType,
    ) -> Self {
        let executor = create_compaction_executor(executor_type);
        Self {
            config,
            executor,
            catalog,
        }
    }

    pub async fn compact(&self, compaction_type: CompactionType) -> Result<RewriteFilesStat> {
        match compaction_type {
            CompactionType::Full(table_id) => self.full_compact(table_id).await,
        }
    }

    async fn full_compact(&self, table_ident: TableIdent) -> Result<RewriteFilesStat> {
        let table = self.catalog.load_table(&table_ident).await?;
        let (data_files, delete_files) = get_old_files_from_table(table.clone()).await?;
        let input_file_scan_tasks = get_tasks_from_table(table.clone()).await?;

        let file_io = table.file_io().clone();
        let schema = table.metadata().current_schema();
        let default_location_generator =
            DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let rewrite_files_request = RewriteFilesRequest {
            file_io,
            schema: schema.clone(),
            input_file_scan_tasks,
            config: self.config.clone(),
            dir_path: default_location_generator.dir_path,
            partition_spec: table.metadata().default_partition_spec().clone(),
        };
        let RewriteFilesResponse {
            data_files: output_data_files,
            stat,
        } = DataFusionExecutor::default()
            .rewrite_files(rewrite_files_request)
            .await?;

        let commit_manager = CommitManager::new(
            CommitManagerConfig::default(),
            self.catalog.clone(),
            table_ident.clone(),
        );

        commit_manager
            .rewrite_files(
                output_data_files,
                data_files.into_iter().chain(delete_files.into_iter()),
            )
            .await?;

        Ok(RewriteFilesStat {
            rewritten_files_count: stat.rewritten_files_count,
            added_files_count: stat.added_files_count,
            rewritten_bytes: stat.rewritten_bytes,
            failed_data_files_count: stat.failed_data_files_count,
        })
    }

    pub async fn expire_snapshot(&self, table_ident: TableIdent) -> Result<()> {
        let table = self.catalog.load_table(&table_ident).await?;
        let txn = Transaction::new(&table);
        let txn = txn.expire_snapshot().apply().await?;
        txn.commit(self.catalog.as_ref()).await?;
        Ok(())
    }
}

async fn get_old_files_from_table(table: Table) -> Result<(Vec<DataFile>, Vec<DataFile>)> {
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();

    let mut data_file = vec![];
    let mut delete_file = vec![];
    for manifest_file in manifest_list.entries() {
        let a = manifest_file.load_manifest(table.file_io()).await.unwrap();
        let (entry, _) = a.into_parts();
        for i in entry {
            match i.content_type() {
                iceberg::spec::DataContentType::Data => {
                    data_file.push(i.data_file().clone());
                }
                iceberg::spec::DataContentType::EqualityDeletes => {
                    delete_file.push(i.data_file().clone());
                }
                iceberg::spec::DataContentType::PositionDeletes => {
                    delete_file.push(i.data_file().clone());
                }
            }
        }
    }
    Ok((data_file, delete_file))
}

async fn get_tasks_from_table(table: Table) -> Result<InputFileScanTasks> {
    let snapshot_id = table.metadata().current_snapshot_id().unwrap();

    let scan = table
        .scan()
        .snapshot_id(snapshot_id)
        .with_delete_file_processing_enabled(true)
        .build()?;
    let file_scan_stream = scan.plan_files().await?;

    let mut position_delete_files = HashMap::new();
    let mut data_files = vec![];
    let mut equality_delete_files = HashMap::new();

    #[for_await]
    for task in file_scan_stream {
        let task: FileScanTask = task?;
        match task.data_file_content {
            iceberg::spec::DataContentType::Data => {
                for delete_task in task.deletes.iter() {
                    match &delete_task.data_file_content {
                        iceberg::spec::DataContentType::PositionDeletes => {
                            let mut delete_task = delete_task.clone();
                            delete_task.project_field_ids = vec![];
                            position_delete_files
                                .insert(delete_task.data_file_path.clone(), delete_task);
                        }
                        iceberg::spec::DataContentType::EqualityDeletes => {
                            let mut delete_task = delete_task.clone();
                            delete_task.project_field_ids = delete_task.equality_ids.clone();
                            equality_delete_files
                                .insert(delete_task.data_file_path.clone(), delete_task);
                        }
                        _ => {
                            unreachable!()
                        }
                    }
                }
                data_files.push(task);
            }
            _ => {
                unreachable!()
            }
        }
    }
    Ok(InputFileScanTasks {
        data_files,
        position_delete_files: position_delete_files.into_values().collect(),
        equality_delete_files: equality_delete_files.into_values().collect(),
    })
}

#[derive(Debug, Clone)]
pub struct CommitManagerConfig {
    pub max_retries: u32, // This can be used to configure the backon strategy
    pub retry_initial_delay: Duration, // For exponential backoff
    pub retry_max_delay: Duration, // For exponential backoff
}

impl Default for CommitManagerConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_initial_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(10),
        }
    }
}

// Manages the commit process with retries
pub struct CommitManager {
    config: CommitManagerConfig,
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
}

impl CommitManager {
    pub fn new(
        config: CommitManagerConfig,
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
    ) -> Self {
        Self {
            config,
            catalog,
            table_ident,
        }
    }
}

impl CommitManager {
    pub async fn rewrite_files(
        &self,
        data_files: impl IntoIterator<Item = DataFile>,
        delete_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<()> {
        let data_files: Vec<DataFile> = data_files.into_iter().collect();
        let delete_files: Vec<DataFile> = delete_files.into_iter().collect();
        let operation = || {
            let catalog = self.catalog.clone();
            let table_ident = self.table_ident.clone();
            let data_files = data_files.clone();
            let delete_files = delete_files.clone();
            async move {
                // reload the table to get the latest state
                let table = catalog.load_table(&table_ident).await?;
                let txn = Transaction::new(&table);
                let mut rewrite_action = txn.rewrite_files(None, vec![])?;
                rewrite_action.add_data_files(data_files)?;
                rewrite_action.delete_files(delete_files)?;
                let txn = rewrite_action.apply().await?;
                txn.commit(catalog.as_ref()).await?;
                Ok(())
            }
        };

        let retry_strategy = ExponentialBuilder::default()
            .with_min_delay(self.config.retry_initial_delay)
            .with_max_delay(self.config.retry_max_delay)
            .with_max_times(self.config.max_retries as usize);

        operation
            .retry(&retry_strategy)
            .when(|e: &iceberg::Error| matches!(e.kind(), iceberg::ErrorKind::DataInvalid))
            .await
            .map_err(|e: iceberg::Error| CompactionError::from(e)) // Convert backon::Error to your CompactionError
    }
}

#[cfg(test)]
mod tests {
    use iceberg::Catalog;
    use iceberg::{TableIdent, io::FileIOBuilder};
    use iceberg_catalog_sql::{SqlBindStyle, SqlCatalog, SqlCatalogConfig};
    use std::sync::Arc;

    use crate::CompactionConfig;
    use crate::compaction::Compaction;
    use crate::executor::ExecutorType;

    async fn build_catalog() -> SqlCatalog {
        let sql_lite_uri = "postgresql://xxhx:123456@localhost:5432/demo_iceberg";
        let warehouse_location = "s3a://hummock001/iceberg-data".to_owned();
        let config = SqlCatalogConfig::builder()
            .uri(sql_lite_uri.to_owned())
            .name("demo1".to_owned())
            .warehouse_location(warehouse_location)
            .file_io(
                FileIOBuilder::new("s3a")
                    .with_prop("s3.secret-access-key", "hummockadmin")
                    .with_prop("s3.access-key-id", "hummockadmin")
                    .with_prop("s3.endpoint", "http://127.0.0.1:9301")
                    .with_prop("s3.region", "")
                    .build()
                    .unwrap(),
            )
            .sql_bind_style(SqlBindStyle::DollarNumeric)
            .build();
        SqlCatalog::new(config).await.unwrap()
    }

    #[tokio::test]
    async fn test_compact() {
        let catalog: Arc<dyn Catalog> = Arc::new(build_catalog().await);
        let table_id = TableIdent::from_strs(vec!["demo_db", "test_all_delete"]).unwrap();
        let compaction_config = Arc::new(CompactionConfig {
            batch_parallelism: Some(4),
            target_partitions: Some(4),
            data_file_prefix: None,
        });
        let compaction = Compaction::new(compaction_config, catalog, ExecutorType::DataFusion);
        compaction
            .compact(crate::compaction::CompactionType::Full(table_id))
            .await
            .unwrap();
    }
}
