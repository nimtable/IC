use crate::error::Result;
/*
 * Copyright 2025 IC
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

use ::datafusion::{
    parquet::file::properties::WriterProperties,
    prelude::{SessionConfig, SessionContext},
};
use async_trait::async_trait;
use datafusion_processor::{DataFusionTaskContext, DatafusionProcessor};
use futures::{StreamExt, future::try_join_all};
use iceberg::{
    io::FileIO,
    spec::{DataFile, PartitionSpec, Schema},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        },
        function_writer::fanout_partition_writer::FanoutPartitionWriterBuilder,
    },
};
use sqlx::types::Uuid;
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::CompactionError;

use super::{CompactionExecutor, InputFileScanTasks, RewriteFilesStat};
pub mod datafusion_processor;
use super::{RewriteFilesRequest, RewriteFilesResponse};
pub mod file_scan_task_table_provider;
pub mod iceberg_file_task_scan;

const DEFAULT_PREFIX: &str = "10";

#[derive(Default)]
pub struct DataFusionExecutor {}

#[async_trait]
impl CompactionExecutor for DataFusionExecutor {
    async fn rewrite_files(&self, request: RewriteFilesRequest) -> Result<RewriteFilesResponse> {
        let RewriteFilesRequest {
            file_io,
            schema,
            input_file_scan_tasks,
            config,
            dir_path,
            partition_spec,
        } = request;
        let batch_parallelism = config.batch_parallelism.unwrap_or(4);
        let target_partitions = config.target_partitions.unwrap_or(4);
        let data_file_prefix = config
            .data_file_prefix
            .clone()
            .unwrap_or(DEFAULT_PREFIX.to_owned());
        let mut session_config = SessionConfig::new();
        session_config = session_config.with_target_partitions(target_partitions);
        let ctx = Arc::new(SessionContext::new_with_config(session_config));

        let mut stat = RewriteFilesStat::default();
        let rewritten_files_count = input_file_scan_tasks.input_files_count();

        let InputFileScanTasks {
            data_files,
            position_delete_files,
            equality_delete_files,
        } = input_file_scan_tasks;

        let datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(schema)
            .with_datafile(data_files)
            .with_position_delete_files(position_delete_files)
            .with_equality_delete_files(equality_delete_files)
            .build_merge_on_read()?;
        let (df, input_schema) =
            DatafusionProcessor::new(ctx, datafusion_task_ctx, batch_parallelism, file_io.clone())
                .execute()
                .await?;
        let arc_input_schema = Arc::new(input_schema);
        let batchs = df.execute_stream_partitioned().await?;
        let mut futures = Vec::with_capacity(batch_parallelism);
        // build iceberg writer for each partition
        for mut batch in batchs {
            let dir_path = dir_path.clone();
            let schema = arc_input_schema.clone();
            let data_file_prefix = data_file_prefix.clone();
            let file_io = file_io.clone();
            let partition_spec = partition_spec.clone();
            let future: JoinHandle<
                std::result::Result<Vec<iceberg::spec::DataFile>, CompactionError>,
            > = tokio::spawn(async move {
                let mut data_file_writer = Self::build_iceberg_writer(
                    data_file_prefix,
                    dir_path,
                    schema,
                    file_io,
                    partition_spec,
                )
                .await?;
                while let Some(b) = batch.as_mut().next().await {
                    data_file_writer.write(b?).await?;
                }
                let data_files = data_file_writer.close().await?;
                Ok(data_files)
            });
            futures.push(future);
        }
        // collect all data files from all partitions
        let output_data_files: Vec<DataFile> = try_join_all(futures)
            .await
            .map_err(|e| CompactionError::Execution(e.to_string()))?
            .into_iter()
            .map(|res| res.map(|v| v.into_iter()))
            .collect::<Result<Vec<_>>>()
            .map(|iters| iters.into_iter().flatten().collect())?;

        stat.added_files_count = output_data_files.len() as u32;
        stat.rewritten_bytes = output_data_files
            .iter()
            .map(|f| f.file_size_in_bytes())
            .sum();
        stat.rewritten_files_count = rewritten_files_count;

        Ok(RewriteFilesResponse {
            data_files: output_data_files,
            stat,
        })
    }
}

impl DataFusionExecutor {
    async fn build_iceberg_writer(
        data_file_prefix: String,
        dir_path: String,
        schema: Arc<Schema>,
        file_io: FileIO,
        partition_spec: Arc<PartitionSpec>,
    ) -> Result<Box<dyn IcebergWriter>> {
        let location_generator = DefaultLocationGenerator { dir_path };
        let unique_uuid_suffix = Uuid::now_v7();
        let file_name_generator = DefaultFileNameGenerator::new(
            data_file_prefix,
            Some(unique_uuid_suffix.to_string()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            schema.clone(),
            file_io,
            location_generator,
            file_name_generator,
        );
        let data_file_builder =
            DataFileWriterBuilder::new(parquet_writer_builder, None, partition_spec.spec_id());
        let iceberg_output_writer = if partition_spec.fields().is_empty() {
            Box::new(data_file_builder.build().await?) as Box<dyn IcebergWriter>
        } else {
            Box::new(
                FanoutPartitionWriterBuilder::new(
                    data_file_builder,
                    partition_spec.clone(),
                    schema,
                )?
                .build()
                .await?,
            ) as Box<dyn IcebergWriter>
        };
        Ok(iceberg_output_writer)
    }
}