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
use file_scan_task_table_provider::IcebergFileScanTaskTableProvider;
use futures::{StreamExt, future::try_join_all};
use iceberg::{
    arrow::schema_to_arrow_schema,
    io::FileIO,
    scan::FileScanTask,
    spec::{DataFile, NestedField, PartitionSpec, PrimitiveType, Schema, Type},
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

use super::{
    CompactionExecutor, InputFileScanTasks, RewriteFilesRequest, RewriteFilesResponse,
    RewriteFilesStat,
};
pub mod file_scan_task_table_provider;
pub mod iceberg_file_task_scan;
pub mod sql_builder;

pub const SYS_HIDDEN_SEQ_NUM: &str = "sys_hidden_seq_num";
pub const SYS_HIDDEN_FILE_PATH: &str = "sys_hidden_file_path";
pub const SYS_HIDDEN_POS: &str = "sys_hidden_pos";
const SYS_HIDDEN_COLS: [&str; 3] = [SYS_HIDDEN_SEQ_NUM, SYS_HIDDEN_FILE_PATH, SYS_HIDDEN_POS];

const DATA_FILE_TABLE: &str = "data_file_table";
const POSITION_DELETE_TABLE: &str = "position_delete_table";
const EQUALITY_DELETE_TABLE: &str = "equality_delete_table";
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
        let ctx = SessionContext::new_with_config(session_config);

        let mut stat = RewriteFilesStat::default();
        let rewritten_files_count = input_file_scan_tasks.input_files_count();

        let InputFileScanTasks {
            data_files,
            position_delete_files,
            equality_delete_files,
        } = input_file_scan_tasks;

        let mut data_fusion_task_context = DataFusionTaskContext::builder()?
            .with_schema(schema)
            .with_datafile(data_files)
            .with_position_delete_files(position_delete_files)
            .with_equality_delete_files(equality_delete_files)
            .build()?;

        let need_seq_num = data_fusion_task_context.need_seq_num();
        let need_file_path_and_pos = data_fusion_task_context.need_file_path_and_pos();

        let data_file_schema = data_fusion_task_context.data_file_schema.take().unwrap();
        let input_schema = data_fusion_task_context.input_schema.take().unwrap();

        // register data file table provider
        Self::register_data_table_provider(
            &data_file_schema,
            data_fusion_task_context.data_files.take().unwrap(),
            file_io.clone(),
            &ctx,
            DATA_FILE_TABLE,
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        )?;

        if let Some(position_delete_schema) = data_fusion_task_context.position_delete_schema.take()
        {
            Self::register_delete_table_provider(
                &position_delete_schema,
                data_fusion_task_context
                    .position_delete_files
                    .take()
                    .unwrap(),
                file_io.clone(),
                &ctx,
                POSITION_DELETE_TABLE,
                batch_parallelism,
            )?;
        }

        if let Some(equality_delete_metadatas) =
            data_fusion_task_context.equality_delete_metadatas.take()
        {
            for EqualityDeleteMetadata {
                equality_delete_schema,
                equality_delete_table_name,
                file_scan_tasks,
            } in equality_delete_metadatas
            {
                Self::register_delete_table_provider(
                    &equality_delete_schema,
                    file_scan_tasks,
                    file_io.clone(),
                    &ctx,
                    &equality_delete_table_name,
                    batch_parallelism,
                )?;
            }
        }

        let arc_input_schema = Arc::new(input_schema);

        let df = ctx.sql(&data_fusion_task_context.merge_on_read_sql).await?;
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
    #[allow(clippy::too_many_arguments)]
    fn register_data_table_provider(
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        file_io: FileIO,
        ctx: &SessionContext,
        table_name: &str,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        batch_parallelism: usize,
    ) -> Result<()> {
        Self::register_table_provider_impl(
            schema,
            file_scan_tasks,
            file_io,
            ctx,
            table_name,
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        )
    }

    fn register_delete_table_provider(
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        file_io: FileIO,
        ctx: &SessionContext,
        table_name: &str,
        batch_parallelism: usize,
    ) -> Result<()> {
        Self::register_table_provider_impl(
            schema,
            file_scan_tasks,
            file_io,
            ctx,
            table_name,
            false,
            false,
            batch_parallelism,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn register_table_provider_impl(
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        file_io: FileIO,
        ctx: &SessionContext,
        table_name: &str,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        batch_parallelism: usize,
    ) -> Result<()> {
        let schema = schema_to_arrow_schema(schema)?;
        let data_file_table_provider = IcebergFileScanTaskTableProvider::new(
            file_scan_tasks,
            Arc::new(schema),
            file_io,
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        );

        ctx.register_table(table_name, Arc::new(data_file_table_provider))
            .unwrap();
        Ok(())
    }

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

struct DataFusionTaskContext {
    pub(crate) data_file_schema: Option<Schema>,
    pub(crate) input_schema: Option<Schema>,
    pub(crate) data_files: Option<Vec<FileScanTask>>,
    pub(crate) position_delete_files: Option<Vec<FileScanTask>>,
    pub(crate) equality_delete_files: Option<Vec<FileScanTask>>,
    pub(crate) position_delete_schema: Option<Schema>,
    pub(crate) equality_delete_metadatas: Option<Vec<EqualityDeleteMetadata>>,
    pub(crate) merge_on_read_sql: String,
}

struct DataFusionTaskContextBuilder {
    schema: Arc<Schema>,
    data_files: Vec<FileScanTask>,
    position_delete_files: Vec<FileScanTask>,
    equality_delete_files: Vec<FileScanTask>,
}

impl DataFusionTaskContextBuilder {
    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = schema;
        self
    }

    pub fn with_datafile(mut self, data_files: Vec<FileScanTask>) -> Self {
        self.data_files = data_files;
        self
    }

    pub fn with_position_delete_files(mut self, position_delete_files: Vec<FileScanTask>) -> Self {
        self.position_delete_files = position_delete_files;
        self
    }

    pub fn with_equality_delete_files(mut self, equality_delete_files: Vec<FileScanTask>) -> Self {
        self.equality_delete_files = equality_delete_files;
        self
    }

    // build data fusion task context
    pub fn build(self) -> Result<DataFusionTaskContext> {
        let mut highest_field_id = self.schema.highest_field_id();
        // Build schema for position delete file, file_path + pos
        let position_delete_schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::new(
                    1,
                    SYS_HIDDEN_FILE_PATH,
                    Type::Primitive(PrimitiveType::String),
                    true,
                )),
                Arc::new(NestedField::new(
                    2,
                    SYS_HIDDEN_POS,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )),
            ])
            .build()?;

        // Build schema for equality delete files, equality_ids + seq_num
        let mut equality_ids: Option<Vec<i32>> = None;
        let mut equality_delete_metadatas = Vec::new();
        let mut table_idx = 0;

        for task in &self.equality_delete_files {
            if equality_ids
                .as_ref()
                .is_none_or(|ids| !ids.eq(&task.equality_ids))
            {
                // If ids are different or not assigned, create a new metadata
                let equality_delete_schema =
                    self.build_equality_delete_schema(&task.equality_ids, &mut highest_field_id)?;
                let equality_delete_table_name = format!("{}_{}", EQUALITY_DELETE_TABLE, table_idx);
                equality_delete_metadatas.push(EqualityDeleteMetadata::new(
                    equality_delete_schema,
                    equality_delete_table_name,
                ));
                equality_ids = Some(task.equality_ids.clone());
                table_idx += 1;
            }

            // Add the file scan task to the last metadata
            if let Some(last_metadata) = equality_delete_metadatas.last_mut() {
                last_metadata.add_file_scan_task(task.clone());
            }
        }

        let need_file_path_and_pos = !self.position_delete_files.is_empty();
        let need_seq_num = !equality_delete_metadatas.is_empty();

        // Build schema for data file, old schema + seq_num + file_path + pos
        let project_names: Vec<_> = self
            .schema
            .as_struct()
            .fields()
            .iter()
            .map(|i| i.name.clone())
            .collect();
        let highest_field_id = self.schema.highest_field_id();
        let mut add_schema_fields = vec![];
        // add sequence number column if needed
        if need_seq_num {
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 1,
                SYS_HIDDEN_SEQ_NUM,
                Type::Primitive(PrimitiveType::Long),
                true,
            )));
        }
        // add file path and position column if needed
        if need_file_path_and_pos {
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 2,
                SYS_HIDDEN_FILE_PATH,
                Type::Primitive(PrimitiveType::String),
                true,
            )));
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 3,
                SYS_HIDDEN_POS,
                Type::Primitive(PrimitiveType::Long),
                true,
            )));
        }
        // data file schema is old schema + seq_num + file_path + pos. used for data file table provider
        let data_file_schema = self
            .schema
            .as_ref()
            .clone()
            .into_builder()
            .with_fields(add_schema_fields)
            .build()?;
        // input schema is old schema. used for data file writer
        let input_schema = self.schema.as_ref().clone();

        let sql_builder = sql_builder::SqlBuilder::new(
            &project_names,
            &equality_delete_metadatas,
            need_file_path_and_pos,
        );
        let merge_on_read_sql = sql_builder.build_merge_on_read_sql();

        Ok(DataFusionTaskContext {
            data_file_schema: Some(data_file_schema),
            input_schema: Some(input_schema),
            data_files: Some(self.data_files),
            position_delete_files: Some(self.position_delete_files),
            equality_delete_files: Some(self.equality_delete_files),
            position_delete_schema: if need_file_path_and_pos {
                Some(position_delete_schema)
            } else {
                None
            },
            equality_delete_metadatas: if need_seq_num {
                Some(equality_delete_metadatas)
            } else {
                None
            },
            merge_on_read_sql,
        })
    }

    /// Builds an equality delete schema based on the given equality_ids
    fn build_equality_delete_schema(
        &self,
        equality_ids: &[i32],
        highest_field_id: &mut i32,
    ) -> Result<Schema> {
        let mut equality_delete_fields = Vec::with_capacity(equality_ids.len());
        for id in equality_ids {
            let field = self
                .schema
                .field_by_id(*id)
                .ok_or_else(|| CompactionError::Config("equality_ids not found".to_owned()))?;
            equality_delete_fields.push(field.clone());
        }
        *highest_field_id += 1;
        equality_delete_fields.push(Arc::new(NestedField::new(
            *highest_field_id,
            SYS_HIDDEN_SEQ_NUM,
            Type::Primitive(PrimitiveType::Long),
            true,
        )));

        Schema::builder()
            .with_fields(equality_delete_fields)
            .build()
            .map_err(CompactionError::Iceberg)
    }
}

impl DataFusionTaskContext {
    pub fn builder() -> Result<DataFusionTaskContextBuilder> {
        Ok(DataFusionTaskContextBuilder {
            schema: Arc::new(Schema::builder().build()?),
            data_files: vec![],
            position_delete_files: vec![],
            equality_delete_files: vec![],
        })
    }

    pub fn need_file_path_and_pos(&self) -> bool {
        self.position_delete_files
            .as_ref()
            .is_some_and(|v| !v.is_empty())
    }

    pub fn need_seq_num(&self) -> bool {
        self.equality_delete_files
            .as_ref()
            .is_some_and(|v| !v.is_empty())
    }
}

/// Metadata for equality delete files
#[derive(Debug, Clone)]
pub(crate) struct EqualityDeleteMetadata {
    pub(crate) equality_delete_schema: Schema,
    pub(crate) equality_delete_table_name: String,
    pub(crate) file_scan_tasks: Vec<FileScanTask>,
}

impl EqualityDeleteMetadata {
    pub fn new(equality_delete_schema: Schema, equality_delete_table_name: String) -> Self {
        Self {
            equality_delete_schema,
            equality_delete_table_name,
            file_scan_tasks: Vec::new(),
        }
    }

    pub fn equality_delete_join_names(&self) -> Vec<&str> {
        self.equality_delete_schema
            .as_struct()
            .fields()
            .iter()
            .map(|i| i.name.as_str())
            .filter(|name| !SYS_HIDDEN_COLS.contains(name))
            .collect()
    }

    pub fn add_file_scan_task(&mut self, file_scan_task: FileScanTask) {
        self.file_scan_tasks.push(file_scan_task);
    }
}

#[cfg(test)]
mod tests {
    use futures_async_stream::for_await;
    use iceberg::Catalog;
    use iceberg::scan::FileScanTask;
    use iceberg::spec::{NestedField, PrimitiveType, Schema};
    use iceberg::table::Table;
    use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
    use iceberg::{TableIdent, io::FileIOBuilder, transaction::Transaction};
    use iceberg_catalog_sql::{SqlBindStyle, SqlCatalog, SqlCatalogConfig};
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::CompactionError;
    use crate::executor::datafusion::{DataFusionTaskContextBuilder, EqualityDeleteMetadata};
    use crate::executor::{InputFileScanTasks, RewriteFilesRequest, RewriteFilesResponse};
    use crate::{CompactionConfig, CompactionExecutor, executor::DataFusionExecutor};

    #[test]
    fn test_build_equality_delete_schema() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::new(
                    1,
                    "id",
                    iceberg::spec::Type::Primitive(PrimitiveType::Int),
                    true,
                )),
                Arc::new(NestedField::new(
                    2,
                    "name",
                    iceberg::spec::Type::Primitive(PrimitiveType::String),
                    true,
                )),
            ])
            .build()
            .unwrap();

        let mut highest_field_id = schema.highest_field_id();

        let builder = DataFusionTaskContextBuilder {
            schema: Arc::new(schema),
            data_files: vec![],
            position_delete_files: vec![],
            equality_delete_files: vec![],
        };

        let equality_ids = vec![1, 2];
        let equality_delete_schema = builder
            .build_equality_delete_schema(&equality_ids, &mut highest_field_id)
            .unwrap();

        assert_eq!(equality_delete_schema.as_struct().fields().len(), 3);
        assert_eq!(equality_delete_schema.as_struct().fields()[0].name, "id");
        assert_eq!(equality_delete_schema.as_struct().fields()[1].name, "name");
        assert_eq!(
            equality_delete_schema.as_struct().fields()[2].name,
            "seq_num"
        );
        assert_eq!(highest_field_id, 3);
    }

    #[test]
    fn test_equality_delete_join_names() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;

        // schema
        let fields = vec![
            Arc::new(NestedField::new(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
                true,
            )),
            Arc::new(NestedField::new(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
                true,
            )),
            Arc::new(NestedField::new(
                3,
                "sys_hidden_seq_num",
                Type::Primitive(PrimitiveType::Long),
                true,
            )),
            Arc::new(NestedField::new(
                4,
                "sys_hidden_file_path",
                Type::Primitive(PrimitiveType::String),
                true,
            )),
        ];
        let schema = Schema::builder().with_fields(fields).build().unwrap();

        let meta = EqualityDeleteMetadata {
            equality_delete_schema: schema,
            equality_delete_table_name: "test_table".to_string(),
            file_scan_tasks: vec![],
        };

        let join_names = meta.equality_delete_join_names();
        assert_eq!(join_names, vec!["id", "name"]);
    }
}
