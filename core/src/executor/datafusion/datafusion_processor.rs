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

use std::sync::Arc;

use crate::error::{CompactionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::{
    arrow::schema_to_arrow_schema,
    io::FileIO,
    scan::FileScanTask,
    spec::{NestedField, PrimitiveType, Schema, Type},
};

use super::file_scan_task_table_provider::IcebergFileScanTaskTableProvider;

pub const SYS_HIDDEN_SEQ_NUM: &str = "sys_hidden_seq_num";
pub const SYS_HIDDEN_FILE_PATH: &str = "sys_hidden_file_path";
pub const SYS_HIDDEN_POS: &str = "sys_hidden_pos";
const SYS_HIDDEN_COLS: [&str; 3] = [SYS_HIDDEN_SEQ_NUM, SYS_HIDDEN_FILE_PATH, SYS_HIDDEN_POS];

const DATA_FILE_TABLE: &str = "data_file_table";
const POSITION_DELETE_TABLE: &str = "position_delete_table";
const EQUALITY_DELETE_TABLE: &str = "equality_delete_table";

pub struct DatafusionProcessor {
    datafusion_task_ctx: DataFusionTaskContext,
    table_register: DatafusionTableRegister,
    batch_parallelism: usize,
    ctx: Arc<SessionContext>,
}

impl DatafusionProcessor {
    pub fn new(
        ctx: Arc<SessionContext>,
        datafusion_task_ctx: DataFusionTaskContext,
        batch_parallelism: usize,
        file_io: FileIO,
    ) -> Self {
        let table_register = DatafusionTableRegister::new(file_io, ctx.clone());
        Self {
            datafusion_task_ctx,
            table_register,
            batch_parallelism,
            ctx,
        }
    }

    pub fn register_tables(&mut self) -> Result<()> {
        if let Some(datafile_schema) = self.datafusion_task_ctx.data_file_schema.take() {
            self.table_register.register_data_table_provider(
                &datafile_schema,
                self.datafusion_task_ctx.data_files.take().unwrap(),
                DATA_FILE_TABLE,
                self.datafusion_task_ctx.need_seq_num(),
                self.datafusion_task_ctx.need_file_path_and_pos(),
                self.batch_parallelism,
            )?;
        }

        if let Some(position_delete_schema) = self.datafusion_task_ctx.position_delete_schema.take()
        {
            self.table_register.register_delete_table_provider(
                &position_delete_schema,
                self.datafusion_task_ctx
                    .position_delete_files
                    .take()
                    .unwrap(),
                POSITION_DELETE_TABLE,
                self.batch_parallelism,
            )?;
        }

        if let Some(equality_delete_metadatas) =
            self.datafusion_task_ctx.equality_delete_metadatas.take()
        {
            for EqualityDeleteMetadata {
                equality_delete_schema,
                equality_delete_table_name,
                file_scan_tasks,
            } in equality_delete_metadatas
            {
                self.table_register.register_delete_table_provider(
                    &equality_delete_schema,
                    file_scan_tasks,
                    &equality_delete_table_name,
                    self.batch_parallelism,
                )?;
            }
        }
        Ok(())
    }

    pub async fn execute(&mut self) -> Result<(DataFrame, Schema)> {
        self.register_tables()?;
        let df = self.ctx.sql(&self.datafusion_task_ctx.exec_sql).await?;
        Ok((df, self.datafusion_task_ctx.input_schema.take().unwrap()))
    }
}

pub struct DatafusionTableRegister {
    file_io: FileIO,
    ctx: Arc<SessionContext>,
}

impl DatafusionTableRegister {
    pub fn new(file_io: FileIO, ctx: Arc<SessionContext>) -> Self {
        DatafusionTableRegister { file_io, ctx }
    }

    pub fn register_data_table_provider(
        &self,
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        table_name: &str,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        batch_parallelism: usize,
    ) -> Result<()> {
        self.register_table_provider_impl(
            schema,
            file_scan_tasks,
            table_name,
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        )
    }

    pub fn register_delete_table_provider(
        &self,
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        table_name: &str,
        batch_parallelism: usize,
    ) -> Result<()> {
        self.register_table_provider_impl(
            schema,
            file_scan_tasks,
            table_name,
            false,
            false,
            batch_parallelism,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn register_table_provider_impl(
        &self,
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        table_name: &str,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        batch_parallelism: usize,
    ) -> Result<()> {
        let schema = schema_to_arrow_schema(schema)?;
        let data_file_table_provider = IcebergFileScanTaskTableProvider::new(
            file_scan_tasks,
            Arc::new(schema),
            self.file_io.clone(),
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        );

        self.ctx
            .register_table(table_name, Arc::new(data_file_table_provider))
            .unwrap();
        Ok(())
    }
}

/// SQL Builder for generating merge-on-read SQL queries
struct SqlBuilder<'a> {
    /// Column names to be projected in the query
    project_names: &'a Vec<String>,

    /// Position delete table name
    position_delete_table_name: Option<String>,

    /// Data file table name
    data_file_table_name: Option<String>,

    /// Flag indicating if file path and position columns are needed
    equality_delete_metadatas: &'a Vec<EqualityDeleteMetadata>,

    /// Flag indicating if position delete files are needed
    need_file_path_and_pos: bool,
}

impl<'a> SqlBuilder<'a> {
    /// Creates a new SQL Builder with the specified parameters
    fn new(
        project_names: &'a Vec<String>,
        position_delete_table_name: Option<String>,
        data_file_table_name: Option<String>,
        equality_delete_metadatas: &'a Vec<EqualityDeleteMetadata>,
        need_file_path_and_pos: bool,
    ) -> Self {
        Self {
            project_names,
            position_delete_table_name,
            data_file_table_name,
            equality_delete_metadatas,
            need_file_path_and_pos,
        }
    }

    /// Builds a merge-on-read SQL query
    ///
    /// This method constructs a SQL query that:
    /// 1. Selects the specified columns from the data file table
    /// 2. Optionally joins with position delete files to exclude deleted rows
    /// 3. Optionally joins with equality delete files to exclude rows based on equality conditions
    pub fn build_merge_on_read_sql(self) -> Result<String> {
        let data_file_table_name = self.data_file_table_name.as_ref().ok_or_else(|| {
            CompactionError::Config("Data file table name is not provided".to_string())
        })?;
        // Start with a basic SELECT query from the data file table
        let mut sql = format!(
            "SELECT {} FROM {}",
            self.project_names.join(","),
            data_file_table_name
        );

        // Add position delete join if needed
        // This excludes rows that have been deleted by position
        if self.need_file_path_and_pos {
            let position_delete_table_name =
                self.position_delete_table_name.as_ref().ok_or_else(|| {
                    CompactionError::Config(
                        "Position delete table name is not provided".to_string(),
                    )
                })?;
            sql.push_str(&format!(
                    " LEFT ANTI JOIN {position_delete_table_name} ON {data_file_table_name}.{SYS_HIDDEN_FILE_PATH} = {position_delete_table_name}.{SYS_HIDDEN_FILE_PATH} AND {data_file_table_name}.{SYS_HIDDEN_POS} = {position_delete_table_name}.{SYS_HIDDEN_POS}",
                ));
        }

        // Add equality delete join if needed
        // This excludes rows that match the equality conditions in the delete files
        if !self.equality_delete_metadatas.is_empty() {
            for metadata in self.equality_delete_metadatas {
                // LEFT ANTI JOIN ON equality delete table
                sql.push_str(&format!(
                    " LEFT ANTI JOIN {} ON {}",
                    metadata.equality_delete_table_name,
                    metadata
                        .equality_delete_join_names()
                        .iter()
                        .map(|name| format!(
                            "{data_file_table_name}.{name} = {}.{name}",
                            metadata.equality_delete_table_name
                        ))
                        .collect::<Vec<_>>()
                        .join(" AND ")
                ));

                // Add sequence number comparison if needed
                // This ensures that only newer deletes are applied
                sql.push_str(&format!(
                    " AND {data_file_table_name}.{SYS_HIDDEN_SEQ_NUM} < {}.{SYS_HIDDEN_SEQ_NUM}",
                    metadata.equality_delete_table_name
                ));
            }
        }

        Ok(sql)
    }
}

pub struct DataFusionTaskContext {
    pub(crate) data_file_schema: Option<Schema>,
    pub(crate) input_schema: Option<Schema>,
    pub(crate) data_files: Option<Vec<FileScanTask>>,
    pub(crate) position_delete_files: Option<Vec<FileScanTask>>,
    pub(crate) equality_delete_files: Option<Vec<FileScanTask>>,
    pub(crate) position_delete_schema: Option<Schema>,
    pub(crate) equality_delete_metadatas: Option<Vec<EqualityDeleteMetadata>>,
    pub(crate) exec_sql: String,
}

pub struct DataFusionTaskContextBuilder {
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

    fn build_position_schema() -> Result<Schema> {
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
        Ok(position_delete_schema)
    }

    // build data fusion task context
    pub fn build_merge_on_read(self) -> Result<DataFusionTaskContext> {
        let mut highest_field_id = self.schema.highest_field_id();
        // Build scheam for position delete file, file_path + pos
        let position_delete_schema = Self::build_position_schema()?;
        // Build schema for equality delete file, equality_ids + seq_num
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

        let sql_builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_delete_metadatas,
            need_file_path_and_pos,
        );
        let exec_sql = sql_builder.build_merge_on_read_sql()?;

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
            exec_sql,
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
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    /// Test building SQL with no delete files
    #[test]
    fn test_build_merge_on_read_sql_no_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_join_names = Vec::new();

        let builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_join_names,
            false,
        );
        assert_eq!(
            builder.build_merge_on_read_sql().unwrap(),
            format!(
                "SELECT {} FROM {}",
                project_names.join(","),
                DATA_FILE_TABLE
            )
        );
    }

    /// Test building SQL with position delete files
    #[test]
    fn test_build_merge_on_read_sql_with_position_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_join_names = Vec::new();

        let builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_join_names,
            true,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();

        assert!(sql.contains(&format!(
            "LEFT ANTI JOIN {POSITION_DELETE_TABLE} ON {DATA_FILE_TABLE}",
        )));
        assert!(sql.contains(&format!(
            "{POSITION_DELETE_TABLE} ON {DATA_FILE_TABLE}.{SYS_HIDDEN_FILE_PATH} = {POSITION_DELETE_TABLE}.{SYS_HIDDEN_FILE_PATH} AND {DATA_FILE_TABLE}.{SYS_HIDDEN_POS} = {POSITION_DELETE_TABLE}.{SYS_HIDDEN_POS}",
        )));
    }

    /// Test building SQL with equality delete files
    #[test]
    fn test_build_merge_on_read_sql_with_equality_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_delete_table_name = "test".to_owned();
        let equality_delete_metadatas = vec![EqualityDeleteMetadata::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::new(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                    true,
                ))])
                .build()
                .unwrap(),
            equality_delete_table_name.clone(),
        )];

        let builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_delete_metadatas,
            false,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();
        assert!(sql.contains(&format!(
            "LEFT ANTI JOIN {equality_delete_table_name} ON {DATA_FILE_TABLE}",
        )));
        assert!(sql.contains(&format!(
            "{equality_delete_table_name} ON {DATA_FILE_TABLE}.id = {equality_delete_table_name}.id",
        )));
    }

    /// Test building SQL with equality delete files AND sequence number comparison
    #[test]
    fn test_build_merge_on_read_sql_with_equality_deletes_and_seq_num() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];

        let equality_delete_table_name = "test".to_owned();
        let equality_delete_metadatas = vec![EqualityDeleteMetadata::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::new(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                    true,
                ))])
                .build()
                .unwrap(),
            equality_delete_table_name.clone(),
        )];

        let builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_delete_metadatas,
            false,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();
        assert!(sql.contains(&format!(
            "{DATA_FILE_TABLE}.{SYS_HIDDEN_SEQ_NUM} < {equality_delete_table_name}.{SYS_HIDDEN_SEQ_NUM}",
        )));
    }

    /// Test building SQL with both position AND equality delete files
    #[test]
    fn test_build_merge_on_read_sql_with_both_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_delete_table_name = "test".to_owned();
        let equality_delete_metadatas = vec![EqualityDeleteMetadata::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::new(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                    true,
                ))])
                .build()
                .unwrap(),
            equality_delete_table_name.clone(),
        )];

        let builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_delete_metadatas,
            true,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();
        assert!(sql.contains(&format!(
            "LEFT ANTI JOIN {POSITION_DELETE_TABLE} ON {DATA_FILE_TABLE}"
        )));
        assert!(sql.contains(&format!(
            "LEFT ANTI JOIN {equality_delete_table_name} ON {DATA_FILE_TABLE}",
        )));
        assert!(sql.contains(&format!(
            "{POSITION_DELETE_TABLE} ON {DATA_FILE_TABLE}.{SYS_HIDDEN_FILE_PATH} = {POSITION_DELETE_TABLE}.{SYS_HIDDEN_FILE_PATH} AND {DATA_FILE_TABLE}.{SYS_HIDDEN_POS} = {POSITION_DELETE_TABLE}.{SYS_HIDDEN_POS}",
        )));
        assert!(sql.contains(&format!(
            "{equality_delete_table_name} ON {DATA_FILE_TABLE}.id = {equality_delete_table_name}.id",
        )));
        assert!(sql.contains(&format!(
            "{DATA_FILE_TABLE}.{SYS_HIDDEN_SEQ_NUM} < {equality_delete_table_name}.{SYS_HIDDEN_SEQ_NUM}",
        )));
    }

    /// Test building SQL with multiple equality delete files
    #[test]
    fn test_build_merge_on_read_sql_with_multiple_equality_deletes_schema() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];

        let equality_delete_table_name_1 = "test_1".to_owned();
        let equality_delete_table_name_2 = "test_2".to_owned();
        let equality_delete_metadatas = vec![
            EqualityDeleteMetadata::new(
                Schema::builder()
                    .with_fields(vec![Arc::new(NestedField::new(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                        true,
                    ))])
                    .build()
                    .unwrap(),
                equality_delete_table_name_1.clone(),
            ),
            EqualityDeleteMetadata::new(
                Schema::builder()
                    .with_fields(vec![Arc::new(NestedField::new(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                        true,
                    ))])
                    .build()
                    .unwrap(),
                equality_delete_table_name_2.clone(),
            ),
        ];

        let builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_delete_metadatas,
            false,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();

        assert!(sql.contains(
            &("LEFT ANTI JOIN ".to_owned()
                + &equality_delete_table_name_1
                + " ON "
                + DATA_FILE_TABLE)
        ));
        assert!(sql.contains(
            &("LEFT ANTI JOIN ".to_owned()
                + &equality_delete_table_name_2
                + " ON "
                + DATA_FILE_TABLE)
        ));
        assert!(sql.contains(
            &(equality_delete_table_name_1.clone()
                + " ON "
                + DATA_FILE_TABLE
                + ".id = "
                + &equality_delete_table_name_1
                + ".id")
        ));
        assert!(sql.contains(
            &(equality_delete_table_name_2.clone()
                + " ON "
                + DATA_FILE_TABLE
                + ".id = "
                + &equality_delete_table_name_2
                + ".id")
        ));

        // Check that the sequence number comparison is present for both equality delete tables
        assert!(sql.contains(
            &(DATA_FILE_TABLE.to_owned()
                + "."
                + SYS_HIDDEN_SEQ_NUM
                + " < "
                + &equality_delete_table_name_1
                + "."
                + SYS_HIDDEN_SEQ_NUM)
        ));
        assert!(sql.contains(
            &(DATA_FILE_TABLE.to_owned()
                + "."
                + SYS_HIDDEN_SEQ_NUM
                + " < "
                + &equality_delete_table_name_2
                + "."
                + SYS_HIDDEN_SEQ_NUM)
        ));
    }

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
            "sys_hidden_seq_num"
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
