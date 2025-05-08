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

use super::{
    DATA_FILE_TABLE, EqualityDeleteMetadata, POSITION_DELETE_TABLE, SYS_HIDDEN_FILE_PATH,
    SYS_HIDDEN_POS, SYS_HIDDEN_SEQ_NUM,
};

/// SQL Builder for generating merge-on-read SQL queries
pub struct SqlBuilder<'a> {
    /// Column names to be projected in the query
    project_names: &'a Vec<String>,
    /// Equality delete metadata to be used in the query
    equality_delete_metadatas: &'a Vec<EqualityDeleteMetadata>,

    /// Whether to include file path AND position in position delete joins
    need_file_path_and_pos: bool,
}

impl<'a> SqlBuilder<'a> {
    /// Creates a new SQL Builder with the specified parameters
    pub(crate) fn new(
        project_names: &'a Vec<String>,
        equality_delete_metadatas: &'a Vec<EqualityDeleteMetadata>,
        need_file_path_and_pos: bool,
    ) -> Self {
        Self {
            project_names,
            equality_delete_metadatas,
            need_file_path_and_pos,
        }
    }

    /// Builds a merge-on-read SQL query
    ///
    /// This method constructs a SQL query that:
    /// 1. Selects the specified columns from the data file table
    /// 2. Optionally joins with position delete files to exclude deleted rows
    /// 3. Optionally joins with equality delete files to exclude rows based ON equality conditions
    pub fn build_merge_on_read_sql(&self) -> String {
        // Start with a basic SELECT query from the data file table
        let mut sql = format!(
            "SELECT {} FROM {}",
            self.project_names.join(","),
            DATA_FILE_TABLE
        );

        // Add position delete join if needed
        // This excludes rows that have been deleted by position
        if self.need_file_path_and_pos {
            sql.push_str(&format!(
                " LEFT ANTI JOIN {POSITION_DELETE_TABLE} ON {DATA_FILE_TABLE}.{SYS_HIDDEN_FILE_PATH} = {POSITION_DELETE_TABLE}.{SYS_HIDDEN_FILE_PATH} AND {DATA_FILE_TABLE}.{SYS_HIDDEN_POS} = {POSITION_DELETE_TABLE}.{SYS_HIDDEN_POS}",
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
                            "{DATA_FILE_TABLE}.{name} = {}.{name}",
                            metadata.equality_delete_table_name
                        ))
                        .collect::<Vec<_>>()
                        .join(" AND ")
                ));

                // Add sequence number comparison if needed
                // This ensures that only newer deletes are applied
                sql.push_str(&format!(
                    " AND {DATA_FILE_TABLE}.{SYS_HIDDEN_SEQ_NUM} < {}.{SYS_HIDDEN_SEQ_NUM}",
                    metadata.equality_delete_table_name
                ));
            }
        }

        sql
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::{
        scan::FileScanTask,
        spec::{NestedField, PrimitiveType, Schema, Type},
    };
    use std::sync::Arc;

    /// Creates a test schema with id AND name fields
    fn create_test_schema() -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_fields(vec![
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
                ])
                .build()
                .unwrap(),
        )
    }

    /// Creates a test file scan task
    fn create_test_file_scan_task() -> FileScanTask {
        FileScanTask {
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: "test.parquet".to_owned(),
            data_file_content: iceberg::spec::DataContentType::Data,
            data_file_format: iceberg::spec::DataFileFormat::Parquet,
            schema: create_test_schema(),
            project_field_ids: vec![1, 2],
            predicate: None,
            deletes: vec![],
            sequence_number: 0,
            equality_ids: vec![],
            file_size_in_bytes: 0,
        }
    }

    /// Test building SQL with no delete files
    #[test]
    fn test_build_merge_on_read_sql_no_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_join_names = Vec::new();

        let builder = SqlBuilder::new(&project_names, &equality_join_names, false);
        assert_eq!(
            builder.build_merge_on_read_sql(),
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

        let builder = SqlBuilder::new(&project_names, &equality_join_names, true);
        let sql = builder.build_merge_on_read_sql();

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
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1];
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

        let builder = SqlBuilder::new(&project_names, &equality_delete_metadatas, false);
        let sql = builder.build_merge_on_read_sql();
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
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1];

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

        let builder = SqlBuilder::new(&project_names, &equality_delete_metadatas, false);
        let sql = builder.build_merge_on_read_sql();
        assert!(sql.contains(&format!(
            "{DATA_FILE_TABLE}.{SYS_HIDDEN_SEQ_NUM} < {equality_delete_table_name}.{SYS_HIDDEN_SEQ_NUM}",
        )));
    }

    /// Test building SQL with both position AND equality delete files
    #[test]
    fn test_build_merge_on_read_sql_with_both_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1];
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

        let builder = SqlBuilder::new(&project_names, &equality_delete_metadatas, true);
        let sql = builder.build_merge_on_read_sql();
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
        let mut task = create_test_file_scan_task();
        task.equality_ids = vec![1, 2];

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

        let builder = SqlBuilder::new(&project_names, &equality_delete_metadatas, false);
        let sql = builder.build_merge_on_read_sql();

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
}
