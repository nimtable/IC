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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;

use super::iceberg_file_task_scan::IcebergFileTaskScan;

/// A table provider for iceberg file scan tasks
#[derive(Debug, Clone)]
pub struct IcebergFileScanTaskTableProvider {
    file_scan_tasks: Vec<FileScanTask>,
    schema: ArrowSchemaRef,
    file_io: FileIO,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
    batch_parallelism: usize,
}
impl IcebergFileScanTaskTableProvider {
    pub fn new(
        file_scan_tasks: Vec<FileScanTask>,
        schema: ArrowSchemaRef,
        file_io: FileIO,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        batch_parallelism: usize,
    ) -> Self {
        Self {
            file_scan_tasks,
            schema,
            file_io,
            need_seq_num,
            need_file_path_and_pos,
            batch_parallelism,
        }
    }
}
#[async_trait]
impl TableProvider for IcebergFileScanTaskTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Scans the iceberg file scan tasks
    ///
    /// This function creates an execution plan for scanning the iceberg file scan tasks.
    /// It uses the IcebergFileTaskScan struct to create the execution plan.    
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IcebergFileTaskScan::new(
            self.file_scan_tasks.clone(),
            self.schema.clone(),
            projection,
            filters,
            &self.file_io,
            self.need_seq_num,
            self.need_file_path_and_pos,
            self.batch_parallelism,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, datafusion::error::DataFusionError>
    {
        // Push down all filters, as a single source of truth, the scanner will drop the filters which couldn't be push down
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
