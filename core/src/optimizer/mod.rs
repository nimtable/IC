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

pub mod big_size_data_file_without_position;

use std::sync::LazyLock;

use async_trait::async_trait;
use big_size_data_file_without_position::BigSizeDataFileWithoutPosition;
use iceberg::io::FileIO;

use crate::error::Result;
use crate::executor::InputFileScanTasks;

pub static ICEBERG_FULL_COMPACTION_OPTIMIZER: LazyLock<IcebergCompactionOptimizer> =
    LazyLock::new(|| {
        IcebergCompactionOptimizer::new(vec![Box::new(BigSizeDataFileWithoutPosition::crated())])
    });

pub struct IcebergCompactionOptimizer {
    compaction_rules: Vec<Box<dyn Rule>>,
}

impl IcebergCompactionOptimizer {
    pub fn new(compaction_rules: Vec<Box<dyn Rule>>) -> Self {
        Self { compaction_rules }
    }

    pub async fn optimize(&self, optimizer_context: OptimizerContext) -> Result<OptimizerContext> {
        let mut optimizer_context = optimizer_context;
        for rule in &self.compaction_rules {
            optimizer_context = rule.apply(optimizer_context).await?;
        }
        Ok(optimizer_context)
    }
}

pub struct OptimizerContext {
    pub input_file_scan_tasks: InputFileScanTasks,
    pub file_io: FileIO,
}

#[async_trait]
pub trait Rule: Send + Sync {
    async fn apply(&self, optimizer_context: OptimizerContext) -> Result<OptimizerContext>;
}
