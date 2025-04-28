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

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::array::StringArray,
    prelude::{SessionConfig, SessionContext},
};
use futures::StreamExt;

use crate::{
    error::Result,
    executor::{
        InputFileScanTasks,
        datafusion::datafusion_processor::{
            DataFusionTaskContext, DatafusionProcessor, SYS_HIDDEN_FILE_PATH,
        },
    },
};

use super::{OptimizerContext, Rule};

// Need to set a more precise value.
const LARGE_DATAFILE_SIZE_THRESHOLD: u64 = 1024 * 1024 * 1024; // 1GB
const POSITION_DELETE_MAX_ROWS: u64 = 1000;

pub struct BigSizeDataFileWithoutPosition {}

impl BigSizeDataFileWithoutPosition {
    pub fn crated() -> Self {
        BigSizeDataFileWithoutPosition {}
    }
}

#[async_trait]
impl Rule for BigSizeDataFileWithoutPosition {
    async fn apply(&self, optimizer_context: OptimizerContext) -> Result<OptimizerContext> {
        let session_config = SessionConfig::new();
        let ctx = Arc::new(SessionContext::new_with_config(session_config));
        let InputFileScanTasks {
            data_files,
            position_delete_files,
            equality_delete_files,
        } = optimizer_context.input_file_scan_tasks;
        if !equality_delete_files.is_empty()
            || position_delete_files.is_empty()
            || data_files.is_empty()
        {
            return Ok(OptimizerContext {
                input_file_scan_tasks: InputFileScanTasks {
                    data_files,
                    position_delete_files,
                    equality_delete_files,
                },
                file_io: optimizer_context.file_io,
            });
        }
        let position_delete_rows_num: u64 = position_delete_files
            .iter()
            .map(|f| f.record_count.unwrap_or(0))
            .sum();
        if position_delete_rows_num > POSITION_DELETE_MAX_ROWS {
            return Ok(OptimizerContext {
                input_file_scan_tasks: InputFileScanTasks {
                    data_files,
                    position_delete_files,
                    equality_delete_files,
                },
                file_io: optimizer_context.file_io,
            });
        }

        let mut data_file_path_set = data_files
            .iter()
            .filter_map(|f| {
                if f.file_size_in_bytes > LARGE_DATAFILE_SIZE_THRESHOLD {
                    Some(f.data_file_path().to_string())
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();

        let datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_position_delete_files(position_delete_files.clone())
            .build_position_scan()?;
        let (df, _) = DatafusionProcessor::new(
            ctx,
            datafusion_task_ctx,
            1,
            optimizer_context.file_io.clone(),
        )
        .execute()
        .await?;
        let mut batch = df.execute_stream().await?;

        while let Some(b) = batch.as_mut().next().await {
            let b = b?;
            let b = b
                .column_by_name(SYS_HIDDEN_FILE_PATH)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for s in b.iter().flatten() {
                data_file_path_set.remove(s);
                if data_file_path_set.is_empty() {
                    break;
                }
            }
        }

        let new_data_files = if !data_file_path_set.is_empty() {
            data_files
                .into_iter()
                .filter(|f| !data_file_path_set.contains(f.data_file_path()))
                .collect::<Vec<_>>()
        } else {
            data_files
        };
        Ok(OptimizerContext {
            input_file_scan_tasks: InputFileScanTasks {
                data_files: new_data_files,
                position_delete_files,
                equality_delete_files,
            },
            file_io: optimizer_context.file_io,
        })
    }
}
