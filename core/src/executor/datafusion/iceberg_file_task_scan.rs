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

use std::any::Any;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::sync::Arc;
use std::vec;

use async_stream::try_stream;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef as ArrowSchemaRef};
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg_datafusion::physical_plan::expr_to_predicate::convert_filters_to_predicate;
use iceberg_datafusion::to_datafusion_error;

use super::SYS_HIDDEN_SEQ_NUM;

/// An execution plan for scanning iceberg file scan tasks
#[derive(Debug)]
pub(crate) struct IcebergFileTaskScan {
    file_scan_tasks_group: Vec<Vec<FileScanTask>>,
    plan_properties: PlanProperties,
    projection: Option<Vec<String>>,
    predicates: Option<Predicate>,
    file_io: FileIO,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
}

impl IcebergFileTaskScan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        file_scan_tasks: Vec<FileScanTask>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        file_io: &FileIO,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        batch_parallelism: usize,
    ) -> Self {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let file_scan_tasks_group = split_n_vecs(file_scan_tasks, batch_parallelism);
        let plan_properties =
            Self::compute_properties(output_schema.clone(), file_scan_tasks_group.len());
        let projection = get_column_names(schema.clone(), projection);
        let predicates = convert_filters_to_predicate(filters);

        Self {
            file_scan_tasks_group,
            plan_properties,
            projection,
            predicates,
            file_io: file_io.clone(),
            need_seq_num,
            need_file_path_and_pos,
        }
    }

    /// Computes [`PlanProperties`] used in query optimization.
    fn compute_properties(schema: ArrowSchemaRef, partitioning_size: usize) -> PlanProperties {
        // TODO:
        // This is more or less a placeholder, to be replaced
        // once we support output-partitioning
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partitioning_size),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

/// Uniformly distribute scan tasks to compute nodes.
/// It's deterministic so that it can best utilize the data locality.
///
/// # Arguments
/// * `file_scan_tasks`: The file scan tasks to be split.
/// * `split_num`: The number of splits to be created.
///
/// This algorithm is based on a min-heap. It will push all groups into the heap, and then pop the smallest group and add the file scan task to it.
/// Ensure that the total length of each group is as balanced as possible.
/// The time complexity is O(n log k), where n is the number of file scan tasks and k is the number of splits.
/// The space complexity is O(k), where k is the number of splits.
/// The algorithm is stable, so the order of the file scan tasks will be preserved.
fn split_n_vecs(file_scan_tasks: Vec<FileScanTask>, split_num: usize) -> Vec<Vec<FileScanTask>> {
    use std::cmp::{Ordering, Reverse};

    #[derive(Default)]
    struct FileScanTaskGroup {
        idx: usize,
        tasks: Vec<FileScanTask>,
        total_length: u64,
    }

    impl Ord for FileScanTaskGroup {
        fn cmp(&self, other: &Self) -> Ordering {
            // when total_length is the same, we will sort by index
            if self.total_length == other.total_length {
                self.idx.cmp(&other.idx)
            } else {
                self.total_length.cmp(&other.total_length)
            }
        }
    }

    impl PartialOrd for FileScanTaskGroup {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Eq for FileScanTaskGroup {}

    impl PartialEq for FileScanTaskGroup {
        fn eq(&self, other: &Self) -> bool {
            self.total_length == other.total_length
        }
    }

    let mut heap = BinaryHeap::new();
    // push all groups into heap
    for idx in 0..split_num {
        heap.push(Reverse(FileScanTaskGroup {
            idx,
            tasks: vec![],
            total_length: 0,
        }));
    }

    for file_task in file_scan_tasks {
        let mut group = heap.peek_mut().unwrap();
        group.0.total_length += file_task.length;
        group.0.tasks.push(file_task);
    }

    // convert heap into vec and extract tasks
    heap.into_vec()
        .into_iter()
        .map(|reverse_group| reverse_group.0.tasks)
        .collect()
}

impl ExecutionPlan for IcebergFileTaskScan {
    fn name(&self) -> &str {
        "IcebergFileTaskScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let fut = get_batch_stream(
            self.file_io.clone(),
            self.file_scan_tasks_group[partition].clone(),
            self.need_seq_num,
            self.need_file_path_and_pos,
        );
        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Gets a stream of record batches from a list of file scan tasks
async fn get_batch_stream(
    file_io: FileIO,
    file_scan_tasks: Vec<FileScanTask>,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let stream = try_stream! {
        for task in file_scan_tasks {
            let file_path = task.data_file_path.clone();
            let data_file_content = task.data_file_content;
            let sequence_number = task.sequence_number;
            let task_stream = futures::stream::iter(vec![Ok(task)]).boxed();
            let arrow_reader_builder = ArrowReaderBuilder::new(file_io.clone());
            let mut batch_stream = arrow_reader_builder.build()
                .read(task_stream)
                .await
                .map_err(to_datafusion_error)?;
            let mut index_start = 0;
            while let Some(batch) = batch_stream.next().await {
                let mut batch = batch.map_err(to_datafusion_error)?;
                let batch = match data_file_content {
                    iceberg::spec::DataContentType::Data => {
                        // add sequence number if needed
                        if need_seq_num {
                            batch = add_seq_num_into_batch(batch, sequence_number)?;
                        }
                        // add file path and position if needed
                        if need_file_path_and_pos {
                            batch = add_file_path_pos_into_batch(batch, &file_path, index_start)?;
                            index_start += batch.num_rows() as i64;
                        }
                        batch
                    }
                    iceberg::spec::DataContentType::PositionDeletes => {
                        batch
                    },
                    iceberg::spec::DataContentType::EqualityDeletes => {
                        add_seq_num_into_batch(batch, sequence_number)?
                    },
                };
                yield batch;
            }
        }
    };
    Ok(Box::pin(stream))
}

/// Adds a sequence number column to a record batch
fn add_seq_num_into_batch(batch: RecordBatch, seq_num: i64) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let seq_num_field = Arc::new(Field::new(
        SYS_HIDDEN_SEQ_NUM,
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    ));
    let mut new_fields = schema.fields().to_vec();
    new_fields.push(seq_num_field);
    let new_schema = Arc::new(Schema::new(new_fields));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(vec![seq_num; batch.num_rows()])));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))
}

/// Adds a file path and position column to a record batch
fn add_file_path_pos_into_batch(
    batch: RecordBatch,
    file_path: &str,
    index_start: i64,
) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let file_path_field = Arc::new(Field::new(
        "file_path",
        datafusion::arrow::datatypes::DataType::Utf8,
        false,
    ));
    let pos_field = Arc::new(Field::new(
        "pos",
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    ));
    let mut new_fields = schema.fields().to_vec();
    new_fields.push(file_path_field);
    new_fields.push(pos_field);
    let new_schema = Arc::new(Schema::new(new_fields));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![
        file_path;
        batch.num_rows()
    ])));
    columns.push(Arc::new(Int64Array::from_iter(
        (index_start..(index_start + batch.num_rows() as i64)).collect::<Vec<i64>>(),
    )));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))
}

impl DisplayAs for IcebergFileTaskScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "IcebergTableScan projection:[{}] predicate:[{}]",
            self.projection
                .clone()
                .map_or(String::new(), |v| v.join(",")),
            self.predicates
                .clone()
                .map_or(String::from(""), |p| format!("{}", p))
        )
    }
}

pub fn get_column_names(
    schema: ArrowSchemaRef,
    projection: Option<&Vec<usize>>,
) -> Option<Vec<String>> {
    projection.map(|v| {
        v.iter()
            .map(|p| schema.field(*p).name().clone())
            .collect::<Vec<String>>()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::scan::FileScanTask;
    use iceberg::spec::{DataContentType, Schema};
    use std::sync::Arc;

    fn create_file_scan_task(length: u64, file_id: u64) -> FileScanTask {
        FileScanTask {
            length,
            start: 0,
            record_count: Some(0),
            data_file_path: format!("test_{}.parquet", file_id),
            data_file_content: DataContentType::Data,
            data_file_format: iceberg::spec::DataFileFormat::Parquet,
            schema: Arc::new(Schema::builder().build().unwrap()),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            sequence_number: 0,
            equality_ids: vec![],
            file_size_in_bytes: 0,
        }
    }

    #[test]
    fn test_split_n_vecs_basic() {
        let file_scan_tasks = (1..=12)
            .map(|i| create_file_scan_task(i + 100, i))
            .collect::<Vec<_>>();

        let groups = split_n_vecs(file_scan_tasks, 3);

        assert_eq!(groups.len(), 3);

        let group_lengths: Vec<u64> = groups
            .iter()
            .map(|group| group.iter().map(|task| task.length).sum())
            .collect();

        let max_length = *group_lengths.iter().max().unwrap();
        let min_length = *group_lengths.iter().min().unwrap();
        assert!(max_length - min_length <= 10, "Groups should be balanced");

        let total_tasks: usize = groups.iter().map(|group| group.len()).sum();
        assert_eq!(total_tasks, 12);
    }

    #[test]
    fn test_split_n_vecs_empty() {
        let file_scan_tasks = Vec::new();
        let groups = split_n_vecs(file_scan_tasks, 3);
        assert_eq!(groups.len(), 3);
        assert!(groups.iter().all(|group| group.is_empty()));
    }

    #[test]
    fn test_split_n_vecs_single_task() {
        let file_scan_tasks = vec![create_file_scan_task(100, 1)];
        let groups = split_n_vecs(file_scan_tasks, 3);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups.iter().filter(|group| !group.is_empty()).count(), 1);
    }

    #[test]
    fn test_split_n_vecs_uneven_distribution() {
        let file_scan_tasks = vec![
            create_file_scan_task(1000, 1),
            create_file_scan_task(100, 2),
            create_file_scan_task(100, 3),
            create_file_scan_task(100, 4),
            create_file_scan_task(100, 5),
        ];

        let groups = split_n_vecs(file_scan_tasks, 2);
        assert_eq!(groups.len(), 2);

        let group_with_large_task = groups
            .iter()
            .find(|group| group.iter().any(|task| task.length == 1000))
            .unwrap();
        assert_eq!(group_with_large_task.len(), 1);
    }

    #[test]
    fn test_split_n_vecs_same_files_distribution() {
        let file_scan_tasks = vec![
            create_file_scan_task(100, 1),
            create_file_scan_task(100, 2),
            create_file_scan_task(100, 3),
            create_file_scan_task(100, 4),
            create_file_scan_task(100, 5),
            create_file_scan_task(100, 6),
            create_file_scan_task(100, 7),
            create_file_scan_task(100, 8),
        ];

        let groups = split_n_vecs(file_scan_tasks.clone(), 4)
            .iter()
            .map(|g| {
                g.iter()
                    .map(|task| task.data_file_path.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for _ in 0..10000 {
            let groups_2 = split_n_vecs(file_scan_tasks.clone(), 4)
                .iter()
                .map(|g| {
                    g.iter()
                        .map(|task| task.data_file_path.clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            assert_eq!(groups, groups_2);
        }
    }
}
