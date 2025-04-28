use std::sync::Arc;

use crate::error::Result;
use ic_codegen::compactor::DataFile;
use ic_codegen::compactor::FileIoBuilder;
use ic_codegen::compactor::FileScanTaskDescriptor;
use ic_codegen::compactor::Literal;
use ic_codegen::compactor::MapLiteral;
use ic_codegen::compactor::NestedFieldDescriptor;
use ic_codegen::compactor::OptionalLiteral;
use ic_codegen::compactor::PartitionSpec;
use ic_codegen::compactor::PrimitiveLiteral;
use ic_codegen::compactor::RewriteFilesRequest as PbRewriteFilesRequest;
use ic_codegen::compactor::RewriteFilesResponse as PbRewriteFilesResponse;
use ic_codegen::compactor::RewriteFilesStat as PbRewriteFilesStat;
use ic_codegen::compactor::SchemaDescriptor;
use ic_codegen::compactor::StructLiteralDescriptor;
use ic_codegen::compactor::Transform;
use ic_codegen::compactor::literal;
use ic_codegen::compactor::nested_field_descriptor::FieldType;
use ic_codegen::compactor::primitive_literal::KindLiteral;
use ic_codegen::compactor::primitive_literal::KindWithoutInnerLiteral;
use ic_codegen::compactor::primitive_type::Kind;
use ic_codegen::compactor::primitive_type::KindWithoutInner;
use iceberg::spec::DataContentType;
use iceberg::spec::NestedField;
use iceberg::spec::Type;
use iceberg::{scan::FileScanTask, spec::Schema};

use crate::CompactionConfig;
use crate::CompactionError;
use crate::executor::InputFileScanTasks;
use crate::executor::RewriteFilesRequest;
use crate::executor::RewriteFilesResponse;

pub struct PbRewriteFilesRequestDecoder {
    rewrite_file_request_proto: PbRewriteFilesRequest,
}

impl PbRewriteFilesRequestDecoder {
    pub fn new(rewrite_file_request_proto: PbRewriteFilesRequest) -> Self {
        Self {
            rewrite_file_request_proto,
        }
    }

    pub fn decode(self) -> Result<RewriteFilesRequest> {
        let PbRewriteFilesRequest {
            file_io_builder,
            file_scan_task_descriptor,
            schema,
            dir_path,
            rewrite_file_config,
            partition_spec,
        } = self.rewrite_file_request_proto;
        let file_io = Self::decode_file_io(
            file_io_builder
                .ok_or_else(|| CompactionError::Config("file_io is required".to_owned()))?,
        )?;
        let (input_file_scan_tasks, schema) = Self::decode_file_scan_tasks_and_schema(
            file_scan_task_descriptor,
            schema.ok_or_else(|| CompactionError::Config("schema is required".to_owned()))?,
        )
        .map_err(|e| {
            CompactionError::Config(format!("Failed to decode file scan tasks schema: {}", e))
        })?;
        let config = serde_json::from_value::<CompactionConfig>(
            serde_json::to_value(rewrite_file_config).map_err(|e| {
                CompactionError::Config(format!(
                    "Failed to convert rewrite_file_config to JSON value: {}",
                    e
                ))
            })?,
        )
        .map_err(|e| {
            CompactionError::Config(format!("Failed to decode CompactionConfig: {}", e))
        })?;

        let partition_spec = Self::decode_partition_spec(partition_spec, schema.clone())?
            .unwrap_or_else(iceberg::spec::PartitionSpec::unpartition_spec);

        Ok(RewriteFilesRequest {
            file_io,
            schema,
            input_file_scan_tasks,
            config: Arc::new(config),
            dir_path,
            partition_spec: Arc::new(partition_spec),
        })
    }

    /// Decode an Iceberg FileIO from a protobuf FileIOBuilder
    fn decode_file_io(file_io_builder_pb: FileIoBuilder) -> Result<iceberg::io::FileIO> {
        let file_io_builder = iceberg::io::FileIO::from_path(file_io_builder_pb.scheme_str)?;
        Ok(file_io_builder
            .with_props(file_io_builder_pb.props)
            .build()?)
    }

    /// Decode file scan tasks and schema from protobuf descriptors
    ///
    /// This function converts protobuf descriptors into Iceberg file scan tasks and schema.
    /// It handles different types of files: data files, position delete files, and equality delete files.
    pub fn decode_file_scan_tasks_and_schema(
        file_scan_task_descriptors: Vec<FileScanTaskDescriptor>,
        schema: SchemaDescriptor,
    ) -> Result<(InputFileScanTasks, Arc<Schema>)> {
        let mut data_files = vec![];
        let mut position_delete_files = vec![];
        let mut equality_delete_files = vec![];
        let schema = Arc::new(Self::decode_schema(schema)?);
        for file_scan_task_descriptor in file_scan_task_descriptors {
            let mut file_scan_task = FileScanTask {
                start: 0,
                length: file_scan_task_descriptor.length,
                record_count: Some(file_scan_task_descriptor.record_count),
                data_file_path: file_scan_task_descriptor.data_file_path,
                data_file_content: DataContentType::try_from(
                    file_scan_task_descriptor.data_file_content,
                )?,
                data_file_format: Self::decode_data_file_format(
                    file_scan_task_descriptor.data_file_format,
                ),
                schema: schema.clone(),
                project_field_ids: file_scan_task_descriptor.project_field_ids,
                predicate: None,
                deletes: vec![],
                sequence_number: file_scan_task_descriptor.sequence_number,
                equality_ids: file_scan_task_descriptor.equality_ids,
                file_size_in_bytes: file_scan_task_descriptor.file_size_in_bytes,
            };
            match file_scan_task.data_file_content {
                iceberg::spec::DataContentType::Data => {
                    data_files.push(file_scan_task);
                }
                iceberg::spec::DataContentType::PositionDeletes => {
                    file_scan_task.project_field_ids = vec![];
                    position_delete_files.push(file_scan_task);
                }
                iceberg::spec::DataContentType::EqualityDeletes => {
                    file_scan_task.project_field_ids = file_scan_task.equality_ids.clone();
                    equality_delete_files.push(file_scan_task);
                }
            }
        }
        Ok((
            InputFileScanTasks {
                data_files,
                position_delete_files,
                equality_delete_files,
            },
            schema,
        ))
    }

    /// decode an Iceberg schema from a protobuf schema descriptor
    fn decode_schema(schema: SchemaDescriptor) -> Result<Schema> {
        let iceberg_schema_builder = Schema::builder();
        let fields = schema
            .fields
            .into_iter()
            .map(|field| {
                let iceberg_field = Self::decode_field(&field)?;
                Ok::<Arc<NestedField>, CompactionError>(Arc::new(iceberg_field))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(iceberg_schema_builder.with_fields(fields).build()?)
    }

    /// Builds an Iceberg nested field from a protobuf field descriptor
    fn decode_field(field: &NestedFieldDescriptor) -> Result<NestedField> {
        let field_type = match field
            .field_type
            .as_ref()
            .ok_or_else(|| CompactionError::Config("field_type is null".to_owned()))?
        {
            FieldType::Primitive(primitive_type) => {
                match primitive_type
                    .kind
                    .as_ref()
                    .ok_or_else(|| CompactionError::Config("kind is null".to_owned()))?
                {
                    Kind::KindWithoutInner(inner) => {
                        match KindWithoutInner::try_from(*inner).map_err(|e| {
                            CompactionError::Config(format!("failed to parse kind: {}", e))
                        })? {
                            KindWithoutInner::Boolean => {
                                Type::Primitive(iceberg::spec::PrimitiveType::Boolean)
                            }
                            KindWithoutInner::Int => {
                                Type::Primitive(iceberg::spec::PrimitiveType::Int)
                            }
                            KindWithoutInner::Long => {
                                Type::Primitive(iceberg::spec::PrimitiveType::Long)
                            }
                            KindWithoutInner::Float => {
                                Type::Primitive(iceberg::spec::PrimitiveType::Float)
                            }
                            KindWithoutInner::Double => {
                                Type::Primitive(iceberg::spec::PrimitiveType::Double)
                            }
                            KindWithoutInner::Date => {
                                Type::Primitive(iceberg::spec::PrimitiveType::Date)
                            }
                            KindWithoutInner::Time => {
                                Type::Primitive(iceberg::spec::PrimitiveType::Time)
                            }
                            KindWithoutInner::Timestamp => {
                                Type::Primitive(iceberg::spec::PrimitiveType::Timestamp)
                            }
                            KindWithoutInner::Timestamptz => {
                                Type::Primitive(iceberg::spec::PrimitiveType::Timestamptz)
                            }
                            KindWithoutInner::TimestampNs => {
                                Type::Primitive(iceberg::spec::PrimitiveType::TimestampNs)
                            }
                            KindWithoutInner::TimestamptzNs => {
                                Type::Primitive(iceberg::spec::PrimitiveType::TimestamptzNs)
                            }
                            KindWithoutInner::String => {
                                Type::Primitive(iceberg::spec::PrimitiveType::String)
                            }
                            KindWithoutInner::Uuid => {
                                Type::Primitive(iceberg::spec::PrimitiveType::Uuid)
                            }
                            KindWithoutInner::Binary => {
                                Type::Primitive(iceberg::spec::PrimitiveType::Binary)
                            }
                        }
                    }
                    Kind::Decimal(decimal) => {
                        Type::Primitive(iceberg::spec::PrimitiveType::Decimal {
                            precision: decimal.precision,
                            scale: decimal.scale,
                        })
                    }
                    Kind::Fixed(size) => {
                        Type::Primitive(iceberg::spec::PrimitiveType::Fixed(*size))
                    }
                }
            }
            FieldType::Struct(struct_type) => {
                let fields = struct_type
                    .fields
                    .iter()
                    .map(|field| {
                        Ok::<Arc<NestedField>, CompactionError>(Arc::new(Self::decode_field(
                            field,
                        )?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Type::Struct(iceberg::spec::StructType::new(fields))
            }
            FieldType::List(nested_field_descriptor) => {
                let element_field = Self::decode_field(nested_field_descriptor)?;
                Type::List(iceberg::spec::ListType::new(Arc::new(element_field)))
            }
            FieldType::Map(map_type) => {
                let key_field =
                    Self::decode_field(map_type.key_field.as_ref().ok_or_else(|| {
                        CompactionError::Config("can't find key_field in map".to_owned())
                    })?)?;
                let value_field =
                    Self::decode_field(map_type.value_field.as_ref().ok_or_else(|| {
                        CompactionError::Config("can't find value_field in map".to_owned())
                    })?)?;
                Type::Map(iceberg::spec::MapType::new(
                    Arc::new(key_field),
                    Arc::new(value_field),
                ))
            }
        };
        Ok(NestedField::new(
            field.id,
            field.name.clone(),
            field_type,
            field.required,
        ))
    }

    /// Converts a protobuf data file format to an Iceberg data file format
    fn decode_data_file_format(data_file_format: i32) -> iceberg::spec::DataFileFormat {
        match data_file_format {
            0 => iceberg::spec::DataFileFormat::Avro,
            1 => iceberg::spec::DataFileFormat::Orc,
            2 => iceberg::spec::DataFileFormat::Parquet,
            _ => unreachable!(),
        }
    }

    /// Builds an Iceberg PartitionSpec from a protobuf PartitionSpec
    ///
    /// This function converts a protobuf PartitionSpec into an Iceberg PartitionSpec.
    /// It handles the conversion of partition fields and their transforms.
    pub fn decode_partition_spec(
        partition_spec: Option<PartitionSpec>,
        schema: Arc<Schema>,
    ) -> Result<Option<iceberg::spec::PartitionSpec>> {
        match partition_spec {
            None => Ok(None),
            Some(partition_spec) => {
                let mut builder = iceberg::spec::PartitionSpec::builder(schema);
                builder = builder.with_spec_id(partition_spec.spec_id);
                let fields = partition_spec
                    .partition_fields
                    .into_iter()
                    .map(|field| {
                        Ok::<iceberg::spec::UnboundPartitionField, CompactionError>(
                            iceberg::spec::UnboundPartitionField {
                                source_id: field.source_id,
                                field_id: field.field_id,
                                name: field.name,
                                transform: Self::decode_transform(&field.transform.ok_or_else(
                                    || {
                                        CompactionError::Config(
                                            "cannot find transform from partition_field".to_owned(),
                                        )
                                    },
                                )?)?,
                            },
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                builder = builder.add_unbound_fields(fields)?;
                Ok(Some(builder.build()?))
            }
        }
    }

    /// Builds an Iceberg Transform from a protobuf Transform
    ///
    /// This function converts a protobuf Transform into an Iceberg Transform.
    /// It handles different transform types and their parameters.
    fn decode_transform(transform: &Transform) -> Result<iceberg::spec::Transform> {
        match transform.params {
            Some(ic_codegen::compactor::transform::Params::TransformWithoutInner(
                transform_type,
            )) => {
                match ic_codegen::compactor::transform::TransformWithoutInner::try_from(
                    transform_type,
                )
                .map_err(|e| CompactionError::Config(format!("failed to parse kind: {}", e)))?
                {
                    ic_codegen::compactor::transform::TransformWithoutInner::Identity => {
                        Ok(iceberg::spec::Transform::Identity)
                    }
                    ic_codegen::compactor::transform::TransformWithoutInner::Year => {
                        Ok(iceberg::spec::Transform::Year)
                    }
                    ic_codegen::compactor::transform::TransformWithoutInner::Month => {
                        Ok(iceberg::spec::Transform::Month)
                    }
                    ic_codegen::compactor::transform::TransformWithoutInner::Day => {
                        Ok(iceberg::spec::Transform::Day)
                    }
                    ic_codegen::compactor::transform::TransformWithoutInner::Hour => {
                        Ok(iceberg::spec::Transform::Hour)
                    }
                    ic_codegen::compactor::transform::TransformWithoutInner::Void => {
                        Ok(iceberg::spec::Transform::Void)
                    }
                    ic_codegen::compactor::transform::TransformWithoutInner::Unknown => {
                        Ok(iceberg::spec::Transform::Unknown)
                    }
                }
            }
            Some(ic_codegen::compactor::transform::Params::Bucket(bucket_num)) => {
                Ok(iceberg::spec::Transform::Bucket(bucket_num))
            }
            Some(ic_codegen::compactor::transform::Params::Truncate(truncate_width)) => {
                Ok(iceberg::spec::Transform::Truncate(truncate_width))
            }
            None => Err(CompactionError::Config(
                "Transform params is None".to_owned(),
            )),
        }
    }
}

pub struct RewriteFilesResponseProtoEncoder {
    rewrite_files_response: RewriteFilesResponse,
}

impl RewriteFilesResponseProtoEncoder {
    pub fn new(rewrite_files_response: RewriteFilesResponse) -> Self {
        Self {
            rewrite_files_response,
        }
    }

    pub fn encode(self) -> PbRewriteFilesResponse {
        let RewriteFilesResponse { data_files, stat } = self.rewrite_files_response;
        let data_files = data_files.into_iter().map(Self::encode_data_file).collect();
        let stat = Some(PbRewriteFilesStat {
            rewritten_files_count: stat.rewritten_files_count,
            added_files_count: stat.added_files_count,
            rewritten_bytes: stat.rewritten_bytes,
            failed_data_files_count: stat.failed_data_files_count,
        });
        PbRewriteFilesResponse { data_files, stat }
    }

    /// Converts an Iceberg data file to a protobuf DataFile
    pub fn encode_data_file(data_file: iceberg::spec::DataFile) -> DataFile {
        DataFile {
            content: data_file.content_type() as i32,
            file_path: data_file.file_path().to_owned(),
            file_format: Self::encode_data_file_format(data_file.file_format()),
            partition: Some(Self::encode_struct(data_file.partition().clone())),
            record_count: data_file.record_count(),
            file_size_in_bytes: data_file.file_size_in_bytes(),
            column_sizes: data_file.column_sizes().clone(),
            value_counts: data_file.value_counts().clone(),
            null_value_counts: data_file.null_value_counts().clone(),
            nan_value_counts: data_file.nan_value_counts().clone(),
            lower_bounds: data_file
                .lower_bounds()
                .clone()
                .into_iter()
                .map(|(k, v)| (k, v.to_bytes().unwrap().into_vec()))
                .collect(),
            upper_bounds: data_file
                .lower_bounds()
                .clone()
                .into_iter()
                .map(|(k, v)| (k, v.to_bytes().unwrap().into_vec()))
                .collect(),
            key_metadata: data_file.key_metadata().map(|k| k.to_vec()),
            split_offsets: data_file.split_offsets().to_vec(),
            equality_ids: data_file.equality_ids().to_vec(),
            sort_order_id: data_file.sort_order_id(),
            partition_spec_id: 0,
        }
    }

    /// Converts an Iceberg data file format to a protobuf data file format
    fn encode_data_file_format(data_file_format: iceberg::spec::DataFileFormat) -> i32 {
        match data_file_format {
            iceberg::spec::DataFileFormat::Avro => 0,
            iceberg::spec::DataFileFormat::Orc => 1,
            iceberg::spec::DataFileFormat::Parquet => 2,
        }
    }

    /// Converts an Iceberg primitive literal to a protobuf PrimitiveLiteral
    fn encode_primitive_literal(
        primitive_literal: iceberg::spec::PrimitiveLiteral,
    ) -> PrimitiveLiteral {
        match primitive_literal {
            iceberg::spec::PrimitiveLiteral::Boolean(b) => PrimitiveLiteral {
                kind_literal: Some(KindLiteral::Boolean(b)),
            },
            iceberg::spec::PrimitiveLiteral::Int(i) => PrimitiveLiteral {
                kind_literal: Some(KindLiteral::Int(i)),
            },
            iceberg::spec::PrimitiveLiteral::Long(l) => PrimitiveLiteral {
                kind_literal: Some(KindLiteral::Long(l)),
            },
            iceberg::spec::PrimitiveLiteral::Float(f) => PrimitiveLiteral {
                kind_literal: Some(KindLiteral::Float(f.0)),
            },
            iceberg::spec::PrimitiveLiteral::Double(f) => PrimitiveLiteral {
                kind_literal: Some(KindLiteral::Double(f.0)),
            },
            iceberg::spec::PrimitiveLiteral::String(s) => PrimitiveLiteral {
                kind_literal: Some(KindLiteral::String(s)),
            },
            iceberg::spec::PrimitiveLiteral::Binary(b) => PrimitiveLiteral {
                kind_literal: Some(KindLiteral::Binary(b)),
            },
            iceberg::spec::PrimitiveLiteral::Int128(i) => PrimitiveLiteral {
                kind_literal: Some(KindLiteral::Int128(i.to_be_bytes().to_vec())),
            },
            iceberg::spec::PrimitiveLiteral::UInt128(i) => PrimitiveLiteral {
                kind_literal: Some(KindLiteral::Uint128(i.to_be_bytes().to_vec())),
            },
            iceberg::spec::PrimitiveLiteral::AboveMax => PrimitiveLiteral {
                kind_literal: Some(KindLiteral::KindWithoutInnerLiteral(
                    KindWithoutInnerLiteral::AboveMax as i32,
                )),
            },
            iceberg::spec::PrimitiveLiteral::BelowMin => PrimitiveLiteral {
                kind_literal: Some(KindLiteral::KindWithoutInnerLiteral(
                    KindWithoutInnerLiteral::BelowMin as i32,
                )),
            },
        }
    }

    /// Converts an Iceberg struct to a protobuf StructLiteralDescriptor
    fn encode_struct(structs: iceberg::spec::Struct) -> StructLiteralDescriptor {
        let literals = structs
            .into_iter()
            .map(|literal| {
                let literal = literal.map(Self::encode_literal);
                OptionalLiteral { value: literal }
            })
            .collect();
        StructLiteralDescriptor { inner: literals }
    }

    /// Converts an Iceberg literal to a protobuf Literal
    fn encode_literal(literal: iceberg::spec::Literal) -> Literal {
        match literal {
            iceberg::spec::Literal::Primitive(primitive_literal) => {
                let primitive_literal = Self::encode_primitive_literal(primitive_literal);
                Literal {
                    literal: Some(literal::Literal::Primitive(primitive_literal)),
                }
            }
            iceberg::spec::Literal::Struct(literals) => {
                let literals = Self::encode_struct(literals);
                Literal {
                    literal: Some(literal::Literal::Struct(literals)),
                }
            }
            iceberg::spec::Literal::List(literals) => {
                let literals = literals
                    .into_iter()
                    .map(|literal| {
                        let literal = literal.map(Self::encode_literal);
                        OptionalLiteral { value: literal }
                    })
                    .collect();
                Literal {
                    literal: Some(literal::Literal::List(StructLiteralDescriptor {
                        inner: literals,
                    })),
                }
            }
            iceberg::spec::Literal::Map(map) => {
                let mut keys = Vec::with_capacity(map.len());
                let mut values = Vec::with_capacity(map.len());
                for (k, v) in map.into_iter() {
                    keys.push(Self::encode_literal(k));
                    let value = OptionalLiteral {
                        value: v.map(Self::encode_literal),
                    };
                    values.push(value);
                }
                Literal {
                    literal: Some(literal::Literal::Map(MapLiteral { keys, values })),
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use ic_codegen::compactor::{MapType, NestedFieldDescriptor, PrimitiveType, StructType};

    /// Test building a struct field from protobuf
    #[test]
    fn test_decode_field_from_pb_struct() {
        let nested_field = NestedFieldDescriptor {
            id: 1,
            name: "nested".to_owned(),
            field_type: Some(FieldType::Primitive(PrimitiveType {
                kind: Some(Kind::KindWithoutInner(KindWithoutInner::Int.into())),
            })),
            required: true,
        };

        let struct_field = NestedFieldDescriptor {
            id: 2,
            name: "struct_field".to_owned(),
            field_type: Some(FieldType::Struct(StructType {
                fields: vec![nested_field],
            })),
            required: true,
        };

        let result = PbRewriteFilesRequestDecoder::decode_field(&struct_field);
        assert!(result.is_ok());
        let field = result.unwrap();
        assert_eq!(field.id, 2);
        assert_eq!(field.name, "struct_field");
        assert!(field.required);

        match *field.field_type {
            Type::Struct(struct_type) => {
                assert_eq!(struct_type.fields().len(), 1);
                let nested = *struct_type.fields()[0].field_type.clone();
                assert!(matches!(
                    nested,
                    Type::Primitive(iceberg::spec::PrimitiveType::Int)
                ));
            }
            _ => panic!("Expected Struct type"),
        }
    }

    /// Test building a list field from protobuf
    #[test]
    fn test_decode_field_from_pb_list() {
        let element_field = NestedFieldDescriptor {
            id: 1,
            name: "element".to_owned(),
            field_type: Some(FieldType::Primitive(PrimitiveType {
                kind: Some(Kind::KindWithoutInner(KindWithoutInner::String.into())),
            })),
            required: true,
        };

        let list_field = NestedFieldDescriptor {
            id: 2,
            name: "list_field".to_owned(),
            field_type: Some(FieldType::List(Box::new(element_field))),
            required: true,
        };

        let result = PbRewriteFilesRequestDecoder::decode_field(&list_field);
        assert!(result.is_ok());
        let field = result.unwrap();
        assert_eq!(field.id, 2);
        assert_eq!(field.name, "list_field");
        assert!(field.required);

        match *field.field_type {
            Type::List(list_type) => {
                let element_type = *list_type.element_field.field_type.clone();
                assert!(matches!(
                    element_type,
                    Type::Primitive(iceberg::spec::PrimitiveType::String)
                ));
            }
            _ => panic!("Expected List type"),
        }
    }

    /// Test building a map field from protobuf
    #[test]
    fn test_build_field_from_pb_map() {
        let key_field = NestedFieldDescriptor {
            id: 1,
            name: "key".to_owned(),
            field_type: Some(FieldType::Primitive(PrimitiveType {
                kind: Some(Kind::KindWithoutInner(KindWithoutInner::String.into())),
            })),
            required: true,
        };

        let value_field = NestedFieldDescriptor {
            id: 2,
            name: "value".to_owned(),
            field_type: Some(FieldType::Primitive(PrimitiveType {
                kind: Some(Kind::KindWithoutInner(KindWithoutInner::Int.into())),
            })),
            required: true,
        };

        let map_field = NestedFieldDescriptor {
            id: 3,
            name: "map_field".to_owned(),
            field_type: Some(FieldType::Map(Box::new(MapType {
                key_field: Some(Box::new(key_field)),
                value_field: Some(Box::new(value_field)),
            }))),
            required: true,
        };

        let result = PbRewriteFilesRequestDecoder::decode_field(&map_field);
        assert!(result.is_ok());
        let field = result.unwrap();
        assert_eq!(field.id, 3);
        assert_eq!(field.name, "map_field");
        assert!(field.required);

        match *field.field_type {
            Type::Map(map_type) => {
                let key_type = *map_type.key_field.field_type.clone();
                let value_type = *map_type.value_field.field_type.clone();
                assert!(matches!(
                    key_type,
                    Type::Primitive(iceberg::spec::PrimitiveType::String)
                ));
                assert!(matches!(
                    value_type,
                    Type::Primitive(iceberg::spec::PrimitiveType::Int)
                ));
            }
            _ => panic!("Expected Map type"),
        }
    }

    /// Test building a deeply nested field from protobuf
    #[test]
    fn test_build_field_from_pb_deeply_nested() {
        let inner_struct_field1 = NestedFieldDescriptor {
            id: 1,
            name: "int_field".to_owned(),
            field_type: Some(FieldType::Primitive(PrimitiveType {
                kind: Some(Kind::KindWithoutInner(KindWithoutInner::Int.into())),
            })),
            required: true,
        };

        let inner_struct_field2 = NestedFieldDescriptor {
            id: 2,
            name: "string_field".to_owned(),
            field_type: Some(FieldType::Primitive(PrimitiveType {
                kind: Some(Kind::KindWithoutInner(KindWithoutInner::String.into())),
            })),
            required: true,
        };

        let inner_struct = NestedFieldDescriptor {
            id: 3,
            name: "inner_struct".to_owned(),
            field_type: Some(FieldType::Struct(StructType {
                fields: vec![inner_struct_field1, inner_struct_field2],
            })),
            required: true,
        };

        let list_field = NestedFieldDescriptor {
            id: 4,
            name: "list_field".to_owned(),
            field_type: Some(FieldType::List(Box::new(inner_struct))),
            required: true,
        };

        let key_field = NestedFieldDescriptor {
            id: 5,
            name: "key".to_owned(),
            field_type: Some(FieldType::Primitive(PrimitiveType {
                kind: Some(Kind::KindWithoutInner(KindWithoutInner::String.into())),
            })),
            required: true,
        };

        let map_field = NestedFieldDescriptor {
            id: 6,
            name: "map_field".to_owned(),
            field_type: Some(FieldType::Map(Box::new(MapType {
                key_field: Some(Box::new(key_field)),
                value_field: Some(Box::new(list_field)),
            }))),
            required: true,
        };

        let result = PbRewriteFilesRequestDecoder::decode_field(&map_field);
        assert!(result.is_ok());
        let field = result.unwrap();
        assert_eq!(field.id, 6);
        assert_eq!(field.name, "map_field");
        assert!(field.required);

        match *field.field_type {
            Type::Map(map_type) => {
                let key_type = *map_type.key_field.field_type.clone();
                assert!(matches!(
                    key_type,
                    Type::Primitive(iceberg::spec::PrimitiveType::String)
                ));

                let value_type = *map_type.value_field.field_type.clone();
                match value_type {
                    Type::List(list_type) => {
                        let element_type = *list_type.element_field.field_type.clone();
                        match element_type {
                            Type::Struct(struct_type) => {
                                assert_eq!(struct_type.fields().len(), 2);
                                let field1_type = *struct_type.fields()[0].field_type.clone();
                                let field2_type = *struct_type.fields()[1].field_type.clone();
                                assert!(matches!(
                                    field1_type,
                                    Type::Primitive(iceberg::spec::PrimitiveType::Int)
                                ));
                                assert!(matches!(
                                    field2_type,
                                    Type::Primitive(iceberg::spec::PrimitiveType::String)
                                ));
                            }
                            _ => panic!("Expected Struct type in List"),
                        }
                    }
                    _ => panic!("Expected List type in Map value"),
                }
            }
            _ => panic!("Expected Map type"),
        }
    }
}
