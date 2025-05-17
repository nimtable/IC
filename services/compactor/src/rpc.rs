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

use bergloom_codegen::compactor::compactor_service_server::CompactorService;
use bergloom_codegen::compactor::{EchoRequest, EchoResponse};
use bergloom_core::CompactionExecutor;
use bergloom_core::executor::DataFusionExecutor;

use bergloom_codegen::compactor::{
    RewriteFilesRequest as PbRewriteFilesRequest, RewriteFilesResponse as PbRewriteFilesResponse,
};

#[derive(Default)]
pub struct CompactorServiceImpl;

#[async_trait::async_trait]
impl CompactorService for CompactorServiceImpl {
    async fn rewrite_files(
        &self,
        request: tonic::Request<PbRewriteFilesRequest>,
    ) -> std::result::Result<tonic::Response<PbRewriteFilesResponse>, tonic::Status> {
        let request = request.into_inner();
        let response = DataFusionExecutor::default()
            .rewrite_file_proto(request)
            .await
            .map_err(|e| {
                tracing::error!("Error processing request: {:?}", e);
                tonic::Status::internal(format!("Internal error: {}", e))
            })?;
        Ok(tonic::Response::new(response))
    }

    async fn echo(
        &self,
        request: tonic::Request<EchoRequest>,
    ) -> std::result::Result<tonic::Response<EchoResponse>, tonic::Status> {
        tracing::info!("Echo request: {:?}", request);
        Ok(tonic::Response::new(EchoResponse {
            message: format!("Echo: {}", request.into_inner().message),
        }))
    }
}
