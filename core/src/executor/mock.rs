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

use async_trait::async_trait;

use super::{CompactionExecutor, RewriteFilesRequest, RewriteFilesResponse};
use crate::error::Result;

pub struct MockExecutor;

#[async_trait]
impl CompactionExecutor for MockExecutor {
    async fn rewrite_files(&self, _request: RewriteFilesRequest) -> Result<RewriteFilesResponse> {
        Ok(RewriteFilesResponse::default())
    }
}
