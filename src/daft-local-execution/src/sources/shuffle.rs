use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::source::Source;
use crate::sources::source::SourceStream;

pub struct ShuffleSource {
    schema: SchemaRef,
}

impl ShuffleSource {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

#[async_trait]
impl Source for ShuffleSource {
    #[instrument(name = "ShuffleSource::get_data", level = "info", skip_all)]
    async fn get_data(
        &self,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
    ) -> DaftResult<SourceStream<'static>> {
        let empty = Arc::new(MicroPartition::empty(Some(self.schema.clone())));
        Ok(Box::pin(futures::stream::once(async { Ok(empty) })))
    }
    fn name(&self) -> &'static str {
        "ShuffleSource"
    }
    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("ShuffleSource:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res
    }
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
