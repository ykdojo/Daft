use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_shuffles::shuffle_cache::{InProgressShuffleCache, ShuffleCache};
use futures::future::join_all;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::ExecutionTaskSpawner;

struct ShuffleSinkState {
    in_progress: InProgressShuffleCache,
    finalized: Option<ShuffleCache>,
}

impl ShuffleSinkState {
    fn get_cache(&mut self) -> &mut InProgressShuffleCache {
        &mut self.in_progress
    }

    async fn finalize(&mut self) -> DaftResult<ShuffleCache> {
        self.finalized = Some(self.in_progress.close().await?);
        Ok(self.finalized.take().unwrap())
    }
}

impl BlockingSinkState for ShuffleSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct ShuffleSink {
    partition_by: Vec<ExprRef>,
    _schema: SchemaRef,
}

impl ShuffleSink {
    pub fn new(partition_by: Vec<ExprRef>, schema: SchemaRef) -> Self {
        Self {
            partition_by,
            _schema: schema,
        }
    }
}

impl BlockingSink for ShuffleSink {
    #[instrument(skip_all, name = "ShuffleSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        spawner
            .spawn(
                async move {
                    let shuffle_cache = state
                        .as_any_mut()
                        .downcast_mut::<ShuffleSinkState>()
                        .expect("ShuffleSink should have shuffle state")
                        .get_cache();

                    shuffle_cache.push_partitions(vec![input]).await?;
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "ShuffleSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        spawner
            .spawn(
                async move {
                    let parts = states.into_iter().map(|mut state| {
                        tokio::spawn(async move {
                            let state = state
                                .as_any_mut()
                                .downcast_mut::<ShuffleSinkState>()
                                .expect("State type mismatch");
                            state.finalize().await
                        })
                    });

                    let finished_caches = join_all(parts)
                        .await
                        .into_iter()
                        .map(|res| res.unwrap().unwrap())
                        .collect_vec();
                    let _shuffle_cache = ShuffleCache::merge_all(finished_caches.into_iter());
                    Ok(None)
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "ShuffleSink"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![];
        lines.push(format!(
            "ShuffleSink: Partition by = {}",
            self.partition_by.iter().map(|e| e.to_string()).join(", ")
        ));
        lines
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(ShuffleSinkState {
            in_progress: InProgressShuffleCache::try_new(
                0,                 // num_partitions: usize,
                vec![].as_slice(), // dirs: &[String],
                String::new(),     // node_id: String,
                0,                 // shuffle_stage_id: usize,
                0,                 // target_filesize: usize,
                None,
                Some(self.partition_by.clone()),
            )?,
            finalized: None,
        }))
    }
}
