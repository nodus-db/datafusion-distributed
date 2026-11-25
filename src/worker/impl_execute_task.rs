use crate::distributed_planner::ProducerHead;
use crate::worker::worker_service::Worker;
use crate::ExecuteTaskRequest;
use crate::LocalWorkerContext;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::exec_datafusion_err;
use datafusion::common::{exec_err, Result};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::Stream;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context as TaskCx, Poll};
use std::time::Duration;

const WAIT_PLAN_TIMEOUT_SECS: u64 = 10;

/// The exchange kind a produced boundary represents (L13 DEC-599 §3a; DFD-SQL-EXCHANGE
/// kind=shuffle|broadcast|coalesce): derived from the producer head the consumer asked for.
fn exchange_kind(head: &ProducerHead) -> &'static str {
    match head {
        ProducerHead::RepartitionExec { .. } => "shuffle",
        ProducerHead::BroadcastExec { .. } => "broadcast",
        ProducerHead::None => "coalesce",
    }
}

/// Shared accumulator for one produced network boundary (one execute_task request): the sending
/// worker's rows/bytes across all demanded partitions. One DFD-SQL-EXCHANGE line is printed when
/// the last produced stream for the boundary is dropped (producer side, one line per sending
/// worker and boundary; a zero-moved boundary is still emitted — zero is evidence, silence is not).
struct ExchangeAccum {
    rows: AtomicU64,
    bytes: AtomicU64,
    live: AtomicUsize,
    /// Prebuilt line, minus the trailing `rows=`/`bytes=`. attempt=-: the fork's distributed
    /// executor carries no attempt id (TaskKey/Stage/ExecuteTaskRequest have query_id only), so a
    /// produced exchange line cannot name its attempt; the head-side DFD-SQL-TASKS/DFD-SQL-LEAF
    /// carry the real attempt, and attribution falls back to query= (RefqHarness's escape hatch).
    line: String,
}

impl ExchangeAccum {
    fn finish(&self) {
        if self.live.fetch_sub(1, Ordering::AcqRel) - 1 == 0 {
            eprintln!(
                "{} rows={} bytes={}",
                self.line,
                self.rows.load(Ordering::Acquire),
                self.bytes.load(Ordering::Acquire)
            );
        }
    }
}

/// Wraps one produced partition stream, counting records and payload bytes as the consumer pulls,
/// and (on drop) marks that partition finished for the shared boundary accumulator.
struct ExchangeProbe {
    inner: SendableRecordBatchStream,
    accum: Arc<ExchangeAccum>,
}

impl Stream for ExchangeProbe {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskCx<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // `inner` is itself a pinned boxed stream (`SendableRecordBatchStream`); poll it through
        // `as_mut()`. Re-pinning `&mut **inner` would need the trait object to be `Unpin`, which it
        // is not (E0277 on the pinned fork rev a452171).
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // rows = records crossing this boundary (produced side); bytes = the in-memory
                // payload handed to the transport, before compression (arrow would-be-encoded size).
                this.accum
                    .rows
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                this.accum
                    .bytes
                    .fetch_add(batch.get_array_memory_size() as u64, Ordering::Relaxed);
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(other) => Poll::Ready(other),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl Drop for ExchangeProbe {
    fn drop(&mut self) {
        self.accum.finish();
    }
}

/// Builds several per-partition streams by retrieving the appropriate entry from [TaskDataEntries]
/// based on the task key extracted from [ExecuteTaskRequest].
///
/// This method is async mainly for the key retrieval operation from [TaskDataEntries], but it does
/// not start polling any stream, it just instantiates them.
impl Worker {
    pub async fn execute_task(
        &self,
        request: ExecuteTaskRequest,
    ) -> Result<(Vec<SendableRecordBatchStream>, Arc<TaskContext>)> {
        let entry = self
            .task_data_entries
            .get_with(request.task_key, async { Default::default() })
            .await;

        // Other request is responsible for writing the plan that belongs to this TaskKey, so
        // we'll resolve immediately if it was already there, or wait until it's ready.
        let task_data = entry
            .read(Duration::from_secs(WAIT_PLAN_TIMEOUT_SECS))
            .await
            .map_err(|e| exec_datafusion_err!("Worker::execute_task timed-out while waiting for the plan to be set by the coordinator. ({e})"))?
            .map_err(DataFusionError::Shared)?;
        task_data.task_data_metrics.mark_execution_started_once();

        // ProducerHead is not Copy and plan() takes it by value: derive the exchange kind first.
        let kind = exchange_kind(&request.producer_head);
        let plan = task_data.plan(request.producer_head)?;
        let task_ctx = task_data.task_ctx;
        let partition_count = plan.properties().partitioning.partition_count();
        let plan_name = plan.name();

        // Execute all the requested partitions at once, and collect all the streams so that they
        // can be merged into a single one at the end of this function.
        let n_streams = request.target_partition_end - request.target_partition_start;
        let mut streams = Vec::with_capacity(n_streams);
        let node = LocalWorkerContext::from_ctx(&task_ctx)
            .map(|lw| lw.self_url.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let accum = Arc::new(ExchangeAccum {
            rows: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            live: AtomicUsize::new(0),
            line: format!(
                "DFD-SQL-EXCHANGE query={} attempt=- node={node} worker={} kind={kind}",
                request.task_key.query_id, request.task_key.task_number,
            ),
        });
        for partition in request.target_partition_start..request.target_partition_end {
            if partition >= partition_count {
                return exec_err!(
                    "partition {partition} not available. The head plan {plan_name} of the stage just has {partition_count} partitions"
                );
            }

            let stream = plan.execute(partition, Arc::clone(&task_ctx))?;
            let stream_schema = plan.schema();
            accum.live.fetch_add(1, Ordering::Relaxed);
            let probe = ExchangeProbe {
                inner: stream,
                accum: Arc::clone(&accum),
            };
            streams.push(Box::pin(RecordBatchStreamAdapter::new(stream_schema, probe)) as _);
        }
        Ok((streams, task_ctx))
    }
}
