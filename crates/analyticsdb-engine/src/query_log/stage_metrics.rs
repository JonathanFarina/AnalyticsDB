use std::path::PathBuf;
use std::sync::Arc;

use analyticsdb_control::QueryLogConfig;
use anyhow::Result;
use datafusion::physical_plan::ExecutionPlan;
use tokio::sync::mpsc;

use super::cleanup_expired_logs;

pub struct StageMetrics {
    sender: Option<mpsc::UnboundedSender<QueryStageMetric>>,
    root_location: String,
}

impl StageMetrics {
    pub fn new(config: QueryLogConfig, root: PathBuf) -> Self {
        let root_location = format!("file://{}", root.display());
        if let Err(e) = std::fs::create_dir_all(&root) {
            tracing::debug!(
                "stage metrics disabled: failed to create root {}: {}",
                root.display(),
                e
            );
            return Self {
                sender: None,
                root_location,
            };
        }

        let (sender, receiver) = mpsc::unbounded_channel();
        let writer = StageMetricsWriter::new(config, root_location.clone(), receiver);
        tokio::spawn(async move {
            writer.run().await;
        });

        Self {
            sender: Some(sender),
            root_location,
        }
    }

    pub fn disabled() -> Self {
        Self {
            sender: None,
            root_location: "file://analyticsdb-catalog.managed/system/query_stage_metrics".to_string(),
        }
    }

    pub fn send(&self, metric: QueryStageMetric) {
        if let Some(sender) = &self.sender {
            let _ = sender.send(metric);
        }
    }

    pub fn extract_and_send(&self, plan: &dyn ExecutionPlan, query_id: &str) {
        if let Some(sender) = &self.sender {
            let mut stage_id = 0;
            extract_metrics_recursive(plan, query_id, &mut stage_id, sender);
        }
    }
}

// ... rest of existing code (QueryStageMetric, schema, StageMetricsWriter, etc.)
