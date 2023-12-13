pub struct Cleaner {}

impl Cleaner {
    pub fn new() -> Self {
        Cleaner {}
    }

    pub async fn cleanup(&self) {
        println!("Running cleanup task...");

        // TODO: cleanup failed/completed job rows
        // TODO: push metrics about those rows to Kafka `app_metrics` and `plugin_log_entries` topics
    }
}
