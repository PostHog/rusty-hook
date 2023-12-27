use std::time;

#[derive(Clone, Debug)]
/// The retry policy that PgQueue will use to determine how to set scheduled_at when enqueuing a retry.
pub struct RetryPolicy {
    /// Coefficient to multiply initial_interval with for every past attempt.
    pub backoff_coefficient: u32,
    /// The backoff interval for the first retry.
    pub initial_interval: time::Duration,
    /// The maximum possible backoff between retries.
    pub maximum_interval: Option<time::Duration>,
    /// An optional queue to send WebhookJob retries to.
    pub queue: Option<String>,
}

impl RetryPolicy {
    pub fn new(backoff_coefficient: u32, initial_interval: time::Duration) -> RetryPolicyBuilder {
        RetryPolicyBuilder::new(backoff_coefficient, initial_interval)
    }

    /// Calculate the time until the next retry for a given attempt number.
    pub fn time_until_next_retry(
        &self,
        attempt: u32,
        preferred_retry_interval: Option<time::Duration>,
    ) -> time::Duration {
        let candidate_interval = self.initial_interval * self.backoff_coefficient.pow(attempt);

        match (preferred_retry_interval, self.maximum_interval) {
            (Some(duration), Some(max_interval)) => std::cmp::min(
                std::cmp::max(std::cmp::min(candidate_interval, max_interval), duration),
                max_interval,
            ),
            (Some(duration), None) => std::cmp::max(candidate_interval, duration),
            (None, Some(max_interval)) => std::cmp::min(candidate_interval, max_interval),
            (None, None) => candidate_interval,
        }
    }

    /// Determine the queue to be used for retrying.
    /// Only whether a queue is configured in this RetryPolicy is used to determine which queue to use for retrying.
    /// This may be extended in the future to support more decision parameters.
    pub fn retry_queue<'s>(&'s self, current_queue: &'s str) -> &'s str {
        if let Some(new_queue) = &self.queue {
            new_queue
        } else {
            &current_queue
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicyBuilder::default().provide()
    }
}

pub struct RetryPolicyBuilder {
    /// Coefficient to multiply initial_interval with for every past attempt.
    pub backoff_coefficient: u32,
    /// The backoff interval for the first retry.
    pub initial_interval: time::Duration,
    /// The maximum possible backoff between retries.
    pub maximum_interval: Option<time::Duration>,
    /// An optional queue to send WebhookJob retries to.
    pub queue: Option<String>,
}

impl Default for RetryPolicyBuilder {
    fn default() -> Self {
        Self {
            backoff_coefficient: 2,
            initial_interval: time::Duration::from_secs(1),
            maximum_interval: None,
            queue: None,
        }
    }
}

impl RetryPolicyBuilder {
    pub fn new(backoff_coefficient: u32, initial_interval: time::Duration) -> Self {
        Self {
            backoff_coefficient,
            initial_interval,
            ..RetryPolicyBuilder::default()
        }
    }

    pub fn maximum_interval(mut self, interval: time::Duration) -> RetryPolicyBuilder {
        self.maximum_interval = Some(interval);
        self
    }

    pub fn queue(mut self, queue: &str) -> RetryPolicyBuilder {
        self.queue = Some(queue.to_owned());
        self
    }

    pub fn provide(&self) -> RetryPolicy {
        RetryPolicy {
            backoff_coefficient: self.backoff_coefficient,
            initial_interval: self.initial_interval,
            maximum_interval: self.maximum_interval,
            queue: self.queue.clone(),
        }
    }
}
