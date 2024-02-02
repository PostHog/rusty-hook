DROP INDEX idx_queue_scheduled_at;

CREATE INDEX idx_queue_dequeue_partial ON job_queue(queue, attempt, scheduled_at) where status = 'available' :: job_status;
