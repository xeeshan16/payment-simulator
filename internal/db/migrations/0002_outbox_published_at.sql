-- Lets the pruner find published rows past the retention window without
-- scanning the table. Partial, so unpublished rows are not indexed twice.
CREATE INDEX outbox_events_published_at ON outbox_events (published_at) WHERE published_at IS NOT NULL;
