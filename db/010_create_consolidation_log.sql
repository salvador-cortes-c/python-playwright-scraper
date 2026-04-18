-- Migration: Create consolidation logging table for semantic deduplication
-- Purpose: Track product consolidations for audit trail and pattern learning

CREATE TABLE IF NOT EXISTS consolidation_log (
    id SERIAL PRIMARY KEY,
    source_product_key VARCHAR(255) NOT NULL,
    canonical_product_key VARCHAR(255) NOT NULL,
    source_product_name VARCHAR(500),
    canonical_product_name VARCHAR(500),
    method VARCHAR(50) NOT NULL,  -- 'semantic', 'manual', 'pattern', 'hardcoded'
    similarity_score FLOAT,  -- For semantic deduplication (0.0-1.0)
    reason TEXT,  -- Optional human-readable explanation
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    executed_at TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',  -- 'pending', 'executed', 'rejected', 'failed'
    snapshots_migrated INT DEFAULT 0,
    categories_migrated INT DEFAULT 0,
    error_message TEXT,
    CONSTRAINT unique_consolidation UNIQUE (source_product_key, canonical_product_key)
);

CREATE INDEX idx_consolidation_source ON consolidation_log(source_product_key);
CREATE INDEX idx_consolidation_canonical ON consolidation_log(canonical_product_key);
CREATE INDEX idx_consolidation_status ON consolidation_log(status);
CREATE INDEX idx_consolidation_method ON consolidation_log(method);
CREATE INDEX idx_consolidation_created ON consolidation_log(created_at);

-- View: Active consolidations (for monitoring)
CREATE OR REPLACE VIEW active_consolidations AS
SELECT 
    id,
    source_product_key,
    canonical_product_key,
    source_product_name,
    canonical_product_name,
    method,
    similarity_score,
    status,
    created_at
FROM consolidation_log
WHERE status IN ('pending', 'executed')
ORDER BY created_at DESC;

-- View: Consolidation statistics
CREATE OR REPLACE VIEW consolidation_stats AS
SELECT 
    method,
    status,
    COUNT(*) as count,
    AVG(similarity_score) as avg_similarity,
    SUM(snapshots_migrated) as total_snapshots_migrated,
    MAX(created_at) as last_consolidation
FROM consolidation_log
GROUP BY method, status
ORDER BY method, status;

COMMIT;
