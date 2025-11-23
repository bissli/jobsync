# Operator Guide - JobSync

## Table of Contents

1. [Quick Start](#quick-start)
2. [Deployment](#deployment)
3. [Monitoring](#monitoring)
4. [Troubleshooting](#troubleshooting)
5. [Maintenance](#maintenance)

## Quick Start

JobSync is a Python library for coordinating task processing. Workers automatically register, balance load, and handle failures.

**Key points:**
- Tables created automatically on first startup
- Leader elected automatically (oldest node)
- Tokens redistributed automatically on membership changes
- No manual intervention needed for normal operations

## Deployment

### Initial Deployment

For **batch workloads** (all tasks known upfront), start all nodes together within the `wait_on_enter` timeout window to ensure fair task distribution.

1. **Start all nodes simultaneously** (or within 60 seconds):
```bash
# Terminal 1
python worker.py --node-name worker-01

# Terminal 2 
python worker.py --node-name worker-02

# Terminal 3
python worker.py --node-name worker-03
```

2. **Verify registration**:
```sql
SELECT * FROM sync_node ORDER BY created_on;
```

3. **Verify token distribution**:
```sql
SELECT node, COUNT(*) FROM sync_token GROUP BY node;
```

4. **Verify task distribution** (for batch jobs):
```sql
SELECT node, COUNT(*) FROM sync_claim GROUP BY node;
```

**For streaming workloads** (tasks arrive continuously), startup order doesn't matter - new tasks automatically distribute to all nodes.

### Rolling Update

1. Start new nodes with updated code
2. Wait for token redistribution (~60 seconds)
3. Stop old nodes gracefully (SIGTERM)
4. System automatically rebalances

### Scaling

**Add nodes:** Just start them - automatic rebalancing
**Remove nodes:** Stop gracefully (SIGTERM) - automatic rebalancing
**Rule:** Always use SIGTERM, never SIGKILL

## Monitoring

### Key Metrics

Monitor these metrics for cluster health:

| Metric | Expected | Alert If |
|--------|----------|----------|
| Active nodes | Match cluster size | Count != expected |
| Token balance | ±10% across nodes | Node has <10% or >50% |
| Heartbeat lag | <10 seconds | Any node >10s |
| Rebalance frequency | <1 per hour | >5 per hour |
| Leader changes | Rare | Changed in last 5 min |

### SQL Queries

See [Cheatsheet.sql](Cheatsheet.sql) for complete reference.

**Active nodes:**
```sql
SELECT COUNT(*) FROM sync_node 
WHERE last_heartbeat > NOW() - INTERVAL '15 seconds';
```

**Token distribution:**
```sql
SELECT node, COUNT(*) as tokens,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
FROM sync_token GROUP BY node;
```

**Rebalance frequency:**
```sql
SELECT COUNT(*) FROM sync_rebalance 
WHERE triggered_at > NOW() - INTERVAL '1 hour';
```

## Troubleshooting

### Node has no tokens

**Check:**
```sql
SELECT COUNT(*) FROM sync_token WHERE node = 'worker-01';
```

**Causes:**
- Node joined after distribution (wait 30-60s for rebalance)
- All tokens locked to other nodes (check `sync_lock` table)
- Token version mismatch (restart node)

### Tasks processed twice

**Causes:**
- Coordination disabled (`coordination_config=None`)
- Network partition (check connectivity)

**Fix:**
- Verify `CoordinationConfig` passed to Job
- Check all nodes can reach database

### Constant rebalancing

**Check:**
```sql
SELECT COUNT(*) FROM sync_rebalance 
WHERE triggered_at > NOW() - INTERVAL '1 hour';
```

**Causes (if >5 per hour):**
- Nodes crashing (check logs)
- Heartbeat timeout too short (increase to 30s)
- Resource starvation (check CPU/memory)

### Orphaned locks

**Find them:**
```sql
SELECT * FROM sync_lock l
LEFT JOIN sync_node n ON l.created_by = n.name
WHERE n.name IS NULL;
```

**Remove them:**
```python
with Job('admin', coordination_config=coord_config) as job:
    job.clear_locks_by_creator('old-worker-name')
```

## Maintenance

### Database Cleanup

**Old rebalance history:**
```sql
DELETE FROM sync_rebalance 
WHERE triggered_at < NOW() - INTERVAL '30 days';
```

**Dead nodes:**
```sql
DELETE FROM sync_node 
WHERE last_heartbeat < NOW() - INTERVAL '7 days';
```

**Orphaned locks:**
```sql
DELETE FROM sync_lock
WHERE created_by NOT IN (SELECT name FROM sync_node);
```

### Vacuum

```sql
VACUUM ANALYZE sync_node;
VACUUM ANALYZE sync_token;
VACUUM ANALYZE sync_lock;
```

Run weekly or when tables grow large.

## Performance Tuning

See [Usage Guide - Configuration](USAGE_GUIDE.md#configuration) for tuning parameters.

**Common scenarios:**

| Scenario | Recommended Settings |
|----------|---------------------|
| Default (2-10 nodes) | Use defaults |
| Large cluster (10+) | `total_tokens=50000`, `heartbeat_interval_sec=3` |
| Stable cluster | `heartbeat_timeout_sec=30`, `rebalance_check_interval_sec=60` |
| High churn (K8s) | `heartbeat_interval_sec=3`, `heartbeat_timeout_sec=9` |

---

For usage examples and detailed API reference, see the [Usage Guide](USAGE_GUIDE.md).
