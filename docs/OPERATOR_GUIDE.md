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

For **batch workloads** (all tasks known upfront), all nodes start within the `wait_on_enter` grace period, so the task distribution is fair.

1. **All nodes start together** (or within the grace period):
```bash
# Terminal 1
python worker.py --node-name worker-01

# Terminal 2 
python worker.py --node-name worker-02

# Terminal 3
python worker.py --node-name worker-03
```

2. **Registration check**:
```sql
SELECT * FROM sync_node ORDER BY created_on;
```

3. **Token distribution check**:
```sql
SELECT node, COUNT(*) FROM sync_token GROUP BY node;
```

4. **Task distribution check** (for batch jobs):
```sql
SELECT node, COUNT(*) FROM sync_claim GROUP BY node;
```

**For streaming workloads** (tasks arrive continuously), startup order has no effect: new tasks distribute to all nodes.

### Rolling Update

1. New nodes start with the updated code
2. Token redistribution completes (~60 seconds)
3. Old nodes stop on SIGTERM
4. The cluster rebalances

### Scaling

**Adding nodes:** A started node joins and the cluster rebalances
**Removing nodes:** A node stopped with SIGTERM leaves and the cluster rebalances
**Rule:** SIGTERM is the stop signal. SIGKILL skips the node's cleanup, so its row stays until the leader's dead-node sweep removes it

## Monitoring

### Key Metrics

Cluster health metrics:

| Metric              | Expected           | Alert If              |
| ------------------- | ------------------ | --------------------- |
| Active nodes        | Match cluster size | Count != expected     |
| Token balance       | ±10% across nodes  | Node has <10% or >50% |
| Heartbeat lag       | <10 seconds        | Any node >10s         |
| Rebalance frequency | <1 per hour        | >5 per hour           |
| Leader changes      | Rare               | Changed in last 5 min |

### SQL Queries

[CheatSheet.sql](CheatSheet.sql) holds the complete reference.

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
- Node joined after distribution (a rebalance follows within 30-60s)
- All tokens locked to other nodes (the `sync_lock` table shows them)
- Token version mismatch (a node restart clears it)

### Tasks processed twice

**Causes:**
- Coordination disabled (`coordination_config=None`)
- Network partition (connectivity)

**Fix:**
- A `CoordinationConfig` is passed to `Job`
- Every node can reach the database

### Constant rebalancing

**Check:**
```sql
SELECT COUNT(*) FROM sync_rebalance 
WHERE triggered_at > NOW() - INTERVAL '1 hour';
```

**Causes (if >5 per hour):**
- Nodes crashing (the logs show it)
- Heartbeat timeout too short (30s is a safer value)
- Resource starvation (CPU or memory)

### Orphaned locks

**Query:**
```sql
SELECT * FROM sync_lock l
LEFT JOIN sync_node n ON l.created_by = n.name
WHERE n.name IS NULL;
```

**Removal:**
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

A weekly run, or one when tables grow large, suffices.

## Performance Tuning

The [Usage Guide - Configuration](USAGE_GUIDE.md#configuration) covers the tuning parameters.

**Common scenarios:**

| Scenario             | Recommended Settings                                          |
| -------------------- | ------------------------------------------------------------- |
| Default (2-10 nodes) | Defaults                                                      |
| Large cluster (10+)  | `total_tokens=50000`, `heartbeat_interval_sec=3`              |
| Stable cluster       | `heartbeat_timeout_sec=30`, `rebalance_check_interval_sec=60` |
| High churn (K8s)     | `heartbeat_interval_sec=3`, `heartbeat_timeout_sec=9`         |

---

The [Usage Guide](USAGE_GUIDE.md) covers usage examples and the detailed API reference.
