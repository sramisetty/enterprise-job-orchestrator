# Performance Optimization Guide

## Overview

This guide provides comprehensive performance optimization strategies for processing 70 million identity records efficiently using the Enterprise Job Orchestrator.

---

## Table of Contents

1. [Database Optimization](#database-optimization)
2. [Caching Strategies](#caching-strategies)
3. [Worker Optimization](#worker-optimization)
4. [Network Optimization](#network-optimization)
5. [Algorithm Optimization](#algorithm-optimization)
6. [Resource Management](#resource-management)
7. [Performance Monitoring](#performance-monitoring)

---

## Database Optimization

### PostgreSQL Configuration

```sql
-- postgresql.conf optimizations for 70M records

-- Memory Configuration
shared_buffers = 16GB                # 25% of total RAM
effective_cache_size = 48GB          # 75% of total RAM
maintenance_work_mem = 2GB           # For VACUUM, CREATE INDEX
work_mem = 512MB                     # For sorting, joins per connection
huge_pages = try                     # Use huge pages if available

-- Checkpoint Configuration
checkpoint_completion_target = 0.9
checkpoint_segments = 64
wal_buffers = 16MB
wal_level = minimal                  # If replication not needed
max_wal_size = 16GB
min_wal_size = 2GB

-- Connection Configuration
max_connections = 200
superuser_reserved_connections = 5
max_prepared_transactions = 100

-- Parallel Query
max_worker_processes = 16
max_parallel_workers_per_gather = 8
max_parallel_workers = 16
max_parallel_maintenance_workers = 8
parallel_leader_participation = on

-- Write Performance
synchronous_commit = off             # Faster writes, slight durability risk
commit_delay = 100                   # microseconds
commit_siblings = 5
wal_compression = on
full_page_writes = off               # If using battery-backed cache

-- Query Planner
random_page_cost = 1.1               # For SSD
seq_page_cost = 1.0
cpu_tuple_cost = 0.01
cpu_index_tuple_cost = 0.005
cpu_operator_cost = 0.0025
effective_io_concurrency = 200       # For SSD

-- Statistics
default_statistics_target = 100
track_io_timing = on
track_functions = pl                 # Track PL/pgSQL functions
```

### Critical Indexes

```sql
-- Job management indexes
CREATE INDEX CONCURRENTLY idx_jobs_status_priority
    ON jobs(status, priority DESC, created_at ASC)
    WHERE status IN ('queued', 'running');

CREATE INDEX CONCURRENTLY idx_jobs_worker_status
    ON jobs(assigned_worker, status)
    WHERE status = 'running';

CREATE INDEX CONCURRENTLY idx_jobs_created_date
    ON jobs(date_trunc('day', created_at));

-- Chunk processing indexes
CREATE INDEX CONCURRENTLY idx_chunks_job_status
    ON chunks(job_id, status, chunk_number);

CREATE INDEX CONCURRENTLY idx_chunks_retry
    ON chunks(retry_count, next_retry_at)
    WHERE status = 'failed' AND retry_count < max_retries;

-- Worker management indexes
CREATE INDEX CONCURRENTLY idx_workers_status_heartbeat
    ON workers(status, last_heartbeat)
    WHERE status = 'online';

CREATE INDEX CONCURRENTLY idx_workers_capabilities
    ON workers USING gin(capabilities);

-- Identity matching indexes
CREATE INDEX CONCURRENTLY idx_identities_ssn
    ON identities(ssn)
    WHERE ssn IS NOT NULL;

CREATE INDEX CONCURRENTLY idx_identities_name_trgm
    ON identities USING gin(full_name gin_trgm_ops);

CREATE INDEX CONCURRENTLY idx_identities_dob
    ON identities(date_of_birth);

CREATE INDEX CONCURRENTLY idx_identities_phone
    ON identities(phone_normalized)
    WHERE phone_normalized IS NOT NULL;

CREATE INDEX CONCURRENTLY idx_identities_address
    ON identities USING gin(to_tsvector('english', address));

-- Match results indexes
CREATE INDEX CONCURRENTLY idx_matches_confidence
    ON matches(confidence DESC);

CREATE INDEX CONCURRENTLY idx_matches_identity_pair
    ON matches(identity_id1, identity_id2);
```

### Partitioning Strategy

```sql
-- Partition large tables for better performance

-- Partition identities by state
CREATE TABLE identities_partitioned (
    LIKE identities INCLUDING ALL
) PARTITION BY LIST (state);

CREATE TABLE identities_ca PARTITION OF identities_partitioned
    FOR VALUES IN ('CA');
CREATE TABLE identities_tx PARTITION OF identities_partitioned
    FOR VALUES IN ('TX');
CREATE TABLE identities_ny PARTITION OF identities_partitioned
    FOR VALUES IN ('NY');
-- ... create partitions for all states

-- Partition jobs by date
CREATE TABLE jobs_partitioned (
    LIKE jobs INCLUDING ALL
) PARTITION BY RANGE (created_at);

CREATE TABLE jobs_2024_01 PARTITION OF jobs_partitioned
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE jobs_2024_02 PARTITION OF jobs_partitioned
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
-- ... create monthly partitions

-- Partition chunks by job_id hash
CREATE TABLE chunks_partitioned (
    LIKE chunks INCLUDING ALL
) PARTITION BY HASH (job_id);

CREATE TABLE chunks_0 PARTITION OF chunks_partitioned
    FOR VALUES WITH (modulus 10, remainder 0);
CREATE TABLE chunks_1 PARTITION OF chunks_partitioned
    FOR VALUES WITH (modulus 10, remainder 1);
-- ... create 10 hash partitions
```

### Query Optimization

```python
# Optimized queries for job processing

class OptimizedQueries:
    """Optimized database queries for performance"""

    @staticmethod
    async def get_next_job(db_pool):
        """Get next job with skip-locked for concurrency"""
        query = """
        UPDATE jobs
        SET status = 'assigned',
            assigned_at = NOW(),
            assigned_worker = $1
        WHERE job_id = (
            SELECT job_id
            FROM jobs
            WHERE status = 'queued'
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING *;
        """
        return await db_pool.fetchrow(query, worker_id)

    @staticmethod
    async def bulk_update_chunks(db_pool, chunk_updates):
        """Bulk update chunk statuses"""
        query = """
        UPDATE chunks AS c
        SET status = u.status,
            completed_at = u.completed_at,
            result = u.result
        FROM (VALUES ($1::uuid, $2::text, $3::timestamp, $4::jsonb))
        AS u(chunk_id, status, completed_at, result)
        WHERE c.chunk_id = u.chunk_id;
        """

        # Use prepared statement for better performance
        stmt = await db_pool.prepare(query)
        await stmt.executemany(chunk_updates)

    @staticmethod
    async def get_job_statistics(db_pool):
        """Get statistics with materialized view"""
        # Create materialized view for frequently accessed stats
        await db_pool.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS job_statistics AS
        SELECT
            status,
            COUNT(*) as count,
            AVG(EXTRACT(epoch FROM (completed_at - started_at))) as avg_duration,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
                EXTRACT(epoch FROM (completed_at - started_at))) as median_duration
        FROM jobs
        WHERE completed_at IS NOT NULL
        GROUP BY status
        WITH DATA;

        CREATE UNIQUE INDEX ON job_statistics(status);
        """)

        # Refresh materialized view periodically
        await db_pool.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY job_statistics;")

        return await db_pool.fetch("SELECT * FROM job_statistics;")
```

---

## Caching Strategies

### Redis Configuration

```python
# Redis configuration for optimal performance

REDIS_CONFIG = {
    "connection": {
        "host": "localhost",
        "port": 6379,
        "db": 0,
        "password": None,
        "socket_keepalive": True,
        "socket_keepalive_options": {
            1: 1,  # TCP_KEEPIDLE
            2: 3,  # TCP_KEEPINTVL
            3: 5   # TCP_KEEPCNT
        },
        "connection_pool": {
            "max_connections": 100,
            "max_connections_per_db": 20
        }
    },
    "memory": {
        "maxmemory": "10gb",
        "maxmemory_policy": "allkeys-lru",
        "maxmemory_samples": 5
    },
    "persistence": {
        "save": "",  # Disable persistence for performance
        "stop_writes_on_bgsave_error": "no",
        "rdbcompression": "no"
    },
    "performance": {
        "lazyfree_lazy_eviction": "yes",
        "lazyfree_lazy_expire": "yes",
        "lazyfree_lazy_server_del": "yes",
        "io_threads": 4,
        "io_threads_do_reads": "yes"
    }
}
```

### Multi-Layer Caching

```python
import asyncio
import hashlib
import pickle
from typing import Any, Optional
import redis.asyncio as redis
from functools import lru_cache

class MultiLayerCache:
    """Multi-layer caching system for optimal performance"""

    def __init__(self):
        # L1: In-memory LRU cache (per worker)
        self.l1_cache = {}
        self.l1_max_size = 10000

        # L2: Redis shared cache
        self.redis_client = redis.Redis(
            host='localhost',
            port=6379,
            decode_responses=False,
            connection_pool_kwargs={'max_connections': 50}
        )

        # L3: Database (persistent storage)
        self.db_pool = None  # Set during initialization

    def _generate_key(self, *args, **kwargs) -> str:
        """Generate cache key from arguments"""
        key_data = f"{args}_{kwargs}"
        return hashlib.md5(key_data.encode()).hexdigest()

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache hierarchy"""

        # Check L1 (memory)
        if key in self.l1_cache:
            return self.l1_cache[key]

        # Check L2 (Redis)
        value = await self.redis_client.get(key)
        if value:
            decoded_value = pickle.loads(value)
            # Promote to L1
            self._add_to_l1(key, decoded_value)
            return decoded_value

        # L3 (Database) - cache miss
        return None

    async def set(self, key: str, value: Any, ttl: int = 3600):
        """Set value in cache hierarchy"""

        # Store in L1 (memory)
        self._add_to_l1(key, value)

        # Store in L2 (Redis)
        serialized = pickle.dumps(value)
        await self.redis_client.setex(key, ttl, serialized)

    def _add_to_l1(self, key: str, value: Any):
        """Add to L1 cache with LRU eviction"""
        if len(self.l1_cache) >= self.l1_max_size:
            # Evict oldest item (simple FIFO for demonstration)
            oldest_key = next(iter(self.l1_cache))
            del self.l1_cache[oldest_key]

        self.l1_cache[key] = value

    async def bulk_get(self, keys: list) -> dict:
        """Bulk get from Redis for efficiency"""
        pipeline = self.redis_client.pipeline()
        for key in keys:
            pipeline.get(key)

        results = await pipeline.execute()
        return {
            key: pickle.loads(value) if value else None
            for key, value in zip(keys, results)
        }

    async def invalidate(self, pattern: str):
        """Invalidate cache entries matching pattern"""
        # Clear from L1
        keys_to_remove = [k for k in self.l1_cache if pattern in k]
        for key in keys_to_remove:
            del self.l1_cache[key]

        # Clear from L2
        cursor = 0
        while True:
            cursor, keys = await self.redis_client.scan(
                cursor, match=f"*{pattern}*", count=1000
            )
            if keys:
                await self.redis_client.delete(*keys)
            if cursor == 0:
                break
```

### Cache Warming

```python
async def warm_cache_for_identity_matching():
    """Pre-load frequently accessed data into cache"""

    cache = MultiLayerCache()

    # Warm up common identity patterns
    common_patterns = [
        "SELECT * FROM identities WHERE ssn = $1",
        "SELECT * FROM identities WHERE phone_normalized = $1",
        "SELECT * FROM identities WHERE full_name_normalized = $1"
    ]

    # Pre-load frequently matched records
    frequent_matches = await db_pool.fetch("""
        SELECT identity_id1, identity_id2, confidence
        FROM matches
        WHERE confidence > 0.9
        ORDER BY match_count DESC
        LIMIT 10000
    """)

    for match in frequent_matches:
        key = f"match_{match['identity_id1']}_{match['identity_id2']}"
        await cache.set(key, match, ttl=7200)

    # Pre-load high-frequency identities
    high_frequency = await db_pool.fetch("""
        SELECT i.*
        FROM identities i
        JOIN (
            SELECT identity_id, COUNT(*) as cnt
            FROM matches
            GROUP BY identity_id
            ORDER BY cnt DESC
            LIMIT 5000
        ) m ON i.identity_id = m.identity_id
    """)

    for identity in high_frequency:
        key = f"identity_{identity['identity_id']}"
        await cache.set(key, identity, ttl=7200)

    print(f"Cache warmed with {len(frequent_matches) + len(high_frequency)} entries")
```

---

## Worker Optimization

### Worker Configuration

```python
class OptimizedWorkerConfig:
    """Optimized worker configuration for 70M records"""

    WORKER_POOLS = {
        "matching": {
            "count": 30,
            "capabilities": ["identity_matching", "deduplication"],
            "resources": {
                "cpu_cores": 4,
                "memory_gb": 16,
                "gpu": True
            },
            "concurrency": {
                "max_chunks": 2,
                "prefetch": 5
            }
        },
        "validation": {
            "count": 10,
            "capabilities": ["data_validation", "cleansing"],
            "resources": {
                "cpu_cores": 2,
                "memory_gb": 8,
                "gpu": False
            },
            "concurrency": {
                "max_chunks": 4,
                "prefetch": 10
            }
        },
        "aggregation": {
            "count": 10,
            "capabilities": ["result_aggregation", "reporting"],
            "resources": {
                "cpu_cores": 4,
                "memory_gb": 32,
                "gpu": False
            },
            "concurrency": {
                "max_chunks": 1,
                "prefetch": 2
            }
        }
    }

    WORKER_TUNING = {
        "heartbeat_interval": 30,
        "chunk_timeout": 300,
        "retry_delay": 60,
        "max_retries": 3,
        "memory_limit_percent": 80,
        "cpu_limit_percent": 90
    }
```

### Worker Pool Management

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp

class OptimizedWorkerPool:
    """Optimized worker pool with advanced features"""

    def __init__(self, pool_config):
        self.config = pool_config
        self.executor = ThreadPoolExecutor(
            max_workers=pool_config['count'],
            thread_name_prefix="worker"
        )
        self.semaphore = asyncio.Semaphore(pool_config['concurrency']['max_chunks'])

    async def process_chunk(self, chunk_data):
        """Process chunk with resource management"""

        async with self.semaphore:
            # CPU-bound processing in thread pool
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                self._process_chunk_cpu_bound,
                chunk_data
            )

            return result

    def _process_chunk_cpu_bound(self, chunk_data):
        """CPU-intensive processing"""

        # Set process affinity for better cache usage
        if hasattr(os, 'sched_setaffinity'):
            cpu_count = mp.cpu_count()
            # Bind to specific CPU cores
            os.sched_setaffinity(0, {i % cpu_count for i in range(4)})

        # Process with optimizations
        results = []
        for record in chunk_data:
            result = self._process_record_optimized(record)
            results.append(result)

        return results

    def _process_record_optimized(self, record):
        """Optimized record processing"""

        # Use vectorized operations where possible
        # Minimize memory allocations
        # Use compiled extensions (Cython, numba)

        return processed_record
```

### Load Balancing

```python
class IntelligentLoadBalancer:
    """Intelligent load balancing for workers"""

    def __init__(self):
        self.worker_stats = {}
        self.assignment_history = []

    async def assign_chunk_to_worker(self, chunk, available_workers):
        """Assign chunk to optimal worker"""

        # Calculate worker scores
        worker_scores = []
        for worker in available_workers:
            score = self._calculate_worker_score(worker)
            worker_scores.append((worker, score))

        # Sort by score (higher is better)
        worker_scores.sort(key=lambda x: x[1], reverse=True)

        # Select best worker
        selected_worker = worker_scores[0][0]

        # Update statistics
        self._update_assignment_stats(selected_worker, chunk)

        return selected_worker

    def _calculate_worker_score(self, worker):
        """Calculate worker suitability score"""

        base_score = 100.0

        # Factor 1: Current load (lower is better)
        load_penalty = worker['current_chunks'] * 20
        base_score -= load_penalty

        # Factor 2: Recent success rate
        success_bonus = worker['success_rate'] * 10
        base_score += success_bonus

        # Factor 3: Resource availability
        cpu_available = (100 - worker['cpu_usage']) / 100
        mem_available = (100 - worker['memory_usage']) / 100
        resource_score = (cpu_available * 0.5 + mem_available * 0.5) * 30
        base_score += resource_score

        # Factor 4: Network latency
        latency_penalty = min(worker['latency_ms'] / 10, 20)
        base_score -= latency_penalty

        # Factor 5: Specialization match
        if 'identity_matching' in worker['capabilities']:
            base_score += 15

        return max(0, base_score)
```

---

## Network Optimization

### Data Compression

```python
import zstandard as zstd
import lz4.frame
import snappy

class OptimizedDataTransfer:
    """Optimized data transfer with compression"""

    def __init__(self):
        # Initialize compressors
        self.zstd_compressor = zstd.ZstdCompressor(level=3)
        self.zstd_decompressor = zstd.ZstdDecompressor()

    def compress_chunk(self, data: bytes, algorithm='snappy') -> bytes:
        """Compress data for network transfer"""

        if algorithm == 'zstd':
            # Zstandard - best compression ratio
            return self.zstd_compressor.compress(data)

        elif algorithm == 'lz4':
            # LZ4 - fastest compression
            return lz4.frame.compress(data, compression_level=0)

        elif algorithm == 'snappy':
            # Snappy - good balance
            return snappy.compress(data)

        else:
            return data

    def decompress_chunk(self, data: bytes, algorithm='snappy') -> bytes:
        """Decompress received data"""

        if algorithm == 'zstd':
            return self.zstd_decompressor.decompress(data)
        elif algorithm == 'lz4':
            return lz4.frame.decompress(data)
        elif algorithm == 'snappy':
            return snappy.decompress(data)
        else:
            return data

    def calculate_compression_ratio(self, original: bytes, compressed: bytes) -> float:
        """Calculate compression ratio"""
        return len(original) / len(compressed)
```

### Batch Processing

```python
class BatchProcessor:
    """Batch processing for network efficiency"""

    def __init__(self, batch_size=1000):
        self.batch_size = batch_size
        self.pending_items = []
        self.flush_interval = 5  # seconds

    async def add_item(self, item):
        """Add item to batch"""
        self.pending_items.append(item)

        if len(self.pending_items) >= self.batch_size:
            await self.flush()

    async def flush(self):
        """Send batch over network"""
        if not self.pending_items:
            return

        # Batch serialize
        batch_data = self._serialize_batch(self.pending_items)

        # Compress
        compressed = self.compress_chunk(batch_data)

        # Send
        await self._send_batch(compressed)

        # Clear pending
        self.pending_items.clear()

    def _serialize_batch(self, items):
        """Efficiently serialize batch"""
        # Use msgpack or protocol buffers for efficiency
        import msgpack
        return msgpack.packb(items)
```

---

## Algorithm Optimization

### Optimized Identity Matching

```python
import numpy as np
from numba import jit, prange

class OptimizedIdentityMatcher:
    """Highly optimized identity matching algorithms"""

    @staticmethod
    @jit(nopython=True, parallel=True)
    def calculate_similarity_matrix(features1, features2):
        """Calculate similarity matrix using SIMD operations"""

        n1, d = features1.shape
        n2, _ = features2.shape
        similarity_matrix = np.zeros((n1, n2), dtype=np.float32)

        for i in prange(n1):
            for j in range(n2):
                similarity = 0.0
                for k in range(d):
                    diff = features1[i, k] - features2[j, k]
                    similarity += diff * diff

                similarity_matrix[i, j] = 1.0 / (1.0 + np.sqrt(similarity))

        return similarity_matrix

    @staticmethod
    def find_matches_vectorized(identities1, identities2, threshold=0.85):
        """Vectorized matching for better performance"""

        # Convert to numpy arrays for vectorization
        features1 = np.array([i.get_features() for i in identities1])
        features2 = np.array([i.get_features() for i in identities2])

        # Calculate similarity matrix
        similarity_matrix = OptimizedIdentityMatcher.calculate_similarity_matrix(
            features1, features2
        )

        # Find matches above threshold
        matches = np.argwhere(similarity_matrix > threshold)

        return [(identities1[i], identities2[j], similarity_matrix[i, j])
                for i, j in matches]
```

### Parallel Processing

```python
import asyncio
from concurrent.futures import ProcessPoolExecutor

class ParallelProcessor:
    """Parallel processing for maximum throughput"""

    def __init__(self, num_processes=None):
        self.executor = ProcessPoolExecutor(
            max_workers=num_processes or mp.cpu_count()
        )

    async def process_chunks_parallel(self, chunks):
        """Process multiple chunks in parallel"""

        loop = asyncio.get_event_loop()
        tasks = []

        for chunk in chunks:
            task = loop.run_in_executor(
                self.executor,
                self._process_chunk_optimized,
                chunk
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks)
        return results

    @staticmethod
    def _process_chunk_optimized(chunk_data):
        """Optimized chunk processing"""

        # Use numpy for vectorized operations
        import numpy as np

        # Convert to numpy array
        data_array = np.array(chunk_data)

        # Vectorized operations
        # Example: normalize all values
        normalized = (data_array - np.mean(data_array, axis=0)) / np.std(data_array, axis=0)

        return normalized.tolist()
```

---

## Resource Management

### Memory Management

```python
import gc
import tracemalloc
import psutil

class MemoryManager:
    """Intelligent memory management"""

    def __init__(self, max_memory_percent=80):
        self.max_memory_percent = max_memory_percent
        tracemalloc.start()

    def check_memory_usage(self):
        """Check current memory usage"""
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()

        return {
            'rss_mb': memory_info.rss / 1024 / 1024,
            'vms_mb': memory_info.vms / 1024 / 1024,
            'percent': memory_percent
        }

    def optimize_if_needed(self):
        """Optimize memory if usage is high"""
        usage = self.check_memory_usage()

        if usage['percent'] > self.max_memory_percent:
            # Force garbage collection
            gc.collect()
            gc.collect()  # Second pass for circular references

            # Clear caches
            self._clear_caches()

            # Reduce chunk sizes
            self._adjust_chunk_sizes()

    def _clear_caches(self):
        """Clear various caches"""
        # Clear LRU caches
        for cache_func in []:  # Add cached functions
            cache_func.cache_clear()

        # Clear custom caches
        # ...

    def _adjust_chunk_sizes(self):
        """Dynamically adjust chunk sizes based on memory"""
        current_usage = self.check_memory_usage()

        if current_usage['percent'] > 90:
            # Critical - reduce chunk size significantly
            return 1000
        elif current_usage['percent'] > 70:
            # High - reduce moderately
            return 2000
        else:
            # Normal - use optimal size
            return 2500
```

### CPU Management

```python
import os
import threading

class CPUManager:
    """CPU resource management"""

    def __init__(self):
        self.cpu_count = mp.cpu_count()

    def set_thread_affinity(self, thread_id, cpu_cores):
        """Set CPU affinity for thread"""
        if hasattr(os, 'sched_setaffinity'):
            os.sched_setaffinity(thread_id, cpu_cores)

    def optimize_thread_pool(self):
        """Optimize thread pool size"""
        # Formula: min(32, (cpu_count + 4))
        optimal_threads = min(32, self.cpu_count + 4)
        return optimal_threads

    def enable_numa_awareness(self):
        """Enable NUMA awareness for better performance"""
        # Set NUMA policy
        try:
            import numa
            numa.set_localalloc()
        except ImportError:
            pass
```

---

## Performance Monitoring

### Metrics Collection

```python
import time
from dataclasses import dataclass
from typing import Dict

@dataclass
class PerformanceMetrics:
    """Performance metrics container"""
    records_processed: int = 0
    processing_time: float = 0.0
    avg_chunk_time: float = 0.0
    peak_memory_mb: float = 0.0
    peak_cpu_percent: float = 0.0
    error_rate: float = 0.0

class PerformanceMonitor:
    """Real-time performance monitoring"""

    def __init__(self):
        self.metrics = PerformanceMetrics()
        self.chunk_times = []
        self.start_time = time.time()

    def record_chunk_processing(self, chunk_size: int, duration: float):
        """Record chunk processing metrics"""
        self.metrics.records_processed += chunk_size
        self.chunk_times.append(duration)

        # Update average
        self.metrics.avg_chunk_time = sum(self.chunk_times) / len(self.chunk_times)

        # Calculate rate
        elapsed = time.time() - self.start_time
        self.metrics.processing_time = elapsed

        return self.get_current_rate()

    def get_current_rate(self) -> float:
        """Get current processing rate (records/sec)"""
        if self.metrics.processing_time > 0:
            return self.metrics.records_processed / self.metrics.processing_time
        return 0.0

    def get_eta(self, total_records: int) -> float:
        """Calculate ETA in seconds"""
        rate = self.get_current_rate()
        if rate > 0:
            remaining = total_records - self.metrics.records_processed
            return remaining / rate
        return float('inf')
```

### Performance Dashboard

```python
async def performance_dashboard(orchestrator):
    """Real-time performance dashboard"""

    monitor = PerformanceMonitor()

    while True:
        # Collect metrics
        stats = await orchestrator.get_system_health()
        queue_stats = await orchestrator.get_queue_statistics()

        # Display dashboard
        print("\033[2J\033[H")  # Clear screen
        print("="*60)
        print("PERFORMANCE DASHBOARD - 70M Record Processing")
        print("="*60)

        # Processing metrics
        rate = monitor.get_current_rate()
        eta = monitor.get_eta(70_000_000)

        print(f"\nProcessing Performance:")
        print(f"  Current Rate: {rate:.0f} records/sec")
        print(f"  Avg Chunk Time: {monitor.metrics.avg_chunk_time:.2f}s")
        print(f"  Total Processed: {monitor.metrics.records_processed:,}")
        print(f"  ETA: {eta/3600:.1f} hours")

        # Resource metrics
        print(f"\nResource Usage:")
        print(f"  CPU Usage: {stats.get('cpu_percent', 0):.1f}%")
        print(f"  Memory Usage: {stats.get('memory_percent', 0):.1f}%")
        print(f"  Active Workers: {stats.get('active_workers', 0)}")
        print(f"  Queue Depth: {queue_stats.get('queue_depth', 0)}")

        # Optimization suggestions
        if rate < 2000:
            print("\n⚠️ Performance Below Target:")
            if stats.get('cpu_percent', 0) > 90:
                print("  - CPU bottleneck detected, consider adding workers")
            if stats.get('memory_percent', 0) > 80:
                print("  - High memory usage, reduce chunk sizes")
            if queue_stats.get('queue_depth', 0) > 10000:
                print("  - Queue backlog, scale up workers")

        await asyncio.sleep(5)
```

---

## Optimization Checklist

### Before Processing
- [ ] Database indexes created
- [ ] PostgreSQL configured for performance
- [ ] Redis cache warmed up
- [ ] Worker pools optimized
- [ ] Network compression enabled

### During Processing
- [ ] Monitor resource usage
- [ ] Adjust chunk sizes dynamically
- [ ] Balance worker loads
- [ ] Clear caches periodically
- [ ] Track performance metrics

### After Processing
- [ ] Analyze performance metrics
- [ ] Identify bottlenecks
- [ ] Update optimization settings
- [ ] Document lessons learned
- [ ] Plan improvements

---

## Next Steps

1. Review [Monitoring & Operations Guide](./05-monitoring-operations.md)
2. Check [Troubleshooting Guide](./06-troubleshooting-guide.md)

---

*Document Version: 1.0*
*Target Performance: 3,000-5,000 records/second*
*Optimization Level: Production*