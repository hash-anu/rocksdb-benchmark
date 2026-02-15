/*
** RocksDB Performance Benchmark - Fair Comparison with SNKV
**
** Tests: Sequential writes, random reads, sequential scan,
**        random updates, random deletes, bulk operations
**
** Includes memory consumption tracking
** Configured to match KVStore's resource profile
**
** DURABILITY: sync=true on every WriteBatch commit to match
** SNKV's kvstore_commit() which fsyncs the WAL on each call
** (SQLite default: synchronous=FULL in WAL mode).
*/

#include <iostream>
#include <string>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <sys/time.h>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"

#define DB_FILE      "benchmark_rocksdb"
#define NUM_RECORDS  1000000
#define BATCH_SIZE   1000
#define NUM_READS    50000
#define NUM_UPDATES  10000
#define NUM_DELETES  5000

#define COLOR_BLUE   "\x1b[34m"
#define COLOR_GREEN  "\x1b[32m"
#define COLOR_YELLOW "\x1b[33m"
#define COLOR_CYAN   "\x1b[36m"
#define COLOR_RESET  "\x1b[0m"

using namespace rocksdb;

/* High-resolution timer */
static double get_time(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

/* Format numbers with commas */
static void format_number(long long num, char *buf, size_t size) {
    if (num >= 1000000) {
        snprintf(buf, size, "%lld,%03lld,%03lld",
                 num/1000000, (num/1000)%1000, num%1000);
    } else if (num >= 1000) {
        snprintf(buf, size, "%lld,%03lld", num/1000, num%1000);
    } else {
        snprintf(buf, size, "%lld", num);
    }
}

/* Get process memory usage in KB */
static long get_memory_usage() {
    long rss = 0;
    FILE* fp = fopen("/proc/self/status", "r");
    if (fp) {
        char line[128];
        while (fgets(line, sizeof(line), fp)) {
            if (strncmp(line, "VmRSS:", 6) == 0) {
                sscanf(line + 6, "%ld", &rss);
                break;
            }
        }
        fclose(fp);
    }
    return rss;
}

/* Format memory size */
static void format_memory(long kb, char *buf, size_t size) {
    if (kb >= 1024 * 1024) {
        snprintf(buf, size, "%.2f GB", kb / (1024.0 * 1024.0));
    } else if (kb >= 1024) {
        snprintf(buf, size, "%.2f MB", kb / 1024.0);
    } else {
        snprintf(buf, size, "%ld KB", kb);
    }
}

static void print_result(const char *test, double elapsed, int ops) {
    double ops_per_sec = ops / elapsed;
    char buf[32];
    format_number((long long)ops_per_sec, buf, sizeof(buf));

    printf("  %-30s: ", test);
    printf(COLOR_GREEN "%s ops/sec" COLOR_RESET " ", buf);
    printf("(%.3f seconds for %d ops)\n", elapsed, ops);
}

static void print_header(const char *title) {
    printf("\n" COLOR_CYAN);
    printf("════════════════════════════════════════════════════════\n");
    printf("  %s\n", title);
    printf("════════════════════════════════════════════════════════\n");
    printf(COLOR_RESET);
}

/* Configure RocksDB options for small database (matching KVStore) */
static void configure_small_db_options(Options &options) {
    // Basic settings
    options.create_if_missing = true;
    options.error_if_exists = false;

    // Disable compression (KVStore doesn't use compression)
    options.compression = kNoCompression;

    // Block cache: 2MB (matching SQLite 2000 pages x 1KB)
    BlockBasedTableOptions table_options;
    table_options.block_cache = NewLRUCache(2 * 1024 * 1024);  // 2MB
    table_options.block_size = 4 * 1024;  // 4KB blocks (closer to SQLite page size)

    // Disable bloom filters for small DB (saves memory)
    table_options.filter_policy = nullptr;

    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    // Small memtable (KVStore commits more frequently)
    options.write_buffer_size = 2 * 1024 * 1024;  // 2MB memtable
    options.max_write_buffer_number = 2;
    options.min_write_buffer_number_to_merge = 1;

    // Reduce number of levels for small DB
    options.num_levels = 4;

    // Smaller file sizes
    options.target_file_size_base = 2 * 1024 * 1024;  // 2MB
    options.max_bytes_for_level_base = 8 * 1024 * 1024;  // 8MB

    // Reduce background threads for small DB
    options.max_background_jobs = 2;
    options.max_background_compactions = 1;
    options.max_background_flushes = 1;

    // Optimize for sequential access patterns
    options.allow_mmap_reads = false;
    options.allow_mmap_writes = false;

    // Reduce internal cache sizes
    options.max_open_files = 100;  // Limit file descriptors

    // Statistics
    options.statistics = CreateDBStatistics();

    printf("  Configuration:\n");
    printf("    - Block cache:       2 MB\n");
    printf("    - Write buffer:      2 MB\n");
    printf("    - Block size:        4 KB\n");
    printf("    - Compression:       Disabled\n");
    printf("    - Bloom filters:     Disabled\n");
    printf("    - Num levels:        4\n");
    printf("    - Target file size:  2 MB\n");
    printf("    - Max open files:    100\n");
    printf("    - Sync on commit:    Yes (matching SNKV)\n");
}

/* ==================== BENCHMARK 1: Sequential Writes ==================== */
static void bench_sequential_writes(DB *db) {
    print_header("BENCHMARK 1: Sequential Writes");
    printf("  Writing %d records in batches of %d...\n\n", NUM_RECORDS, BATCH_SIZE);

    char key[32], value[128];
    int i;
    double start, end;

    WriteOptions write_opts;
    write_opts.sync = true;  // Match SNKV's per-commit fsync

    start = get_time();

    for (i = 0; i < NUM_RECORDS; ) {
        WriteBatch batch;

        for (int j = 0; j < BATCH_SIZE && i < NUM_RECORDS; j++, i++) {
            snprintf(key, sizeof(key), "key_%08d", i);
            snprintf(value, sizeof(value),
                     "value_%08d_with_some_additional_data_to_make_it_realistic", i);

            batch.Put(Slice(key, strlen(key)), Slice(value, strlen(value)));
        }

        db->Write(write_opts, &batch);
    }

    end = get_time();

    print_result("Sequential writes", end - start, NUM_RECORDS);
}

/* ==================== BENCHMARK 2: Random Reads ==================== */
static void bench_random_reads(DB *db) {
    print_header("BENCHMARK 2: Random Reads");
    printf("  Reading %d random records...\n\n", NUM_READS);

    char key[32];
    std::string value;
    int i;
    double start, end;

    start = get_time();

    ReadOptions read_opts;
    for (i = 0; i < NUM_READS; i++) {
        int idx = rand() % NUM_RECORDS;
        snprintf(key, sizeof(key), "key_%08d", idx);

        db->Get(read_opts, Slice(key, strlen(key)), &value);
    }

    end = get_time();

    print_result("Random reads", end - start, NUM_READS);
}

/* ==================== BENCHMARK 3: Sequential Scan ==================== */
static void bench_sequential_scan(DB *db) {
    print_header("BENCHMARK 3: Sequential Scan");
    printf("  Scanning all records...\n\n");

    int count = 0;
    double start, end;

    start = get_time();

    ReadOptions read_opts;
    Iterator* it = db->NewIterator(read_opts);

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        Slice key = it->key();
        Slice value = it->value();
        count++;
    }

    delete it;

    end = get_time();

    print_result("Sequential scan", end - start, count);
}

/* ==================== BENCHMARK 4: Random Updates ==================== */
static void bench_random_updates(DB *db) {
    print_header("BENCHMARK 4: Random Updates");
    printf("  Updating %d random records...\n\n", NUM_UPDATES);

    char key[32], value[128];
    int i;
    double start, end;

    start = get_time();

    WriteBatch batch;

    for (i = 0; i < NUM_UPDATES; i++) {
        int idx = rand() % NUM_RECORDS;
        snprintf(key, sizeof(key), "key_%08d", idx);
        snprintf(value, sizeof(value), "updated_value_%08d", idx);

        batch.Put(Slice(key, strlen(key)), Slice(value, strlen(value)));
    }

    WriteOptions write_opts;
    write_opts.sync = true;  // Match SNKV's per-commit fsync
    db->Write(write_opts, &batch);

    end = get_time();

    print_result("Random updates", end - start, NUM_UPDATES);
}

/* ==================== BENCHMARK 5: Random Deletes ==================== */
static void bench_random_deletes(DB *db) {
    print_header("BENCHMARK 5: Random Deletes");
    printf("  Deleting %d random records...\n\n", NUM_DELETES);

    char key[32];
    int i;
    double start, end;

    start = get_time();

    WriteBatch batch;

    for (i = 0; i < NUM_DELETES; i++) {
        int idx = rand() % NUM_RECORDS;
        snprintf(key, sizeof(key), "key_%08d", idx);
        batch.Delete(Slice(key, strlen(key)));
    }

    WriteOptions write_opts;
    write_opts.sync = true;  // Match SNKV's per-commit fsync
    db->Write(write_opts, &batch);

    end = get_time();

    print_result("Random deletes", end - start, NUM_DELETES);
}

/* ==================== BENCHMARK 6: Exists Checks ==================== */
static void bench_exists_checks(DB *db) {
    print_header("BENCHMARK 6: Exists Checks");
    printf("  Checking existence of %d keys...\n\n", NUM_READS);

    char key[32];
    std::string value;
    int i;
    double start, end;

    start = get_time();

    ReadOptions read_opts;
    for (i = 0; i < NUM_READS; i++) {
        int idx = rand() % NUM_RECORDS;
        snprintf(key, sizeof(key), "key_%08d", idx);

        // Note: RocksDB has no direct "exists" API equivalent to
        // kvstore_exists(). db->Get() reads the full value.
        // SNKV's kvstore_exists() only checks key presence without
        // reading the value, giving it a natural advantage here.
        Status s = db->Get(read_opts, Slice(key, strlen(key)), &value);
    }

    end = get_time();

    print_result("Exists checks", end - start, NUM_READS);
}

/* ==================== BENCHMARK 7: Mixed Workload ==================== */
static void bench_mixed_workload(DB *db) {
    print_header("BENCHMARK 7: Mixed Workload");
    printf("  70%% reads, 20%% writes, 10%% deletes...\n\n");

    int total_ops = 20000;
    char key[32], value[128];
    std::string val;
    int i;
    double start, end;

    start = get_time();

    WriteBatch batch;
    WriteOptions write_opts;
    write_opts.sync = true;  // Match SNKV's per-commit fsync
    ReadOptions read_opts;

    for (i = 0; i < total_ops; i++) {
        int idx = rand() % NUM_RECORDS;
        int op = rand() % 100;

        snprintf(key, sizeof(key), "key_%08d", idx);

        if (op < 70) {
            /* Read */
            db->Get(read_opts, Slice(key, strlen(key)), &val);
        } else if (op < 90) {
            /* Write */
            snprintf(value, sizeof(value), "mixed_value_%08d", idx);
            batch.Put(Slice(key, strlen(key)), Slice(value, strlen(value)));
        } else {
            /* Delete */
            batch.Delete(Slice(key, strlen(key)));
        }

        // Flush batch periodically (every 100 write ops, matching commit cadence)
        if (batch.Count() > 100) {
            db->Write(write_opts, &batch);
            batch.Clear();
        }
    }

    // Flush remaining operations
    if (batch.Count() > 0) {
        db->Write(write_opts, &batch);
    }

    end = get_time();

    print_result("Mixed workload", end - start, total_ops);
}

/* ==================== BENCHMARK 8: Bulk Insert ==================== */
static void bench_bulk_insert(void) {
    print_header("BENCHMARK 8: Bulk Insert (Single Transaction)");
    printf("  Inserting %d records in one transaction...\n\n", NUM_RECORDS);

    char key[32], value[128];
    int i;
    double start, end;

    // Open separate database for this test
    Options options;
    configure_small_db_options(options);

    DB* db;
    Status status = DB::Open(options, "benchmark_bulk_rocksdb", &db);
    if (!status.ok()) {
        fprintf(stderr, "Failed to open database for bulk insert\n");
        return;
    }

    start = get_time();

    WriteBatch batch;

    for (i = 0; i < NUM_RECORDS; i++) {
        snprintf(key, sizeof(key), "bulk_key_%08d", i);
        snprintf(value, sizeof(value), "bulk_value_%08d", i);
        batch.Put(Slice(key, strlen(key)), Slice(value, strlen(value)));
    }

    WriteOptions write_opts;
    write_opts.sync = true;  // Match SNKV's per-commit fsync
    db->Write(write_opts, &batch);

    end = get_time();

    delete db;

    // Cleanup
    DestroyDB("benchmark_bulk_rocksdb", options);

    print_result("Bulk insert", end - start, NUM_RECORDS);
}

/* ==================== Main ==================== */
int main(void) {
    DB *db = NULL;
    double total_start, total_end;
    long mem_start, mem_end, mem_peak;
    char mem_buf[64];

    printf("\n");
    printf(COLOR_BLUE "╔══════════════════════════════════════════════════════════════╗\n");
    printf("║          RocksDB Performance Benchmark (Small DB)           ║\n");
    printf("║                                                              ║\n");
    printf("║  Database: %-50s║\n", DB_FILE);
    printf("║  Records:  %-50d║\n", NUM_RECORDS);
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf(COLOR_RESET);

    srand(time(NULL));

    /* Measure initial memory */
    mem_start = get_memory_usage();

    /* Initialize database */
    printf("\n" COLOR_YELLOW "Initializing database..." COLOR_RESET "\n");

    Options options;
    configure_small_db_options(options);

    // Cleanup existing database
    DestroyDB(DB_FILE, options);

    Status status = DB::Open(options, DB_FILE, &db);
    if (!status.ok()) {
        fprintf(stderr, "Failed to open RocksDB: %s\n", status.ToString().c_str());
        return 1;
    }

    long mem_after_open = get_memory_usage();
    format_memory(mem_after_open - mem_start, mem_buf, sizeof(mem_buf));
    printf("  Memory after opening DB: %s\n", mem_buf);

    total_start = get_time();
    mem_peak = mem_after_open;

    /* Run benchmarks */
    bench_sequential_writes(db);
    long mem_after_writes = get_memory_usage();
    if (mem_after_writes > mem_peak) mem_peak = mem_after_writes;

    bench_random_reads(db);
    long mem_after_reads = get_memory_usage();
    if (mem_after_reads > mem_peak) mem_peak = mem_after_reads;

    bench_sequential_scan(db);
    bench_random_updates(db);
    bench_random_deletes(db);
    bench_exists_checks(db);
    bench_mixed_workload(db);

    mem_end = get_memory_usage();
    if (mem_end > mem_peak) mem_peak = mem_end;

    // Print statistics
    std::string stats_str;
    if (options.statistics) {
        printf("\n");
        printf("  Total operations:\n");
        printf("    - Puts:    %llu\n",
               (unsigned long long)options.statistics->getTickerCount(NUMBER_KEYS_WRITTEN));
        printf("    - Gets:    %llu\n",
               (unsigned long long)options.statistics->getTickerCount(NUMBER_KEYS_READ));
        printf("    - Deletes: %llu\n",
               (unsigned long long)options.statistics->getTickerCount(NUMBER_KEYS_UPDATED));
    }

    // Get RocksDB memory stats
    std::string mem_usage;
    db->GetProperty("rocksdb.estimate-table-readers-mem", &mem_usage);
    uint64_t table_readers_mem = std::stoull(mem_usage);

    db->GetProperty("rocksdb.cur-size-all-mem-tables", &mem_usage);
    uint64_t memtable_mem = std::stoull(mem_usage);

    db->GetProperty("rocksdb.block-cache-usage", &mem_usage);
    uint64_t cache_mem = std::stoull(mem_usage);

    printf("\n");
    printf("  RocksDB Internal Memory Usage:\n");
    format_memory(cache_mem / 1024, mem_buf, sizeof(mem_buf));
    printf("    - Block cache:     %s\n", mem_buf);
    format_memory(memtable_mem / 1024, mem_buf, sizeof(mem_buf));
    printf("    - Memtables:       %s\n", mem_buf);
    format_memory(table_readers_mem / 1024, mem_buf, sizeof(mem_buf));
    printf("    - Table readers:   %s\n", mem_buf);
    format_memory((cache_mem + memtable_mem + table_readers_mem) / 1024, mem_buf, sizeof(mem_buf));
    printf("    - Total internal:  %s\n", mem_buf);

    delete db;

    bench_bulk_insert();

    total_end = get_time();

    /* Summary */
    printf("\n" COLOR_CYAN);
    printf("════════════════════════════════════════════════════════\n");
    printf("  SUMMARY\n");
    printf("════════════════════════════════════════════════════════\n");
    printf(COLOR_RESET);
    printf("  Total benchmark time: " COLOR_GREEN "%.2f seconds" COLOR_RESET "\n",
           total_end - total_start);

    printf("\n");
    printf("  Process Memory Usage:\n");
    format_memory(mem_start, mem_buf, sizeof(mem_buf));
    printf("    - Initial:  %s\n", mem_buf);
    format_memory(mem_end, mem_buf, sizeof(mem_buf));
    printf("    - Final:    %s\n", mem_buf);
    format_memory(mem_peak, mem_buf, sizeof(mem_buf));
    printf("    - Peak:     %s\n", mem_buf);
    format_memory(mem_end - mem_start, mem_buf, sizeof(mem_buf));
    printf("    - Delta:    %s\n", mem_buf);

    printf("\n" COLOR_GREEN "✓ Benchmark complete!" COLOR_RESET "\n\n");

    /* Cleanup */
    DestroyDB(DB_FILE, options);

    return 0;
}
