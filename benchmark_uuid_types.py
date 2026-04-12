"""
Benchmark v2: UUID v1 vs v4 vs v6 vs v7 vs Snowflake ID
=========================================================
Versão corrigida com:
  - 7 runs com descarte do melhor e pior (trimmed mean de 5)
  - Warmup real do InnoDB buffer pool antes de cada cenário
  - Flush tables entre cenários para estado limpo
  - Mediana reportada junto com a média
  - CSV com todas as runs individuais para análise

Pré-requisitos:
  pip install mysql-connector-python uuid6

Docker MySQL:
  docker run --name mysql-bench -e MYSQL_ROOT_PASSWORD=root \
    -e MYSQL_DATABASE=test -p 3306:3306 -d mysql:8.0
"""

import mysql.connector
import time
import random
import statistics
import uuid
import os
from concurrent.futures import ThreadPoolExecutor

try:
    import uuid6
    HAS_UUID6 = True
except ImportError:
    HAS_UUID6 = False
    os.system("pip install uuid6 --break-system-packages -q 2>/dev/null || pip install uuid6 -q")
    try:
        import uuid6
        HAS_UUID6 = True
    except ImportError:
        HAS_UUID6 = False
        print("❌  uuid6 não disponível. v6/v7 serão simulados.")

CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "root",
    "database": "test",
}

THREADS = 4
RUNS = 7
ROW_COUNTS = [50_000, 200_000]
BATCH_SIZE = 1000

ID_TYPES = ["int_auto", "uuid_v1", "uuid_v4", "uuid_v7", "snowflake"]
if HAS_UUID6:
    ID_TYPES = ["int_auto", "uuid_v1", "uuid_v4", "uuid_v6", "uuid_v7", "snowflake"]

INDEX_SCENARIOS = {
    "baseline": [],
    "few": [
        "CREATE INDEX idx_a ON test_bench(col_a)",
        "CREATE INDEX idx_b ON test_bench(col_b)",
    ],
    "medium": [
        "CREATE INDEX idx_a ON test_bench(col_a)",
        "CREATE INDEX idx_b ON test_bench(col_b)",
        "CREATE INDEX idx_c ON test_bench(col_c)",
        "CREATE INDEX idx_d ON test_bench(col_d)",
        "CREATE INDEX idx_ab ON test_bench(col_a, col_b)",
    ],
    "many": [
        "CREATE INDEX idx_a ON test_bench(col_a)",
        "CREATE INDEX idx_b ON test_bench(col_b)",
        "CREATE INDEX idx_c ON test_bench(col_c)",
        "CREATE INDEX idx_d ON test_bench(col_d)",
        "CREATE INDEX idx_e ON test_bench(col_e)",
        "CREATE INDEX idx_f ON test_bench(col_f)",
        "CREATE INDEX idx_ab ON test_bench(col_a, col_b)",
        "CREATE INDEX idx_cd ON test_bench(col_c, col_d)",
        "CREATE INDEX idx_ef ON test_bench(col_e, col_f)",
    ],
}

class SnowflakeIDGenerator:
    EPOCH = 1700000000000

    def __init__(self, machine_id=1):
        self.machine_id = machine_id & 0x3FF
        self.sequence = 0
        self.last_ts = -1

    def _current_ms(self):
        return int(time.time() * 1000)

    def generate(self):
        ts = self._current_ms() - self.EPOCH
        if ts == self.last_ts:
            self.sequence = (self.sequence + 1) & 0xFFF
            if self.sequence == 0:
                while ts <= self.last_ts:
                    ts = self._current_ms() - self.EPOCH
        else:
            self.sequence = 0
        self.last_ts = ts
        return (ts << 22) | (self.machine_id << 12) | self.sequence


def generate_uuid_v1():
    return str(uuid.uuid1())

def generate_uuid_v4():
    return str(uuid.uuid4())

def generate_uuid_v6():
    if HAS_UUID6:
        return str(uuid6.uuid6())
    v1 = uuid.uuid1()
    h = v1.hex
    reordered = h[13:16] + h[8:12] + h[0:8] + h[16:]
    return f"{reordered[:8]}-{reordered[8:12]}-6{reordered[13:16]}-{h[16:20]}-{h[20:]}"

def generate_uuid_v7():
    if HAS_UUID6:
        return str(uuid6.uuid7())
    ts_ms = int(time.time() * 1000)
    ts_bytes = ts_ms.to_bytes(6, "big")
    rand_bytes = os.urandom(10)
    b = bytearray(ts_bytes + rand_bytes)
    b[6] = (b[6] & 0x0F) | 0x70
    b[8] = (b[8] & 0x3F) | 0x80
    h = b.hex()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"

def generate_snowflake():
    return SnowflakeIDGenerator(machine_id=random.randint(0, 1023)).generate()

def get_conn():
    return mysql.connector.connect(**CONFIG)


def reset_and_flush():
    """Reset completo: drop/create database + flush do InnoDB."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DROP DATABASE IF EXISTS test")
    cur.execute("CREATE DATABASE test")
    cur.execute("USE test")
    cur.execute("SET GLOBAL innodb_buffer_pool_dump_now = ON")
    cur.close()
    conn.close()
    time.sleep(0.5)


def create_table(cursor, id_type):
    cursor.execute("DROP TABLE IF EXISTS test_bench")

    if id_type == "int_auto":
        pk_def = "id BIGINT AUTO_INCREMENT PRIMARY KEY"
        col_type = "INT"
    elif id_type == "snowflake":
        pk_def = "id BIGINT PRIMARY KEY"
        col_type = "BIGINT"
    else:
        pk_def = "id BINARY(16) PRIMARY KEY"
        col_type = "BINARY(16)"

    cursor.execute(f"""
        CREATE TABLE test_bench (
            {pk_def},
            col_a {col_type},
            col_b {col_type},
            col_c {col_type},
            col_d {col_type},
            col_e {col_type},
            col_f {col_type},
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB
    """)


def create_indexes(cursor, scenario):
    for q in INDEX_SCENARIOS.get(scenario, []):
        cursor.execute(q)

def uuid_str_to_bin(uuid_str):
    return uuid.UUID(uuid_str).bytes


def generate_row_data(n, id_type):
    rows = []

    if id_type == "int_auto":
        for _ in range(n):
            cols = tuple(random.randint(1, 1_000_000) for _ in range(6))
            rows.append(cols)
        return rows

    if id_type == "snowflake":
        gen = SnowflakeIDGenerator(machine_id=random.randint(0, 1023))
        for _ in range(n):
            sid = gen.generate()
            cols = tuple(random.randint(1, 1_000_000) for _ in range(6))
            rows.append((sid,) + cols)
        return rows

    generators = {
        "uuid_v1": generate_uuid_v1,
        "uuid_v4": generate_uuid_v4,
        "uuid_v6": generate_uuid_v6,
        "uuid_v7": generate_uuid_v7,
    }
    gen_fn = generators[id_type]

    for _ in range(n):
        pk = uuid_str_to_bin(gen_fn())
        cols = tuple(uuid_str_to_bin(gen_fn()) for _ in range(6))
        rows.append((pk,) + cols)

    return rows

def insert_worker(args):
    data_chunk, id_type = args
    conn = get_conn()
    cursor = conn.cursor()

    if id_type == "int_auto":
        sql = """INSERT INTO test_bench (col_a, col_b, col_c, col_d, col_e, col_f)
                 VALUES (%s, %s, %s, %s, %s, %s)"""
    else:
        sql = """INSERT INTO test_bench (id, col_a, col_b, col_c, col_d, col_e, col_f)
                 VALUES (%s, %s, %s, %s, %s, %s, %s)"""

    for i in range(0, len(data_chunk), BATCH_SIZE):
        batch = data_chunk[i : i + BATCH_SIZE]
        cursor.executemany(sql, batch)
        conn.commit()

    cursor.close()
    conn.close()


def run_concurrent_insert(data, id_type):
    chunk_size = len(data) // THREADS
    chunks = []
    for i in range(THREADS):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < THREADS - 1 else len(data)
        chunks.append((data[start:end], id_type))

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        executor.map(insert_worker, chunks)
    return time.time() - t0

def warmup_innodb(id_type, scenario):
    """
    Faz uma run descartável para aquecer o InnoDB buffer pool,
    caches do OS e connection pool. Resultado é jogado fora.
    """
    reset_and_flush()
    conn = get_conn()
    cur = conn.cursor()
    create_table(cur, id_type)
    create_indexes(cur, scenario)
    conn.commit()

    # Insere 10k rows como warmup
    warmup_data = generate_row_data(10_000, id_type)
    run_concurrent_insert(warmup_data, id_type)

    cur.close()
    conn.close()

def trimmed_mean(values):
    """Remove o maior e o menor, retorna média dos restantes."""
    if len(values) <= 2:
        return statistics.mean(values)
    s = sorted(values)
    trimmed = s[1:-1]
    return statistics.mean(trimmed)

def run_scenario(id_type, scenario, n_rows):
    print(f"    [warmup]", end=" ", flush=True)
    warmup_innodb(id_type, scenario)

    times = []
    for run in range(RUNS):
        reset_and_flush()
        conn = get_conn()
        cur = conn.cursor()

        create_table(cur, id_type)
        create_indexes(cur, scenario)
        conn.commit()

        data = generate_row_data(n_rows, id_type)

        duration = run_concurrent_insert(data, id_type)
        times.append(duration)
        print(f"{duration:.2f}s", end=" ", flush=True)

        cur.close()
        conn.close()

    print()

    avg = trimmed_mean(times)
    med = statistics.median(times)
    return avg, med, times


def main():
    print("=" * 70)
    print("  BENCHMARK v2: UUID v1 vs v4 vs v6 vs v7 vs Snowflake vs INT")
    print("=" * 70)
    print(f"  Threads: {THREADS} | Runs: {RUNS} (trimmed mean descarta min/max)")
    print(f"  Batch size: {BATCH_SIZE}")
    print(f"  ID types: {ID_TYPES}")
    print(f"  Row counts: {ROW_COUNTS}")
    print("=" * 70)

    all_results = []
    all_runs_detail = []  # CSV com cada run individual

    for id_type in ID_TYPES:
        print(f"\n{'─'*70}")
        print(f"  TIPO DE ID: {id_type.upper()}")
        print(f"{'─'*70}")

        for n_rows in ROW_COUNTS:
            print(f"\n  ── {n_rows:,} linhas ──\n")

            for scenario in INDEX_SCENARIOS:
                print(f"  {scenario:10}:", end=" ", flush=True)

                avg, med, times = run_scenario(id_type, scenario, n_rows)
                throughput_avg = n_rows / avg
                throughput_med = n_rows / med

                result = {
                    "id_type": id_type,
                    "rows": n_rows,
                    "scenario": scenario,
                    "avg_time_s": round(avg, 3),
                    "median_time_s": round(med, 3),
                    "throughput_avg": round(throughput_avg, 0),
                    "throughput_med": round(throughput_med, 0),
                    "min_time": round(min(times), 3),
                    "max_time": round(max(times), 3),
                    "std_dev": round(statistics.stdev(times), 3) if len(times) > 1 else 0,
                }
                all_results.append(result)

                for i, t in enumerate(times):
                    all_runs_detail.append({
                        "id_type": id_type,
                        "rows": n_rows,
                        "scenario": scenario,
                        "run": i + 1,
                        "time_s": round(t, 3),
                    })

                print(
                    f"  ✔ trimmed_avg={avg:.2f}s  median={med:.2f}s  "
                    f"throughput={throughput_avg:,.0f}/s  "
                    f"(std={result['std_dev']:.3f})"
                )

    print("\n" + "=" * 90)
    print("  RESUMO FINAL (trimmed mean — sem outliers)")
    print("=" * 90)
    print(
        f"  {'ID Type':<12} {'Rows':>8} {'Scenario':<10} "
        f"{'Avg Time':>10} {'Median':>10} {'Throughput':>14} {'StdDev':>8}"
    )
    print("  " + "-" * 75)
    for r in all_results:
        print(
            f"  {r['id_type']:<12} {r['rows']:>8,} {r['scenario']:<10} "
            f"{r['avg_time_s']:>9.3f}s {r['median_time_s']:>9.3f}s "
            f"{r['throughput_avg']:>13,.0f}/s {r['std_dev']:>7.3f}"
        )

    csv_path = "benchmark_uuid_results_v2.csv"
    with open(csv_path, "w") as f:
        f.write("id_type,rows,scenario,avg_time_s,median_time_s,throughput_avg,throughput_med,min_time,max_time,std_dev\n")
        for r in all_results:
            f.write(
                f"{r['id_type']},{r['rows']},{r['scenario']},"
                f"{r['avg_time_s']},{r['median_time_s']},"
                f"{r['throughput_avg']},{r['throughput_med']},"
                f"{r['min_time']},{r['max_time']},{r['std_dev']}\n"
            )

    csv_runs_path = "benchmark_uuid_all_runs.csv"
    with open(csv_runs_path, "w") as f:
        f.write("id_type,rows,scenario,run,time_s\n")
        for r in all_runs_detail:
            f.write(f"{r['id_type']},{r['rows']},{r['scenario']},{r['run']},{r['time_s']}\n")

if __name__ == "__main__":
    main()
