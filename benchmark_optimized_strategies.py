"""
Benchmark: Estratégias Otimizadas de INSERT com UUID no MySQL
==============================================================
Script separado que testa APENAS as diferentes técnicas de inserção.
Foco: "Qual a melhor forma de fazer INSERT massivo com cada tipo de ID?"

Estratégias testadas:
  1. executemany batch 100   (ingênuo)
  2. executemany batch 1000  (padrão)
  3. executemany batch 5000  (otimizado)
  4. executemany batch 10000 (agressivo)
  5. multi-row VALUES 5000   (single SQL statement)
  6. sort prévio + batch 5000 (ordena dados antes de inserir)

Cada estratégia é testada com: uuid_v4, uuid_v7, snowflake, int_auto
Em dois cenários de índice: baseline (0 idx) e many (9 idx)

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
    os.system("pip install uuid6 --break-system-packages -q 2>/dev/null || pip install uuid6 -q")
    try:
        import uuid6
        HAS_UUID6 = True
    except ImportError:
        HAS_UUID6 = False
        print("❌ uuid6 não disponível.")


# ═══════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════
CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "root",
    "database": "test",
}

THREADS = 4
RUNS = 7
ROW_COUNTS = [50_000, 200_000]

INDEX_SCENARIOS = {
    "baseline": [],
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

# Tipos de ID a testar nas estratégias
ID_TYPES = ["int_auto", "uuid_v4", "uuid_v7", "snowflake"]

# Estratégias de INSERT
STRATEGIES = [
    {"name": "executemany_b100",    "batch": 100,   "multi_row": False, "sort": False},
    {"name": "executemany_b1000",   "batch": 1000,  "multi_row": False, "sort": False},
    {"name": "executemany_b5000",   "batch": 5000,  "multi_row": False, "sort": False},
    {"name": "executemany_b10000",  "batch": 10000, "multi_row": False, "sort": False},
    {"name": "multi_row_b5000",     "batch": 5000,  "multi_row": True,  "sort": False},
    {"name": "sorted_b5000",        "batch": 5000,  "multi_row": False, "sort": True},
    {"name": "sorted_multi_b5000",  "batch": 5000,  "multi_row": True,  "sort": True},
]


# ═══════════════════════════════════════════════════
# ID GENERATORS
# ═══════════════════════════════════════════════════

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


def gen_uuid_v4():
    return uuid.uuid4().bytes

def gen_uuid_v7():
    if HAS_UUID6:
        return uuid6.uuid7().bytes
    ts_ms = int(time.time() * 1000)
    ts_bytes = ts_ms.to_bytes(6, "big")
    rand_bytes = os.urandom(10)
    b = bytearray(ts_bytes + rand_bytes)
    b[6] = (b[6] & 0x0F) | 0x70
    b[8] = (b[8] & 0x3F) | 0x80
    return bytes(b)

GEN_MAP = {"uuid_v4": gen_uuid_v4, "uuid_v7": gen_uuid_v7}


# ═══════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════

def get_conn():
    return mysql.connector.connect(**CONFIG)

def reset_and_flush():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DROP DATABASE IF EXISTS test")
    cur.execute("CREATE DATABASE test")
    cur.execute("USE test")
    try:
        cur.execute("SET GLOBAL innodb_buffer_pool_dump_now = ON")
    except:
        pass
    cur.close()
    conn.close()
    time.sleep(0.3)

def create_table(cursor, id_type):
    cursor.execute("DROP TABLE IF EXISTS test_bench")
    if id_type == "int_auto":
        pk = "id BIGINT AUTO_INCREMENT PRIMARY KEY"
        col = "INT"
    elif id_type == "snowflake":
        pk = "id BIGINT PRIMARY KEY"
        col = "BIGINT"
    else:
        pk = "id BINARY(16) PRIMARY KEY"
        col = "BINARY(16)"

    cursor.execute(f"""
        CREATE TABLE test_bench (
            {pk},
            col_a {col}, col_b {col}, col_c {col},
            col_d {col}, col_e {col}, col_f {col},
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB
    """)

def create_indexes(cursor, scenario):
    for q in INDEX_SCENARIOS.get(scenario, []):
        cursor.execute(q)


# ═══════════════════════════════════════════════════
# DATA GENERATION
# ═══════════════════════════════════════════════════

def generate_rows(n, id_type):
    rows = []
    if id_type == "int_auto":
        for _ in range(n):
            rows.append(tuple(random.randint(1, 1_000_000) for _ in range(6)))
        return rows

    if id_type == "snowflake":
        gen = SnowflakeIDGenerator(machine_id=random.randint(0, 1023))
        for _ in range(n):
            sid = gen.generate()
            cols = tuple(random.randint(1, 1_000_000) for _ in range(6))
            rows.append((sid,) + cols)
        return rows

    gen_fn = GEN_MAP[id_type]
    for _ in range(n):
        pk = gen_fn()
        cols = tuple(gen_fn() for _ in range(6))
        rows.append((pk,) + cols)
    return rows


# ═══════════════════════════════════════════════════
# INSERT WORKERS
# ═══════════════════════════════════════════════════

def _get_sql(id_type):
    if id_type == "int_auto":
        return "INSERT INTO test_bench (col_a, col_b, col_c, col_d, col_e, col_f) VALUES (%s, %s, %s, %s, %s, %s)"
    return "INSERT INTO test_bench (id, col_a, col_b, col_c, col_d, col_e, col_f) VALUES (%s, %s, %s, %s, %s, %s, %s)"


def worker_executemany(args):
    data_chunk, id_type, batch_size = args
    conn = get_conn()
    cur = conn.cursor()
    sql = _get_sql(id_type)
    for i in range(0, len(data_chunk), batch_size):
        cur.executemany(sql, data_chunk[i:i + batch_size])
        conn.commit()
    cur.close()
    conn.close()


def worker_multi_row(args):
    """Single SQL com múltiplos VALUES — menos round-trips."""
    data_chunk, id_type, batch_size = args
    conn = get_conn()
    cur = conn.cursor()

    n_cols = 6 if id_type == "int_auto" else 7
    if id_type == "int_auto":
        cols = "(col_a, col_b, col_c, col_d, col_e, col_f)"
    else:
        cols = "(id, col_a, col_b, col_c, col_d, col_e, col_f)"
    ph = "(" + ", ".join(["%s"] * n_cols) + ")"

    for i in range(0, len(data_chunk), batch_size):
        batch = data_chunk[i:i + batch_size]
        sql = f"INSERT INTO test_bench {cols} VALUES {', '.join([ph] * len(batch))}"
        params = []
        for row in batch:
            params.extend(row)
        cur.execute(sql, params)
        conn.commit()

    cur.close()
    conn.close()


def run_insert(data, id_type, batch_size, multi_row=False):
    chunk_size = len(data) // THREADS
    chunks = []
    for i in range(THREADS):
        s = i * chunk_size
        e = (i + 1) * chunk_size if i < THREADS - 1 else len(data)
        chunks.append((data[s:e], id_type, batch_size))

    worker = worker_multi_row if multi_row else worker_executemany
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        ex.map(worker, chunks)
    return time.time() - t0


# ═══════════════════════════════════════════════════
# WARMUP & STATS
# ═══════════════════════════════════════════════════

def warmup(id_type, scenario):
    reset_and_flush()
    conn = get_conn()
    cur = conn.cursor()
    create_table(cur, id_type)
    create_indexes(cur, scenario)
    conn.commit()
    d = generate_rows(10_000, id_type)
    run_insert(d, id_type, 1000)
    cur.close()
    conn.close()

def trimmed_mean(vals):
    if len(vals) <= 2:
        return statistics.mean(vals)
    s = sorted(vals)
    return statistics.mean(s[1:-1])


# ═══════════════════════════════════════════════════
# RUN SCENARIO
# ═══════════════════════════════════════════════════

def run_scenario(id_type, scenario, n_rows, strategy):
    warmup(id_type, scenario)

    times = []
    for run in range(RUNS):
        reset_and_flush()
        conn = get_conn()
        cur = conn.cursor()
        create_table(cur, id_type)
        create_indexes(cur, scenario)
        conn.commit()

        data = generate_rows(n_rows, id_type)

        if strategy["sort"] and id_type != "int_auto":
            data.sort(key=lambda r: r[0])

        duration = run_insert(data, id_type, strategy["batch"], strategy["multi_row"])
        times.append(duration)
        print(f"{duration:.2f}s", end=" ", flush=True)

        cur.close()
        conn.close()

    print()
    avg = trimmed_mean(times)
    med = statistics.median(times)
    std = statistics.stdev(times) if len(times) > 1 else 0
    return avg, med, std, times


# ═══════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════

def main():
    print("=" * 90)
    print("  BENCHMARK: Estratégias Otimizadas de INSERT")
    print("=" * 90)
    print(f"  Threads: {THREADS} | Runs: {RUNS} (trimmed mean)")
    print(f"  ID types: {ID_TYPES}")
    print(f"  Strategies: {[s['name'] for s in STRATEGIES]}")
    print(f"  Scenarios: {list(INDEX_SCENARIOS.keys())}")
    print(f"  Row counts: {ROW_COUNTS}")
    print("=" * 90)

    all_results = []

    for id_type in ID_TYPES:
        print(f"\n{'█'*90}")
        print(f"  {id_type.upper()}")
        print(f"{'█'*90}")

        for n_rows in ROW_COUNTS:
            print(f"\n  ═══ {n_rows:,} linhas ═══")

            for scenario in INDEX_SCENARIOS:
                print(f"\n  ── índices: {scenario} ──\n")

                for strat in STRATEGIES:
                    # Sort só faz sentido p/ uuid_v4 (os outros já são sequenciais)
                    if strat["sort"] and id_type not in ("uuid_v4",):
                        continue

                    print(f"    {strat['name']:25}: ", end="", flush=True)

                    avg, med, std, times = run_scenario(id_type, scenario, n_rows, strat)
                    tp = n_rows / avg

                    all_results.append({
                        "id_type": id_type,
                        "rows": n_rows,
                        "scenario": scenario,
                        "strategy": strat["name"],
                        "batch_size": strat["batch"],
                        "multi_row": strat["multi_row"],
                        "sorted": strat["sort"],
                        "avg_time_s": round(avg, 3),
                        "median_time_s": round(med, 3),
                        "throughput": round(tp),
                        "std_dev": round(std, 3),
                    })

                    print(f"    ✔ avg={avg:.2f}s  tp={tp:>10,.0f}/s  std={std:.3f}")

    # ─── Resumo ───
    print("\n" + "=" * 110)
    print("  RESUMO FINAL")
    print("=" * 110)
    print(f"  {'ID Type':<12} {'Rows':>8} {'Scenario':<10} {'Strategy':<26} "
          f"{'Batch':>6} {'Avg':>8} {'Throughput':>14} {'StdDev':>8}")
    print("  " + "-" * 100)

    for r in all_results:
        print(f"  {r['id_type']:<12} {r['rows']:>8,} {r['scenario']:<10} "
              f"{r['strategy']:<26} {r['batch_size']:>6} "
              f"{r['avg_time_s']:>7.3f}s {r['throughput']:>13,}/s {r['std_dev']:>7.3f}")

    # ─── CSV ───
    csv_path = "benchmark_optimized_strategies.csv"
    with open(csv_path, "w") as f:
        f.write("id_type,rows,scenario,strategy,batch_size,multi_row,sorted,"
                "avg_time_s,median_time_s,throughput,std_dev\n")
        for r in all_results:
            f.write(f"{r['id_type']},{r['rows']},{r['scenario']},{r['strategy']},"
                    f"{r['batch_size']},{r['multi_row']},{r['sorted']},"
                    f"{r['avg_time_s']},{r['median_time_s']},{r['throughput']},{r['std_dev']}\n")


if __name__ == "__main__":
    main()
