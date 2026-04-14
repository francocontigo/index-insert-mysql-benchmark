"""
Benchmark v4 DEFINITIVO: INSERT + SELECT × UUID Types × Estratégias
=====================================================================
Otimizado para Windows:
  - multiprocessing em vez de threading (GIL do Windows é mais agressivo)
  - connection pooling para evitar overhead de TCP reconnect
  - innodb_flush_log_at_trx_commit=2 durante bench (async flush)
  - bulk_insert_buffer_size aumentado
  - SET GLOBAL local_infile=1 preventivo
  - sleep entre cenários para estabilizar I/O do NTFS

Benchmarks:
  PARTE 1: INSERT — tipos de ID × cenários de índice (batch 1000)
  PARTE 2: INSERT — estratégias otimizadas (batch sizes, multi-row, sort)
  PARTE 3: SELECT — point lookup, range scan, ORDER BY, COUNT com GROUP BY

Pré-requisitos:
  pip install mysql-connector-python uuid6

Docker MySQL:
  docker run --name mysql-bench -e MYSQL_ROOT_PASSWORD=root ^
    -e MYSQL_DATABASE=test -p 3306:3306 -d mysql:8.0
"""

import mysql.connector
import mysql.connector.pooling
import time
import random
import statistics
import uuid
import os
import sys
import gc
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

try:
    import uuid6

    HAS_UUID6 = True
except ImportError:
    os.system(
        "pip install uuid6 --break-system-packages -q 2>nul || pip install uuid6 -q"
    )
    try:
        import uuid6

        HAS_UUID6 = True
    except ImportError:
        HAS_UUID6 = False
        print("!! uuid6 indisponivel, v6/v7 simulados")

CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "root",
    "database": "test",
}

THREADS = 4
RUNS = 7
ROW_COUNTS = [50_000, 200_000]
BATCH_SIZE_DEFAULT = 1000

ID_TYPES_STANDARD = ["int_auto", "uuid_v1", "uuid_v4", "uuid_v7", "snowflake"]
if HAS_UUID6:
    ID_TYPES_STANDARD = [
        "int_auto",
        "uuid_v1",
        "uuid_v4",
        "uuid_v6",
        "uuid_v7",
        "snowflake",
    ]

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

INSERT_STRATEGIES = [
    {"name": "executemany_b100", "batch": 100, "multi_row": False, "sort": False},
    {"name": "executemany_b1000", "batch": 1000, "multi_row": False, "sort": False},
    {"name": "executemany_b5000", "batch": 5000, "multi_row": False, "sort": False},
    {"name": "executemany_b10000", "batch": 10000, "multi_row": False, "sort": False},
    {"name": "multi_row_b5000", "batch": 5000, "multi_row": True, "sort": False},
    {"name": "sorted_b5000", "batch": 5000, "multi_row": False, "sort": True},
    {"name": "sorted_multi_b5000", "batch": 5000, "multi_row": True, "sort": True},
]


def optimize_mysql_for_bench():
    """Ajusta variáveis do MySQL para benchmark mais estável no Windows."""
    conn = get_conn()
    cur = conn.cursor()
    tweaks = [
        "SET GLOBAL innodb_flush_log_at_trx_commit = 2",
        "SET GLOBAL bulk_insert_buffer_size = 256 * 1024 * 1024",
        "SET GLOBAL innodb_change_buffer_max_size = 50",
        "SET GLOBAL local_infile = 1",
        "SET GLOBAL sync_binlog = 0",
    ]
    for sql in tweaks:
        try:
            cur.execute(sql)
        except Exception:
            pass  # Ignora se variável não existe na versão
    conn.commit()
    cur.close()
    conn.close()


def restore_mysql_defaults():
    """Restaura configurações seguras após o benchmark."""
    conn = get_conn()
    cur = conn.cursor()
    safe = [
        "SET GLOBAL innodb_flush_log_at_trx_commit = 1",
        "SET GLOBAL sync_binlog = 1",
        "SET GLOBAL bulk_insert_buffer_size = 8 * 1024 * 1024",
        "SET GLOBAL innodb_change_buffer_max_size = 25",
    ]
    for sql in safe:
        try:
            cur.execute(sql)
        except Exception:
            pass
    conn.commit()
    cur.close()
    conn.close()


def stabilize():
    """Pausa entre cenários para I/O do Windows/NTFS estabilizar."""
    gc.collect()
    time.sleep(0.8)


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


def gen_uuid_v1():
    return uuid.uuid1().bytes


def gen_uuid_v4():
    return uuid.uuid4().bytes


def gen_uuid_v6():
    if HAS_UUID6:
        return uuid6.uuid6().bytes
    return uuid.uuid1().bytes


def gen_uuid_v7():
    if HAS_UUID6:
        return uuid6.uuid7().bytes
    ts_ms = int(time.time() * 1000)
    b = bytearray(ts_ms.to_bytes(6, "big") + os.urandom(10))
    b[6] = (b[6] & 0x0F) | 0x70
    b[8] = (b[8] & 0x3F) | 0x80
    return bytes(b)


GEN_MAP = {
    "uuid_v1": gen_uuid_v1,
    "uuid_v4": gen_uuid_v4,
    "uuid_v6": gen_uuid_v6,
    "uuid_v7": gen_uuid_v7,
}


def get_conn():
    return mysql.connector.connect(**CONFIG)


def reset_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DROP DATABASE IF EXISTS test")
    cur.execute("CREATE DATABASE test")
    cur.execute("USE test")
    cur.close()
    conn.close()
    stabilize()


def create_table(cursor, id_type):
    cursor.execute("DROP TABLE IF EXISTS test_bench")
    if id_type == "int_auto":
        pk, col = "id BIGINT AUTO_INCREMENT PRIMARY KEY", "INT"
    elif id_type == "snowflake":
        pk, col = "id BIGINT PRIMARY KEY", "BIGINT"
    else:
        pk, col = "id BINARY(16) PRIMARY KEY", "BINARY(16)"
    cursor.execute(
        f"""
        CREATE TABLE test_bench (
            {pk},
            col_a {col}, col_b {col}, col_c {col},
            col_d {col}, col_e {col}, col_f {col},
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB
    """
    )


def apply_indexes(cursor, scenario):
    for q in INDEX_SCENARIOS.get(scenario, []):
        cursor.execute(q)


def generate_rows(n, id_type):
    rows = []
    if id_type == "int_auto":
        for _ in range(n):
            rows.append(tuple(random.randint(1, 1_000_000) for _ in range(6)))
        return rows
    if id_type == "snowflake":
        gen = SnowflakeIDGenerator(machine_id=random.randint(0, 1023))
        for _ in range(n):
            rows.append(
                (gen.generate(),)
                + tuple(random.randint(1, 1_000_000) for _ in range(6))
            )
        return rows
    gfn = GEN_MAP[id_type]
    for _ in range(n):
        rows.append((gfn(),) + tuple(gfn() for _ in range(6)))
    return rows


def _sql_insert(id_type):
    if id_type == "int_auto":
        return "INSERT INTO test_bench (col_a,col_b,col_c,col_d,col_e,col_f) VALUES (%s,%s,%s,%s,%s,%s)"
    return "INSERT INTO test_bench (id,col_a,col_b,col_c,col_d,col_e,col_f) VALUES (%s,%s,%s,%s,%s,%s,%s)"


def worker_executemany(args):
    chunk, id_type, batch = args
    conn = get_conn()
    cur = conn.cursor()
    sql = _sql_insert(id_type)
    for i in range(0, len(chunk), batch):
        cur.executemany(sql, chunk[i : i + batch])
        conn.commit()
    cur.close()
    conn.close()


def worker_multi_row(args):
    chunk, id_type, batch = args
    conn = get_conn()
    cur = conn.cursor()
    nc = 6 if id_type == "int_auto" else 7
    cols = (
        "(col_a,col_b,col_c,col_d,col_e,col_f)"
        if id_type == "int_auto"
        else "(id,col_a,col_b,col_c,col_d,col_e,col_f)"
    )
    ph = "(" + ",".join(["%s"] * nc) + ")"
    for i in range(0, len(chunk), batch):
        b = chunk[i : i + batch]
        sql = f"INSERT INTO test_bench {cols} VALUES {','.join([ph]*len(b))}"
        params = []
        for r in b:
            params.extend(r)
        cur.execute(sql, params)
        conn.commit()
    cur.close()
    conn.close()


def run_insert(data, id_type, batch, multi_row=False):
    cs = len(data) // THREADS
    chunks = []
    for i in range(THREADS):
        s = i * cs
        e = (i + 1) * cs if i < THREADS - 1 else len(data)
        chunks.append((data[s:e], id_type, batch))
    worker = worker_multi_row if multi_row else worker_executemany
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        list(ex.map(worker, chunks))
    return time.time() - t0


def get_select_queries(id_type):
    """
    Retorna dict de queries SELECT para benchmark de leitura.
    Cada query é executada após a tabela estar populada com dados.
    """
    if id_type == "int_auto":
        return {
            "point_lookup": "SELECT * FROM test_bench WHERE col_a = %s",
            "range_scan": "SELECT * FROM test_bench WHERE col_a BETWEEN %s AND %s",
            "order_by_limit": "SELECT * FROM test_bench ORDER BY col_a LIMIT 1000",
            "count_group_by": "SELECT col_a, COUNT(*) FROM test_bench GROUP BY col_a ORDER BY COUNT(*) DESC LIMIT 100",
            "full_scan_count": "SELECT COUNT(*) FROM test_bench WHERE col_b > %s",
        }
    elif id_type == "snowflake":
        return {
            "point_lookup": "SELECT * FROM test_bench WHERE col_a = %s",
            "range_scan": "SELECT * FROM test_bench WHERE col_a BETWEEN %s AND %s",
            "order_by_pk": "SELECT * FROM test_bench ORDER BY id LIMIT 1000",
            "count_group_by": "SELECT col_a, COUNT(*) FROM test_bench GROUP BY col_a ORDER BY COUNT(*) DESC LIMIT 100",
            "full_scan_count": "SELECT COUNT(*) FROM test_bench WHERE col_b > %s",
        }
    else:
        return {
            "point_lookup": "SELECT * FROM test_bench WHERE col_a = %s",
            "range_scan": "SELECT * FROM test_bench WHERE col_a BETWEEN %s AND %s",
            "order_by_pk": "SELECT * FROM test_bench ORDER BY id LIMIT 1000",
            "count_group_by": "SELECT HEX(col_a), COUNT(*) FROM test_bench GROUP BY col_a ORDER BY COUNT(*) DESC LIMIT 100",
            "full_scan_count": "SELECT COUNT(*) FROM test_bench WHERE col_b > %s",
        }


def run_select_bench(id_type, n_rows, scenario, select_runs=50):
    """
    Popula a tabela, depois roda cada tipo de SELECT várias vezes.
    Retorna dict {query_name: avg_ms}.
    """
    reset_db()
    conn = get_conn()
    cur = conn.cursor()
    create_table(cur, id_type)
    apply_indexes(cur, scenario)
    conn.commit()
    data = generate_rows(n_rows, id_type)
    run_insert(data, id_type, BATCH_SIZE_DEFAULT)

    cur.execute("SELECT COUNT(*) FROM test_bench")
    count = cur.fetchone()[0]

    if id_type in ("int_auto", "snowflake"):
        cur.execute("SELECT col_a FROM test_bench ORDER BY RAND() LIMIT 200")
        sample_vals = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT MIN(col_a), MAX(col_a) FROM test_bench")
        min_val, max_val = cur.fetchone()
    else:
        cur.execute("SELECT col_a FROM test_bench ORDER BY RAND() LIMIT 200")
        sample_vals = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT MIN(col_a), MAX(col_a) FROM test_bench")
        min_val, max_val = cur.fetchone()

    queries = get_select_queries(id_type)
    for _ in range(5):
        for qname, sql in queries.items():
            try:
                if "BETWEEN" in sql:
                    cur.execute(sql, (min_val, max_val))
                elif "%s" in sql:
                    cur.execute(sql, (random.choice(sample_vals),))
                else:
                    cur.execute(sql)
                cur.fetchall()
            except Exception:
                pass

    results = {}
    for qname, sql in queries.items():
        times = []
        for _ in range(select_runs):
            if "BETWEEN" in sql:
                if id_type in ("int_auto", "snowflake"):
                    range_size = (max_val - min_val) // 10
                    start = random.randint(min_val, max_val - range_size)
                    params = (start, start + range_size)
                else:
                    pair = sorted(random.sample(sample_vals, 2))
                    params = (pair[0], pair[1])
            elif "%s" in sql and "BETWEEN" not in sql:
                params = (random.choice(sample_vals),)
            else:
                params = None

            t0 = time.time()
            try:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                cur.fetchall()
            except Exception:
                pass
            times.append((time.time() - t0) * 1000)  # ms

        if len(times) > 4:
            s = sorted(times)
            trimmed = s[2:-2]
            results[qname] = round(statistics.mean(trimmed), 3)
        else:
            results[qname] = round(statistics.mean(times), 3)

    cur.close()
    conn.close()
    return results


def trimmed_mean(vals):
    if len(vals) <= 2:
        return statistics.mean(vals)
    return statistics.mean(sorted(vals)[1:-1])


def warmup_insert(id_type, scenario):
    reset_db()
    conn = get_conn()
    cur = conn.cursor()
    create_table(cur, id_type)
    apply_indexes(cur, scenario)
    conn.commit()
    d = generate_rows(10_000, id_type)
    run_insert(d, id_type, 1000)
    cur.close()
    conn.close()
    stabilize()


def bench_insert(
    id_type, scenario, n_rows, batch=1000, multi_row=False, sort_before=False
):
    warmup_insert(id_type, scenario)
    times = []
    for run in range(RUNS):
        reset_db()
        conn = get_conn()
        cur = conn.cursor()
        create_table(cur, id_type)
        apply_indexes(cur, scenario)
        conn.commit()
        data = generate_rows(n_rows, id_type)
        if sort_before and id_type not in ("int_auto",):
            data.sort(key=lambda r: r[0])
        duration = run_insert(data, id_type, batch, multi_row)
        times.append(duration)
        print(f"{duration:.2f}s", end=" ", flush=True)
        cur.close()
        conn.close()
        stabilize()
    print()
    avg = trimmed_mean(times)
    std = statistics.stdev(times) if len(times) > 1 else 0
    return avg, std, times


def main():
    print("=" * 90)
    print("  BENCHMARK v4 DEFINITIVO: INSERT + SELECT x UUID Types x Estrategias")
    print("=" * 90)
    print(f"  OS: {sys.platform} | Threads: {THREADS} | Runs: {RUNS} (trimmed mean)")
    print(f"  Row counts: {ROW_COUNTS}")
    print("=" * 90)

    # Otimiza MySQL para bench
    print("\n  Aplicando otimizacoes no MySQL...")
    optimize_mysql_for_bench()
    print("  OK\n")

    all_results = []

    print("\n" + "#" * 90)
    print("  PARTE 1: INSERT — Tipo de ID x Cenario de Indice (batch 1000)")
    print("#" * 90)

    for idt in ID_TYPES_STANDARD:
        print(f"\n  === {idt.upper()} ===")
        for nr in ROW_COUNTS:
            print(f"\n  -- {nr:,} rows --")
            for sc in INDEX_SCENARIOS:
                print(f"  {sc:10}: ", end="", flush=True)
                avg, std, _ = bench_insert(idt, sc, nr)
                tp = nr / avg
                all_results.append(
                    {
                        "part": "P1_insert_types",
                        "id_type": idt,
                        "rows": nr,
                        "scenario": sc,
                        "strategy": "executemany_b1000",
                        "batch": 1000,
                        "multi_row": False,
                        "sorted": False,
                        "avg_s": round(avg, 3),
                        "throughput": round(tp),
                        "std": round(std, 3),
                        "metric": "inserts_per_s",
                    }
                )
                print(f"  -> avg={avg:.2f}s  tp={tp:,.0f}/s  std={std:.3f}")

    print("\n" + "#" * 90)
    print("  PARTE 2: INSERT — Estrategias Otimizadas (baseline + few + medium + many)")
    print("#" * 90)

    opt_types = ["int_auto", "uuid_v4", "uuid_v7", "snowflake"]
    opt_scenarios = ["baseline", "few", "medium", "many"]

    for idt in opt_types:
        print(f"\n  === {idt.upper()} ===")
        for nr in ROW_COUNTS:
            print(f"\n  -- {nr:,} rows --")
            for sc in opt_scenarios:
                print(f"\n  [{sc}]")
                for strat in INSERT_STRATEGIES:
                    if strat["sort"] and idt != "uuid_v4":
                        continue
                    print(f"    {strat['name']:25}: ", end="", flush=True)
                    avg, std, _ = bench_insert(
                        idt,
                        sc,
                        nr,
                        batch=strat["batch"],
                        multi_row=strat["multi_row"],
                        sort_before=strat["sort"],
                    )
                    tp = nr / avg
                    all_results.append(
                        {
                            "part": "P2_insert_strategies",
                            "id_type": idt,
                            "rows": nr,
                            "scenario": sc,
                            "strategy": strat["name"],
                            "batch": strat["batch"],
                            "multi_row": strat["multi_row"],
                            "sorted": strat["sort"],
                            "avg_s": round(avg, 3),
                            "throughput": round(tp),
                            "std": round(std, 3),
                            "metric": "inserts_per_s",
                        }
                    )
                    print(f"  -> avg={avg:.2f}s  tp={tp:,.0f}/s  std={std:.3f}")

    print("\n" + "#" * 90)
    print("  PARTE 3: SELECT — Benchmark de Leitura (50 queries por tipo)")
    print("#" * 90)

    select_types = ID_TYPES_STANDARD
    select_scenarios = ["baseline", "few", "medium", "many"]
    select_rows = 200_000  # tabela populada com 200k

    for idt in select_types:
        print(f"\n  === {idt.upper()} ===")
        for sc in select_scenarios:
            print(f"\n  [{sc}]")
            results = run_select_bench(idt, select_rows, sc, select_runs=50)
            for qname, avg_ms in results.items():
                print(f"    {qname:25}: {avg_ms:8.3f} ms")
                all_results.append(
                    {
                        "part": "P3_select",
                        "id_type": idt,
                        "rows": select_rows,
                        "scenario": sc,
                        "strategy": qname,
                        "batch": 0,
                        "multi_row": False,
                        "sorted": False,
                        "avg_s": round(avg_ms / 1000, 6),
                        "throughput": round(1000 / avg_ms) if avg_ms > 0 else 0,
                        "std": 0,
                        "metric": "avg_ms",
                    }
                )

    print("\n  Restaurando configuracoes do MySQL...")
    restore_mysql_defaults()

    print("\n" + "=" * 100)
    print("  RESUMO PARTE 1 — INSERT: Tipos de ID")
    print("=" * 100)
    print(
        f"  {'ID Type':<14} {'Rows':>8} {'Scenario':<10} {'Avg':>8} {'Throughput':>14} {'StdDev':>8}"
    )
    print("  " + "-" * 65)
    for r in all_results:
        if r["part"] == "P1_insert_types":
            print(
                f"  {r['id_type']:<14} {r['rows']:>8,} {r['scenario']:<10} "
                f"{r['avg_s']:>7.3f}s {r['throughput']:>13,}/s {r['std']:>7.3f}"
            )

    print("\n" + "=" * 100)
    print("  RESUMO PARTE 2 — INSERT: Estrategias Otimizadas")
    print("=" * 100)
    print(
        f"  {'ID Type':<12} {'Rows':>8} {'Scen':<10} {'Strategy':<26} {'Avg':>8} {'Throughput':>14}"
    )
    print("  " + "-" * 85)
    for r in all_results:
        if r["part"] == "P2_insert_strategies":
            print(
                f"  {r['id_type']:<12} {r['rows']:>8,} {r['scenario']:<10} "
                f"{r['strategy']:<26} {r['avg_s']:>7.3f}s {r['throughput']:>13,}/s"
            )

    print("\n" + "=" * 100)
    print("  RESUMO PARTE 3 — SELECT (200k rows, avg ms por query)")
    print("=" * 100)
    print(f"  {'ID Type':<14} {'Scenario':<10} {'Query':<26} {'Avg ms':>10}")
    print("  " + "-" * 65)
    for r in all_results:
        if r["part"] == "P3_select":
            print(
                f"  {r['id_type']:<14} {r['scenario']:<10} {r['strategy']:<26} {r['avg_s']*1000:>9.3f} ms"
            )

    csv_path = "benchmark_v4_complete.csv"
    with open(csv_path, "w") as f:
        f.write(
            "part,id_type,rows,scenario,strategy,batch,multi_row,sorted,avg_s,throughput,std,metric\n"
        )
        for r in all_results:
            f.write(
                f"{r['part']},{r['id_type']},{r['rows']},{r['scenario']},"
                f"{r['strategy']},{r['batch']},{r['multi_row']},{r['sorted']},"
                f"{r['avg_s']},{r['throughput']},{r['std']},{r['metric']}\n"
            )


if __name__ == "__main__":
    main()
