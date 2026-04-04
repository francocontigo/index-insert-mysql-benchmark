import mysql.connector
import time
import random
import statistics
import uuid
from concurrent.futures import ThreadPoolExecutor

CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "root",
    "database": "test"
}

THREADS = 4
RUNS = 5
ROW_COUNTS = [50000, 200000]


# =========================
# DB
# =========================

def get_conn():
    return mysql.connector.connect(**CONFIG)


def reset_database():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS test")
    cursor.execute("CREATE DATABASE test")
    cursor.close()
    conn.close()


def recreate_table(cursor, use_uuid=False):
    cursor.execute("DROP TABLE IF EXISTS test_indexes")

    if use_uuid:
        cursor.execute("""
            CREATE TABLE test_indexes (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                col_a CHAR(36),
                col_b CHAR(36),
                col_c CHAR(36),
                col_d CHAR(36),
                col_e CHAR(36),
                col_f CHAR(36),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        """)
    else:
        cursor.execute("""
            CREATE TABLE test_indexes (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                col_a INT,
                col_b INT,
                col_c INT,
                col_d INT,
                col_e INT,
                col_f INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        """)


def create_indexes(cursor, scenario):
    queries = {
        "few": [
            "CREATE INDEX idx_a ON test_indexes(col_a)",
            "CREATE INDEX idx_b ON test_indexes(col_b)"
        ],
        "medium": [
            "CREATE INDEX idx_a ON test_indexes(col_a)",
            "CREATE INDEX idx_b ON test_indexes(col_b)",
            "CREATE INDEX idx_c ON test_indexes(col_c)",
            "CREATE INDEX idx_d ON test_indexes(col_d)",
            "CREATE INDEX idx_ab ON test_indexes(col_a, col_b)"
        ],
        "many": [
            "CREATE INDEX idx_a ON test_indexes(col_a)",
            "CREATE INDEX idx_b ON test_indexes(col_b)",
            "CREATE INDEX idx_c ON test_indexes(col_c)",
            "CREATE INDEX idx_d ON test_indexes(col_d)",
            "CREATE INDEX idx_e ON test_indexes(col_e)",
            "CREATE INDEX idx_f ON test_indexes(col_f)",
            "CREATE INDEX idx_ab ON test_indexes(col_a, col_b)",
            "CREATE INDEX idx_cd ON test_indexes(col_c, col_d)",
            "CREATE INDEX idx_ef ON test_indexes(col_e, col_f)"
        ]
    }

    for q in queries.get(scenario, []):
        cursor.execute(q)


# =========================
# DATA
# =========================

def generate_data(n, use_uuid=False):
    if use_uuid:
        return [
            tuple(str(uuid.uuid4()) for _ in range(6))
            for _ in range(n)
        ]
    else:
        return [
            tuple(random.randint(1, 100000) for _ in range(6))
            for _ in range(n)
        ]


# =========================
# INSERT
# =========================

def insert_worker(data_chunk):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT INTO test_indexes (col_a, col_b, col_c, col_d, col_e, col_f)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, data_chunk)

    conn.commit()

    cursor.close()
    conn.close()


def run_concurrent_insert(data):
    chunk_size = len(data) // THREADS

    chunks = []
    for i in range(THREADS):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < THREADS - 1 else len(data)
        chunks.append(data[start:end])

    start = time.time()

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        executor.map(insert_worker, chunks)

    return time.time() - start


# =========================
# WARMUP
# =========================

def warmup():
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS warmup_table (
            id INT
        )
    """)

    cursor.executemany("INSERT INTO warmup_table (id) VALUES (%s)", [(i,) for i in range(1000)])
    conn.commit()

    cursor.execute("DROP TABLE warmup_table")

    cursor.close()
    conn.close()


# =========================
# RUN
# =========================

def run_scenario(scenario, n_rows, use_uuid):
    times = []

    for _ in range(RUNS):
        reset_database()

        conn = get_conn()
        cursor = conn.cursor()

        recreate_table(cursor, use_uuid)
        create_indexes(cursor, scenario)

        data = generate_data(n_rows, use_uuid)

        warmup()

        duration = run_concurrent_insert(data)
        times.append(duration)

        cursor.close()
        conn.close()

    return statistics.mean(times)


# =========================
# MAIN
# =========================

def main():
    scenarios = ["baseline", "few", "medium", "many"]

    print(f"\nTHREADS: {THREADS} | RUNS: {RUNS}\n")

    for use_uuid in [False, True]:
        dtype = "UUID" if use_uuid else "INT"
        print(f"\n===== TESTE COM {dtype} =====\n")

        for n_rows in ROW_COUNTS:
            print(f"\n--- {n_rows} linhas ---\n")

            for scenario in scenarios:
                print(f"Rodando: {scenario}...")

                avg_time = run_scenario(scenario, n_rows, use_uuid)
                throughput = n_rows / avg_time

                print(f"{scenario:10} | {avg_time:6.2f}s | {throughput:10.0f} inserts/s\n")


if __name__ == "__main__":
    main()
