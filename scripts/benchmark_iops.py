#!/usr/bin/env python3
"""
Бенчмарк IOPS (insert operations per second) для Field Generator.

Измеряет:
- Один процесс: rows/sec при вставке в ClickHouse
- Многопроцессность (4 процесса): суммарный throughput (multiprocessing, без GIL)

Запуск:
  python scripts/benchmark_iops.py [--rows 10000] [--batch 1000] [--threads 4]
  python scripts/benchmark_iops.py --rows 50000 --batch 5000
"""
import argparse
import time
import multiprocessing
from typing import List

# ClickHouse порт из docker-compose: 18123
DEFAULT_CONN = {
    "host": "localhost",
    "port": 18123,
    "username": "default",
    "password": "ch_pass",
    "database": "default",
    "secure": False,
}


def cleanup_bench_tables(conn: dict) -> int:
    """Удалить все таблицы bench_* в default. Возвращает количество удалённых."""
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host=conn["host"], port=conn["port"],
        username=conn.get("username", "default"),
        password=conn.get("password", ""),
        database=conn.get("database", "default"),
        secure=conn.get("secure", False),
    )
    try:
        result = client.query("SHOW TABLES FROM default")
        tables = [row[0] for row in (result.result_rows or []) if row[0].startswith("bench_")]
        for t in tables:
            client.command(f"DROP TABLE IF EXISTS default.{t}")
        client.close()
        return len(tables)
    except Exception:
        try:
            client.close()
        except Exception:
            pass
        return 0


def create_test_profile(conn: dict, table_suffix: str, light: bool = False):
    """Профиль: light=id+value (~20 байт, 200k+ IOPS), иначе полный с regex (~220 байт)."""
    import tempfile
    import json
    import os
    from ch_synth.profile import Profile

    light_fields = [
        {"name": "id", "type": "UUID", "generator": {"kind": "uuid4", "params": {}}},
        {"name": "value", "type": "Int32", "generator": {"kind": "random_int", "params": {"min": 0, "max": 100000}}},
    ]
    full_fields = [
        {"name": "id", "type": "UUID", "generator": {"kind": "uuid4", "params": {}}},
        {"name": "ts", "type": "DateTime", "generator": {"kind": "timestamp_asc", "params": {"start": "now", "step": "1s"}}},
        {"name": "value", "type": "Int32", "generator": {"kind": "random_int", "params": {"min": 0, "max": 100000}}},
        {"name": "seq", "type": "Int64", "generator": {"kind": "sequence_int", "params": {"start": 1, "step": 1}}},
        {"name": "amount", "type": "Float64", "generator": {"kind": "random_float", "params": {"min": 0.0, "max": 99999.99, "precision": 2}}},
        {"name": "pct", "type": "Float32", "generator": {"kind": "percentage", "params": {"min": 0, "max": 100, "precision": 2}}},
        {"name": "passport_1", "type": "String", "generator": {"kind": "regex", "params": {"preset": "ru_passport"}}},
        {"name": "passport_2", "type": "String", "generator": {"kind": "regex", "params": {"preset": "ru_passport"}}},
        {"name": "phone_1", "type": "String", "generator": {"kind": "regex", "params": {"preset": "ru_phone"}}},
        {"name": "phone_2", "type": "String", "generator": {"kind": "regex", "params": {"preset": "ru_phone"}}},
        {"name": "mac", "type": "String", "generator": {"kind": "regex", "params": {"preset": "mac_address"}}},
        {"name": "card_digits", "type": "String", "generator": {"kind": "random_digits", "params": {"length": 16}}},
        {"name": "region", "type": "String", "generator": {"kind": "enum_choice", "params": {"values": ["Moscow", "SPb", "Kazan", "Novosibirsk", "Yekaterinburg"], "mode": "random"}}},
        {"name": "status", "type": "String", "generator": {"kind": "enum_choice", "params": {"values": ["active", "pending", "done", "failed"], "mode": "random"}}},
        {"name": "url", "type": "String", "generator": {"kind": "url_template", "params": {"pattern": "https://api.example.com/items/{row}?id={uuid}"}}},
    ]

    profile_data = {
        "connection": conn,
        "target": {
            "database": conn.get("database", "default"),
            "table": f"bench_{table_suffix}",
            "order_by": "(id)",
            "partition_by": None,
        },
        "fields": light_fields if light else full_fields,
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(profile_data, f, indent=2)
        path = f.name
    try:
        return Profile.load(path)
    finally:
        os.unlink(path)


def run_single_thread(profile, rows: int, batch_size: int) -> dict:
    """Один поток: вставка rows строк батчами по batch_size (как в приложении)."""
    return run_batch_single_thread(profile, rows, batch_size)


def run_max_stress(profile_factory, rows_per_thread: int, batch_size: int, max_threads: int, light: bool = False) -> None:
    """Увеличивать процессы до падения ClickHouse."""
    best = {"processes": 0, "iops": 0, "rows": 0}
    process_counts = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]
    process_counts = [p for p in process_counts if p <= max_threads]

    print("\n[STRESS] Гонка до падения ClickHouse:")
    for n in process_counts:
        try:
            r = run_multi_process(profile_factory, rows_per_thread, batch_size, n, light=light)
            iops = r["rows_per_sec"]
            print(f"  {n:4} процессов: {iops:,.0f} IOPS, {r['rows']} строк - OK")
            if iops > best["iops"]:
                best = {"processes": n, "iops": iops, "rows": r["rows"]}
        except Exception as e:
            print(f"  {n:4} процессов: PADENIE - {e}")
            print(f"\n  >>> ClickHouse упал при {n} процессах. Макс до падения: {best['processes']} процессов, {best['iops']:,.0f} IOPS <<<")
            return

    print(f"\n  >>> Не упал до {max_threads} процессов. Макс: {best['processes']} процессов, {best['iops']:,.0f} IOPS <<<")


def _worker_process(worker_id: int, conn: dict, rows_per_thread: int, batch_size: int, light: bool = False, catch_errors: bool = False) -> dict:
    """Воркер для multiprocessing: каждый процесс вставляет в свою таблицу."""
    profile = create_test_profile(conn, f"t{worker_id}", light=light)
    return run_batch_single_thread(profile, rows_per_thread, batch_size, catch_errors=catch_errors)


def run_multi_process(profile_factory, rows_per_thread: int, batch_size: int, num_processes: int, light: bool = False, catch_errors: bool = False) -> dict:
    """Несколько процессов: каждый вставляет rows_per_thread батчами в свою таблицу (без GIL)."""
    conn = profile_factory("_").connection  # достаём conn из любого профиля
    conn_dict = {
        "host": conn.host, "port": conn.port,
        "username": conn.username, "password": conn.password,
        "database": conn.database, "secure": conn.secure,
    }
    args_list = [(i, conn_dict, rows_per_thread, batch_size, light, catch_errors) for i in range(num_processes)]

    t0 = time.perf_counter()
    with multiprocessing.Pool(num_processes) as pool:
        results = pool.starmap(_worker_process, args_list)
    total_elapsed = time.perf_counter() - t0

    total_rows = sum(r["rows"] for r in results)
    errors = [r.get("error") for r in results if r.get("error")]
    return {
        "rows": total_rows,
        "elapsed_sec": total_elapsed,
        "rows_per_sec": total_rows / total_elapsed if total_elapsed > 0 else 0,
        "threads": num_processes,
        "per_thread": [r["rows_per_sec"] for r in results],
        "errors": errors,
    }


def run_batch_single_thread(profile, rows: int, batch_size: int, catch_errors: bool = False) -> dict:
    """Один поток с батчами (как в реальном приложении). При catch_errors — при OOM возвращает частичный результат."""
    from ch_synth.client import ClickHouseService
    from ch_synth.generators import build_generator

    service = ClickHouseService(profile)
    service.ensure_database()
    service.ensure_table()

    gens = [build_generator(f.generator.kind, f.generator.params) for f in profile.fields]
    col_names = profile.column_names()

    total_inserted = 0
    current_batch = []
    t0 = time.perf_counter()
    error_msg = None

    try:
        for row_idx in range(rows):
            row = [g.next(row_idx) for g in gens]
            current_batch.append(row)
            if len(current_batch) >= batch_size:
                service._client.insert(profile.fq_table(), current_batch, column_names=col_names)
                total_inserted += len(current_batch)
                current_batch = []

        if current_batch:
            service._client.insert(profile.fq_table(), current_batch, column_names=col_names)
            total_inserted += len(current_batch)
    except Exception as e:
        if catch_errors:
            error_msg = str(e)[:200]  # обрезать длинное сообщение
        else:
            raise
    finally:
        elapsed = time.perf_counter() - t0
        service.close()

    result = {
        "rows": total_inserted,
        "elapsed_sec": elapsed,
        "rows_per_sec": total_inserted / elapsed if elapsed > 0 else 0,
        "batch_size": batch_size,
        "threads": 1,
    }
    if error_msg:
        result["error"] = error_msg
    return result


def main():
    parser = argparse.ArgumentParser(description="Benchmark IOPS (inserts/sec)")
    parser.add_argument("--rows", type=int, default=10000, help="Строк на поток")
    parser.add_argument("--batch", type=int, default=1000, help="Размер батча")
    parser.add_argument("--threads", type=int, default=4, help="Процессов для многопроцессного теста")
    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=18123, help="ClickHouse port")
    parser.add_argument("--mode", choices=["single", "multi", "both", "max"], default="both",
                        help="single=1 поток, multi=N потоков, both=оба, max=найти максимум до падения")
    parser.add_argument("--kill", action="store_true", help="Стресс до падения: --mode max --oom --max-threads 4096")
    parser.add_argument("--kill-container", action="store_true", help="Уронить контейнер: docker kill clickhouse (гарантированно)")
    parser.add_argument("--max-threads", type=int, default=2048, help="Макс. потоков для режима max (до падения)")
    parser.add_argument("--oom", action="store_true", help="Режим OOM: огромные батчи (50k) чтобы выесть память")
    parser.add_argument("--cleanup", action="store_true", help="Удалить таблицы bench_* после прогона (освободить память)")
    parser.add_argument("--cleanup-only", action="store_true", help="Только удалить таблицы bench_*, без бенчмарка")
    parser.add_argument("--light", action="store_true", help="Минимальный профиль (id+value) — для 200k+ IOPS")
    parser.add_argument("--catch-errors", action="store_true", help="При OOM/ошибках — вернуть частичный IOPS вместо падения")
    args = parser.parse_args()

    if getattr(args, "kill", False):
        args.mode = "max"
        args.rows = 100000
        args.batch = 50000
        args.oom = True
        args.max_threads = 4096

    conn = {**DEFAULT_CONN, "host": args.host, "port": args.port}

    if getattr(args, "kill_container", False):
        import subprocess
        try:
            subprocess.run(["docker", "kill", "clickhouse"], check=True, capture_output=True)
            print("ClickHouse контейнер убит.")
        except Exception as e:
            print(f"Ошибка: {e}. Запусти: docker kill clickhouse")
        return

    if args.cleanup_only:
        n = cleanup_bench_tables(conn)
        print(f"Cleanup: удалено таблиц bench_*: {n}")
        return

    def make_profile(suffix):
        return create_test_profile(conn, suffix, light=getattr(args, "light", False))

    print("=" * 60)
    print("Benchmark IOPS - Field Generator -> ClickHouse")
    print(f"  Rows: {args.rows}, Batch: {args.batch}, Processes: {args.threads}" + (" [light profile]" if getattr(args, "light", False) else ""))
    print(f"  Host: {conn['host']}:{conn['port']}")
    print("=" * 60)

    if args.mode == "max":
        rows = args.rows
        batch = args.batch
        if getattr(args, "oom", False):
            rows, batch = 100000, 50000
            print("  [OOM mode] rows=100k, batch=50k - давление на память")
        run_max_stress(make_profile, rows, batch, args.max_threads, light=getattr(args, "light", False))
    elif args.mode in ("single", "batch", "both"):
        print("\n[1] Один поток (батч {}):".format(args.batch))
        r = run_batch_single_thread(make_profile("single"), args.rows, args.batch)
        print(f"    Rows: {r['rows']}, Time: {r['elapsed_sec']:.2f}s")
        print(f"    IOPS: {r['rows_per_sec']:.0f} inserts/sec")

    if args.mode in ("multi", "both"):
        light = getattr(args, "light", False)
        catch = getattr(args, "catch_errors", False)
        print(f"\n[2] {args.threads} процессов (по {args.rows} строк каждый)" + (" [light]" if light else "") + (" [catch]" if catch else "") + ":")
        r = run_multi_process(make_profile, args.rows, args.batch, args.threads, light=light, catch_errors=catch)
        print(f"    Total rows: {r['rows']}, Time: {r['elapsed_sec']:.2f}s")
        print(f"    IOPS (суммарно): {r['rows_per_sec']:.0f} inserts/sec")
        if r.get("per_thread"):
            print(f"    Per thread: {[f'{x:.0f}' for x in r['per_thread']]}")
        if r.get("errors"):
            print(f"    [!] Ошибки ({len(r['errors'])}): {r['errors'][0][:100]}...")

    if args.cleanup:
        n = cleanup_bench_tables(conn)
        print(f"\n[Cleanup] Удалено таблиц bench_*: {n}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
