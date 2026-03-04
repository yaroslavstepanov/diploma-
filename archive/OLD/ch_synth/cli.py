"""CLI точка входа для генерации синтетических данных и записи в ClickHouse или CSV."""
from __future__ import annotations

import argparse
import csv
import math
import os
import random
from typing import Iterable, Iterator, List, Sequence

from tqdm import tqdm

from .profile import Profile
from .generators import build_generator
from .client import ClickHouseService


def iter_rows(profile: Profile, total_rows: int) -> Iterator[List]:
    field_generators = [build_generator(field.generator.kind, field.generator.params) for field in profile.fields]
    for row_index in range(total_rows):
        yield [generator.next(row_index) for generator in field_generators]


def batched(rows_iterable: Iterable[List], batch_size: int) -> Iterator[List[List]]:
    current_batch: List[List] = []
    for row in rows_iterable:
        current_batch.append(row)
        if len(current_batch) >= batch_size:
            yield current_batch
            current_batch = []
    if current_batch:
        yield current_batch


def run_to_clickhouse(profile_path: str, rows: int, batch_size: int, create_table: bool) -> None:
    profile = Profile.load(profile_path)
    service = ClickHouseService(profile)
    try:
        service.ensure_database()
        if create_table:
            service.ensure_table()
        column_names = profile.column_names()
        with tqdm(total=rows, unit="rows") as progress_bar:
            for rows_batch in batched(iter_rows(profile, rows), batch_size):
                service.insert_rows(rows_batch, column_names=column_names)
                progress_bar.update(len(rows_batch))
    finally:
        service.close()


def run_to_csv(profile_path: str, rows: int, batch_size: int, csv_path: str) -> None:
    profile = Profile.load(profile_path)
    column_names = profile.column_names()
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out)
        writer.writerow(column_names)
        with tqdm(total=rows, unit="rows") as progress_bar:
            for rows_batch in batched(iter_rows(profile, rows), batch_size):
                writer.writerows(rows_batch)
                progress_bar.update(len(rows_batch))


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic data generator for ClickHouse")
    parser.add_argument("--profile", required=True, help="Path to JSON profile")
    parser.add_argument("--rows", type=int, required=True, help="Total number of rows to generate")
    parser.add_argument("--batch-size", type=int, default=10000, help="Insert/Write batch size")
    parser.add_argument("--create-table", action="store_true", help="Create table if not exists")
    parser.add_argument("--output-csv", default=None, help="Write to CSV instead of ClickHouse")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    if args.output_csv:
        run_to_csv(args.profile, args.rows, args.batch_size, args.output_csv)
    else:
        run_to_clickhouse(args.profile, args.rows, args.batch_size, args.create_table)


if __name__ == "__main__":
    main()
