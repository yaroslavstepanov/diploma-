"""Тонкая обертка над clickhouse-connect клиентом для DDL/DML операций."""
from __future__ import annotations

from typing import List, Sequence

import clickhouse_connect

from .profile import Profile


class ClickHouseService:
	"""Управление подключением к ClickHouse, создание БД/таблиц и вставка данных."""
	def __init__(self, profile: Profile) -> None:
		"""Создать HTTP клиент используя настройки подключения из профиля."""
		self._profile = profile
		self._client = clickhouse_connect.get_client(
			host=profile.connection.host,
			port=profile.connection.port,
			username=profile.connection.username,
			password=profile.connection.password,
			database=profile.connection.database,
			secure=profile.connection.secure,
		)

	def close(self) -> None:
		"""Закрыть HTTP клиент (игнорировать ошибки при завершении)."""
		try:
			self._client.close()
		except Exception:
			pass

	def ensure_database(self) -> None:
		"""Создать базу данных если она не существует."""
		database_name = self._profile.target.database
		self._client.command(f"CREATE DATABASE IF NOT EXISTS {database_name}")

	def ensure_table(self) -> None:
		"""Создать MergeTree таблицу на основе определений полей из профиля.

		ORDER BY и опциональный PARTITION BY берутся из profile.target.
		"""
		profile_config = self._profile
		fully_qualified_table = profile_config.fq_table()
		column_definitions = profile_config.ch_ddl_columns()
		order_by = profile_config.target.order_by or "tuple()"
		partition_by_expr = profile_config.target.partition_by
		partition_clause = f"\nPARTITION BY {partition_by_expr}" if partition_by_expr else ""
		create_table_sql = (
			f"CREATE TABLE IF NOT EXISTS {fully_qualified_table} (\n{column_definitions}\n)\n"
			f"ENGINE = MergeTree\n"
			f"ORDER BY {order_by}{partition_clause}"
		)
		self._client.command(create_table_sql)

	def insert_rows(self, rows_to_insert: Sequence[Sequence], column_names: List[str]) -> None:
		"""Вставить батч строк используя порядок колонок из column_names."""
		fully_qualified_table = self._profile.fq_table()
		self._client.insert(fully_qualified_table, rows_to_insert, column_names=column_names)
