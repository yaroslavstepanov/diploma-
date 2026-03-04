"""
Business Logic Layer - сервис генерации данных
"""
from typing import List, Dict, Any
from ch_synth.generators import build_generator, BaseGenerator
from backend.dictionaries import resolve_enum_params
from ch_synth.profile import Profile
import tempfile
import json
import os


class GeneratorService:
    """Сервис для генерации данных"""
    
    @staticmethod
    def create_generator(generator_kind: str, params: Dict[str, Any]) -> BaseGenerator:
        """
        Создать генератор по типу и параметрам
        
        Args:
            generator_kind: Тип генератора (random_int, timestamp_asc, и т.д.)
            params: Параметры генератора (без field_name и field_type)
        
        Returns:
            Экземпляр генератора
        """
        # Убираем служебные поля
        clean_params = {k: v for k, v in params.items() 
                       if k not in ['field_name', 'field_type']}
        if generator_kind == "enum_choice" and clean_params.get("dictionary"):
            clean_params = resolve_enum_params(clean_params)
        return build_generator(generator_kind, clean_params)
    
    @staticmethod
    def generate_preview(generator: BaseGenerator, count: int = 10) -> List[str]:
        """
        Сгенерировать данные для предпросмотра
        
        Args:
            generator: Генератор данных
            count: Количество значений (максимум 10)
        
        Returns:
            Список сгенерированных значений
        """
        preview_count = min(count, 10)
        results = []
        for i in range(preview_count):
            value = generator.next(i)
            results.append(str(value))
        return results
    
    @staticmethod
    def create_profile_from_fields(
        fields_spec: list,
        connection: Dict[str, Any],
        target_table: str
    ) -> Profile:
        """
        Создать профиль с несколькими полями.
        fields_spec: список dict с ключами name, type, generator_kind, generator_params
        """
        order_cols = ",".join(f["name"] for f in fields_spec)
        fields_data = []
        for f in fields_spec:
            params = f.get("generator_params", {})
            clean = {k: v for k, v in params.items() if k not in ["field_name", "field_type"]}
            fields_data.append({
                "name": f["name"],
                "type": f["type"],
                "generator": {"kind": f["generator_kind"], "params": clean}
            })
        profile_data = {
            "connection": connection,
            "target": {
                "database": connection.get("database", "default"),
                "table": target_table,
                "order_by": f"({order_cols})" if order_cols else "tuple()",
                "partition_by": None
            },
            "fields": fields_data
        }
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tf:
            json.dump(profile_data, tf, indent=2)
            profile_path = tf.name
        try:
            return Profile.load(profile_path)
        finally:
            if os.path.exists(profile_path):
                os.unlink(profile_path)

    @staticmethod
    def create_profile(
        field_name: str,
        field_type: str,
        generator_kind: str,
        generator_params: Dict[str, Any],
        connection: Dict[str, Any],
        target_table: str
    ) -> Profile:
        """
        Создать профиль для генерации в БД
        
        Args:
            field_name: Название поля
            field_type: Тип данных ClickHouse
            generator_kind: Тип генератора
            generator_params: Параметры генератора
            connection: Параметры подключения к ClickHouse
            target_table: Название целевой таблицы
        
        Returns:
            Загруженный профиль
        """
        # Убираем служебные поля из параметров
        clean_params = {k: v for k, v in generator_params.items() 
                       if k not in ['field_name', 'field_type']}
        
        profile_data = {
            "connection": connection,
            "target": {
                "database": connection.get("database", "default"),
                "table": target_table,
                "order_by": f"({field_name})",
                "partition_by": None
            },
            "fields": [{
                "name": field_name,
                "type": field_type,
                "generator": {
                    "kind": generator_kind,
                    "params": clean_params
                }
            }]
        }
        
        # Создаем временный файл профиля
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(profile_data, f, indent=2)
            profile_path = f.name
        
        try:
            profile = Profile.load(profile_path)
            return profile
        finally:
            # Удаляем временный файл после загрузки
            if os.path.exists(profile_path):
                os.unlink(profile_path)
    
    @staticmethod
    def generate_rows(
        profile: Profile,
        rows: int,
        batch_size: int
    ) -> List[BaseGenerator]:
        """
        Создать генераторы для всех полей профиля
        
        Args:
            profile: Профиль с полями
            rows: Количество строк (не используется, но может быть полезно)
            batch_size: Размер батча (не используется, но может быть полезно)
        
        Returns:
            Список генераторов для каждого поля
        """
        generators = [
            GeneratorService.create_generator(
                field.generator.kind,
                field.generator.params
            )
            for field in profile.fields
        ]
        return generators
