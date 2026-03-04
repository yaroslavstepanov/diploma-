from typing import List, Dict, Any
from ch_synth.generators import build_generator, BaseGenerator
from ch_synth.profile import Profile
import tempfile
import json
import os


class GeneratorService:
    @staticmethod
    def create_generator(generator_kind: str, params: Dict[str, Any]) -> BaseGenerator:
        clean = {k: v for k, v in params.items() if k not in ['field_name', 'field_type']}
        return build_generator(generator_kind, clean)

    @staticmethod
    def generate_preview(generator: BaseGenerator, count: int = 10) -> List[str]:
        n = min(count, 10)
        return [str(generator.next(i)) for i in range(n)]

    @staticmethod
    def create_profile(field_name: str, field_type: str, generator_kind: str,
                      generator_params: Dict[str, Any], connection: Dict[str, Any],
                      target_table: str) -> Profile:
        clean = {k: v for k, v in generator_params.items() if k not in ['field_name', 'field_type']}
        profile_data = {
            "connection": connection,
            "target": {"database": connection.get("database", "default"), "table": target_table,
                       "order_by": f"({field_name})", "partition_by": None},
            "fields": [{"name": field_name, "type": field_type,
                       "generator": {"kind": generator_kind, "params": clean}}]
        }
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(profile_data, f, indent=2)
            path = f.name
        try:
            return Profile.load(path)
        finally:
            if os.path.exists(path):
                os.unlink(path)

    @staticmethod
    def generate_rows(profile: Profile, rows: int, batch_size: int) -> List[BaseGenerator]:
        return [GeneratorService.create_generator(f.generator.kind, f.generator.params) for f in profile.fields]
