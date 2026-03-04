"""
Скрипт запуска OLD версии Field Generator (одно поле).
Запускает на порту 5051. Использует OLD/ch_synth (копия) — чтобы изменения в корневом ch_synth не ломали старую версию.
Запуск: python scripts/run_old.py
"""
import sys
from pathlib import Path

# OLD в archive/ — импорты идут из archive/OLD
_archive = Path(__file__).resolve().parent.parent / "archive"
if str(_archive) not in sys.path:
    sys.path.insert(0, str(_archive))

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "OLD.backend.server:app",
        host="127.0.0.1",
        port=5051,
        reload=False,
    )
