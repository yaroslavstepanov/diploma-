"""
Скрипт запуска OLD версии Field Generator (одно поле).
Запускает на порту 5051. Использует OLD/ch_synth (копия) — чтобы изменения в корневом ch_synth не ломали старую версию.
Запуск: python run_old.py
"""
import sys
from pathlib import Path

# OLD первым в path — импорты ch_synth идут из OLD/ch_synth
_old_dir = Path(__file__).resolve().parent / "OLD"
if str(_old_dir) not in sys.path:
    sys.path.insert(0, str(_old_dir))

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "OLD.backend.server:app",
        host="127.0.0.1",
        port=5051,
        reload=False,
    )
