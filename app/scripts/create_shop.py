import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import re
import json
from typing import Dict, Any

# зачем.
def to_float(value) -> float:
    """
    Преобразует строку вида '51,848551' или '51.848551' в float.
    """
    if value is None:
        raise ValueError("Пустое значение координаты")
    return float(str(value).strip().replace(",", "."))



def get_short_address(full_address: str) -> tuple[str, str]:
    """
    Из полного адреса вида:
    '670002,Бурятия Респ, Улан-Удэ г, Буйко ул,20а'
    возвращает:
    city='Улан-Удэ'
    short_address='Буйко ул,20а'
    """
    if not full_address or not str(full_address).strip():
        raise ValueError("Пустой полный адрес")

    parts = [p.strip() for p in str(full_address).split(",") if p.strip()]
    if not parts:
        raise ValueError(f"Не удалось распарсить адрес: {full_address}")

    city_index = None
    city_value = ""

    for i, part in enumerate(parts):
        lowered = part.lower()

        # ищем кусок с городом
        if lowered.endswith(" г") or lowered.endswith(" г.") or re.search(r"\bг\.?$", lowered):
            city_index = i
            city_value = normalize_city(part)
            break

    if city_index is None:
        # запасной вариант — просто ищем Улан-Удэ
        for i, part in enumerate(parts):
            if "улан-удэ" in part.lower():
                city_index = i
                city_value = normalize_city(part)
                break

    if city_index is None:
        raise ValueError(f"Не удалось выделить город из адреса: {full_address}")

    short_parts = parts[city_index + 1 :]
    short_address = ",".join(short_parts).strip()

    if not short_address:
        raise ValueError(f"Не удалось выделить короткий адрес из: {full_address}")

    return city_value, short_address
