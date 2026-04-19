import os
from typing import Optional, List, Dict, Any

from dotenv import load_dotenv
import oracledb
from oracledb import DatabaseError

load_dotenv()

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise ValueError(f"Не задана переменная окружения: {name}")
    return "" if value is None else str(value).strip()


class SMConnect:

    def __init__(self):
        try:
            self.dsn = get_env("SM_DSN", required=True)
            self.username = get_env("SM_USERNAME", required=True)
            self.password = get_env("SM_PASSWORD", required=True)

            self.connect_mode = get_env("SM_CONNECT_MODE", default="normal").lower()
            self.local_host = get_env("SM_LOCAL_HOST", default="")
            self.local_port = get_env("SM_LOCAL_PORT", default="1521")

            self.connection = None
            self.cursor = None

        except Exception as e:
            print(f"Ошибка чтения конфигурации SM из env: {e}")
            raise

    def _build_connect_kwargs(self, dsn_value: str) -> dict:
        kwargs = {
            "user": self.username,
            "password": self.password,
            "dsn": dsn_value,
        }

        if self.connect_mode == "sysdba":
            kwargs["mode"] = oracledb.AUTH_MODE_SYSDBA

        return kwargs

    def connect_SM(self):
        """Подключение к глобальной БД."""
        try:
            self.close()

            kwargs = self._build_connect_kwargs(self.dsn)
            self.connection = oracledb.connect(**kwargs)
            self.cursor = self.connection.cursor()

            print(f"Подключение к SM успешно: {self.username}@{self.dsn}")

        except DatabaseError as e:
            print(f"SM: Ошибка подключения: {self.username}@{self.dsn} -> {e}")
            raise

    def connect_SM_LOCAL(self, service_name: str):
     
        try:
            self.close()

            service_name = str(service_name).strip()
            if not service_name:
                raise ValueError("Не передан service_name для локального подключения")

            if "/" in service_name or ":" in service_name or "(" in service_name:
                local_dsn = service_name
            elif self.local_host:
                local_dsn = f"{self.local_host}:{self.local_port}/{service_name}"
            else:
                local_dsn = service_name

            kwargs = self._build_connect_kwargs(local_dsn)
            self.connection = oracledb.connect(**kwargs)
            self.cursor = self.connection.cursor()

            print(f"Подключение к локальной БД успешно: {self.username}@{local_dsn}")

        except DatabaseError as e:
            print(f"SM LOCAL: Ошибка подключения ({service_name}): {e}")
            raise

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection:
                self.connection.close()
                self.connection = None
        except Exception as e:
            print(f"Ошибка при закрытии соединения с SM: {e}")

    def execute_sql(
        self,
        sql: str,
        params: Optional[dict | list | tuple] = None,
        commit: bool = False,
    ):
      
        try:
            if not self.connection or not self.cursor:
                self.connect_SM()

            if params is None:
                self.cursor.execute(sql)
            else:
                self.cursor.execute(sql, params)

            if self.cursor.description:
                columns = [col[0].lower() for col in self.cursor.description]
                rows = self.cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]

            if commit:
                self.connection.commit()

            return {"rowcount": self.cursor.rowcount}

        except Exception:
            print("Ошибка выполнения SQL-запроса")
            if commit and self.connection:
                try:
                    self.connection.rollback()
                except Exception:
                    print("Ошибка rollback после неудачного SQL-запроса")
            raise
