import os
from typing import Optional
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

import psycopg2
import psycopg2.extras

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


class SystemAccess:
    def __init__(self):
        self.ssh_host = get_env("SSH_HOST", required=True)
        self.ssh_port = int(get_env("SSH_PORT", default="22"))
        self.ssh_user = get_env("SSH_USER", required=True)
        self.ssh_password = get_env("SSH_PASSWORD", required=True)

        self.db_host = get_env("DATABASE_HOST", required=True)
        self.db_port = int(get_env("DATABASE_PORT", default="5432"))
        self.db_name = get_env("DATABASE_NAME", required=True)
        self.db_user = get_env("DATABASE_USER", required=True)
        self.db_password = get_env("DATABASE_PASSWORD", required=True)

        self.server = None
        self.connection = None
        self.cursor = None

    def _connect_via_ssh(self):
        try:
            if self.server:
                return

            self.server = SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_user,
                ssh_password=self.ssh_password,
                remote_bind_address=(self.db_host, self.db_port),
            )
            self.server.start()
            print(f"SSH-туннель поднят: 127.0.0.1:{self.server.local_bind_port}")
        except Exception as e:
            print(f"Ошибка подключения к БД через SSH-туннель: {e}")
            raise

    def connect_db(self):
        try:
            if not self.server:
                self._connect_via_ssh()

            if self.connection:
                return

            self.connection = psycopg2.connect(
                host="127.0.0.1",
                port=self.server.local_bind_port,
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
            )
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            print("Подключение к PostgreSQL успешно")
        except Exception as e:
            print(f"Ошибка подключения к PostgreSQL: {e}")
            raise

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None

            if self.connection:
                self.connection.close()
                self.connection = None

            if self.server:
                self.server.stop()
                self.server = None

        except Exception as e:
            print(f"Ошибка при закрытии соединений: {e}")

    def execute_sql(
        self,
        sql: str,
        params: Optional[dict | list | tuple] = None,
        commit: bool = False,
    ):
    
        try:
            if not self.connection or not self.cursor:
                self.connect_db()

            if params is None:
                self.cursor.execute(sql)
            else:
                self.cursor.execute(sql, params)

            if self.cursor.description:
                rows = self.cursor.fetchall()
                return [dict(row) for row in rows]

            if commit:
                self.connection.commit()

            return {"rowcount": self.cursor.rowcount}

        except Exception as e:
            print(f"Ошибка выполнения SQL-запроса: {e}")
            if commit and self.connection:
                try:
                    self.connection.rollback()
                except Exception as rollback_error:
                    print(f"Ошибка rollback: {rollback_error}")
            raise

    def execute_sql_one(
        self,
        sql: str,
        params: Optional[dict | list | tuple] = None,
    ):
        """
        Выполнение SQL с возвратом одной строки.
        """
        try:
            if not self.connection or not self.cursor:
                self.connect_db()

            if params is None:
                self.cursor.execute(sql)
            else:
                self.cursor.execute(sql, params)

            if not self.cursor.description:
                return None

            row = self.cursor.fetchone()
            return dict(row) if row else None

        except Exception as e:
            print(f"Ошибка выполнения SQL-запроса (fetchone): {e}")
            raise

