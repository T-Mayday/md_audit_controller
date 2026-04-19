import os
from typing import Optional
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

load_dotenv()


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

        self.server = None

    def _connect_via_ssh(self):
        try:
            self.server = SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_user,
                ssh_password=self.ssh_password,
                remote_bind_address=(self.db_host, self.db_port),
            )
            self.server.start()
            print(f"SSH-туннель поднят: localhost:{self.server.local_bind_port}")
        except Exception as e:
            print(f"Ошибка подключения к БД через SSH-туннель: {e}")
            raise

    def close_ssh(self):
        try:
            if self.server:
                self.server.stop()
                self.server = None
                print("SSH-туннель закрыт")
        except Exception as e:
            print(f"Ошибка закрытия SSH-туннеля: {e}")