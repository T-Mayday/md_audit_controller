# Файл подключения для работы со супермаг

# Библиотеки
import os
import re
import configparser
from typing import Optional, Any
from dotenv import load_dotenv
load_dotenv()

import oracledb
from oracledb import DatabaseError


class superMag:
    def __init__(self):
        try:
            self.service_name = os.getenv("SM_SERVICE_NAME")
            self.username = os.getenv("SM_USERNAME")
            self.password = os.getenv("SM_PASSWORD")

            missing = []
            if not self.service_name:
                missing.append("SM_SERVICE_NAME")
            if not self.username:
                missing.append("SM_USERNAME")
            if not self.password:
                missing.append("SM_PASSWORD")

            if missing:
                raise ValueError(
                    f"Не заданы переменные окружения: {', '.join(missing)}"
                )
            self.connection = None
            self.cursor = None


        except Exception:
            print("Ошибка чтения конфигурации SM из env")
            raise
    def connect(self):
        try:
            self.connection = oracledb.connect(
                user=self.username,
                password = self.password,
                dns= self.service_name
            )
            self.cursor = self.connection.cursor()
        except DatabaseError as e:
            print(f"SM^ Ошибка подключения: {self.username}@{self.service_name} {e}")
            raise
    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except Exception as e:
            print(f"Ошибка при закрытии соединения с SM: {e}")

    def execute_sql(self,
        sql: str,
        params: Optional[dict | list | tuple] = None,
        commit: bool = False,
    ):
        """
        Универсальное выполнение SQL.

        Примеры:
            self.execute_sql(STORES_QUERY)
            self.execute_sql("SELECT * FROM SMSTORELOCATIONS WHERE ID = :id", {"id": 123})
            self.execute_sql("UPDATE table_name SET col = :val WHERE id = :id", {"val": 1, "id": 10}, commit=True)

        Что возвращает:
          - для SELECT / WITH / запросов с результатом:
                list[dict]
          - для UPDATE / INSERT / DELETE:
                {"rowcount": N}
        """
        try:
            if not self.connection or not self.cursor:
                self.connect_SM()

            if params is None:
                self.cursor.execute(sql)
            else:
                self.cursor.execute(sql, params)

            # Если запрос вернул строки
            if self.cursor.description:
                columns = [col[0].lower() for col in self.cursor.description]
                rows = self.cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]

            # Если это DML
            if commit:
                self.connection.commit()

            return {"rowcount": self.cursor.rowcount}

        except Exception:
            log.exception("Ошибка выполнения SQL-запроса")
            if commit and self.connection:
                try:
                    self.connection.rollback()
                except Exception:
                    log.exception("Ошибка rollback после неудачного SQL-запроса")
            raise