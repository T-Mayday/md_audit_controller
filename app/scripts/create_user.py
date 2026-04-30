import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config.system_access.config import SystemAccess


from app.query.system_access.queries import (
    GET_USER
)

def get_user (employee_id: int):
    sa = SystemAccess()
    result = sa.execute_sql_one(GET_USER, {"employee_id": employee_id})
    print(result)


# def create_user(employee_id: int):

if __name__ == "__main__":
    result = get_user(employee_id='031401853254')
    print(result)