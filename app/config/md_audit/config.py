import os
import json
from typing import Optional, Dict, Any

from dotenv import load_dotenv
import requests

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


class MDAuditConnect:
    def __init__(self):
        try:
            self.base_url = get_env("MDAUDIT_BASE_URL", required=True).rstrip("/")
            self.token = get_env("MDAUDIT_TOKEN", required=True)

            self.timeout = int(get_env("MDAUDIT_TIMEOUT", default="30"))
            self.verify_ssl = get_env("MDAUDIT_VERIFY_SSL", default="false").lower() == "true"

            self.use_bearer = get_env("MDAUDIT_USE_BEARER", default="true").lower() == "true"
            self.use_x_public_token = get_env("MDAUDIT_USE_X_PUBLIC_TOKEN", default="true").lower() == "true"
            self.use_x_auth_token = get_env("MDAUDIT_USE_X_AUTH_TOKEN", default="true").lower() == "true"

            self.session = requests.Session()

        except Exception as e:
            print(f"Ошибка чтения конфигурации MD Audit из env: {e}")
            raise

    def _build_headers(self) -> dict:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        if self.use_bearer:
            headers["Authorization"] = f"Bearer {self.token}"

        if self.use_x_public_token:
            headers["x-public-token"] = self.token

        if self.use_x_auth_token:
            headers["x-auth-token"] = self.token

        return headers

    def _build_url(self, path: str) -> str:
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self.base_url}{path}"

    def _extract_api_error(self, response: requests.Response) -> Optional[Dict[str, Any]]:
        error_class = response.headers.get("x-error-class", "")
        error = response.headers.get("x-error", "")
        error_message = response.headers.get("x-error-message") or response.headers.get("x-error-text") or ""

        if error_class or error or error_message:
            return {
                "http_status": response.status_code,
                "error_class": error_class,
                "error": error,
                "error_message": error_message,
                "body": response.text,
            }

        return None

    def _parse_response(self, response: requests.Response) -> Dict[str, Any]:
        api_error = self._extract_api_error(response)
        if api_error:
            return {
                "ok": False,
                "status_code": response.status_code,
                "api_error": api_error,
            }

        if not response.ok:
            try:
                data = response.json()
            except Exception:
                data = response.text

            return {
                "ok": False,
                "status_code": response.status_code,
                "http_error": True,
                "data": data,
            }

        try:
            data = response.json()
        except Exception:
            data = response.text

        return {
            "ok": True,
            "status_code": response.status_code,
            "data": data,
        }

    def post_json(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = self._build_url(path)
        print(f"POST URL: {url}")
        print(f"POST PAYLOAD: {json.dumps(payload, ensure_ascii=False)}")
        try:
            response = self.session.post(
                url,
                headers=self._build_headers(),
                json=payload,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            return self._parse_response(response)
        except Exception as e:
            print(f"Ошибка POST-запроса в MD Audit: {e}")
            raise

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self._build_url(path)
        print(f"GET URL: {url}")
        try:
            response = self.session.get(
                url,
                headers=self._build_headers(),
                params=params,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            return self._parse_response(response)
        except Exception as e:
            print(f"Ошибка GET-запроса в MD Audit: {e}")
            raise

    def put_json(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = self._build_url(path)
        print(f"PUT URL: {url}")
        print(f"PUT PAYLOAD: {json.dumps(payload, ensure_ascii=False)}")
        try:
            response = self.session.put(
                url,
                headers=self._build_headers(),
                json=payload,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            return self._parse_response(response)
        except Exception as e:
            print(f"Ошибка PUT-запроса в MD Audit: {e}")
            raise

    def upsert_division_external(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post_json("/orgstruct/divisions/external", payload)

    def upsert_region_external(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post_json("/orgstruct/regions/external", payload)

    def upsert_shop_external(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post_json("/orgstruct/shops/external", payload)

    def upsert_user_external(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post_json("/orgstruct/users/external", payload)

    # Сотрудники
    # GET BY ID
    def get_user(self, user_id: int | str) -> Dict[str, Any]:
        return self.get_json(f"/orgstruct/users/{user_id}")
    # GET ALL
    def get_users(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.get_json("/orgstruct/users", params=params)
    # POST CREATE ONE
    def create_user(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post_json("/orgstruct/users", payload)
    # POST CREATE MANY
    def create_users(self, payloads: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        results = []

        for payload in payloads:
            try:
                result = self.create_user(payload)
                results.append({
                    "payload": payload,
                    "result": result,
                })
            except Exception as e:
                results.append({
                    "payload": payload,
                    "result": {
                        "ok": False,
                        "error": str(e),
                    }
                })

        return results




    # Дивизионы
    # GET BY ID
    def get_division(self, division_id:  int | str) -> Dict[str, Any]:
        return self.get_json(f"/orgstruct/divisions/{division_id}")
    # GET ALL
    def get_divisions(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.get_json("/orgstruct/divisions", params=params)
    # POST CREATE
    def create_division(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post_json("/orgstruct/divisions", payload)
    # POST CREATE SIMPLE
    def create_division_simple(
        self,
        name: str,
        active: bool = True,
        external_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "name": name,
            "active": active,
        }

        if external_id is not None and str(external_id).strip():
            payload["externalId"] = str(external_id).strip()

        return self.post_json("/orgstruct/divisions", payload)
    # POST CREATE MANY
    def create_divisions(self, payloads: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        results = []

        for payload in payloads:
            try:
                result = self.create_division(payload)
                results.append({
                    "payload": payload,
                    "result": result,
                })
            except Exception as e:
                results.append({
                    "payload": payload,
                    "result": {
                        "ok": False,
                        "error": str(e),
                    }
                })

        return results
    # PUT
    def update_division(self, division_id: int | str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.put_json(f"/orgstruct/divisions/{division_id}", payload)
    # PUT MANY
    def update_division_simple(
        self,
        division_id: int | str,
        name: str,
        active: bool,
        external_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "name": name,
            "active": active,
        }

        if external_id is not None and str(external_id).strip():
            payload["externalId"] = str(external_id).strip()

        return self.put_json(f"/orgstruct/divisions/{division_id}", payload)





    # Регионы
    # GET BY ID
    def get_region(self, region_id: int | str) -> Dict[str, Any]:
        return self.get_json(f"/orgstruct/regions/{region_id}")
    # GET ALL
    def get_regions(self,params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.get_json("/orgstruct/regions", params=params)
    # POST CREATE ONE
    def create_region(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post_json("/orgstruct/regions", payload)
    
     # POST CREATE MANY
    # POST CREATE MANY
    def create_regions(self, payloads: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        results = []

        for payload in payloads:
            try:
                result = self.create_region(payload)
                results.append({
                    "payload": payload,
                    "result": result,
                })
            except Exception as e:
                results.append({
                    "payload": payload,
                    "result": {
                        "ok": False,
                        "error": str(e),
                    }
                })

        return results
    # PUT
    def update_region(self, region_id: int | str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.put_json(f"/orgstruct/regions/{region_id}", payload)
    # PUT MANY
    def update_region_simple(
        self,
        region_id: int | str,
        name: str,
        division_id: int | str,
        active: bool = True,
        external_id: Optional[str] = None,
        division_external_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "name": name,
            "divisionId": division_id,
            "active": active,
        }

        if external_id is not None and str(external_id).strip():
            payload["externalId"] = str(external_id).strip()

        if division_external_id is not None and str(division_external_id).strip():
            payload["divisionExternalId"] = str(division_external_id).strip()

        return self.put_json(f"/orgstruct/regions/{region_id}", payload)


    # Магазины
    # GET BY ID
    def get_shop(self, shop_id: int | str) -> Dict[str, Any]:
        return self.get_json(f"/orgstruct/shops/{shop_id}")
    # GET ALL
    def get_shops(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.get_json("/orgstruct/shops", params=params)  
    
    # GET ONLY ACTIVE - TRUE, ID, NAME
    def get_active_shops_short(self) -> Dict[str, Any]:
        return self.get_json(
            "/orgstruct/shops",
            params={
                "active": "true",
                "fields": "id,locality"
            }
        )

    # GET какие поля захочешь
    def get_shops_filtered(
        self,
        active: Optional[bool] = None,
        fields: Optional[list[str]] = None,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}

        if active is not None:
            params["active"] = str(active).lower()

        if fields:
            params["fields"] = ",".join(fields)

        if extra_params:
            params.update(extra_params)

        return self.get_json("/orgstruct/shops", params=params)

    # POST CREATE ONE
    def create_shop(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.post_json("/orgstruct/shops", payload)
    # POST CREATE MANY
    def create_shops(self, payloads: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        results = []

        for payload in payloads:
            try:
                result = self.create_shop(payload)
                results.append({
                    "payload": payload,
                    "result": result,
                })
            except Exception as e:
                results.append({
                    "payload": payload,
                    "result": {
                        "ok": False,
                        "error": str(e),
                    }
                })

        return results

    def update_shop(self, shop_id: int | str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.put_json(f"/orgstruct/shops/{shop_id}", payload)





    def close(self):
        try:
            if self.session:
                self.session.close()
        except Exception as e:
            print(f"Ошибка при закрытии соединения с MD Audit: {e}")



if __name__ == "__main__":
    md = MDAuditConnect()
    try:
        result = md.get_regions()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        md.close()




# if __name__ == "__main__":
#     md = MDAuditConnect()
#     try:
#         result = md.get_divisions()
#         print(json.dumps(result, ensure_ascii=False, indent=2))
#     finally:
#         md.close()

# if __name__ == "__main__":
#     md = MDAuditConnect()
#     try:
#         payload = {
#             "login": "t.maydarkhanov@binuu.ru",
#             "email": "t.maydarkhanov@binuu.ru",
#             "firstName": "Цырендоржо",
#             "lastName": "Майдарханов",
#             "position": "Тестовая Должность",
#             "password": "Test12345!",
#             "authType": "LOCAL",
#             "level": "DIVISION",
#             "active": True,
#             "supervisor": False,
#             "admin": False,
#             "surveyAdmin": False,
#             "taskManager": False,
#             "userChecklistsOrganizer": False,
#             "shopDirector": False,
#             "canEditAllProcesses": False,
#             "canViewAllProcesses": False,
#             "canEditAllChlTemplates": False,
#             "canEditSelectedChlTemplates": False,
#             "lang": "ru_RU",
#             "businessDirId": 1002,
#             "divisionIds": [38,39],
#             "timeZoneId": "Asia/Irkutsk"
#         }

#         result = md.create_user(payload)
#         print(json.dumps(result, ensure_ascii=False, indent=2))

#     finally:
#         md.close()












# if __name__ == "__main__":
#     md = MDAuditConnect()
#     try:

#         result = md.get_user(2064)
#         print(json.dumps(result, ensure_ascii=False, indent=2))
#     finally:
#         md.close()

# if __name__ == "__main__":
#     md = MDAuditConnect()
#     try:
#         divisions = [
#             {
#                 "name": "БУРЯТИЯ",
#                 "active": True,
#                 "externalId": "buryatia"
#             },
#             {
#                 "name": "ЗАБАЙКАЛЬСКИЙ КРАЙ",
#                 "active": True,
#                 "externalId": "zabaykalsky-krai"
#             }
#         ]

#         result = md.create_divisions(divisions)
#         print(json.dumps(result, ensure_ascii=False, indent=2))

#     finally:
#         md.close()


# if __name__ == "__main__":
#     md = MDAuditConnect()
#     try:
#         payload = {
#             "active": True,
#             "regionId": 107,
#             "sap": "598",
#             "address": "Буйко ул,20а",
#             "locality": "ДИСКАУНТЕР16 Буйко 20а",
#             "city": "Улан-Удэ",
#             "latitude": 51.848551,
#             "longitude": 107.620102,
#             "timeZoneId": "Asia/Irkutsk"
#         }

#         result = md.create_shop(payload)
#         print(json.dumps(result, ensure_ascii=False, indent=2))

#     finally:
#         md.close()


# if __name__ == "__main__":
#     md = MDAuditConnect()
#     try:
#         result = md.get_active_shops_short()

#         if result.get("ok") and isinstance(result.get("data"), list):
#             short_list = [
#                 {
#                     "id": shop.get("id"),
#                     "locality": shop.get("locality")
#                 }
#                 for shop in result["data"]
#             ]
#             print(json.dumps(short_list, ensure_ascii=False, indent=2))
#         else:
#             print(json.dumps(result, ensure_ascii=False, indent=2))

#     finally:
#         md.close()