from typing import Any, Dict
import json

class GetApplicationParameters():
    def __init__(
        self,
        environment_id: str,
    ) -> None:

        with open("./ddk.json") as f:
            self._config_file = json.load(f)

        self._environment_id: str = environment_id
        self._config = self._config_file.get("environments", {}).get(environment_id, {})

    def get_params(self, key) -> Dict[str, Any]:
        return self._config.get(key, {})

    def get_security_lint_flag(self) -> Any:
        security_lint = self._config.get("security_lint", True)
        return security_lint
    
    def get_resource_prefix(self) -> str:
        return self._config.get("resource_prefix", "ddk")

    def get_mode(self, other_params) -> str:
        self._mode = "same_account_region"
        if self._config.get("account") != other_params._config.get("account"):
            self._mode = "cross_account"
        elif self._config.get("region") != other_params._config.get("region"):
            self._mode = "cross_region"
        return self._mode

    def get_compute_params(self) -> str:
        compute_params = self._get_params(f"comp")
        return compute_params

    def get_storage_params(self) -> str:
        storage_params = self._get_params(f"stor")
        return storage_params
