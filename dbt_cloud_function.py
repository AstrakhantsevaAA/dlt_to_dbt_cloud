import time
from typing import Any, Dict, Optional

import dlt
from dlt.common.configuration.inject import with_config
from dlt.sources.helpers import requests

BASE_URL = "https://cloud.getdbt.com/api"


class DBTCloudClient:
    def __init__(
        self,
        api_token: str,
        account_id: str,
        job_id: str,
        base_api_url: str = BASE_URL,
    ):
        self.api_version = "v2"
        self.base_api_url = f"{base_api_url}/{self.api_version}"
        self._api_token = api_token
        self._headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {self._api_token}",
            "Accept": "application/json",
        }
        self.account_id = account_id
        self.job_id = job_id

        self.jobs_url = f"accounts/{self.account_id}/jobs/{self.job_id}/run"
        self.runs_url = f"accounts/{self.account_id}/runs"

    def get_endpoint(self, endpoint: str) -> Dict[Any, Any]:
        response = requests.get(
            f"{self.base_api_url}/{endpoint}", headers=self._headers
        )
        results = response.json()
        return results

    def post_endpoint(
        self, endpoint: str, json_body: Optional[dict] = None
    ) -> Dict[Any, Any]:
        response = requests.post(
            f"{self.base_api_url}/{endpoint}",
            headers=self._headers,
            json=json_body,
        )
        results = response.json()
        return results

    def trigger_job_run(self, data: Optional[dict] = None) -> int:
        json_body = {"cause": "Triggered via API"}
        if data:
            json_body.update(data)

        response = self.post_endpoint(self.jobs_url, json_body=json_body)
        return response["data"]["id"]

    def get_job_status(self, run_id: int) -> Dict[Any, Any]:
        response = self.get_endpoint(f"{self.runs_url}/{run_id}")
        return response["data"]


@with_config(
    sections=("dbt_cloud",),
)
def run_dbt_cloud_job(
    data: Optional[dict],
    credentials: Dict[str, str] = dlt.secrets.value,
    wait_for_outcome: bool = True,
    wait_seconds: int = 10,
) -> Dict[Any, Any]:
    operator = DBTCloudClient(**credentials)
    run_id = operator.trigger_job_run(data=data)

    status = {}
    if wait_for_outcome:
        while True:
            status = operator.get_job_status(run_id)
            if not status["in_progress"]:
                break

            time.sleep(wait_seconds)

    return status


@with_config(
    sections=("dbt_cloud",),
)
def get_dbt_cloud_run_status(
    run_id: int,
    credentials: Dict[str, str] = dlt.secrets.value,
    wait_for_outcome: bool = True,
    wait_seconds: int = 10,
) -> Dict[Any, Any]:
    operator = DBTCloudClient(**credentials)
    status = operator.get_job_status(run_id)

    if wait_for_outcome and status["in_progress"]:
        while True:
            status = operator.get_job_status(run_id)
            if not status["in_progress"]:
                break

            time.sleep(wait_seconds)

    return status
