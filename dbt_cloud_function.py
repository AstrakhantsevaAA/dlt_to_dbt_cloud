import time

import dlt
from dlt.common.configuration.inject import with_config
from dlt.sources.helpers import requests


BASE_URL = "https://cloud.getdbt.com/api"


class DBTCloudClient:
    def __init__(self, api_token: str, account_id: str, job_id: str, base_api_url: str = BASE_URL,):
        self.api_version = "v2"
        self.base_api_url = f"{base_api_url}/{self.api_version}"
        self._api_token = api_token
        self._headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Token {self._api_token}'
        }
        self.account_id = account_id
        self.job_id = job_id
        self.jobs_url = f"accounts/{self.account_id}/jobs/{self.job_id}/run"

    def get_endpoint(self, endpoint: str):
        response = requests.get(
            f"{self.base_api_url}/{endpoint}",
            headers=self._headers
        )
        response.raise_for_status()
        results = response.json()
        return results

    def trigger_job_run(self):
        response = requests.post(
            self.jobs_url, headers=self._headers, data=
            """{
                "cause": "Triggered via API",
                "git_sha": "90ec5acea064ff900604b38b0dcdd276e1a9ca5f",
                "git_branch": "origin/master",
            }"""
                # "azure_pull_request_id": 0,
                # "github_pull_request_id": 0,
                # "gitlab_merge_request_id": 0,
                # "schema_override": "string",
                # "dbt_version_override": "string",
                # "threads_override": 0,
                # "target_name_override": "string",
                # "generate_docs_override": true,
                # "timeout_seconds_override": 0,
                # "steps_override": [
                # "string"
                # ]
            # }"""
        )
        response.raise_for_status()
        return response.json()['id']

    def get_job_status(self, run_id):
        response = self.get_endpoint(f"{self.base_api_url}/{self.jobs_url}/{run_id}")
        return response


@with_config(sections=("dbt_cloud", ))
def run_dbt_cloud_job(credentials=dlt.secrets.value, wait_for_outcome=True):
    operator = DBTCloudClient(**credentials)
    run_id = operator.trigger_job_run()

    if wait_for_outcome:
        while True:
            status = operator.get_job_status(run_id)
            if status['status'] in ['success', 'failed', 'error']:
                break
            time.sleep(10)  # Wait for 10 seconds before checking again

    return run_id


