from typing import Dict, List
import os
import sys
import time

import httpx


URL = "https://api.dune.com/api"


class DuneAPI(object):
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"X-DUNE-API-KEY": api_key}
        self.client = httpx.Client(headers=self.headers)

    def execute_query(self, query_id: int, query_params: Dict = {}) -> Dict:
        body = {}
        if query_params:
            body["query_parameters"] = query_params

        resp = self.client.post(f"{URL}/v1/query/{query_id}/execute", json=body)
        assert resp.is_success
        return resp.json()

    def get_execution_status(self, execution_id: str) -> Dict:
        resp = self.client.get(f"{URL}/v1/execution/{execution_id}/status")
        assert resp.is_success, resp.text
        return resp.json()

    def get_execution_result(self, execution_id: str) -> Dict:
        resp = self.client.get(f"{URL}/v1/execution/{execution_id}/results")
        assert resp.is_success
        return resp.json()

    def cancel_execution(self, execution_id: str) -> Dict:
        resp = self.client.delete(f"{URL}/v1/execution/{execution_id}")
        assert resp.is_success
        return resp.json()

    def wait_for_execution_end(self, execution_id: str, poll_interval_secs=5.0, max_wait_secs=1800) -> Dict:

        maxTime = time.time() + max_wait_secs
        while time.time() < maxTime:
            # here we wait for the execution to complete, cancel or fail
            # these are the 3 terminal states of an execution
            terminal_states = (
                "QUERY_STATE_COMPLETED",
                "QUERY_STATE_FAILED",
                "QUERY_STATE_CANCELLED",
            )
            status = self.get_execution_status(execution_id)
            state = status["status"]  # FIXME bad field name
            if state in terminal_states:
                if state == "QUERY_STATE_COMPLETED":
                    row_count = status["result_metadata"]["total_row_count"]
                    print(f"execution_id: {execution_id} completed, rowCount:{row_count}")
                else:
                    print(f"execution_id: {execution_id} ended with state: {state} :-(")

                return status

            print(f"execution_id: {execution_id} not done yet, state: {state}, sleeping {poll_interval_secs} secs")
            time.sleep(poll_interval_secs)
        raise Exception(
            f"wait_for_execution_end() expired, waited for: {max_wait_secs} seconds, execution_id: {execution_id}"
        )


def execute_query_and_get_results(query_id: int, api_key: str) -> List[Dict]:

    dune = DuneAPI(api_key)

    print(f"Requesting new execution of Query: {query_id}")
    resp = dune.execute_query(query_id)
    execution_id = resp['execution_id']

    status = dune.wait_for_execution_end(execution_id)
    print(f"QueryID: {query_id}, finished, status: {status}")
    resp = dune.get_execution_result(execution_id)
    if resp["status"] == "QUERY_STATE_COMPLETED":
        rows = resp["result"]["payload"]
        metadata = resp["result"]["metadata"]
        print(f"QueryID: {query_id}, result metadata: {metadata}")
        assert len(rows) == metadata['total_row_count']
        return rows
    else:
        error = resp["error"]
        print(f"QueryID: {query_id}, execution_id: {execution_id} full response: {resp.json()}")
        raise Exception(f"QueryID: {query_id}, execution_id: {execution_id} result error: {error}")


if __name__ == "__main__":
    api_key = os.getenv("DUNE_API_KEY")
    query_id = sys.argv[1]

    rows = execute_query_and_get_results(query_id, api_key)
    print(f"row 0:\n\t {rows[0]}\nlast row:\n\t {rows[-1]}")
