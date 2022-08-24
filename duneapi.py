from types import Dict, List
import time
import os

import httpx


URL = "https://api.dune.com/"


class DuneAPI(object):
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"X-DUNE-API-KEY": api_key}
        self.client = httpx.Client(headers=self.headers)

    def execute_query(self, query_id: int, query_params: Dict = {}) -> Dict:
        body = {}
        if query_params:
            body["query_parameters"] = query_params

        resp = self.client.post(f"{URL}v1/query/{query_id}/execute", json=body)
        return resp.json()

    def get_execution_status(self, execution_id: str) -> Dict:
        resp = self.client.get(f"{URL}/v1/execution/{execution_id}/status")
        return resp.json()

    def get_execution_result(self, execution_id: str) -> Dict:
        resp = self.client.get(f"{URL}/v1/execution/{execution_id}/result")
        return resp.json()

    def cancel_execution(self, execution_id: str) -> Dict:
        resp = self.client.delete(f"{URL}/v1/execution/{execution_id}")
        return resp.json()

    def wait_for_execution_end(self, execution_id: str, poll_interval_secs=5.0, max_wait_secs=1800) -> Dict:

        maxTime = time.Time() + max_wait_secs
        while time.Time() < maxTime:
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

            print("execution_id: {execution_id} not done yet, state: {state}, sleeping {poll_interval_secs} secs")
            time.Sleep(poll_interval_secs)
        raise Exception(
            "wait_for_execution_end() expired, waited for: {max_wait_secs} seconds, execution_id: {execution_id}"
        )


def execute_query_and_get_results(query_id: int, api_key: str) -> List[Dict]:

    dune = DuneAPI(api_key)

    execution_id = dune.execute_query(query_id)
    status = dune.wait_for_execution_end(execution_id)
    print(f"QueryID: {query_id}, finished, status: {status}")
    resp = dune.get_execution_result(execution_id)
    if resp["status"] == "QUERY_STATE_COMPLETED":
        rows = resp["result"]["payload"]
        metadata = resp["result"]["metadata"]
        print(f"QueryID: {query_id}, result metadata: {metadata}")
        return rows
    else:
        error = resp["error"]
        print(f"QueryID: {query_id}, execution_id: {execution_id} full response: {resp.json()}")
        raise Exception(f"QueryID: {query_id}, execution_id: {execution_id} result error: {error}")


if __name__ == "__main__":
    api_key = os.getenv("DUNE_API_KEY")
    query_id = os.args[1]

    print(execute_query_and_get_results(query_id, api_key))
