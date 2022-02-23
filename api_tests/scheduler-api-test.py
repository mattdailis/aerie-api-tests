"""
Testing against the GraphQL api
"""
import math
from datetime import datetime
import getpass
import sys
from pathlib import Path
from pprint import pprint

import requests

GQL_SUFFIX = ":8080/v1/graphql"
GATEWAY_AUTH_SUFFIX = ":9000/auth/login"
GATEWAY_FILES_SUFFIX = ":9000/file"

API_URL = "http://localhost"
GRAPHQL_API_URL = API_URL + GQL_SUFFIX
FILES_API_URL = API_URL + GATEWAY_FILES_SUFFIX

BANANANATION_JAR = "/Users/dailis/projects/AERIE/aerie/examples/banananation/build/libs/banananation-0.10.0-SNAPSHOT.jar"
AERIELANDER_JAR = "/Users/dailis/projects/AERIE/aerie/aerielander/build/libs/aerielander-0.10.0-SNAPSHOT.jar"


def main():
    sso_token = login()  # if you get tired of typing in your password, run this once, copy the token, and hard code it here.
    run_test(sso_token=sso_token)


def run_test(**kwargs):
    upload_scheduling_rules_jar(**kwargs)
    mission_name_version = ("aerielander", "aerielander3", "1")
    mission_model_jar = AERIELANDER_JAR
    if mission_model_exists := check_mission_model_exists(*mission_name_version, **kwargs):
        mission_model_id = mission_model_exists[0]
    else:
        mission_model_id = upload_mission_model(mission_model_jar, *mission_name_version, **kwargs)

    plan_start_timestamp = "2021-001T00:00:00"
    plan_end_timestamp = "2021-005T00:00:00"
    plan_id = create_plan(mission_model_id, generate_unique_plan_name(**kwargs), plan_start_timestamp,
                          plan_end_timestamp, **kwargs)
    insert_activity(
        plan_id,
        get_postgres_interval_str(plan_start_timestamp, "2021-002T00:00:00"),
        "HP3TemP",
        {},
        **kwargs)
    goal_id = insert_scheduling_goal(mission_model_id, "Schedule DSN contacts for initial setup")
    plan_revision = get_plan_revision(plan_id)
    spec_id = insert_scheduling_specification(plan_id, plan_revision, horizon_start=plan_start_timestamp,
                                              horizon_end=plan_end_timestamp)
    affected_rows = set_scheduling_spec_goals(spec_id, [goal_id])
    assert affected_rows == 1
    trigger_scheduling_run(spec_id)


def get_plan_revision(plan_id):
    return query(
        """
        query GetPlanRevision($plan_id: Int!) {
            plan_by_pk(id: $plan_id) {
                revision
            }
        }
        """,
        variables={
            "plan_id": plan_id
        }
    )["data"]["plan_by_pk"]["revision"]


def upload_scheduling_rules_jar(**kwargs):
    upload_file("/Users/dailis/projects/AERIE/aerie/scheduler/src/test/resources/merlinsight-rules.jar",
                "scheduler_rules.jar", **kwargs)


def trigger_scheduling_run(spec_id):
    resp = query(
        """
        query TriggerSchedulingRun($spec_id:Int!) {
            schedule(specificationId:$spec_id){
                status
                reason
            }
        }
        """,
        variables={
            "spec_id": spec_id
        }
    )["data"]["schedule"]
    status = resp["status"]
    if "reason" in resp:
        reason = resp["reason"]
        print(f"Status: {status}", f"Reason: {reason}")
    else:
        print(f"Status: {status}")
    if status not in ["complete", "incomplete"]:
        if "reason" in resp:
            raise SchedulingException(f"status={status} reason={resp['reason']}")
        else:
            raise SchedulingException(f"status={status}")


class SchedulingException(Exception):
    def __init__(self, *args):
        super().__init__(*args)


def check_mission_model_exists(mission, name, version, **kwargs):
    """
    Returns a list of matching ids. List will either be empty or have one element
    """
    matches = query(
        """
        query MissionModelExists($mission: String!, $name: String!, $version: String!) {
            mission_model(where: {_and: {mission: {_eq: $mission}, name: {_eq: $name}, version: {_eq: $version}}}) {
                id
            }
        }
        """,
        variables={
            "mission": mission,
            "name": name,
            "version": version
        },
        **kwargs
    )["data"]["mission_model"]
    return [match["id"] for match in matches]


def upload_file(file_path, server_side_file_path, sso_token=None):
    with open(file_path, "rb") as jar_file:
        resp = requests.post(
            FILES_API_URL,
            files={"file": (server_side_file_path, jar_file)},
            headers={"x-auth-sso-token": sso_token} if sso_token else {},
        )
    print(resp.json())
    return resp.json()["id"]


def upload_mission_model(jar_path: str, mission, name: str, version: str, **kwargs) -> int:
    """Create an Aerie adaptation by uploading a jar file."""

    # we need to give each jar a unique name server-side so that the new jar doesn't overwrite old ones and break any existing plans
    server_side_jar_name = Path(jar_path).stem + "--" + version + ".jar"

    jar_id = upload_file(jar_path, server_side_jar_name, **kwargs)

    print(f"Uploaded `{jar_path}` to server as `{server_side_jar_name}`.\nReceived file id `{jar_id}`")

    mission_model = {
        "name": name,
        "mission": mission,
        "version": version,
        "jar_id": jar_id
    }

    data = query(
        """
      mutation CreateModel($model: mission_model_insert_input!) {
        createModel: insert_mission_model_one(object: $model) {
            id
        }
      }
      """,
        variables={
            "model": mission_model
        }, **kwargs)["data"]

    return data["createModel"]["id"]


def insert_scheduling_goal(model_id, definition):
    """
    TODO iron out whether definition is a string or a jsonb
    """
    return query(
        """
        mutation MakeSchedulingGoal($definition: String, $model_id: Int) {
            insert_scheduling_goal_one(object: {definition: $definition, model_id: $model_id}) {
                id
            }
        }
        """,
        variables={
            "definition": definition,  # json.dumps(definition),
            "model_id": model_id
        })["data"]["insert_scheduling_goal_one"]["id"]


def insert_scheduling_specification(plan_id, plan_revision, horizon_start, horizon_end, simulation_arguments={}):
    return query(
        """
        mutation MakeSchedulingSpec($plan_id: Int!, $plan_revision: Int!, $horizon_start: timestamptz, $horizon_end: timestamptz, $simulation_arguments: jsonb) {
            insert_scheduling_spec_one(object: {
                plan_id: $plan_id,
                plan_revision: $plan_revision,
                horizon_start: $horizon_start,
                horizon_end: $horizon_end,
                simulation_arguments: $simulation_arguments}
            ) {
                id
            }
        }
        """,
        variables={
            "plan_id": plan_id,
            "plan_revision": plan_revision,
            "horizon_start": horizon_start,
            "horizon_end": horizon_end,
            "simulation_arguments": simulation_arguments
        })["data"]["insert_scheduling_spec_one"]["id"]


def generate_unique_plan_name(**kwargs):
    resp = query(
        """
        query GetPlans {
            plan {
                name
            }
        }
        """, **kwargs)["data"]["plan"]
    taken_names = {x["name"] for x in resp}
    for i in range(sys.maxsize):
        if f"my_plan_{i}" not in taken_names:
            break
    return f"my_plan_{i}"


def insert_activity(plan_id, start_offset, type, arguments, **kwargs):
    query(
        """
          mutation CreateActivity($activity: activity_insert_input!) {
            createActivity: insert_activity_one(object: $activity) {
              id
            }
          }
        """,
        variables={
            "activity": {
                "plan_id": plan_id,
                "start_offset": start_offset,
                "type": type,
                "arguments": arguments
            }
        },
        **kwargs)


def create_plan(model_id, plan_name: str, startTimestamp, endTimestamp, sso_token: str = None) -> tuple[int, int]:
    resp = query(
        """
          mutation CreatePlan($plan: plan_insert_input!) {
            createPlan: insert_plan_one(object: $plan) {
              id
              revision
            }
          }
        """,
        variables={
            "plan": {
                "model_id": model_id,
                "name": f'{plan_name}',
                "start_time": startTimestamp,
                "duration": get_postgres_interval_str(startTimestamp, endTimestamp)
            }
        },
        sso_token=sso_token,
    )["data"]["createPlan"]
    plan_id = resp["id"]

    query(
        """
          mutation CreateSimulation($simulation: simulation_insert_input!) {
            createSimulation: insert_simulation_one(object: $simulation) {
              id
            }
          }
        """,
        variables={"simulation": {
            "arguments": {},
            "plan_id": plan_id
        }},
        sso_token=sso_token
    )

    print(f"Plan ID: {plan_id}")
    return plan_id


def set_scheduling_spec_goals(spec_id, goal_ids):
    """
    scheduling spec is expected to be empty - priority starts at 0.
    """
    affected_rows = query(
        """
        mutation AddGoalsToSchedulingSpec($objects: [scheduling_spec_goals_insert_input!]!) {
            insert_scheduling_spec_goals(objects:$objects) {
                affected_rows
            }
        }
        """,
        variables={
            "objects": [{
                "goal_id": goal_id,
                "spec_id": spec_id,
                "priority": i
            } for i, goal_id in enumerate(goal_ids)]
        }
    )["data"]["insert_scheduling_spec_goals"]["affected_rows"]
    return affected_rows


def query(query_definition, variables=None, sso_token=None):
    resp = requests.post(
        GRAPHQL_API_URL,
        json={
            "query": query_definition,
            "variables": variables if variables else {}},
        headers={"x-auth-sso-token": sso_token} if sso_token else {}
    ).json()
    if "errors" in resp:
        pprint(resp)
    else:
        print(resp)
    return resp


def login():
    user = getpass.getuser()
    print(f"username: {user}")
    password = getpass.getpass("Enter Password: ")

    ssoToken = get_sso_token(API_URL, user, password)
    return ssoToken


def get_sso_token(api_url: str, username: str, password: str) -> str:
    """Login to the Aerie Gateway and get an SSO Token"""
    auth_resp = requests.post(
        url=api_url + GATEWAY_AUTH_SUFFIX,
        json={"username": username, "password": password}
    )
    print(auth_resp.json())
    return auth_resp.json()["ssoToken"]


def get_postgres_interval_str(start_time: str, end_time: str) -> str:
    """Constructs a PostgresQL interval from two stringified datetimes"""
    DATETIME_FORMAT = "%Y-%jT%H:%M:%S"
    return get_postgres_interval(datetime.strptime(start_time, DATETIME_FORMAT),
                                 datetime.strptime(end_time, DATETIME_FORMAT))


def get_postgres_interval(start_time: datetime, end_time: datetime) -> str:
    """Constructs a PostgresQL interval from two datetimes"""
    delta = end_time - start_time
    (seconds, partial_seconds) = divmod(delta.total_seconds(), 1)
    # what is appropriate rounding method?
    milliseconds = math.floor(partial_seconds * 1000)
    return f"{int(seconds)} seconds {milliseconds} milliseconds"


if __name__ == "__main__":
    main()
