import boto3
import json
import time
from plugin.domain.exceptions import HasFailedEventException
from plugin.utils.logging import logger


def log_stack_event(event: dict):
    status = event["ResourceStatus"]
    resource_id = event["LogicalResourceId"]
    status_reason = event.get("ResourceStatusReason")
    message = f": {status_reason}" if status_reason is not None else ""
    logger.info("%s - %s %s", status, resource_id, message)


def has_failed_event(events: dict):
    statuses = ['CREATE_FAILED', 'DELETE_FAILED', 'UPDATE_FAILED']
    res = [e["ResourceStatus"]
           for e in events["StackEvents"] if e["ResourceStatus"] in statuses]
    return len(res) > 0


def is_created_resource(events: dict, stack_template: dict):
    resources = list(stack_template['Resources'].keys())
    res = [e["ResourceStatus"] for e in events["StackEvents"] if e["ResourceStatus"]
           == "CREATE_COMPLETE" and e["LogicalResourceId"] in resources]
    return len(res) == len(resources)


def trace_stack_events(cf, stack_name, stack_template, first=True):
    events = cf.describe_stack_events(StackName=stack_name)
    if first:
        for event in events['StackEvents']:
            log_stack_event(event)
    else:
        log_stack_event(events['StackEvents'][0])
    if has_failed_event(events):
        raise HasFailedEventException('create stack failed')
    if is_created_resource(events, stack_template) is False:
        time.sleep(5)
        trace_stack_events(cf, stack_name, stack_template, first=False)


def get_client_cloudformation(region: str):
    session = boto3.Session()
    return session.client("cloudformation", region_name=region)


def create_stack(cf, stack_name, stack_template):
    cf.create_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(stack_template),
        OnFailure='ROLLBACK',
        Capabilities=['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM']
    )
    trace_stack_events(cf, stack_name, stack_template)


def update_stack(cf, stack_name, stack_template):
    cf.update_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(stack_template),
        Capabilities=['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM']
    )
    trace_stack_events(cf, stack_name, stack_template)
