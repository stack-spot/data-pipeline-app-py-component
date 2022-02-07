from datetime import datetime
from botocore.stub import Stubber
from botocore.endpoint import Endpoint
from plugin.domain.exceptions import HasFailedEventException
from plugin.infrastructure.resource.aws.cdk.engine.helpers import (
    create_stack,
    get_client_cloudformation,
    update_stack,
    log_stack_event,
    has_failed_event,
    is_created_resource,
    trace_stack_events)
import boto3
import pytest
import json
from mock import patch


def test_get_cloud_formation_client():
    cf_client = get_client_cloudformation(region="sa-east-1")

    assert isinstance(cf_client._endpoint, Endpoint)
    assert cf_client._endpoint.host == "https://cloudformation.sa-east-1.amazonaws.com"


def test_create_log_stack(caplog):
    event = {
        "ResourceStatus": "Success",
        "LogicalResourceId": "resource_id"
    }
    log_stack_event(event)
    assert caplog.records[0].message == "Success - resource_id "


def test_create_log_stack_with_status(caplog):
    event = {
        "ResourceStatus": "Success",
        "LogicalResourceId": "resource_id",
        "ResourceStatusReason": "created"
    }
    log_stack_event(event)
    assert caplog.records[0].message == "Success - resource_id : created"


def test_failed_event():
    events = {
        "StackEvents": [
            {
                "ResourceStatus": "CREATE_FAILED"
            }
        ]
    }
    assert has_failed_event(events)


def test_no_failed_event():
    events = {
        "StackEvents": [
            {
                "ResourceStatus": "CREATE_COMPLETE"
            }
        ]
    }
    assert has_failed_event(events) == False


def test_created_resource():
    stack_template = {
        "Resources": {
            "resource_id_1": "CREATE_COMPLETE"
        }
    }
    events = {
        "StackEvents": [
            {
                "ResourceStatus": "CREATE_COMPLETE",
                "LogicalResourceId": "resource_id_1"
            }
        ]
    }
    assert is_created_resource(events, stack_template)


def test_not_created_resource():
    stack_template = {
        "Resources": {
            "resource_id_1": {},
            "resource_id_2": {},
        }
    }
    events = {
        "StackEvents": [
            {
                "ResourceStatus": "CREATE_COMPLETE",
                "LogicalResourceId": "resource_id_1"
            }
        ]
    }
    assert is_created_resource(events, stack_template) == False


def test_trace_stack_events(caplog):
    cf = boto3.client("cloudformation", region_name="us-east-1")
    stubber = Stubber(cf)
    stack_name = "trace-stack"
    stack_template = {
        "Resources": {
            "resource_id_1": {},
        }
    }
    events_finish = {
        "StackEvents": [
            {
                "StackId": "1",
                "EventId": "1",
                "StackName": stack_name,
                "LogicalResourceId": "resource_id_1",
                "PhysicalResourceId": "resource_id_1",
                "ResourceType": ".",
                "Timestamp": datetime(2021, 1, 1),
                "ResourceStatus": "CREATE_COMPLETE",
                "ResourceStatusReason": "...",
                "ResourceProperties": ".",
                "ClientRequestToken": "."
            },
        ],
        "NextToken": "string"
    }
    stubber.add_response("describe_stack_events", events_finish, {
                         "StackName": stack_name})
    with stubber:
        trace_stack_events(cf, stack_name, stack_template)

    assert caplog.records[0].message == "CREATE_COMPLETE - resource_id_1 : ..."


def test_trace_stack_events_error():
    cf = boto3.client("cloudformation", region_name="us-east-1")
    stubber = Stubber(cf)
    stack_name = "trace-stack"
    stack_template = {
        "Resources": {
            "resource_id_1": {},
        }
    }
    events_finish = {
        "StackEvents": [
            {
                "StackId": "1",
                "EventId": "1",
                "StackName": stack_name,
                "LogicalResourceId": "resource_id_1",
                "PhysicalResourceId": "resource_id_1",
                "ResourceType": ".",
                "Timestamp": datetime(2021, 1, 1),
                "ResourceStatus": "CREATE_FAILED",
                "ResourceStatusReason": "...",
                "ResourceProperties": ".",
                "ClientRequestToken": "."
            },
        ],
        "NextToken": "string"
    }
    stubber.add_response("describe_stack_events", events_finish, {
                         "StackName": stack_name})
    with (stubber, pytest.raises(HasFailedEventException, match=r"create stack failed")):
        trace_stack_events(cf, stack_name, stack_template)


@patch("time.sleep", return_value=None)
def test_trace_stack_multiple_events(patched_time_sleep, caplog):
    cf = boto3.client("cloudformation", region_name="us-east-1")
    stubber = Stubber(cf)
    stack_name = "trace-stack"
    stack_template = {
        "Resources": {
            "resource_id_1": {},
        }
    }
    events = [
        {
            "StackEvents": [
                {
                    "StackId": "1",
                    "EventId": "1",
                    "StackName": stack_name,
                    "LogicalResourceId": "resource_id_1",
                    "PhysicalResourceId": "resource_id_1",
                    "ResourceType": ".",
                    "Timestamp": datetime(2021, 1, 1),
                    "ResourceStatus": "CREATE_IN_PROGRESS",
                    "ResourceStatusReason": "...",
                    "ResourceProperties": ".",
                    "ClientRequestToken": "."
                },
            ],
            "NextToken": "string"
        },
        {
            "StackEvents": [
                {
                    "StackId": "1",
                    "EventId": "1",
                    "StackName": stack_name,
                    "LogicalResourceId": "resource_id_1",
                    "PhysicalResourceId": "resource_id_1",
                    "ResourceType": ".",
                    "Timestamp": datetime(2021, 1, 1),
                    "ResourceStatus": "CREATE_COMPLETE",
                    "ResourceStatusReason": "...",
                    "ResourceProperties": ".",
                    "ClientRequestToken": "."
                },
            ],
            "NextToken": "string"
        }
    ]

    for event in events:
        stubber.add_response("describe_stack_events", event, {
                             "StackName": stack_name})

    stubber.activate()
    trace_stack_events(cf, stack_name, stack_template)
    stubber.deactivate()

    assert caplog.records[0].message == "CREATE_IN_PROGRESS - resource_id_1 : ..."
    assert caplog.records[1].message == "CREATE_COMPLETE - resource_id_1 : ..."


def test_create_stack(caplog):
    cf = boto3.client("cloudformation", region_name="us-east-1")
    stubber = Stubber(cf)
    stack_name = "trace-stack"
    stack_template = {
        "Resources": {
            "resource_id_1": {},
        }
    }
    event_create_stack = {"StackId": "1"}
    events_describe_stack = {
        "StackEvents": [
            {
                "StackId": "1",
                "EventId": "1",
                "StackName": stack_name,
                "LogicalResourceId": "resource_id_1",
                "PhysicalResourceId": "resource_id_1",
                "ResourceType": ".",
                "Timestamp": datetime(2021, 1, 1),
                "ResourceStatus": "CREATE_COMPLETE",
                "ResourceStatusReason": "...",
                "ResourceProperties": ".",
                "ClientRequestToken": "."
            },
        ],
        "NextToken": "string"
    }

    stubber.add_response("create_stack", event_create_stack, {
        "StackName": stack_name,
        "TemplateBody": json.dumps(stack_template),
        "OnFailure": "ROLLBACK",
        "Capabilities": ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"]
    })
    stubber.add_response("describe_stack_events", events_describe_stack, {
                         "StackName": stack_name})
    stubber.activate()
    create_stack(cf, stack_name, stack_template)
    stubber.deactivate()
    assert caplog.records[0].message == "CREATE_COMPLETE - resource_id_1 : ..."


def test_update_stack(caplog):
    cf = boto3.client("cloudformation", region_name="us-east-1")
    stubber = Stubber(cf)
    stack_name = "trace-stack"
    stack_template = {
        "Resources": {
            "resource_id_1": {},
        }
    }
    event_update_stack = {"StackId": "2"}
    events_describe_stack = {
        "StackEvents": [
            {
                "StackId": "1",
                "EventId": "1",
                "StackName": stack_name,
                "LogicalResourceId": "resource_id_1",
                "PhysicalResourceId": "resource_id_1",
                "ResourceType": ".",
                "Timestamp": datetime(2021, 1, 1),
                "ResourceStatus": "CREATE_COMPLETE",
                "ResourceStatusReason": "...",
                "ResourceProperties": ".",
                "ClientRequestToken": "."
            },
        ],
        "NextToken": "string"
    }

    stubber.add_response("update_stack", event_update_stack, {
        "StackName": stack_name,
        "TemplateBody": json.dumps(stack_template),
        "Capabilities": ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"]
    })
    stubber.add_response("describe_stack_events", events_describe_stack, {
                         "StackName": stack_name})
    stubber.activate()
    update_stack(cf, stack_name, stack_template)
    stubber.deactivate()
    assert caplog.records[0].message == "CREATE_COMPLETE - resource_id_1 : ..."
