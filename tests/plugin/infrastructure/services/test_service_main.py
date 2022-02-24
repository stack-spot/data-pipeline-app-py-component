
from botocore.stub import Stubber
from botocore.session import Session
from botocore.exceptions import UnauthorizedSSOTokenError
import pytest
from plugin.infrastructure.resource.aws.services.main import SDK
from unittest import (mock)


class TestServiceMain:

    client = Session().create_client('sts')

    @mock.patch("plugin.infrastructure.resource.aws.services.main.boto3.client", return_value=client)
    def test_main_get_account_id(self, _):
        stubber = Stubber(self.client)
        response = {
            'UserId': 'AKIAI44QH8DHBEXAMPLE',
            'Account': '123456789012',
            'Arn': 'arn:aws:iam::123456789012:user/Alic'
        }
        stubber.add_response(
            'get_caller_identity', response, None)
        stubber.activate()
        account = SDK().account_id
        stubber.deactivate()
        assert account == response['Account']

    @mock.patch("plugin.infrastructure.resource.aws.services.main.boto3.client", return_value=client)
    def test_main_client_error_account_id(self, _):
        stubber = Stubber(self.client)
        stubber.add_client_error("get_caller_identity",
                                 service_error_code="AccessDenied",
                                 service_message="Forbidden",
                                 http_status_code=403)
        stubber.activate()
        with pytest.raises(SystemExit):
            SDK().account_id

    # @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.boto3", autospec=True)
    # def test_main_sso_error_account_id(self, mock_boto3):
    #     mock_sts = mock_boto3.client.return_value
    #     mock_sts.get_caller_identity.side_effect = UnauthorizedSSOTokenError()
    #     with pytest.raises(SystemExit):
    #         SDK().account_id
