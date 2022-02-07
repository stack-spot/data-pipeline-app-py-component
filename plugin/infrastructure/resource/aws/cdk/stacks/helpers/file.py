from plugin.utils.file import read_json, read_plugin_yaml


def get_policy(policy: str):
    return read_json(f"plugin/infrastructure/resource/aws/cdk/stacks/templates/policy/{policy}.json")


def get_role(role: str):
    return read_json(f"plugin/infrastructure/resource/aws/cdk/stacks/templates/role/{role}.json")


def get_api(api: str):
    return read_plugin_yaml(f"plugin/infrastructure/resource/aws/cdk/stacks/templates/api/{api}.yaml")
