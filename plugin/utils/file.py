import yaml
import glob
import logging
import json
import chevron
from tempfile import mkstemp
from plugin import plugin_path
import os
import sys
import zipfile
import subprocess


class ZipUtilities:
    """
    TO DO
    """

    base_path: str

    def add_to_zip(self, path, filename):
        self.base_path = f"{get_current_pwd()}/{path}"
        with zipfile.ZipFile(filename, 'a') as zip_file:
            if os.path.isfile(path):
                zip_file.write(path, path)
            else:
                os.chdir(self.base_path)
                self.add_folder_to_zip(zip_file, '.')
            zip_file.close()

    def add_folder_to_zip(self, zip_file, folder):
        for file in os.listdir(folder):

            full_path = os.path.join(folder, file)

            if os.path.isfile(full_path):
                print(f"File added: {full_path}")
                zip_file.write(full_path)
            elif os.path.isdir(full_path):
                print(f"Entering folder: {full_path}")
                self.add_folder_to_zip(zip_file, full_path)


def get_current_pwd():
    return os.getcwd()


def create_job_package(folder: str, output_zip: str):

    out_dir = get_current_pwd()
    remove_from_os(output_zip)
    utilities = ZipUtilities()
    utilities.add_to_zip(folder, output_zip)
    os.chdir(out_dir)


def create_lambda_package(folder: str, output_zip: str):

    out_dir = get_current_pwd()

    remove_from_os(output_zip)
    utilities = ZipUtilities()
    utilities.add_to_zip(folder, output_zip)

    requirements = "lambda_package"
    remove_from_os(f"{out_dir}/{folder}/{requirements}")
    install_lambda_dependencies(requirements)
    os.chdir(out_dir)
    utilities.add_to_zip(f"{folder}/{requirements}", output_zip)
    remove_from_os(f"{out_dir}/{folder}/{requirements}")
    os.chdir(out_dir)


def install_lambda_dependencies(target: str):

    subprocess.check_call([
        sys.executable,
        "-m",
        "pip",
        "install",
        "-r",
        "./requirements.txt",
        "--disable-pip-version-check",
        "--target",
        target
    ])


def remove_from_os(path: str):
    if os.path.exists(path):
        subprocess.call(f'rm -rf {path}', shell=True)


def get_temporaryfile(data: str) -> str:
    """
    Returns the path of a temporary file containing the data passed.

        Note: after use, remove this file from operating system. Example::
            os.remove(temporaryfile_path)
    """
    fd, temporaryfile_path = mkstemp()

    with open(temporaryfile_path, 'w', encoding='utf-8') as temporaryfile:
        temporaryfile.write(data)
        os.close(fd)
        return temporaryfile_path


def no_duplicates_constructor(loader, node, deep=False):
    """Check for duplicate keys."""

    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        value = loader.construct_object(value_node, deep=deep)
        if key in mapping:
            msg = f"Duplicate key '{key}' found while constructing a mapping in YAML file. \n" + \
                "Please check its structure:\n" + \
                f"{node.start_mark}\n" + f"{key_node.start_mark}"
            raise yaml.constructor.ConstructorError(msg)
        mapping[key] = value

    return loader.construct_mapping(node, deep)


class DuplicateKeysCheckLoader(yaml.SafeLoader):
    """YAML SafeLoader with duplicate keys checking."""


def read_yaml(file: str) -> dict:
    DuplicateKeysCheckLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        no_duplicates_constructor)

    try:
        if os.path.isfile(os.path.abspath(file)):
            with open(file, 'r', encoding='utf-8') as yml:
                return yaml.load(yml, Loader=DuplicateKeysCheckLoader)
        else:
            temp_file_path = get_temporaryfile(file)
            with open(temp_file_path, 'r', encoding='utf-8') as yml:
                output_dict = yaml.load(yml, Loader=DuplicateKeysCheckLoader)
            remove_from_os(temp_file_path)
            return output_dict
    except FileNotFoundError as error:
        logging.error(error)
        sys.exit(1)


def read_plugin_yaml(path: str):
    try:
        full_path = os.path.abspath(os.path.join(plugin_path, f"{path}"))
        with open(full_path, encoding="utf-8") as yml:
            return yaml.load(yml, Loader=yaml.SafeLoader)
    except FileNotFoundError as error:
        logging.error(error)
        sys.exit(0)


def get_full_path(path):
    return os.path.abspath(os.path.join(plugin_path, f"{path}"))


def read_json(path: str):
    try:
        full_path = os.path.abspath(path)
        with open(full_path, encoding="utf-8") as json_file:
            return json.load(json_file)
    except FileNotFoundError as err:
        logging.error(err)
        sys.exit(0)


def interpolate_json_template(template, data: dict):
    try:
        res = chevron.render(json.dumps(template), data)
        return json.loads(res)
    except FileNotFoundError as err:
        logging.error(err)
        sys.exit(0)


def read_avro_schema(file: str) -> dict:
    avro_schema = read_json(file)
    table_name = avro_schema.get('name', None)

    file_name = os.path.basename(file)

    if file_name.removesuffix(".avsc") != table_name:
        logging.error(
            "The file named '%s' must have the same name as the table: '%s.avsc'.", file_name, table_name)
        sys.exit(1)
    return avro_schema


def get_filenames(directory: str, glob_pattern: str) -> list:

    base_path = get_current_pwd()
    abs_dir_path = os.path.abspath(directory)
    os.chdir(abs_dir_path)
    _list = glob.glob(glob_pattern)
    os.chdir(base_path)

    return _list
