import json
from datetime import datetime
from argparse import ArgumentParser
import logging
from aws_log_parser import AwsLogParser, LogType
from aws_log_parser.models import Host, HttpRequest


VERSION = "0.1.0"


def get_argument_parser() -> ArgumentParser:
    """Create an ArgumentParser which will parse arguments from
    the command line parameters passed to this tool.

    :return: The argument parser
    """
    usage = "Parse LoadBalancer logs. v{}".format(
        VERSION
    )

    arguments = ArgumentParser(
        description=usage
    )

    arguments.add_argument(
        "-s", "--source",
        dest="source",
        action="store",
        required=True,
        help="Folder of LoadBalancer logs."
    )

    arguments.add_argument(
        "--logging",
        dest="logging",
        action="store",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
        help="Logging level [default=INFO]"
    )

    return arguments


def set_logging_level(logging_level: str):
    """Set the logging level to use.

    :param logging_level: The logging level's variable name as used in the logging lib.
    :return:
    """
    logging.basicConfig(
        level=getattr(logging, logging_level)
    )


def main():
    arg_parser = get_argument_parser()
    options = arg_parser.parse_args()

    set_logging_level(
        options.logging
    )

    source_folder = options.source
    source_folder = source_folder.replace("\\", "/")
    parser = AwsLogParser(log_type=LogType.ClassicLoadBalancer)
    entries = parser.read_files(source_folder)
    for entry in entries:
        entry_dict = classic_load_balancer_to_dict(entry)
        entry_dict["ips"] = [entry_dict["client"]["ip"], entry_dict["target"]["ip"]]
        print(json.dumps(entry_dict))


def classic_load_balancer_to_dict(entry) -> dict:
    entry_dict = entry.__dict__
    keys = list(entry_dict.keys())
    for k in keys:
        entry_dict[k] = get_json_value(entry_dict[k])
    return entry_dict


def get_json_value(value):
    if isinstance(value, datetime):
        return value.isoformat("T")
    elif isinstance(value, Host):
        return value.__dict__
    elif isinstance(value, HttpRequest):
        return value.__dict__
    return value


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        obj = get_json_value(obj)

        return json.JSONEncoder.default(self, obj)


if __name__ == "__main__":
    main()
