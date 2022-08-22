from argparse import ArgumentParser
import logging
import os
import gzip
import json
import shlex

VERSION = "0.1.0"


def get_argument_parser() -> ArgumentParser:
    """Create an ArgumentParser which will parse arguments from
    the command line parameters passed to this tool.

    :return: The argument parser
    """
    usage = "Parse K8s logs. v{}".format(
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
    for root, _sub_folders, files in os.walk(source_folder):
        for file_name in files:
            if file_name.endswith(".gz"):
                full_path = os.path.join(root, file_name)
                logging.info(f"processing {full_path}")
                file_obj = gzip.open(full_path)
                for line in file_obj:
                    line = line.decode("utf8")
                    line = line.strip()
                    timestamp, json_str = line.split(" ", 1)
                    try:
                        data = json.loads(json_str)
                        if "message" in data:
                            try:
                                msg = json.loads(data["message"])
                                data.update(msg)
                                data.pop("message")
                            except Exception as error:
                                data["msg"] = data.pop("message")

                    except Exception as error:
                        logging.debug(f"Error loading json str: {line}")

                        try:
                            s = shlex.shlex(json_str)
                            s.token = "="
                            s.wordchars = s.wordchars + "/-:[](){},.*"

                            tokens = list(s)
                            data = {}
                            while tokens:
                                key = tokens.pop(0)
                                sep = tokens.pop(0)
                                value = tokens.pop(0)
                                value = value.strip("\"")
                                data[key] = value
                        except Exception as error:
                            data["msg"] = json_str

                        if "client" in data:
                            if data["client"]:
                                ip, port = data["client"].rsplit(":", 1)
                                data["client"] = {
                                    "ip": ip,
                                    "port": port
                                }

                    data["@timestamp"] = timestamp
                    data["log_path"] = full_path
                    print(json.dumps(data))


if __name__ == "__main__":
    main()
