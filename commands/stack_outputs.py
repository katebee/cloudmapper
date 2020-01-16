
import argparse
import csv
import datetime
import json
import logging
import os
import os.path
import sys
from shutil import rmtree
from typing import List, Dict

import boto3
from boto3.session import Session
from botocore.client import BaseClient
from botocore.exceptions import ClientError

__description__ = "Run AWS API calls across accounts to fetch stack outputs"


def make_directory(path):
    try:
        os.mkdir(path)
    except OSError:
        # Already exists
        pass


def custom_serializer(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    elif isinstance(x, bytes):
        return x.decode()
    raise TypeError("Unknown type")


def write_file(contents: Dict, path):
    with open(f"{path}/stack_outputs.json", "w+") as f:
        f.write(json.dumps(contents, indent=4, sort_keys=True, default=custom_serializer))


def create_session(profile: str, region: str) -> Session:
    return boto3.Session(profile_name=profile, region_name=region)


def with_retries(max_retries: int, handler: BaseClient, method_to_call: str, parameters: Dict) -> Dict:
    data = None

    try:
        for retry in range(max_retries):
            if handler.can_paginate(method_to_call):
                paginator = handler.get_paginator(method_to_call)
                page_iterator = paginator.paginate(**parameters)

                for response in page_iterator:
                    if not data:
                        data = response
                    else:
                        print("  ...paginating", flush=True)
                        for k in data:
                            if isinstance(data[k], list):
                                data[k].extend(response[k])
            else:
                function = getattr(handler, method_to_call)
                data = function(**parameters)

    except ClientError as exception:
        print(f"ClientError: {exception}", flush=True)
        return {
            'exception': exception
        }

    return data


def save_report(outputfile: str, path: str, handler: BaseClient, method_to_call: str, parameters):

    make_directory("account-data")
    make_directory(f"account-data/{path}")

    if os.path.isfile(outputfile):
        # Data already collected, so skip
        print(f"Response already collected at {outputfile}", flush=True)

    print(f"Making call for {outputfile}", flush=True)
    output = with_retries(1, handler, method_to_call, parameters)
    write_file(output)


def get_accounts(filename: str) -> List[str]:
    with open(filename, newline='') as csvfile:
        accounts = csv.reader(csvfile, delimiter=',')
        return accounts


def filter_by_name(stacks: List[Dict], target_prefix: str) -> List[Dict]:
    targets = filter(lambda stack: stack.get('StackName', '').startswith(target_prefix), stacks)
    return list(targets)


def collect(profile: str, region: str, target_name: str) -> Dict:
    session = create_session(profile, region)
    cfn_client = session.client('cloudformation')

    response = with_retries(1, cfn_client, 'describe_stacks', {})
    all_stacks = response.get('Stacks')

    if all_stacks is not None:
        target_stacks = filter_by_name(all_stacks, target_name)

        for stack in target_stacks:
            return {
                'outputs': stack.get('Outputs', [])
            }

    if 'exception' in response:
        return {
            'exception': str(response['exception'])
        }


def print_summary(summary: Dict):
    print("--------------------------------------------------------------------")
    failures = []
    for profile in summary:
        failure = summary[profile].get('exception', None)
        if failure is not None:
            failures.append(failure)

    print(f"Summary: {len(summary)} APIs called. {len(failures)} errors")
    if len(failures) > 0:
        print("Failures:")
        for failure in failures:
            print(failure)
        # Ensure errors can be detected
        exit(-1)


def run(arguments):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target",
        help="prefix of the StackSet that should be targeted",
        required=True,
        type=str,
        dest="target",
    )
    parser.add_argument(
        "--region",
        help="AWS region to inspect stacks in",
        required=False,
        type=str,
        dest="region",
        default="eu-west-1"
    )
    parser.add_argument(
        "--profiles",
        help="AWS profile names to iterate through",
        required=False,
        type=str,
        dest="profiles",
        default="default"
    )
    parser.add_argument(
        "--clean",
        help="Remove any existing data before gathering",
        action="store_true",
    )
    args = parser.parse_args(arguments)
    profiles = args.profiles.split(',')
    target = args.target
    region = args.region

    logging.getLogger("botocore").setLevel(logging.WARN)

    # cleaning report stuff
    path = "account-data/stackset"
    if args.clean and os.path.exists(path):
        rmtree(path)

    # generating report stuff
    outcome = {}

    for profile in profiles:
        # TODO: handle botocore.exceptions.ProfileNotFound
        response = collect(profile, region, target)
        outcome[profile] = response

    # saving report stuff
    make_directory("account-data")
    make_directory("account-data/stackset")

    write_file(outcome, "account-data/stackset")
    print_summary(outcome)


if __name__ == "__main__":

    if len(sys.argv) <= 1:
        print('usage: stack_outputs.py [-h] --profiles PROFILES --target TARGET [--clean]')
        exit(-1)

    run(sys.argv[1:])









