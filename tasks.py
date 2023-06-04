import json
import os
import re

from invoke import task

CWD = os.path.abspath(os.path.dirname(__file__))


@task
def run_linters(c):
    "Run linters"

    cmd = "pylint --extension-pkg-whitelist='pydantic' arrlio"
    c.run(cmd)


@task
def tests_small(c):
    """Run small tests"""

    cmd = (
        "coverage run --data-file=artifacts/.coverage --source arrlio -m pytest -v --maxfail=1 tests/small/ && "
        "coverage json --data-file=artifacts/.coverage -o artifacts/coverage.json && "
        "coverage report --data-file=artifacts/.coverage -m"
    )
    c.run(cmd)


@task
def tests_medium(c):
    """Run medium tests"""

    cmd = "pytest -v --maxfail=1 --timeout=300 tests/medium/test_medium.py"
    c.run(cmd)


@task
def run_tests(c):
    tests_small(c)
    tests_medium(c)


@task
def run_checks(c):
    run_linters(c)
    tests_small(c)
    tests_medium(c)


@task
def generate_coverage_gist(c):
    with open("artifacts/coverage.json") as f:
        data = json.load(f)
    percent = int(data["totals"]["percent_covered_display"])
    if percent >= 90:
        color = "brightgreen"
    elif percent >= 75:
        color = "green"
    elif percent >= 50:
        color = "yellow"
    else:
        color = "red"

    data = {
        "description": "Arrlio shields.io coverage json data",
        "files": {
            "coverage.json": {
                "content": json.dumps(
                    {
                        "schemaVersion": 1,
                        "label": "coverage",
                        "message": f"{percent}%",
                        "color": color,
                    }
                )
            },
        },
    }

    with open("artifacts/shields_io_coverage_gist_data.json", "w") as f:
        f.write(json.dumps(data))


@task
def bump_major(c):
    regex = re.compile(
        r'.*__version__ = "(?P<major>\d+)\.(?P<minor>\d+)\.(\d+)".*',
        re.MULTILINE | re.DOTALL,
    )
    filepath = os.path.abspath(os.path.join(CWD, "arrlio", "__init__.py"))
    with open(filepath, "r+") as f:
        text = f.read()
        match = regex.match(text)
        if match:
            groupdict = match.groupdict()
            major = int(groupdict["major"])
            text = re.sub(
                r'(.*__version__ = ")(\d+)\.(\d+)\.(\d+)(".*)',
                rf"\g<1>{major+1}.0.0\g<5>",
                text,
            )
            f.seek(0)
            f.write(text)
            f.truncate()


@task
def bump_minor(c):
    regex = re.compile(
        r'.*__version__ = "(?P<major>\d+)\.(?P<minor>\d+)\.(\d+)".*',
        re.MULTILINE | re.DOTALL,
    )
    filepath = os.path.abspath(os.path.join(CWD, "arrlio", "__init__.py"))
    with open(filepath, "r+") as f:
        text = f.read()
        match = regex.match(text)
        if match:
            groupdict = match.groupdict()
            major = int(groupdict["major"])
            minor = int(groupdict["minor"])
            text = re.sub(
                r'(.*__version__ = ")(\d+)\.(\d+)\.(\d+)(".*)',
                rf"\g<1>{major}.{minor+1}.0\g<5>",
                text,
            )
            f.seek(0)
            f.write(text)
            f.truncate()


@task
def bump_micro(c):
    regex = re.compile(
        r'.*__version__ = "(?P<major>\d+)\.(?P<minor>\d+)\.(?P<micro>\d+)".*',
        re.MULTILINE | re.DOTALL,
    )
    filepath = os.path.abspath(os.path.join(CWD, "arrlio", "__init__.py"))
    with open(filepath, "r+") as f:
        text = f.read()
        match = regex.match(text)
        if match:
            groupdict = match.groupdict()
            major = int(groupdict["major"])
            minor = int(groupdict["minor"])
            micro = int(groupdict["micro"])
            text = re.sub(
                r'(.*__version__ = ")(\d+)\.(\d+)\.(\d+)(".*)',
                rf"\g<1>{major}.{minor}.{micro+1}\g<5>",
                text,
            )
            f.seek(0)
            f.write(text)
            f.truncate()
