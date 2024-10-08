import json
import os

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

    cmd = "pytest -v --maxfail=1 --timeout=300 tests/medium/test_arrlio.py"
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
def pygettext(c):
    cmd = f"""poetry run python {CWD}/scirpts/pygettext.py -d arrlio -o {CWD}/locales/arrlio.pot arrlio"""
    c.run(cmd)
