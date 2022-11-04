import os
import re

from invoke import task

CWD = os.path.abspath(os.path.dirname(__file__))


@task
def tests_small(c):
    """Run small tests"""

    cmd = (
        "pipenv run coverage run --source arrlio -m pytest -sv --maxfail=1 tests/small/ && "
        "pipenv run coverage report -m"
    )
    c.run(cmd)


@task
def tests_medium(c):
    """Run medium tests"""

    cmd = "pipenv run pytest -sv --maxfail=1 --timeout=60 tests/medium/"
    c.run(cmd)


@task
def run_linter(c):
    cmd = (
        "pipenv run flake8 --filename=arrlio/*.py --count --show-source --statistics "
        "&& pipenv run pylint --extension-pkg-whitelist='pydantic' "
        "arrlio/utils.py arrlio/tp.py arrlio/settings.py arrlio/models.py arrlio/executor.py arrlio/exc.py "
        "arrlio/core.py"
    )
    c.run(cmd)


@task
def run_tests(c):
    tests_small(c)
    tests_medium(c)


@task
def bump_major(c):
    regex = re.compile(r'.*__version__ = "(?P<major>\d+)\.(?P<minor>\d+)(\.\d+)?.*"', re.MULTILINE | re.DOTALL)
    filepath = os.path.abspath(os.path.join(CWD, "arrlio", "__init__.py"))
    with open(filepath, "r+") as f:
        text = f.read()
        match = regex.match(text)
        if match:
            groupdict = match.groupdict()
            major = int(groupdict["major"])
            text = re.sub(r'(.*__version__ = ")(\d+)\.(\d+)(\.\d+)?(".*)', fr"\g<1>{major+1}.0.0\g<5>", text)
            f.seek(0)
            f.write(text)
            f.truncate()


@task
def bump_minor(c):
    regex = re.compile(r'.*__version__ = "(?P<major>\d+)\.(?P<minor>\d+)(\.\d+)?.*"', re.MULTILINE | re.DOTALL)
    filepath = os.path.abspath(os.path.join(CWD, "arrlio", "__init__.py"))
    with open(filepath, "r+") as f:
        text = f.read()
        match = regex.match(text)
        if match:
            groupdict = match.groupdict()
            major = int(groupdict["major"])
            minor = int(groupdict["minor"])
            text = re.sub(r'(.*__version__ = ")(\d+)\.(\d+)(\.\d+)?(".*)', fr"\g<1>{major}.{minor+1}.0\g<5>", text)
            f.seek(0)
            f.write(text)
            f.truncate()


@task
def bump_dev(c):
    regex = re.compile(r'.*__version__ = "(?P<major>\d+)\.(?P<minor>\d+)(\.(?P<dev>\d+))?.*"', re.MULTILINE | re.DOTALL)
    filepath = os.path.abspath(os.path.join(CWD, "arrlio", "__init__.py"))
    with open(filepath, "r+") as f:
        text = f.read()
        match = regex.match(text)
        if match:
            groupdict = match.groupdict()
            major = int(groupdict["major"])
            minor = int(groupdict["minor"])
            dev = int(groupdict["dev"] or 0)
            text = re.sub(r'(.*__version__ = ")(\d+)\.(\d+)(\.\d+)?(".*)', fr"\g<1>{major}.{minor}.{dev+1}\g<5>", text)
            f.seek(0)
            f.write(text)
            f.truncate()


@task
def build(c):
    c.run("rm -rf build dist")
    c.run("python -m build")


@task
def publish(c):
    c.run("python -m twine upload --verbose --repository pypi dist/*")
