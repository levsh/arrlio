from setuptools import find_packages, setup


def get_version():
    with open("arrlio/__init__.py", "r") as f:
        for line in f.readlines():
            if line.startswith("__version__ = "):
                return line.split("=")[1].strip().strip('"')
    raise Exception("Can't read version")


setup(
    name="arrlio",
    version=get_version(),
    author="Roma Koshel",
    author_email="roma.koshel@gmail.com",
    license="MIT",
    py_modules=["arrlio"],
    packages=find_packages(
        exclude=(
            "docs",
            "site",
            "examples",
            "tests",
        )
    ),
    include_package_data=True,
    install_requires=[
        "aiormq>=6.6.4",
        "pydantic>=1.9.0",
        "yarl",
        "rich",
        "roview",
        "siderpy[hiredis]>=0.6.0",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X",
    ],
)
