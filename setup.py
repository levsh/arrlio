from setuptools import setup, find_packages

from arrlio import __version__


setup(
    name="arrlio",
    version=__version__,
    author="Roma Koshel",
    author_email="roma.koshel@gmail.com",
    license="MIT",
    py_modules=["arrlio"],
    packages=find_packages(exclude=("docs", "examples", "tests",)),
    include_package_data=True,
    install_requires=[
        "aiormq==5.2.0",
        "cryptography==3.4.7",
        "pydantic==1.8.2",
        "yarl==1.6.3",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X",
    ],
)
