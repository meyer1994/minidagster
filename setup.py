from setuptools import find_packages, setup

setup(
    name="minidagster",
    packages=find_packages(
        exclude=[
            "minidagster_tests",
        ],
    ),
    install_requires=[
        "dagster==1.13.1",
        "dagster-cloud==1.7.3",
    ],
    extras_require={
        "dev": [
            "dagster-webserver==1.7.3",
            "pytest",
        ],
    },
)
