from setuptools import find_packages, setup

setup(
    name="minidagster",
    packages=find_packages(exclude=["minidagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
