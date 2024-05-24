from setuptools import find_packages, setup

setup(
    name="resources",
    packages=find_packages(exclude=["resources_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
