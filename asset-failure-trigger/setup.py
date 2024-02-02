from setuptools import find_packages, setup

setup(
    name="asset_failure_trigger",
    packages=find_packages(exclude=["asset_failure_trigger_tests"]),
    install_requires=[
        "dagster",
        "dagster-pagerduty",
        "dagster-cloud",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
