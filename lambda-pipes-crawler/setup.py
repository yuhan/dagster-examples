from setuptools import find_packages, setup

setup(
    name="lambda_pipes_crawler",
    packages=find_packages(exclude=["lambda_pipes_crawler_tests"]),
    install_requires=[
        "dagster",
        "dagster-pagerduty",
        "dagster-cloud",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
