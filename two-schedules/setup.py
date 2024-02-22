from setuptools import find_packages, setup

setup(
    name="two_schedules",
    packages=find_packages(exclude=["two_schedules_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-aws"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
