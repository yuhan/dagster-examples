from setuptools import find_packages, setup

setup(
    name="sensor_schedule_skip",
    packages=find_packages(exclude=["sensor_schedule_skip_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
