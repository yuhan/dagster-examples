from setuptools import find_packages, setup

setup(
    name="dag_shapes",
    packages=find_packages(exclude=["dag_shapes_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
