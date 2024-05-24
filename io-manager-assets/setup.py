from setuptools import find_packages, setup

setup(
    name="io_manager_assets",
    packages=find_packages(exclude=["io_manager_assets_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
