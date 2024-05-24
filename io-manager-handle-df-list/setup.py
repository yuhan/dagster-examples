from setuptools import find_packages, setup

setup(
    name="io_manager_handle_df_list",
    packages=find_packages(exclude=["io_manager_handle_df_list_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster_polars",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
