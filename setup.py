from setuptools import find_packages, setup

setup(
    name="dagster_project_uno",
    packages=find_packages(exclude=["dagster_project_uno_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
