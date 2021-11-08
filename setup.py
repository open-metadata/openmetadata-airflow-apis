#!/usr/bin/env python

import os
from setuptools import find_namespace_packages, setup


def get_version():
    root = os.path.dirname(__file__)
    changelog = os.path.join(root, "CHANGELOG")
    with open(changelog) as f:
        return f.readline().strip()


def get_long_description():
    root = os.path.dirname(__file__)
    with open(os.path.join(root, "README.md")) as f:
        description = f.read()
    description += "\n\nChangelog\n=========\n\n"
    with open(os.path.join(root, "CHANGELOG")) as f:
        description += f.read()
    return description


base_requirements = {
    "PyYAML~=6.0",
    "pendulum~=2.1.2",
    "packaging~=21.2",
    "setuptools~=58.3.0",
    "apache-airflow[http,kubernetes]>=1.10.2",
    "Flask~=1.1.4",
    "Flask-Admin",
    "pydantic>=1.7.4",
}

dev_requirements = {
    "black",
    "pytest",
    "pylint",
    "pytest-cov",
    "tox"
}

setup(
    name="openmetadata-airflow-managed-apis",
    version="0.1.0",
    url="https://open-metadata.org/",
    author="OpenMetadata Committers",
    license="Apache License 2.0",
    description="Airflow REST APIs to create and manage DAGS",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    package_dir={"": "src"},
    zip_safe=False,
    dependency_links=[],
    project_urls={
        "Documentation": "https://docs.open-metadata.org/",
        "Source": "https://github.com/open-metadata/OpenMetadata",
    },
    packages=find_namespace_packages(where="./src", exclude=["tests*"]),
    install_requires=list(base_requirements),
    extras_requires=list(dev_requirements)
)

