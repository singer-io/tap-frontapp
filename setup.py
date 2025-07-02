#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-frontapp",
    version="2.0.0",
    description="Singer.io tap for extracting data from the FrontApp API",
    author="bytcode.io",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        "singer-python==5.13.2",
        "pendulum",
        "ratelimit",
        "backoff==1.10.0",
        "requests==2.32.4",
    ],
    entry_points="""
    [console_scripts]
    tap-frontapp=tap_frontapp:main
    """,
    packages=find_packages(),
    package_data = {
        "schemas": ["tap_frontapp/schemas/*.json"]
    },
    include_package_data=True
)
