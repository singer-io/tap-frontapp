#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-frontapp",
    version="2.0.1",
    description="Singer.io tap for extracting data from the FrontApp API",
    author="bytcode.io",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        "singer-python==6.1.1",
        "pendulum==3.1.0",
        "ratelimit==2.2.1",
        "backoff==2.2.1",
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
