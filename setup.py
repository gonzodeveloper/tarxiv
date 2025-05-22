#!/usr/bin/env python

from setuptools import setup, find_packages

# Get dependencies
with open("requirements.txt") as f:
    install_requires = f.read().splitlines()

setup(
    name="tarxiv",
    version="0.0.5",
    author="Kyle Hart",
    author_email="kylehart@hawaii.edu",
    license="BSD-3",
    packages=find_packages(),
    install_requires=install_requires,
    zip_safe=False,
)

