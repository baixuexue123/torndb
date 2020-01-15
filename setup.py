#!/usr/bin/env python

import distutils.core

version = "0.5"

distutils.core.setup(
    name="torndb",
    version=version,
    py_modules=["torndb"],
    author="Facebook",
    author_email="python-tornado@googlegroups.com",
    url="https://github.com/bdarnell/torndb",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    description="A lightweight wrapper around MySQL DB-API.",
)
